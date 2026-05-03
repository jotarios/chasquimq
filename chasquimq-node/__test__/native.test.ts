import { describe, it, expect } from 'vitest'
import { encode, decode } from '@msgpack/msgpack'

// Native bindings are exported from the `chasquimq/native` subpath of
// the package. The `exports` map in `package.json` resolves the parent
// path's `./native` entry to the generated `index.js` / `index.d.ts`
// produced by `napi build`. The bare `'..'` import (no subpath) now
// resolves to `./dist/index.js` — the high-level shim — which doesn't
// expose the `Native*` classes.
import {
  NativeProducer,
  NativeConsumer,
  NativePromoter,
  type NativeJob,
} from '../index.js'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)

// CI brings up Redis as a service container and sets REDIS_URL. Local
// runs without it skip; matches the engine's `#[ignore = "requires
// REDIS_URL"]` convention.
const d = HAS_REDIS ? describe : describe.skip

d('NativeProducer + NativeConsumer round-trip', () => {
  it('produces a job and the consumer handler receives the same payload', async () => {
    const queueName = `native-rt-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
    const producer = await NativeProducer.connect(REDIS_URL, { queueName })

    const data = { hello: 'world', n: 42, nested: { ok: true } }
    const id = await producer.add(Buffer.from(encode(data)))
    expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/) // ULID

    const consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      // Disable the inline promoter; this test is on the immediate path.
      delayedEnabled: false,
    })

    const seen: NativeJob[] = []
    let resolveSeen: () => void
    const handlerSettled = new Promise<void>((r) => (resolveSeen = r))

    const runP = consumer.run(async (job) => {
      seen.push(job)
      resolveSeen()
    })

    await Promise.race([
      handlerSettled,
      new Promise((_, rej) => setTimeout(() => rej(new Error('handler-timeout')), 5_000)),
    ])

    consumer.shutdown()
    await Promise.race([
      runP,
      new Promise((_, rej) => setTimeout(() => rej(new Error('drain-timeout')), 5_000)),
    ])

    expect(seen).toHaveLength(1)
    expect(seen[0].id).toBe(id)
    // Engine `Job.attempt` is 0-indexed: first run = 0, subsequent
    // retries 1, 2, … This is the raw struct field. Metric events
    // separately surface a 1-indexed run count — see `JobOutcome` in
    // the engine. The high-level shim above will adapt as needed.
    expect(seen[0].attempt).toBe(0)
    expect(typeof seen[0].createdAtMs).toBe('number')
    expect(seen[0].createdAtMs).toBeGreaterThan(Date.now() - 60_000)
    expect(seen[0].createdAtMs).toBeLessThan(Date.now() + 60_000)

    // The wire-format invariant: the buffer the consumer sees decodes to
    // exactly the object the producer encoded. This is what the high-level
    // shim above will rely on.
    const decoded = decode(seen[0].payload)
    expect(decoded).toEqual(data)
  }, 30_000)

  it('promotes a delayed job', async () => {
    const queueName = `native-delayed-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
    const producer = await NativeProducer.connect(REDIS_URL, { queueName })

    const payload = Buffer.from(encode({ msg: 'delayed' }))
    const id = await producer.addIn(50, payload)

    // Standalone promoter — exercise that path explicitly. The consumer
    // below has `delayedEnabled: false` so promotion isn't double-driven.
    const promoter = new NativePromoter(REDIS_URL, { queueName, pollIntervalMs: 25 })
    const promP = promoter.run()

    const consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      delayedEnabled: false,
    })

    let seen: NativeJob | null = null
    let resolveSeen: () => void
    const settled = new Promise<void>((r) => (resolveSeen = r))

    const runP = consumer.run(async (job) => {
      seen = job
      resolveSeen()
    })

    await Promise.race([
      settled,
      new Promise((_, rej) => setTimeout(() => rej(new Error('promote-timeout')), 8_000)),
    ])

    consumer.shutdown()
    promoter.shutdown()
    await Promise.allSettled([runP, promP])

    expect(seen).not.toBeNull()
    expect(seen!.id).toBe(id)
    expect(decode(seen!.payload)).toEqual({ msg: 'delayed' })
  }, 30_000)

  it('routes a JS handler rejection through the retry then DLQ path', async () => {
    const queueName = `native-fail-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
    const producer = await NativeProducer.connect(REDIS_URL, { queueName })

    await producer.add(Buffer.from(encode({ should: 'fail' })))

    const consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      maxAttempts: 1, // route to DLQ on first failure
      delayedEnabled: false,
      // Tighten the retry relocator so the test doesn't wait for the default
      retry: { initialBackoffMs: 1, maxBackoffMs: 10, multiplier: 1, jitterMs: 0 },
    })

    let attempts = 0
    let resolveDone: () => void
    const done = new Promise<void>((r) => (resolveDone = r))

    const runP = consumer.run(async () => {
      attempts++
      // Every attempt rejects; with maxAttempts=1 the first failure goes
      // straight to DLQ.
      throw new Error('intentional-failure')
    })

    // Wait for the handler to fire at least once.
    const tick = setInterval(() => {
      if (attempts >= 1) resolveDone()
    }, 25)
    await Promise.race([
      done,
      new Promise((_, rej) => setTimeout(() => rej(new Error('handler-never-fired')), 5_000)),
    ])
    clearInterval(tick)

    // Give the DLQ relocator a tick to flush.
    await new Promise((r) => setTimeout(r, 250))

    consumer.shutdown()
    await Promise.allSettled([runP])

    const dlq = await producer.peekDlq(10)
    expect(dlq.length).toBeGreaterThanOrEqual(1)
    expect(dlq[0].reason).toBeTruthy()
  }, 30_000)
})

// This always runs — it doesn't need Redis.
describe('NativeProducer key getters', () => {
  it.skipIf(!HAS_REDIS)('produces queue-derived key strings', async () => {
    const producer = await NativeProducer.connect(REDIS_URL, {
      queueName: 'getter-test',
    })
    expect(producer.streamKey()).toContain('getter-test')
    expect(producer.delayedKey()).toContain('getter-test')
    expect(producer.dlqKey()).toContain('getter-test')
    // Hash-tag form: the prefix should be `{chasqui:<queue>}`.
    expect(producer.streamKey().startsWith('{chasqui:getter-test}')).toBe(true)
  })
})
