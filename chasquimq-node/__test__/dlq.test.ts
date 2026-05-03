import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { encode, decode } from '@msgpack/msgpack'
import { NativeProducer, NativeConsumer } from '../index.js'

// peekDlq / replayDlq are exposed on the native producer but not on the
// high-level Queue shim. These tests exercise them directly — same
// convention as `native.test.ts`.

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)

const d = HAS_REDIS ? describe : describe.skip

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now()
  while (true) {
    if (await predicate()) return
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms`)
    await new Promise((r) => setTimeout(r, 25))
  }
}

d('peekDlq + replayDlq', () => {
  let queueName: string
  let producer: NativeProducer
  let consumer: NativeConsumer | undefined
  let runP: Promise<void> | undefined

  beforeEach(async () => {
    queueName = `qmq-dlq-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    producer = await NativeProducer.connect(REDIS_URL, { queueName })
  })

  afterEach(async () => {
    if (consumer) {
      try { consumer.shutdown() } catch { /* ignore */ }
    }
    await Promise.allSettled([runP].filter(Boolean) as Promise<unknown>[])
    consumer = undefined
    runP = undefined
  })

  it('routes a failed job to DLQ and replays it back to the main stream', async () => {
    let shouldFail = true
    let attempts = 0
    let succeededPayload: unknown = null
    let succeededId: string | null = null

    consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      maxAttempts: 1, // first failure → DLQ
      delayedEnabled: false,
      retry: { initialBackoffMs: 1, maxBackoffMs: 5, multiplier: 1, jitterMs: 0 },
    })

    runP = consumer.run(async (job) => {
      attempts++
      if (shouldFail) {
        throw new Error('intentional-dlq-failure')
      }
      succeededPayload = decode(job.payload)
      succeededId = job.id
    })

    const data = { will: 'fail-then-replay' }
    const id = await producer.add(Buffer.from(encode(data)))

    // Wait for the handler to fire.
    await waitFor(() => attempts >= 1, 5_000, 'first-attempt')

    // Wait for the DLQ relocator to flush.
    await waitFor(async () => {
      const entries = await producer.peekDlq(10)
      return entries.length >= 1
    }, 5_000, 'dlq-routed')

    const dlqEntries = await producer.peekDlq(10)
    expect(dlqEntries).toHaveLength(1)
    const [entry] = dlqEntries
    // `sourceId` is the Redis Streams entry id of the failed message
    // (`<ms>-<seq>`), not the engine's ULID `JobId`. The ULID lives
    // inside the encoded payload (and surfaces on `job.id` when the
    // message is replayed and re-delivered to a worker).
    expect(entry!.sourceId).toMatch(/^\d+-\d+$/)
    expect(entry!.reason).toBeTruthy()
    // Payload round-trips: the engine stores the encoded outer
    // `Job<RawBytes>` (a 4-element msgpack array of
    // [id, raw_bytes, created_at_ms, attempt]); the user's MessagePack
    // payload sits at index 1.
    const outer = decode(entry!.payload) as unknown[]
    expect(Array.isArray(outer)).toBe(true)
    expect(outer.length).toBeGreaterThanOrEqual(2)
    expect(decode(outer[1] as Uint8Array)).toEqual(data)

    // Now flip the handler to succeed and replay.
    shouldFail = false

    const replayed = await producer.replayDlq(10)
    expect(replayed).toBeGreaterThanOrEqual(1)

    // After replay, DLQ should be empty.
    const afterReplay = await producer.peekDlq(10)
    expect(afterReplay).toHaveLength(0)

    // Wait for the worker to pick up the replayed job and succeed.
    await waitFor(() => succeededId !== null, 5_000, 'replayed-success')
    expect(succeededPayload).toEqual(data)
    // Replay preserves the original Job::id — only `attempt` is reset.
    // Redis assigns a fresh stream entry id, but the engine's `job.id`
    // is the ULID baked into the encoded payload.
    expect(succeededId).toBe(id)
  }, 30_000)
})
