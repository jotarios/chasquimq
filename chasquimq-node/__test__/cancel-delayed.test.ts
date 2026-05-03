import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { encode } from '@msgpack/msgpack'
import {
  NativeProducer,
  NativeConsumer,
  NativePromoter,
} from '../index.js'

// The high-level shim doesn't expose cancelDelayed / cancelDelayedBulk
// or peekDlq / replayDlq, but the underlying NativeProducer does. These
// tests reach into the native binding directly — same convention as
// `native.test.ts`.

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)

const d = HAS_REDIS ? describe : describe.skip

d('cancelDelayed / cancelDelayedBulk', () => {
  let queueName: string
  let producer: NativeProducer
  let consumer: NativeConsumer | undefined
  let promoter: NativePromoter | undefined
  let runP: Promise<void> | undefined
  let promP: Promise<void> | undefined

  beforeEach(async () => {
    queueName = `qmq-cancel-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    producer = await NativeProducer.connect(REDIS_URL, { queueName })
  })

  afterEach(async () => {
    if (consumer) {
      try { consumer.shutdown() } catch { /* ignore */ }
    }
    if (promoter) {
      try { promoter.shutdown() } catch { /* ignore */ }
    }
    await Promise.allSettled([runP, promP].filter(Boolean) as Promise<unknown>[])
    consumer = undefined
    promoter = undefined
    runP = undefined
    promP = undefined
  })

  it('cancels a single delayed job before it fires', async () => {
    const jobId = `cancel-one-${Date.now()}`
    await producer.addInWithId(jobId, 2_000, Buffer.from(encode({ should: 'never-fire' })))

    // Let the job land in the delayed ZSET; producer.addInWithId already
    // resolved, but give the side-index write a tick to settle.
    await new Promise((r) => setTimeout(r, 100))

    const removed = await producer.cancelDelayed(jobId)
    expect(removed).toBe(true)

    let counter = 0
    consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      delayedEnabled: true,
    })
    runP = consumer.run(async () => {
      counter++
    })

    // delay (2000ms) + buffer to let promoter run if it were going to.
    await new Promise((r) => setTimeout(r, 3_000))

    expect(counter).toBe(0)
  }, 30_000)

  it('returns false for an unknown id', async () => {
    const removed = await producer.cancelDelayed(`does-not-exist-${Date.now()}`)
    expect(removed).toBe(false)
  })

  it('cancelDelayedBulk cancels a mix of present and absent ids', async () => {
    const ids = [`bulk-a-${Date.now()}`, `bulk-b-${Date.now()}`, `bulk-c-${Date.now()}`]
    await producer.addInWithId(ids[0]!, 60_000, Buffer.from(encode({ n: 1 })))
    await producer.addInWithId(ids[1]!, 60_000, Buffer.from(encode({ n: 2 })))
    // Skip ids[2] — leave it absent.

    const result = await producer.cancelDelayedBulk([ids[0]!, ids[1]!, 'unknown-id-zzz'])
    expect(result).toEqual([true, true, false])
  })

  it('cancelDelayed loses to a successful promotion', async () => {
    const jobId = `racing-${Date.now()}`
    let counter = 0

    consumer = new NativeConsumer(REDIS_URL, {
      queueName,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 1,
      blockMs: 100,
      delayedEnabled: true,
    })
    runP = consumer.run(async () => {
      counter++
    })

    await producer.addInWithId(jobId, 200, Buffer.from(encode({ msg: 'racing' })))

    // Wait long enough for the promoter to fire.
    await new Promise((r) => setTimeout(r, 500))

    const removed = await producer.cancelDelayed(jobId)
    expect(removed).toBe(false)

    // The job is in the stream; the worker will pick it up.
    await new Promise((r) => setTimeout(r, 1_000))
    expect(counter).toBe(1)
  }, 30_000)
})
