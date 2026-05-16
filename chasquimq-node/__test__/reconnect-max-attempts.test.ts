import { describe, it, expect } from 'vitest'
import { Queue } from '../dist/queue.js'
import { Worker } from '../dist/worker.js'
import { Producer, Consumer } from '../dist/index.js'

// `reconnectMaxAttempts` caps fred's exponential reconnect loop so a
// permanently rejecting credentialProvider stops instead of looping
// forever. We can't force the reconnect path from loopback Redis
// without a flapping server, so — mirroring the credential-provider
// suite — these are wiring/acceptance tests: the value must be
// accepted at the native and high-level surface and threaded through
// without breaking the happy path. `0` (the engine default =
// unbounded) must remain a legal value.
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)

const d = HAS_REDIS ? describe : describe.skip

function uniqueQueue(tag: string): string {
  return `rma-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
}

d('Queue accepts connection.reconnectMaxAttempts', () => {
  it('threads a positive cap through without breaking Queue.add', async () => {
    const queue = new Queue(uniqueQueue('add'), {
      connection: { url: REDIS_URL, reconnectMaxAttempts: 5 },
    })
    try {
      const job = await queue.add('ping', { hello: 'world' })
      expect(job.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
    } finally {
      await queue.close()
    }
  }, 30_000)

  it('accepts 0 (the unbounded default) explicitly', async () => {
    const queue = new Queue(uniqueQueue('zero'), {
      connection: { url: REDIS_URL, reconnectMaxAttempts: 0 },
    })
    try {
      const job = await queue.add('ping', { n: 1 })
      expect(job.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
    } finally {
      await queue.close()
    }
  }, 30_000)
})

d('Worker accepts connection.reconnectMaxAttempts', () => {
  it('threads the cap through and still processes a job', async () => {
    const queueName = uniqueQueue('worker')
    const queue = new Queue(queueName, {
      connection: { url: REDIS_URL, reconnectMaxAttempts: 7 },
    })

    let resolveProcessed: (() => void) | undefined
    const processed = new Promise<void>((r) => {
      resolveProcessed = r
    })

    const worker = new Worker(
      queueName,
      async () => {
        resolveProcessed?.()
        return undefined
      },
      {
        connection: { url: REDIS_URL, reconnectMaxAttempts: 7 },
        autorun: true,
        concurrency: 1,
        drainDelay: 100,
      },
    )

    try {
      await queue.add('do-thing', { n: 1 })
      await Promise.race([
        processed,
        new Promise((_, rej) =>
          setTimeout(() => rej(new Error('worker-timeout')), 8_000),
        ),
      ])
    } finally {
      await worker.close()
      await queue.close()
    }
  }, 30_000)
})

d('native Producer / Consumer accept reconnectMaxAttempts opt', () => {
  it('Producer.connect accepts the opt and round-trips', async () => {
    const queueName = uniqueQueue('native-prod')
    const producer = await Producer.connect(REDIS_URL, {
      queueName,
      reconnectMaxAttempts: 3,
    })
    try {
      const id = await producer.add(Buffer.from([0x80]))
      expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
    } finally {
      await producer.shutdown()
    }
  }, 30_000)

  it('Consumer constructor accepts the opt (smoke)', () => {
    const consumer = new Consumer(REDIS_URL, {
      queueName: uniqueQueue('native-cons'),
      reconnectMaxAttempts: 3,
    })
    // shutdown() before run() is a documented no-op.
    consumer.shutdown()
  })
})
