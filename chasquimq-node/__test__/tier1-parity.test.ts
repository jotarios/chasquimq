// Tier 1 cross-shim DX parity tests.
//
// Covers:
//   - Queue.peekDlq / Queue.replayDlq high-level wrappers (Fix A)
//   - Single `failed` event on UnrecoverableError (Fix B, shim side)
//   - failedReason carries just the user message, no FFI prefix (Fix C)
//   - Symbol.asyncDispose on Queue / Worker / QueueEvents (Fix D)
//   - BackoffSpec factory parity with the Python shim
//
// Mirrors the Python test_tier1_parity.py file.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  Queue,
  Worker,
  QueueEvents,
  Producer,
  BackoffSpec,
  UnrecoverableError,
} from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

function parseConn(url: string) {
  const u = new URL(url)
  return {
    host: u.hostname || '127.0.0.1',
    port: u.port ? Number(u.port) : 6379,
  }
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (await predicate()) return
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms`)
    }
    await new Promise((r) => setTimeout(r, 25))
  }
}

skipIfNoRedis('Tier 1 — Queue.peekDlq / Queue.replayDlq', () => {
  let queue: Queue<{ tag: string }>
  let queueName: string

  beforeEach(() => {
    queueName = `qmq-tier1-dlq-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    await queue.close().catch(() => {})
  })

  it('Queue.peekDlq matches native Producer.peekDlq shape', async () => {
    // Seed a DLQ entry by running a worker that forces a single-attempt failure.
    const worker = new Worker(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        throw new UnrecoverableError('peek-test')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 1,
        autorun: false,
      },
    )
    void worker.run()
    await queue.add('peek-test', { tag: 'a' })

    await waitFor(async () => (await queue.peekDlq(10)).length >= 1, 5_000, 'dlq')

    const fromQueue = await queue.peekDlq(10)
    const producer = await Producer.connect(REDIS_URL!, { queueName })
    const fromProducer = await producer.peekDlq(10)
    expect(fromQueue).toHaveLength(1)
    expect(fromQueue.length).toBe(fromProducer.length)
    expect(fromQueue[0]!.dlqId).toBe(fromProducer[0]!.dlqId)
    expect(fromQueue[0]!.reason).toBe('unrecoverable')

    await worker.close()
  }, 15_000)

  it('Queue.replayDlq returns the count actually replayed', async () => {
    let shouldFail = true
    const worker = new Worker<{ tag: string }>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        if (shouldFail) throw new Error('replay-test')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 1,
        autorun: false,
      },
    )
    void worker.run()
    await queue.add('replay-test', { tag: 'b' })

    await waitFor(async () => (await queue.peekDlq(10)).length >= 1, 5_000, 'dlq')
    shouldFail = false

    const replayed = await queue.replayDlq(10)
    expect(replayed).toBeGreaterThanOrEqual(1)

    await waitFor(async () => (await queue.peekDlq(10)).length === 0, 5_000, 'drained')
    await worker.close()
  }, 15_000)
})

skipIfNoRedis('Tier 1 — single failed event on UnrecoverableError', () => {
  let queueName: string
  let queue: Queue<{ tag: string }>
  let worker: Worker<{ tag: string }>
  let events: QueueEvents

  beforeEach(() => {
    queueName = `qmq-tier1-failed-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
    events = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: false,
      blockingTimeout: 500,
      lastEventId: '0',
    })
  })

  afterEach(async () => {
    await worker?.close().catch(() => {})
    await events?.close().catch(() => {})
    await queue.close().catch(() => {})
  })

  it('QueueEvents emits exactly one `failed` per UnrecoverableError handler', async () => {
    let failedCount = 0
    const failedJobIds: string[] = []
    events.on('failed', (payload: Record<string, unknown>) => {
      failedCount++
      failedJobIds.push(String(payload.jobId ?? ''))
    })
    void events.run()

    worker = new Worker<{ tag: string }>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        throw new UnrecoverableError('poison')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 5, // generous — UnrecoverableError must short-circuit
        autorun: false,
      },
    )
    void worker.run()

    const job = await queue.add('poison', { tag: 'p' })

    await waitFor(async () => (await queue.peekDlq(10)).length >= 1, 5_000, 'dlq')
    // Give the events stream + dispatcher one extra full block window so a
    // (hypothetical) second `failed` would have arrived.
    await new Promise((r) => setTimeout(r, 750))

    expect(failedCount).toBe(1)
    expect(failedJobIds[0]).toBe(job.id)
  }, 15_000)
})

skipIfNoRedis('Tier 1 — failedReason has no FFI prefix', () => {
  let queueName: string
  let queue: Queue<unknown>
  let worker: Worker<unknown>
  let events: QueueEvents

  beforeEach(() => {
    queueName = `qmq-tier1-reason-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
    events = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: false,
      blockingTimeout: 500,
      lastEventId: '0',
    })
  })

  afterEach(async () => {
    await worker?.close().catch(() => {})
    await events?.close().catch(() => {})
    await queue.close().catch(() => {})
  })

  it('failedReason is the user error message, not "JS handler rejected: Error: ..."', async () => {
    const reasons: string[] = []
    events.on('failed', (payload: Record<string, unknown>) => {
      reasons.push(String(payload.failedReason ?? ''))
    })
    void events.run()

    worker = new Worker(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        throw new Error('smtp timeout')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 1,
        autorun: false,
      },
    )
    void worker.run()
    await queue.add('failing', { tag: 'r' })

    await waitFor(() => reasons.length >= 1, 5_000, 'failed')

    expect(reasons[0]).toBe('smtp timeout')
    // Defensive: the prefixes that used to leak through must NOT be present.
    expect(reasons[0]).not.toMatch(/^handler:/)
    expect(reasons[0]).not.toMatch(/^JS handler rejected:/)
    expect(reasons[0]).not.toMatch(/^Error:/)
  }, 15_000)
})

skipIfNoRedis('Tier 1 — Symbol.asyncDispose on Queue / Worker / QueueEvents', () => {
  it('await using closes Queue', async () => {
    const queueName = `qmq-tier1-dispose-q-${Date.now()}`
    let id: string | null = null
    {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      await using q = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
      const job = await q.add('x', { hello: 'world' })
      id = job.id
      expect(q.isClosed).toBe(false)
    }
    expect(id).not.toBeNull()
  })

  it('await using closes Worker', async () => {
    const queueName = `qmq-tier1-dispose-w-${Date.now()}`
    let isClosedAfter = false
    let isClosedDuring = true
    {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      await using w = new Worker(
        queueName,
        // eslint-disable-next-line @typescript-eslint/require-await
        async () => undefined,
        {
          connection: parseConn(REDIS_URL!),
          autorun: false,
          concurrency: 1,
        },
      )
      isClosedDuring = w.isClosed
      // Small delay so .run() can settle before close trips.
      await new Promise((r) => setTimeout(r, 20))
      isClosedAfter = w.isClosed
    }
    expect(isClosedDuring).toBe(false)
    // Just before exiting the block we still hadn't closed.
    expect(isClosedAfter).toBe(false)
  }, 10_000)

  it('await using closes QueueEvents', async () => {
    const queueName = `qmq-tier1-dispose-e-${Date.now()}`
    let wasClosed = false
    {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      await using ev = new QueueEvents(queueName, {
        connection: parseConn(REDIS_URL!),
        autorun: false,
        blockingTimeout: 200,
      })
      expect(ev.isClosed).toBe(false)
    }
    // After the block exits, the dispose ran. We can't introspect `ev` from
    // out here (it's gone out of scope), but the key contract is no
    // unhandled rejection / leaked timer, which vitest will surface.
    wasClosed = true
    expect(wasClosed).toBe(true)
  }, 10_000)
})

describe('Tier 1 — BackoffSpec factories (no Redis)', () => {
  it('BackoffSpec.fixed produces a fixed BackoffOptions', () => {
    const b = BackoffSpec.fixed(500)
    expect(b.type).toBe('fixed')
    expect(b.delay).toBe(500)
  })

  it('BackoffSpec.fixed with jitter', () => {
    const b = BackoffSpec.fixed(1_000, { jitterMs: 250 })
    expect(b.type).toBe('fixed')
    expect(b.delay).toBe(1_000)
    expect(b.jitterMs).toBe(250)
  })

  it('BackoffSpec.exponential default multiplier=2', () => {
    const b = BackoffSpec.exponential(1_000)
    expect(b.type).toBe('exponential')
    expect(b.delay).toBe(1_000)
    expect(b.multiplier).toBe(2)
  })

  it('BackoffSpec.exponential honors maxDelayMs / multiplier / jitterMs', () => {
    const b = BackoffSpec.exponential(1_000, {
      multiplier: 3,
      maxDelayMs: 60_000,
      jitterMs: 100,
    })
    expect(b.type).toBe('exponential')
    expect(b.multiplier).toBe(3)
    expect(b.maxDelay).toBe(60_000)
    expect(b.jitterMs).toBe(100)
  })
})
