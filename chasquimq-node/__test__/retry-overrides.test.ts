// End-to-end tests for the per-job retry override path
// (engine slice 8 wired through the NAPI binding) and for
// `UnrecoverableError` → `HandlerError::unrecoverable` mapping.
//
// The retry-override tests pin the contract that
// `Queue.add(name, data, { attempts, backoff })` *beats* the queue-wide
// `maxAttempts` for that one job. They drive the worker through the
// retry path with a small queue-wide budget so a failure of the
// override would land in the DLQ early.
//
// The unrecoverable test pins the inverse: a job that throws
// `UnrecoverableError` must hit the DLQ on the first failed attempt
// regardless of the queue's retry budget, with `reason === 'unrecoverable'`.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Queue, Worker, UnrecoverableError } from '../dist/index.js'
import { NativeProducer } from '../index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Per-job retry overrides + UnrecoverableError', () => {
  let queueName: string
  let queue: Queue<{ value: number }>
  let worker: Worker<{ value: number }, number> | undefined

  beforeEach(() => {
    queueName = `qmq-test-ro-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    if (worker) {
      await worker.close().catch(() => {})
      worker = undefined
    }
    await queue.close().catch(() => {})
  })

  it('Queue.add({ attempts: 5 }) overrides the queue-wide maxAttempts (2)', async () => {
    // Queue-wide budget = 2. Per-job override = 5. The handler counts
    // calls; the assertion is "exactly 5", not "at least 3".
    let calls = 0
    worker = new Worker<{ value: number }, number>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        calls++
        throw new Error('always-fails')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 2,
        autorun: false,
      },
    )
    void worker.run()

    await queue.add(
      'override-attempts',
      { value: 1 },
      {
        attempts: 5,
        // Tight backoff so the test isn't I/O-bound — the engine's
        // `RetryConfig` defaults to 1s initial which would push this to
        // ~16s total wall clock.
        backoff: { type: 'fixed', delay: 30 },
      },
    )

    // 5 attempts × 30ms backoff each = ~150ms baseline; give it slack
    // for handler scheduling jitter.
    await waitFor(() => calls >= 5, 8_000)
    // Settle: nothing else should fire after the 5th attempt — give the
    // engine a beat to not call again, then assert exactly 5.
    await new Promise((r) => setTimeout(r, 500))
    expect(calls).toBe(5)
  }, 30_000)

  it('Queue.add({ backoff: { type: "fixed", delay: 30 } }) honors the per-job backoff', async () => {
    const callTimes: number[] = []
    worker = new Worker<{ value: number }, number>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        callTimes.push(Date.now())
        throw new Error('always-fails')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 5, // reach for the per-job override budget
        autorun: false,
      },
    )
    void worker.run()

    await queue.add(
      'fixed-backoff',
      { value: 2 },
      {
        attempts: 4,
        backoff: { type: 'fixed', delay: 30 },
      },
    )

    await waitFor(() => callTimes.length >= 4, 8_000)
    await new Promise((r) => setTimeout(r, 250))

    expect(callTimes.length).toBeGreaterThanOrEqual(4)
    // Pin retry gaps: at least 25ms between consecutive attempts. We
    // intentionally don't bound the upper end — the worker's read loop
    // and ack flusher add jitter we can't control deterministically in
    // a single-host test.
    for (let i = 1; i < Math.min(callTimes.length, 4); i++) {
      const gap = callTimes[i]! - callTimes[i - 1]!
      expect(gap).toBeGreaterThanOrEqual(20)
    }
  }, 30_000)

  it('UnrecoverableError → 1 attempt, DLQ reason="unrecoverable"', async () => {
    let calls = 0
    worker = new Worker<{ value: number }, number>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        calls++
        throw new UnrecoverableError('terminal failure: bad input')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        // Generous queue-wide budget — the test proves this is *ignored*
        // because the handler signaled terminal-fail.
        maxStalledCount: 10,
        autorun: false,
      },
    )
    void worker.run()

    await queue.add('terminal', { value: 99 })

    await waitFor(() => calls >= 1, 5_000)
    // Give the DLQ relocator a tick to flush.
    await new Promise((r) => setTimeout(r, 500))

    expect(calls).toBe(1)

    // Peek the DLQ via the native producer and assert reason.
    const producer = await NativeProducer.connect(REDIS_URL!, { queueName })
    const dlq = await producer.peekDlq(10)
    expect(dlq).toHaveLength(1)
    expect(dlq[0]!.reason).toBe('unrecoverable')
  }, 30_000)

  it('Regular Error follows the standard retry path (attempts=2 → 2 calls → DLQ reason="retries_exhausted")', async () => {
    let calls = 0
    worker = new Worker<{ value: number }, number>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        calls++
        throw new Error('plain failure')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 2, // queue-wide budget; the test relies on it
        autorun: false,
      },
    )
    void worker.run()

    await queue.add('regular', { value: 0 })

    await waitFor(() => calls >= 2, 8_000)
    // Pause so the engine settles into the DLQ state.
    await new Promise((r) => setTimeout(r, 500))
    expect(calls).toBe(2)

    const producer = await NativeProducer.connect(REDIS_URL!, { queueName })
    const dlq = await producer.peekDlq(10)
    expect(dlq).toHaveLength(1)
    expect(dlq[0]!.reason).toBe('retries_exhausted')
  }, 30_000)

  it('emits failed before throwing UnrecoverableError', async () => {
    // The Worker shim re-throws after emitting `failed`. Pin that the
    // event still fires for unrecoverable errors so users can wire
    // logging / metrics off it the same way they would for any other
    // failure.
    const failedSpy = vi.fn()
    worker = new Worker<{ value: number }, number>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        throw new UnrecoverableError('observed failure')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 5,
        autorun: false,
      },
    )
    worker.on('failed', failedSpy)
    void worker.run()

    await queue.add('observe', { value: 7 })

    await waitFor(() => failedSpy.mock.calls.length >= 1, 5_000)
    const [, err] = failedSpy.mock.calls[0]!
    expect((err as Error).name).toBe('UnrecoverableError')
    expect((err as Error).message).toBe('observed failure')
  }, 30_000)
})

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    await new Promise((res) => setTimeout(res, 25))
  }
}

function parseConn(url: string) {
  const u = new URL(url)
  return {
    host: u.hostname || '127.0.0.1',
    port: u.port ? Number(u.port) : 6379,
    password: u.password || undefined,
    username: u.username || undefined,
    db: u.pathname && u.pathname !== '/' ? Number(u.pathname.slice(1)) : undefined,
  }
}
