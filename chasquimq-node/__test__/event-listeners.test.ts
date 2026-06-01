import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  Queue,
  QueueEvents,
  Worker,
  WaitUntilFinishedTimeoutError,
} from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Worker + Queue + Job event listeners', () => {
  let queueName: string
  let queue: Queue<{ value: number }, number>
  let worker: Worker<{ value: number }, number> | undefined
  let queueEvents: QueueEvents | undefined

  beforeEach(() => {
    queueName = `qmq-test-evl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    if (worker) {
      await worker.close().catch(() => {})
      worker = undefined
    }
    if (queueEvents) {
      await queueEvents.close().catch(() => {})
      queueEvents = undefined
    }
    await queue.close().catch(() => {})
  })

  // --- Worker.on('drained') -------------------------------------------------

  it("Worker emits 'drained' after the engine observes a full→empty transition", async () => {
    const drainedSpy = vi.fn()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        drainDelay: 200,
        autorun: false,
      },
    )
    worker.on('drained', drainedSpy)
    void worker.run()

    // Add then wait for the worker to process the job AND observe an
    // empty XREADGROUP. The engine emits `drained` on the full→empty
    // transition only (not on every empty poll).
    await queue.add('one', { value: 1 })

    await waitFor(() => drainedSpy.mock.calls.length >= 1, 10_000)
    expect(drainedSpy).toHaveBeenCalled()
  }, 30_000)

  it("Worker doesn't spawn a drained subscriber when no listener attaches", async () => {
    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      { connection: parseConn(REDIS_URL!), autorun: false },
    )
    void worker.run()
    await queue.add('one', { value: 1 })

    // Give the worker a tick to process, then close. The internal
    // subscriber field should never have been constructed — observable
    // by `close()` returning quickly (no extra `XREAD BLOCK` to drain).
    await new Promise((r) => setTimeout(r, 100))

    const start = Date.now()
    await worker.close()
    const elapsed = Date.now() - start
    // Native consumer shutdown is fast (<1s). A live QueueEvents would
    // add up to `blockingTimeout` (10s default) wait.
    expect(elapsed).toBeLessThan(3_000)
  })

  // --- Worker.on('paused' | 'resumed') -------------------------------------

  it("Worker emits 'paused' and 'resumed' when pause()/resume() are called", async () => {
    const pausedSpy = vi.fn()
    const resumedSpy = vi.fn()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      { connection: parseConn(REDIS_URL!), autorun: false },
    )
    worker.on('paused', pausedSpy)
    worker.on('resumed', resumedSpy)
    void worker.run()
    // Let it start
    await new Promise((r) => setTimeout(r, 50))

    await worker.pause()
    expect(pausedSpy).toHaveBeenCalledTimes(1)

    worker.resume()
    expect(resumedSpy).toHaveBeenCalledTimes(1)
  })

  // --- QueueEvents per-id channels ------------------------------------------

  it("QueueEvents emits per-id `completed:<jobId>` alongside the broadcast", async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      { connection: parseConn(REDIS_URL!), autorun: false },
    )
    void worker.run()

    const job = await queue.add('compute', { value: 42 })
    const targetedSpy = vi.fn()
    const broadcastSpy = vi.fn()
    queueEvents.on(`completed:${job.id}`, targetedSpy)
    queueEvents.on('completed', broadcastSpy)

    await waitFor(
      () => targetedSpy.mock.calls.length >= 1 && broadcastSpy.mock.calls.length >= 1,
      10_000,
    )
    const [payload] = targetedSpy.mock.calls[0]!
    expect((payload as { jobId: string }).jobId).toBe(job.id)
  }, 30_000)

  it("QueueEvents emits per-id `failed:<jobId>` with the engine reason", async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => {
        throw new Error('boom')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxAttempts: 1,
        autorun: false,
      },
    )
    void worker.run()

    const job = await queue.add('fail-me', { value: 0 })
    const targetedSpy = vi.fn()
    queueEvents.on(`failed:${job.id}`, targetedSpy)

    await waitFor(() => targetedSpy.mock.calls.length >= 1, 10_000)
    const [payload] = targetedSpy.mock.calls[0]!
    expect((payload as { jobId: string; failedReason: string })).toMatchObject({
      jobId: job.id,
      failedReason: 'boom',
    })
  }, 30_000)

  // --- Job.waitUntilFinished -----------------------------------------------

  it('Job.waitUntilFinished resolves on completed with the stored result', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value * 3,
      {
        connection: parseConn(REDIS_URL!),
        autorun: false,
        storeResults: true,
        resultTtlMs: 60_000,
      },
    )
    void worker.run()

    const job = await queue.add('triple', { value: 7 })
    const result = await job.waitUntilFinished(queueEvents, 10_000)
    expect(result).toBe(21)
  }, 30_000)

  it('Job.waitUntilFinished resolves with undefined when storeResults is off', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      { connection: parseConn(REDIS_URL!), autorun: false },
    )
    void worker.run()

    const job = await queue.add('no-store', { value: 7 })
    const result = await job.waitUntilFinished(queueEvents, 10_000)
    // Event-driven completion still detected; the return value is
    // unavailable without the result backend.
    expect(result).toBeUndefined()
  }, 30_000)

  it('Job.waitUntilFinished rejects with the engine reason on failure', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => {
        throw new Error('handler said no')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxAttempts: 1,
        autorun: false,
      },
    )
    void worker.run()

    const job = await queue.add('rejects', { value: 0 })
    await expect(job.waitUntilFinished(queueEvents, 10_000)).rejects.toThrow(
      /handler said no/,
    )
  }, 30_000)

  it('Job.waitUntilFinished throws WaitUntilFinishedTimeoutError on ttl elapse', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    // No worker; the job will sit waiting forever.
    const job = await queue.add('orphan', { value: 0 })
    await expect(job.waitUntilFinished(queueEvents, 300)).rejects.toThrow(
      WaitUntilFinishedTimeoutError,
    )
  })
})

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs)
      throw new Error(`waitFor timed out after ${timeoutMs}ms`)
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
