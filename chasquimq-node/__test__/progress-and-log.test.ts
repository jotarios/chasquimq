import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Queue, QueueEvents, Worker } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Job progress + log', () => {
  let queueName: string
  let queue: Queue<{ value: number }>
  let worker: Worker<{ value: number }> | undefined
  let queueEvents: QueueEvents | undefined

  beforeEach(() => {
    queueName = `qmq-progress-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue<{ value: number }>(queueName, {
      connection: parseConn(REDIS_URL!),
    })
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

  it('handler updateProgress persists; getJob().progress reflects it', async () => {
    const done = deferred<void>()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        await job.updateProgress(50)
        done.resolve()
        // Hold the handler so the job stays Active while we introspect —
        // a completed job with no stored result key disappears from the
        // introspector's view, so we'd race the ack vs the read.
        await sleep(500)
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    void worker.run()

    const added = await queue.add('progress-one', { value: 1 })
    await done.promise

    const fetched = await queue.getJob(added.id)
    expect(fetched).toBeDefined()
    expect(fetched!.progress).toBe(50)
  }, 15_000)

  it('QueueEvents emits per-id progress:<id> with payload { jobId, name, progress }', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()

    const done = deferred<void>()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        await job.updateProgress(33)
        done.resolve()
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    void worker.run()

    const job = await queue.add('emits-progress', { value: 1 })
    const targetedSpy = vi.fn()
    const broadcastSpy = vi.fn()
    queueEvents.on(`progress:${job.id}`, targetedSpy)
    queueEvents.on('progress', broadcastSpy)

    await done.promise
    await waitFor(
      () => targetedSpy.mock.calls.length >= 1 && broadcastSpy.mock.calls.length >= 1,
      10_000,
    )
    const [payload] = targetedSpy.mock.calls[0]!
    expect(payload).toMatchObject({
      jobId: job.id,
      name: 'emits-progress',
      progress: 33,
    })
  }, 15_000)

  it('Worker EE fires progress with (job, n) once per call', async () => {
    const done = deferred<void>()
    const progressSpy = vi.fn()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        await job.updateProgress(10)
        await job.updateProgress(50)
        await job.updateProgress(100)
        done.resolve()
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    worker.on('progress', progressSpy)
    void worker.run()

    await queue.add('three-updates', { value: 1 })
    await done.promise
    await waitFor(() => progressSpy.mock.calls.length >= 3, 10_000)
    expect(progressSpy).toHaveBeenCalledTimes(3)
    const values = progressSpy.mock.calls.map((c) => c[1])
    expect(values).toEqual([10, 50, 100])
  }, 15_000)

  it('Job.log appends lines and Queue.getJobLogs reads them back', async () => {
    const done = deferred<void>()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        await job.log('A')
        await job.log('B')
        done.resolve()
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    void worker.run()

    const added = await queue.add('logs-two', { value: 1 })
    await done.promise

    const logs = await queue.getJobLogs(added.id)
    expect(logs).toEqual({ logs: ['A', 'B'], count: 2 })
  }, 15_000)

  it('Queue.getJobLogs pagination: start=2 end=4 asc=true returns 3 lines in order', async () => {
    const done = deferred<void>()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        for (let i = 0; i < 10; i++) {
          await job.log(`L${i}`)
        }
        done.resolve()
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    void worker.run()

    const added = await queue.add('logs-ten', { value: 1 })
    await done.promise

    const page = await queue.getJobLogs(added.id, 2, 4, true)
    expect(page.logs).toEqual(['L2', 'L3', 'L4'])
    expect(page.count).toBe(10)
  }, 15_000)

  it('updateProgress(150) clamps to 100 instead of throwing', async () => {
    const done = deferred<void>()
    worker = new Worker<{ value: number }>(
      queueName,
      async (job) => {
        await expect(job.updateProgress(150)).resolves.toBeUndefined()
        done.resolve()
        // Keep the job Active while we read it back — same rationale as
        // the persistence test above.
        await sleep(500)
      },
      { connection: parseConn(REDIS_URL!), autorun: false, concurrency: 1 },
    )
    void worker.run()

    const added = await queue.add('clamp', { value: 1 })
    await done.promise
    const fetched = await queue.getJob(added.id)
    expect(fetched).toBeDefined()
    expect(fetched!.progress).toBe(100)
  }, 15_000)

  it('Queue.getJob()-returned Job throws on updateProgress (read-only)', async () => {
    // No worker — the job sits waiting, getJob synthesizes a read-only Job.
    const added = await queue.add('read-only', { value: 1 })
    const fetched = await queue.getJob(added.id)
    expect(fetched).toBeDefined()
    await expect(fetched!.updateProgress(50)).rejects.toThrow(
      /requires the Job be passed to your Worker handler/,
    )
    await expect(fetched!.log('nope')).rejects.toThrow(
      /requires the Job be passed to your Worker handler/,
    )
  })
})

interface Deferred<T> {
  promise: Promise<T>
  resolve: (v: T) => void
  reject: (e: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    }
    await sleep(25)
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms))
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
