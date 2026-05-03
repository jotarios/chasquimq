import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Queue, Worker, NotSupportedError } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Worker integration', () => {
  let queueName: string
  let queue: Queue<{ value: number }>
  let worker: Worker<{ value: number }, number> | undefined

  beforeEach(() => {
    queueName = `qmq-test-w-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    if (worker) {
      await worker.close().catch(() => {})
      worker = undefined
    }
    await queue.close().catch(() => {})
  })

  it('processes a job end-to-end and emits completed', async () => {
    const completedSpy = vi.fn()
    const activeSpy = vi.fn()

    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value * 2,
      { connection: parseConn(REDIS_URL!), concurrency: 4, autorun: false },
    )
    worker.on('active', activeSpy)
    worker.on('completed', completedSpy)
    void worker.run()

    await queue.add('double', { value: 7 })

    await waitFor(() => completedSpy.mock.calls.length >= 1, 5_000)
    expect(activeSpy).toHaveBeenCalled()
    const [job, result] = completedSpy.mock.calls[0]!
    expect(job.data).toEqual({ value: 7 })
    expect(result).toBe(14)
  })

  it('emits failed when the processor rejects', async () => {
    const failedSpy = vi.fn()
    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => {
        throw new Error('processor failure')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxStalledCount: 1,
        autorun: false,
      },
    )
    worker.on('failed', failedSpy)
    void worker.run()

    await queue.add('fail-me', { value: 1 })

    await waitFor(() => failedSpy.mock.calls.length >= 1, 5_000)
    const [, err] = failedSpy.mock.calls[0]!
    expect((err as Error).message).toBe('processor failure')
  })

  it('throws NotSupportedError on string-path processor', () => {
    expect(() => {
      new Worker(queueName, '/path/to/processor.js' as unknown as never, {
        connection: parseConn(REDIS_URL!),
      })
    }).toThrow(NotSupportedError)
  })

  it('close() emits closed event and resolves', async () => {
    const closedSpy = vi.fn()
    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      { connection: parseConn(REDIS_URL!), autorun: false },
    )
    worker.on('closed', closedSpy)
    void worker.run()

    // Let it start
    await new Promise((res) => setTimeout(res, 50))
    await worker.close()
    expect(closedSpy).toHaveBeenCalled()
  })
})

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
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
