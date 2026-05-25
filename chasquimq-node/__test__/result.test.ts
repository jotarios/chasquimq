import { afterEach, beforeEach, describe, expect, expectTypeOf, it } from 'vitest'
import { encode } from '@msgpack/msgpack'
import { Consumer, Producer, Queue, Worker } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Result backend integration', () => {
  let queueName: string
  let queue: Queue<{ value: number }, { ok: number }>
  let worker:
    | Worker<{ value: number }, { ok: number } | undefined>
    | undefined

  beforeEach(() => {
    queueName = `qmq-test-r-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    if (worker) {
      await worker.close().catch(() => {})
      worker = undefined
    }
    await queue.close().catch(() => {})
  })

  it('storeResults: true → handler return value round-trips through getJobResult', async () => {
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      async (job) => ({ ok: job.data.value * 2 }),
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        storeResults: true,
      },
    )
    void worker.run()

    const job = await queue.add('compute', { value: 21 })

    const result = await waitForResult(() => queue.getJobResult(job.id), 5_000)
    expect(result).toEqual({ ok: 42 })
  })

  it('storeResults: false (default) → getJobResult is undefined even after completion', async () => {
    let processed = false
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async (job) => {
        processed = true
        return { ok: job.data.value }
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        // storeResults defaults to false
      },
    )
    void worker.run()

    const job = await queue.add('no-store', { value: 7 })

    await waitFor(() => processed, 5_000)
    // Give the engine a beat to ack-and-flush.
    await sleep(250)

    const result = await queue.getJobResult(job.id)
    expect(result).toBeUndefined()
  })

  it('resultTtlMs: 1000 → result expires after the TTL elapses', async () => {
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      async (job) => ({ ok: job.data.value }),
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        storeResults: true,
        resultTtlMs: 1000,
      },
    )
    void worker.run()

    const job = await queue.add('ttl', { value: 99 })

    const fresh = await waitForResult(() => queue.getJobResult(job.id), 5_000)
    expect(fresh).toEqual({ ok: 99 })

    // Wait past the TTL (1s) plus a safety margin so Redis evicts.
    await sleep(2_000)
    const expired = await queue.getJobResult(job.id)
    expect(expired).toBeUndefined()
  }, 15_000)

  it('failed handler → no result key', async () => {
    let attempts = 0
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => {
        attempts++
        throw new Error('always fail')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        maxAttempts: 1,
        autorun: false,
        storeResults: true,
      },
    )
    void worker.run()

    const job = await queue.add('fail', { value: 1 })

    await waitFor(() => attempts >= 1, 5_000)
    // Let DLQ relocator settle.
    await sleep(500)

    const result = await queue.getJobResult(job.id)
    expect(result).toBeUndefined()
  })

  it('bulk: enqueue 3 jobs, drain, getJobResult returns the right values', async () => {
    let processed = 0
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async (job) => {
        processed++
        return { ok: job.data.value + 1 }
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        storeResults: true,
      },
    )
    void worker.run()

    const jobs = await queue.addBulk([
      { name: 'j', data: { value: 10 } },
      { name: 'j', data: { value: 20 } },
      { name: 'j', data: { value: 30 } },
    ])

    await waitFor(() => processed >= 3, 5_000)
    // Each result lands via a separate per-entry script call; poll until
    // all three are visible.
    const results = await waitForAllResults(
      () => Promise.all(jobs.map((j) => queue.getJobResult(j.id))),
      5_000,
    )
    expect(results).toEqual([{ ok: 11 }, { ok: 21 }, { ok: 31 }])
  })

  it('TYPE: Queue<{x:number}, {y:string}>.getJobResult returns {y:string} | undefined', () => {
    const typedQueue = new Queue<{ x: number }, { y: string }>(queueName, {
      connection: parseConn(REDIS_URL!),
    })
    const ret = typedQueue.getJobResult('any-id')
    expectTypeOf(ret).toEqualTypeOf<Promise<{ y: string } | undefined>>()
  })

  // Pins the engine's `JOB_OK_SCRIPT` `#ARGV[3] > 0` gate end-to-end:
  // when the handler returns `Buffer.from([])` (zero-length result),
  // the engine must short-circuit to the ack-only path and write no
  // result key. Uses the native binding so we control the exact bytes
  // returned (the high-level `Worker` shim msgpack-encodes results, so
  // an empty user value would still encode to non-empty bytes).
  it('native: handler returns Buffer.from([]) → no result key written', async () => {
    const url = REDIS_URL!
    let nativeProducer: Producer | undefined
    let nativeConsumer: Consumer | undefined
    try {
      nativeProducer = await Producer.connect(url, { queueName })
      nativeConsumer = new Consumer(url, {
        queueName,
        concurrency: 1,
        storeResults: true,
      })

      let processed = false
      // eslint-disable-next-line @typescript-eslint/require-await
      const runP = nativeConsumer.run(async () => {
        processed = true
        return Buffer.from([])
      })
      // Don't `await runP` — Consumer.run resolves on shutdown.
      void runP

      const id = await nativeProducer.add(Buffer.from(encode({ v: 1 })))

      await waitFor(() => processed, 5_000)
      // Let the engine's ack-flush + script call settle.
      await sleep(250)

      const result = await nativeProducer.getResult(id)
      expect(result).toBeNull()
    } finally {
      nativeConsumer?.shutdown()
      // Best-effort: give the consumer's drain a beat.
      await sleep(100)
    }
  })
})

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    }
    await sleep(25)
  }
}

async function waitForResult<T>(
  fetch: () => Promise<T | undefined>,
  timeoutMs: number,
): Promise<T> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const v = await fetch()
    if (v !== undefined) return v
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitForResult timed out after ${timeoutMs}ms`)
    }
    await sleep(50)
  }
}

async function waitForAllResults<T>(
  fetch: () => Promise<Array<T | undefined>>,
  timeoutMs: number,
): Promise<T[]> {
  const start = Date.now()
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const arr = await fetch()
    if (arr.every((v) => v !== undefined)) return arr as T[]
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitForAllResults timed out after ${timeoutMs}ms`)
    }
    await sleep(50)
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
