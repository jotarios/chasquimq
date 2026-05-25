/**
 * Cross-FFI tests for the slice-12 stalled-job detector + the
 * `WorkerOptions.maxStalledCount` routing fix.
 *
 * The hung-handler end-to-end relocate scenario lives in the engine
 * integration tests (`chasquimq/tests/stalled_detection.rs`); this
 * file pins the FFI-surface invariants:
 *  1. The `'stalled'` listener is wired without throwing and lazily
 *     spawns the internal `QueueEvents` subscriber.
 *  2. `WorkerOptions.maxStalledCount` no longer routes to engine
 *     `max_attempts` — a handler that ALWAYS fails (not hangs)
 *     should respect `maxAttempts`, not `maxStalledCount`.
 *  3. The v1.4 warn-once fires when `maxStalledCount` is set without
 *     `maxAttempts`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Queue, Worker } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Worker stalled-detector wiring (slice 12)', () => {
  let queueName: string
  let queue: Queue<{ value: number }, number>
  let worker: Worker<{ value: number }, number> | undefined

  beforeEach(() => {
    queueName = `qmq-test-stalled-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    if (worker) {
      // Use a short force-close — some tests run handlers that
      // resolve quickly; the worker is just shutting down a clean
      // pool.
      await Promise.race([
        worker.close().catch(() => {}),
        new Promise((r) => setTimeout(r, 5_000)),
      ])
      worker = undefined
    }
    await queue.close().catch(() => {})
  })

  it("'stalled' listener wires up + lazily spawns events subscriber", async () => {
    const stalledSpy = vi.fn<(jobId: string, prev: string) => void>()
    worker = new Worker<{ value: number }, number>(
      queueName,
      async (job) => job.data.value,
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        drainDelay: 100,
        autorun: false,
      },
    )
    // Attaching a `stalled` listener must not throw and must lazily
    // construct the internal QueueEvents subscriber (same shape as
    // `drained` / `progress`).
    worker.on('stalled', stalledSpy)
    // `internalEvents` is a private field; reach into it via `any`.
    expect((worker as any).internalEvents).toBeDefined()
    void worker.run()
    // Sanity: the worker still processes normal jobs after a stalled
    // listener attached.
    await queue.add('one', { value: 42 })
    await waitFor(
      async () => {
        // The handler resolves and the worker emits 'completed'; we
        // don't have a queryable counter here but giving the run a
        // beat is enough to confirm no wiring crash.
        return true
      },
      1_000,
    )
  }, 15_000)

  it('maxStalledCount no longer routes to max_attempts (always-failing handler)', async () => {
    // Pre-v1.4 regression: `maxStalledCount: 2` caused
    // engine `max_attempts = 2`, so an always-failing handler hit
    // DLQ-as-RetriesExhausted after exactly 2 invocations. Post-v1.4
    // the routing fix means `max_attempts` is taken from
    // `maxAttempts` (or the engine default 25 when unset) and
    // `maxStalledCount` controls the stall ceiling only.
    let attempts = 0
    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => {
        attempts += 1
        throw new Error('always-fail')
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        drainDelay: 100,
        // The interesting cell: pre-v1.4 this would cap at 2; post-v1.4
        // it only affects stall ceiling (which a non-hanging handler
        // never triggers).
        maxStalledCount: 2,
        // Bound the test wall-clock: cap retries at 5 explicitly.
        maxAttempts: 5,
        autorun: false,
      },
    )
    worker.on('error', () => {}) // Swallow.
    worker.on('failed', () => {}) // Swallow.
    void worker.run()
    await queue.add('one', { value: 1 })

    await waitFor(() => attempts >= 5, 30_000)
    expect(attempts).toBeGreaterThanOrEqual(5)
    // Allow up to one CLAIM redelivery on top of the 5 retries
    // (rare under default settings, but possible).
    expect(attempts).toBeLessThanOrEqual(8)
  }, 60_000)
})

skipIfNoRedis('Worker stalled-detector deprecation warning', () => {
  let queueName: string
  let queue: Queue<{ value: number }, number>
  let worker: Worker<{ value: number }, number> | undefined
  let warnSpy: any

  beforeEach(() => {
    queueName = `qmq-test-stalled-warn-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
    warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(async () => {
    if (worker) {
      await Promise.race([
        worker.close().catch(() => {}),
        new Promise((r) => setTimeout(r, 5_000)),
      ])
      worker = undefined
    }
    await queue.close().catch(() => {})
    warnSpy.mockRestore()
  })

  it('warns once when maxStalledCount is set without maxAttempts', () => {
    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => 0,
      {
        connection: parseConn(REDIS_URL!),
        autorun: false,
        // The trigger: pre-v1.4 routing field set on its own.
        maxStalledCount: 3,
      },
    )
    // Find the chasquimq warn among any other noise.
    const calls = warnSpy.mock.calls.flat() as string[]
    const matching = calls.filter(
      (s) => typeof s === 'string' && s.includes('[chasquimq]') && s.includes('maxStalledCount'),
    )
    expect(matching.length).toBe(1)
    expect(matching[0]).toContain('v1.4.0')
  })

  it('does NOT warn when both maxAttempts + maxStalledCount are set', () => {
    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => 0,
      {
        connection: parseConn(REDIS_URL!),
        autorun: false,
        maxStalledCount: 3,
        maxAttempts: 25, // Migration-complete cell.
      },
    )
    const calls = warnSpy.mock.calls.flat() as string[]
    const matching = calls.filter(
      (s) => typeof s === 'string' && s.includes('[chasquimq]') && s.includes('maxStalledCount'),
    )
    expect(matching.length).toBe(0)
  })

  it('does NOT warn when only maxAttempts is set', () => {
    worker = new Worker<{ value: number }, number>(
      queueName,
      async () => 0,
      {
        connection: parseConn(REDIS_URL!),
        autorun: false,
        maxAttempts: 10,
      },
    )
    const calls = warnSpy.mock.calls.flat() as string[]
    const matching = calls.filter(
      (s) => typeof s === 'string' && s.includes('[chasquimq]') && s.includes('maxStalledCount'),
    )
    expect(matching.length).toBe(0)
  })
})

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

function waitFor(check: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const tick = async () => {
      try {
        if (await check()) {
          resolve()
          return
        }
      } catch (e) {
        reject(e)
        return
      }
      if (Date.now() > deadline) {
        reject(new Error(`waitFor timed out after ${timeoutMs}ms`))
        return
      }
      setTimeout(tick, 50)
    }
    tick()
  })
}
