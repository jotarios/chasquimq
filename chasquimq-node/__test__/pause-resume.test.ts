import { afterEach, describe, expect, it } from 'vitest'
import { Queue } from '../dist/queue.js'
import { Worker } from '../dist/worker.js'
import { Consumer, Producer } from '../dist/index.js'

// Pause/resume has two surfaces:
//   - Worker.pause()/resume()/isPaused() — process-local, in-memory
//     switch on the native Consumer. Stops dispatch at the next batch
//     boundary; in-flight jobs drain; producers keep enqueueing.
//   - Queue.pause()/resume()/isPaused() — durable cross-process Redis
//     flag observed by every consumer of the queue.
// These are behavioural where loopback Redis makes that deterministic,
// and wiring-level where it doesn't (matching the repo's FFI test depth).
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)
const d = HAS_REDIS ? describe : describe.skip

function uniqueQueue(tag: string): string {
  return `pr-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  intervalMs = 25,
): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`)
    }
    await new Promise((r) => setTimeout(r, intervalMs))
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

const cleanups: Array<() => Promise<void>> = []
afterEach(async () => {
  while (cleanups.length) {
    await cleanups.pop()!().catch(() => {})
  }
})

d('Worker.pause()/resume()/isPaused() (process-local)', () => {
  it('stops dispatch while paused, producer keeps enqueueing, resume drains', async () => {
    const queueName = uniqueQueue('worker')
    const queue = new Queue(queueName, { connection: parseConn(REDIS_URL) })
    cleanups.push(() => queue.close())

    let processed = 0
    const worker = new Worker<{ n: number }, void>(
      queueName,
      async () => {
        processed++
      },
      {
        connection: parseConn(REDIS_URL),
        concurrency: 4,
        autorun: false,
        delayedEnabled: false,
        runScheduler: false,
        // Tight block so an in-flight XREADGROUP at pause time resolves
        // fast and the next batch boundary (where the gate is checked)
        // comes quickly — keeps the stabilize window deterministic.
        drainDelay: 100,
      },
    )
    cleanups.push(() => worker.close())
    void worker.run()

    for (let n = 0; n < 10; n++) await queue.add('seed', { n })
    await waitFor(() => processed >= 10, 5_000)

    expect(worker.isPaused()).toBe(false)
    await worker.pause()
    expect(worker.isPaused()).toBe(true)

    // Pause stops dispatch at the NEXT batch boundary; any job already
    // handed to a handler still drains. Wait for the count to stabilize
    // (two consecutive equal reads) before snapshotting — racing the
    // snapshot against an in-flight drain is a test bug, not an engine
    // bug (drain-on-pause is the documented semantics).
    let prev = -1
    while (prev !== processed) {
      prev = processed
      await new Promise((r) => setTimeout(r, 300))
    }
    const before = processed

    // Enqueue while paused — producer must not be blocked.
    for (let n = 100; n < 115; n++) await queue.add('while-paused', { n })
    await new Promise((r) => setTimeout(r, 600))
    expect(processed).toBe(before) // nothing NEW dispatched while paused

    worker.resume()
    expect(worker.isPaused()).toBe(false)
    await waitFor(() => processed >= before + 15, 5_000)
    expect(processed).toBeGreaterThanOrEqual(before + 15)
  }, 30_000)

  it('double-pause / double-resume are idempotent', async () => {
    const queueName = uniqueQueue('idem')
    const worker = new Worker<{ n: number }, void>(
      queueName,
      async () => {},
      { connection: parseConn(REDIS_URL), autorun: false },
    )
    cleanups.push(() => worker.close())
    await worker.pause()
    await worker.pause()
    expect(worker.isPaused()).toBe(true)
    worker.resume()
    worker.resume()
    expect(worker.isPaused()).toBe(false)
  }, 15_000)
})

d('Queue.pause()/resume()/isPaused() (cross-process durable flag)', () => {
  it('sets and clears the durable flag and a worker honours it', async () => {
    const queueName = uniqueQueue('queue')
    const queue = new Queue(queueName, { connection: parseConn(REDIS_URL) })
    cleanups.push(() => queue.close())

    expect(await queue.isPaused()).toBe(false)
    await queue.pause()
    expect(await queue.isPaused()).toBe(true)

    let processed = 0
    const worker = new Worker<{ n: number }, void>(
      queueName,
      async () => {
        processed++
      },
      {
        connection: parseConn(REDIS_URL),
        autorun: false,
        delayedEnabled: false,
        runScheduler: false,
        drainDelay: 100,
      },
    )
    cleanups.push(() => worker.close())
    void worker.run()

    for (let n = 0; n < 8; n++) await queue.add('seed', { n })
    // Durable flag is set → the worker must NOT dispatch.
    await new Promise((r) => setTimeout(r, 800))
    expect(processed).toBe(0)

    await queue.resume()
    expect(await queue.isPaused()).toBe(false)
    await waitFor(() => processed >= 8, 5_000)
    expect(processed).toBeGreaterThanOrEqual(8)
  }, 30_000)

  it('Queue.pause is idempotent', async () => {
    const queueName = uniqueQueue('qidem')
    const queue = new Queue(queueName, { connection: parseConn(REDIS_URL) })
    cleanups.push(() => queue.close())
    await queue.pause()
    await queue.pause()
    expect(await queue.isPaused()).toBe(true)
    await queue.resume()
    await queue.resume()
    expect(await queue.isPaused()).toBe(false)
  }, 15_000)
})

d('native Consumer/Producer pause surface', () => {
  it('native Consumer exposes pause/resume/isPaused', () => {
    const c = new Consumer(REDIS_URL, { queueName: uniqueQueue('nc') })
    expect(c.isPaused()).toBe(false)
    c.pause()
    expect(c.isPaused()).toBe(true)
    c.resume()
    expect(c.isPaused()).toBe(false)
  })

  it('native Producer exposes durable pause/resume/isPaused', async () => {
    const queueName = uniqueQueue('np')
    const p = await Producer.connect(REDIS_URL, { queueName })
    cleanups.push(() => p.shutdown())
    expect(await p.isPaused()).toBe(false)
    await p.pause()
    expect(await p.isPaused()).toBe(true)
    await p.resume()
    expect(await p.isPaused()).toBe(false)
  }, 15_000)
})
