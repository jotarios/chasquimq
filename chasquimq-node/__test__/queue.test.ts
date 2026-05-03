import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { encode } from '@msgpack/msgpack'
import { Queue, NotSupportedError } from '../dist/index.js'
import { NativeProducer, NativeConsumer } from '../index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Queue integration', () => {
  let queue: Queue<{ hello: string }>
  let queueName: string

  beforeEach(() => {
    queueName = `qmq-test-q-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    await queue.close().catch(() => {})
  })

  it('add() returns a Job with msgpack-encoded payload', async () => {
    const job = await queue.add('greet', { hello: 'world' })
    expect(job.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/) // ULID
    expect(job.data).toEqual({ hello: 'world' })
    expect(job.opts).toBeDefined()
    expect(job.timestamp).toBeGreaterThan(Date.now() - 60_000)
  })

  it('add() with delay returns immediately and schedules the job', async () => {
    const job = await queue.add('delayed', { hello: 'later' }, { delay: 100 })
    expect(job.delay).toBe(100)
    expect(job.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
  })

  it('add() with jobId routes through addWithId', async () => {
    const customId = `custom-${Date.now()}`
    const job = await queue.add('custom', { hello: 'id' }, { jobId: customId })
    expect(job.id).toBe(customId)
  })

  it('addBulk() with all-simple entries uses fast path', async () => {
    const jobs = await queue.addBulk([
      { name: 'a', data: { hello: 'a' } },
      { name: 'b', data: { hello: 'b' } },
      { name: 'c', data: { hello: 'c' } },
    ])
    expect(jobs).toHaveLength(3)
    for (const j of jobs) {
      expect(j.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
    }
  })

  it('addBulk() with mixed delay falls back to per-entry add', async () => {
    const jobs = await queue.addBulk([
      { name: 'a', data: { hello: 'a' } },
      { name: 'b', data: { hello: 'b' }, opts: { delay: 50 } },
    ])
    expect(jobs).toHaveLength(2)
  })

  it('add() with repeat option throws NotSupportedError', async () => {
    await expect(
      queue.add('repeating', { hello: 'r' }, { repeat: { every: 1000 } }),
    ).rejects.toBeInstanceOf(NotSupportedError)
  })

  it('add() with parent option throws NotSupportedError', async () => {
    await expect(
      queue.add('child', { hello: 'c' }, { parent: { id: 'p1', queue: 'parent-q' } }),
    ).rejects.toBeInstanceOf(NotSupportedError)
  })

  it('getJob() throws NotSupportedError in v1', async () => {
    await expect(queue.getJob('any')).rejects.toBeInstanceOf(NotSupportedError)
  })

  it('pause() / resume() throw NotSupportedError in v1', async () => {
    await expect(queue.pause()).rejects.toBeInstanceOf(NotSupportedError)
    await expect(queue.resume()).rejects.toBeInstanceOf(NotSupportedError)
  })

  it('isPaused() returns false in v1', async () => {
    expect(await queue.isPaused()).toBe(false)
  })

  it('addInBulk schedules 100 delayed jobs that all fire', async () => {
    const N = 100
    const localQueue = `qmq-bulk-d-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const producer = await NativeProducer.connect(REDIS_URL!, { queueName: localQueue })

    const payloads = Array.from({ length: N }, (_, i) =>
      Buffer.from(encode({ idx: i })),
    )
    const ids = await producer.addInBulk(500, payloads)
    expect(ids).toHaveLength(N)

    let counter = 0
    const consumer = new NativeConsumer(REDIS_URL!, {
      queueName: localQueue,
      group: 'g',
      consumerId: `c-${process.pid}`,
      concurrency: 8,
      blockMs: 100,
      delayedEnabled: true,
    })
    const runP = consumer.run(async () => {
      counter++
    })

    // Poll for completion: delay=500ms + tolerance.
    const start = Date.now()
    while (counter < N && Date.now() - start < 10_000) {
      await new Promise((r) => setTimeout(r, 50))
    }
    consumer.shutdown()
    await Promise.allSettled([runP])

    expect(counter).toBe(N)
  }, 30_000)

  // The native binding does not currently expose `add_in_bulk_with_ids`.
  // The engine supports it (chasquimq/src/producer/mod.rs:722), but the
  // NAPI surface in chasquimq-node/src/producer.rs only wires up
  // `add_in_bulk` (no -with-ids variant). Adding it would require Rust
  // changes, which are out of scope for this PR.
  it.skip('addInBulkWithIds schedules N delayed jobs with stable IDs (blocked: not exposed in NAPI binding)', () => {})

  it('remove() of a delayed job by id cancels it', async () => {
    const job = await queue.add(
      'cancellable',
      { hello: 'cx' },
      { jobId: `cx-${Date.now()}`, delay: 60_000 },
    )
    const removed = await queue.remove(job.id)
    expect(removed).toBe(1)
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
