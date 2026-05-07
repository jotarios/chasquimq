import IORedis from 'ioredis'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Queue } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Queue.add jobId + addUnique', () => {
  let queue: Queue<{ k: string }>
  let queueName: string
  let redis: IORedis

  beforeEach(() => {
    queueName = `qmq-unique-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
    redis = new IORedis(REDIS_URL!)
  })

  afterEach(async () => {
    await queue.close().catch(() => {})
    // Sweep all keys for this queue's hash tag.
    const keys = await redis.keys(`{chasqui:${queueName}}*`)
    if (keys.length > 0) await redis.del(keys)
    await redis.quit().catch(() => {})
  })

  it('add() with jobId on the delayed path is strictly idempotent', async () => {
    const id = `dup-${Date.now()}`
    const a = await queue.add('job', { k: 'a' }, { jobId: id, delay: 60_000 })
    const b = await queue.add('job', { k: 'b' }, { jobId: id, delay: 60_000 })
    expect(a.id).toBe(id)
    expect(b.id).toBe(id)
    // Only the first call's payload landed in the delayed ZSET.
    const zcard = await redis.zcard(`{chasqui:${queueName}}:delayed`)
    expect(zcard).toBe(1)
  })

  it('add() with jobId on the immediate path is idempotent within a single producer (XADD IDMP)', async () => {
    const id = `dup-imm-${Date.now()}`
    const a = await queue.add('job', { k: 'a' }, { jobId: id })
    const b = await queue.add('job', { k: 'b' }, { jobId: id })
    expect(a.id).toBe(id)
    expect(b.id).toBe(id)
    // Single stream entry — Redis 8.6 `XADD IDMP <producer_id> <jobId>`
    // gates the second write at the wire layer. Note: scoped per producer
    // instance. Two distinct Queue/Producer instances with different
    // producer IDs would both succeed; for cross-process idempotent
    // enqueue, prefer the delayed path (true Redis-key SET NX EX dedup).
    const xlen = await redis.xlen(`{chasqui:${queueName}}:stream`)
    expect(xlen).toBe(1)
  })

  it('addUnique() throws TypeError when jobId is missing', async () => {
    await expect(queue.addUnique('job', { k: 'x' })).rejects.toBeInstanceOf(
      TypeError,
    )
    await expect(
      queue.addUnique('job', { k: 'x' }, {}),
    ).rejects.toBeInstanceOf(TypeError)
    await expect(
      queue.addUnique('job', { k: 'x' }, { jobId: '' }),
    ).rejects.toBeInstanceOf(TypeError)
  })

  it('addUnique() with delay is strictly idempotent — second call is a no-op', async () => {
    const id = `unique-${Date.now()}`
    const a = await queue.addUnique('job', { k: 'a' }, { jobId: id, delay: 60_000 })
    const b = await queue.addUnique('job', { k: 'b' }, { jobId: id, delay: 60_000 })
    expect(a.id).toBe(id)
    expect(b.id).toBe(id)
    const zcard = await redis.zcard(`{chasqui:${queueName}}:delayed`)
    expect(zcard).toBe(1)
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
