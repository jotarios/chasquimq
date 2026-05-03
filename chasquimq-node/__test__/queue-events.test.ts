import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { QueueEvents } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('QueueEvents integration', () => {
  let queueEvents: QueueEvents | undefined
  let queueName: string

  beforeEach(() => {
    queueName = `qmq-test-qe-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
  })

  afterEach(async () => {
    if (queueEvents) {
      await queueEvents.close().catch(() => {})
      queueEvents = undefined
    }
  })

  it('waitUntilReady() resolves', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: false,
      blockingTimeout: 500,
    })
    await queueEvents.waitUntilReady()
  })

  it('close() resolves within blockingTimeout + grace', async () => {
    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
    })
    const start = Date.now()
    await queueEvents.close()
    const elapsed = Date.now() - start
    // close() awaits the in-flight XREAD BLOCK to time out. Cap is
    // blockingTimeout + 1s grace.
    expect(elapsed).toBeLessThan(2_000)
  })

  // TODO(slice-9 base): once engine slice 9 (PR #15) lands in the base
  // branch, add tests that produce a job via Queue, run a Worker, and
  // assert the QueueEvents subscriber sees `active` -> `completed` for
  // that job's id. Until then, the events stream key
  // `{chasqui:<queue>}:events` doesn't exist and XREAD blocks forever.
  it.skip('observes active -> completed for a processed job (pending slice 9)', () => {})
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
