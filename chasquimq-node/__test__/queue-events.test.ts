import IORedis from 'ioredis'
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

  // Slice 5: subscribers see the dispatch `name` (engine `n` field) on
  // per-job events. We XADD the events stream directly to mimic the
  // engine's wire format — the real worker/producer integration path
  // for naming through the high-level shim is slice 2's scope.
  it('surfaces job dispatch name from the engine `n` field', async () => {
    const writer = new IORedis(REDIS_URL!)
    const streamKey = `{chasqui:${queueName}}:events`

    queueEvents = new QueueEvents(queueName, {
      connection: parseConn(REDIS_URL!),
      autorun: true,
      blockingTimeout: 500,
      lastEventId: '0',
    })

    type CompletedPayload = { jobId: string; name: string; attempt: number }
    type ActivePayload = { jobId: string; name: string }
    const completedSeen: CompletedPayload[] = []
    const activeSeen: ActivePayload[] = []
    queueEvents.on('completed', (payload: CompletedPayload) => {
      completedSeen.push(payload)
    })
    queueEvents.on('active', (payload: ActivePayload) => {
      activeSeen.push(payload)
    })

    try {
      await writer.xadd(
        streamKey, '*',
        'e', 'active',
        'id', 'job-named-1',
        'ts', String(Date.now()),
        'n', 'send-email',
        'attempt', '1',
      )
      await writer.xadd(
        streamKey, '*',
        'e', 'completed',
        'id', 'job-named-1',
        'ts', String(Date.now()),
        'n', 'send-email',
        'attempt', '1',
        'duration_us', '500',
      )
      // No `n` → empty name string on the subscriber side.
      await writer.xadd(
        streamKey, '*',
        'e', 'completed',
        'id', 'job-anonymous-1',
        'ts', String(Date.now()),
        'attempt', '1',
        'duration_us', '500',
      )

      const deadline = Date.now() + 5_000
      while (
        Date.now() < deadline &&
        (completedSeen.length < 2 || activeSeen.length < 1)
      ) {
        await new Promise((r) => setTimeout(r, 50))
      }

      expect(activeSeen).toContainEqual(
        expect.objectContaining({ jobId: 'job-named-1', name: 'send-email' }),
      )
      expect(completedSeen).toContainEqual(
        expect.objectContaining({ jobId: 'job-named-1', name: 'send-email' }),
      )
      // Anonymous: missing `n` surfaces as empty string, not undefined.
      expect(completedSeen).toContainEqual(
        expect.objectContaining({ jobId: 'job-anonymous-1', name: '' }),
      )
    } finally {
      await writer.quit().catch(() => {})
    }
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
