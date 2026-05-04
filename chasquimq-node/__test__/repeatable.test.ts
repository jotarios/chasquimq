import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Queue, Worker } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Queue repeatable / cron jobs (engine slice 10)', () => {
  let queue: Queue<{ idx: number }>
  let worker: Worker<{ idx: number }, void> | undefined
  let queueName: string

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

  it('upserts a cron repeatable spec and lists it', async () => {
    const job = await queue.add(
      'cron-job',
      { idx: 0 },
      { repeat: { pattern: '* * * * *', tz: 'UTC' } },
    )
    expect(job.id).toContain('cron-job::cron:* * * * *:UTC')

    const list = await queue.getRepeatableJobs()
    expect(list).toHaveLength(1)
    expect(list[0]!).toMatchObject({
      key: job.id,
      jobName: 'cron-job',
      patternKind: 'cron',
      pattern: '* * * * *',
      tz: 'UTC',
    })
    expect(list[0]!.nextFireMs).toBeGreaterThan(Date.now())

    // Clean up so left-over Redis specs don't accumulate across runs.
    await queue.removeRepeatableByKey(job.id)
  })

  it('upserts an every spec and the worker fires it', async () => {
    let fires = 0
    worker = new Worker<{ idx: number }, void>(
      queueName,
      async () => {
        fires++
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        // Keep the scheduler tick tight so 200ms intervals fire within the
        // test timeout without timing-induced flakes.
        schedulerTickMs: 50,
      },
    )
    void worker.run()

    await queue.add('every-job', { idx: 0 }, { repeat: { every: 200 } })

    await waitFor(() => fires >= 2, 5_000)
    expect(fires).toBeGreaterThanOrEqual(2)
  }, 10_000)

  it('cron with America/New_York fires the worker', async () => {
    let fires = 0
    worker = new Worker<{ idx: number }, void>(
      queueName,
      async () => {
        fires++
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        schedulerTickMs: 50,
      },
    )
    void worker.run()

    // 6-field cron: every second. Tests the IANA tz path through the NAPI
    // binding; the load-bearing DST-aware behavior itself is unit-tested
    // in the engine — here we just need to confirm the timezone name
    // survives the FFI hop.
    await queue.add(
      'tz-job',
      { idx: 0 },
      { repeat: { pattern: '* * * * * *', tz: 'America/New_York' } },
    )

    await waitFor(() => fires >= 1, 5_000)
    expect(fires).toBeGreaterThanOrEqual(1)
  }, 10_000)

  it('removeRepeatableByKey returns true and stops fires', async () => {
    let fires = 0
    worker = new Worker<{ idx: number }, void>(
      queueName,
      async () => {
        fires++
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        schedulerTickMs: 50,
      },
    )
    void worker.run()

    const job = await queue.add('cancellable', { idx: 0 }, { repeat: { every: 200 } })
    // Wait for at least one fire so we can prove the spec was alive.
    await waitFor(() => fires >= 1, 5_000)
    const firesBeforeRemove = fires

    const removed = await queue.removeRepeatableByKey(job.id)
    expect(removed).toBe(true)

    // Wait long enough for at least 3 more potential fires; assert none
    // happen. There's an unavoidable race where a tick that already pulled
    // the spec but hasn't dispatched yet may slip through, so allow at
    // most one more fire as tolerance.
    await new Promise((r) => setTimeout(r, 800))
    expect(fires - firesBeforeRemove).toBeLessThanOrEqual(1)

    // The spec is gone from the listing.
    const list = await queue.getRepeatableJobs()
    expect(list.find((m) => m.key === job.id)).toBeUndefined()
  }, 10_000)

  it('removeRepeatableByKey returns false for an unknown key', async () => {
    const removed = await queue.removeRepeatableByKey('does-not-exist')
    expect(removed).toBe(false)
  })

  it('add() with both pattern and every throws a clear error', async () => {
    await expect(
      queue.add(
        'bad',
        { idx: 0 },
        { repeat: { pattern: '* * * * *', every: 1000 } },
      ),
    ).rejects.toThrowError(/either `pattern` .* or `every` .* not both/)
  })

  it('repeatJobKey survives the FFI hop and is returned as-is', async () => {
    const explicitKey = `my-stable-${Date.now()}`
    const job = await queue.add(
      'stable-job',
      { idx: 0 },
      { repeat: { every: 60_000 }, repeatJobKey: explicitKey },
    )
    expect(job.id).toBe(explicitKey)
    await queue.removeRepeatableByKey(explicitKey)
  })

  it('Worker.runScheduler === false disables cron fires', async () => {
    // Architectural contract for this PR: Worker auto-spawns an embedded
    // Scheduler by default; opt-out via `runScheduler: false`. This test
    // pins the opt-out path — no scheduler should mean no fires, even
    // with a tight 200ms cadence and a 1.5s observation window.
    let fires = 0
    worker = new Worker<{ idx: number }, void>(
      queueName,
      async () => {
        fires++
      },
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        runScheduler: false,
      },
    )
    void worker.run()

    const job = await queue.add(
      'no-scheduler-job',
      { idx: 0 },
      { repeat: { every: 200 } },
    )

    // Wait 1.5s — long enough for ~7 fires if a scheduler were running.
    await new Promise((r) => setTimeout(r, 1_500))
    // Deterministic: with no scheduler attached and no other fires source,
    // the worker handler MUST never have been invoked. No tolerance.
    expect(fires).toBe(0)

    await queue.removeRepeatableByKey(job.id)
  }, 10_000)
})

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
