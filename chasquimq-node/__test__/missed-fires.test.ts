import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Queue } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL
const skipIfNoRedis = REDIS_URL ? describe : describe.skip

skipIfNoRedis('Queue.add({ repeat, missedFires }) round-trips through Redis', () => {
  let queue: Queue<{ idx: number }>
  let queueName: string

  beforeEach(() => {
    queueName = `qmq-test-mf-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) })
  })

  afterEach(async () => {
    await queue.close().catch(() => {})
  })

  it('omitted missedFires defaults to skip and is absent from listing', async () => {
    const job = await queue.add(
      'mf-default',
      { idx: 0 },
      { repeat: { every: 60_000 } },
    )
    const listed = await queue.getRepeatableJobs()
    const meta = listed.find((m) => m.key === job.id)
    expect(meta).toBeDefined()
    // `Skip` is the engine default — `skip_serializing_if` omits it from
    // the stored spec, so the shim surfaces `missedFires` as undefined.
    expect(meta!.missedFires).toBeUndefined()
    await queue.removeRepeatableByKey(job.id)
  })

  it('fire-once round-trips through the stored spec', async () => {
    const job = await queue.add(
      'mf-fire-once',
      { idx: 0 },
      {
        repeat: {
          pattern: '0 * * * *',
          tz: 'UTC',
          missedFires: { kind: 'fire-once' },
        },
      },
    )
    const listed = await queue.getRepeatableJobs()
    const meta = listed.find((m) => m.key === job.id)
    expect(meta?.missedFires).toEqual({ kind: 'fire-once' })
    await queue.removeRepeatableByKey(job.id)
  })

  it('fire-all carries maxCatchup through to the stored spec', async () => {
    const job = await queue.add(
      'mf-fire-all',
      { idx: 0 },
      {
        repeat: {
          every: 60_000,
          missedFires: { kind: 'fire-all', maxCatchup: 17 },
        },
      },
    )
    const listed = await queue.getRepeatableJobs()
    const meta = listed.find((m) => m.key === job.id)
    expect(meta?.missedFires).toEqual({ kind: 'fire-all', maxCatchup: 17 })
    await queue.removeRepeatableByKey(job.id)
  })

  it('rejects fire-all with negative maxCatchup before any Redis write', async () => {
    await expect(
      queue.add(
        'mf-bad',
        { idx: 0 },
        {
          repeat: {
            every: 60_000,
            missedFires: { kind: 'fire-all', maxCatchup: -1 },
          },
        },
      ),
    ).rejects.toThrowError(/maxCatchup/)
    const list = await queue.getRepeatableJobs()
    expect(list.find((m) => m.jobName === 'mf-bad')).toBeUndefined()
  })

  it('rejects fire-all with maxCatchup=0 (semantically equivalent to skip)', async () => {
    await expect(
      queue.add(
        'mf-zero',
        { idx: 0 },
        {
          repeat: {
            every: 60_000,
            missedFires: { kind: 'fire-all', maxCatchup: 0 },
          },
        },
      ),
    ).rejects.toThrowError(/maxCatchup/)
    const list = await queue.getRepeatableJobs()
    expect(list.find((m) => m.jobName === 'mf-zero')).toBeUndefined()
  })

  it('rejects fire-all with non-integer maxCatchup', async () => {
    // 5.7 would silently truncate to 5 inside napi_get_value_uint32. The
    // JS-side guard catches the float before it reaches the FFI boundary.
    await expect(
      queue.add(
        'mf-frac',
        { idx: 0 },
        {
          repeat: {
            every: 60_000,
            missedFires: { kind: 'fire-all', maxCatchup: 5.7 },
          },
        },
      ),
    ).rejects.toThrowError(/maxCatchup/)
    const list = await queue.getRepeatableJobs()
    expect(list.find((m) => m.jobName === 'mf-frac')).toBeUndefined()
  })

  it('rejects missedFires passed outside `repeat` (defense in depth)', async () => {
    // TS types nest `missedFires` under `RepeatOptions`, but `as any`
    // would let the field through silently — Python's shim raises
    // `ValueError`, so mirror the rejection here at runtime.
    await expect(
      queue.add(
        'mf-orphan',
        { idx: 0 },
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        { missedFires: { kind: 'skip' } } as any,
      ),
    ).rejects.toThrowError(/missedFires is only meaningful with `repeat`/)
  })

  it('explicit { kind: "skip" } is encoded to engine default', async () => {
    const job = await queue.add(
      'mf-explicit-skip',
      { idx: 0 },
      {
        repeat: {
          every: 60_000,
          missedFires: { kind: 'skip' },
        },
      },
    )
    const listed = await queue.getRepeatableJobs()
    const meta = listed.find((m) => m.key === job.id)
    expect(meta?.missedFires).toBeUndefined()
    await queue.removeRepeatableByKey(job.id)
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
