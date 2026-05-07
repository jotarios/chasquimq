// Low-level NAPI binding edge tests. These exercise the Rust-side
// argument validation surface (f64→u64 helpers, kind-tagged variant
// parsing, AddOptions plumbing) by going through the native classes
// directly — NOT the high-level Queue/Worker shim. The high-level shim
// tests live in queue.test.ts / worker.test.ts and cover end-to-end
// behavior; this file pins the FFI translation contract.
//
// Coverage targets (slice 6):
//   1. f64→u64 boundary casts (NaN, Infinity, negative, MAX_SAFE_INTEGER+1)
//   2. BackoffSpec variant round-trip via addWithOptions
//   3. RepeatPattern parsing edges (cron expr, tz, every interval)
//   4. AddOptions name field round-trips through XADD's `n` field

import { beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { encode } from '@msgpack/msgpack'
import IORedis from 'ioredis'
import { Producer } from '../dist/index.js'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
// CI sets REDIS_URL; locally we run anyway against the conventional
// loopback Redis (`docker start chasquimq-bench-redis`).
const HAS_REDIS = true
const d = HAS_REDIS ? describe : describe.skip

// Each test gets its own queue name so concurrent runs (and re-runs after
// a failure) can't collide on stream / DLQ / repeatable ZSET state.
function freshQueue(): string {
  return `native-edges-${Date.now()}-${Math.floor(Math.random() * 1e9)}`
}

const samplePayload = () => Buffer.from(encode({ k: 'v' }))

d('NAPI edge: f64→u64 boundary casts', () => {
  let producer: Producer

  beforeEach(async () => {
    producer = await Producer.connect(REDIS_URL, { queueName: freshQueue() })
  })

  describe('addIn(delayMs) — i64 path', () => {
    it('rejects negative delay', async () => {
      await expect(producer.addIn(-1, samplePayload())).rejects.toThrow(/non-negative/i)
    })

    it('accepts 0 delay', async () => {
      await expect(producer.addIn(0, samplePayload())).resolves.toMatch(/.+/)
    })
  })

  describe('addAt(runAtMs) — i64 path', () => {
    it('rejects negative timestamp', async () => {
      await expect(producer.addAt(-1, samplePayload())).rejects.toThrow(/non-negative/i)
    })

    it('accepts now', async () => {
      await expect(producer.addAt(Date.now(), samplePayload())).resolves.toMatch(/.+/)
    })
  })

  describe('RepeatPattern.intervalMs — f64_to_u64 path', () => {
    const baseSpec = { jobName: 'edge-job', payload: samplePayload() }

    it('rejects NaN', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          pattern: { kind: 'every', intervalMs: Number.NaN },
        }),
      ).rejects.toThrow(/intervalMs|finite|out of range/i)
    })

    it('rejects +Infinity', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          pattern: { kind: 'every', intervalMs: Number.POSITIVE_INFINITY },
        }),
      ).rejects.toThrow(/intervalMs|finite|out of range/i)
    })

    it('rejects negative', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          pattern: { kind: 'every', intervalMs: -1 },
        }),
      ).rejects.toThrow(/intervalMs|> 0/i)
    })

    it('rejects 0 (would hot-loop the scheduler)', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          pattern: { kind: 'every', intervalMs: 0 },
        }),
      ).rejects.toThrow(/intervalMs|> 0/i)
    })

    it('accepts a sane positive interval', async () => {
      const key = await producer.upsertRepeatable({
        ...baseSpec,
        pattern: { kind: 'every', intervalMs: 60_000 },
      })
      expect(key).toContain('edge-job')
      await producer.removeRepeatable(key)
    })

    it('truncates non-integer floats (current contract: f64_to_u64 silent truncate)', async () => {
      // Documents current behavior. f64_to_u64 just does `n as u64` after
      // the finite/non-negative/in-range guard, so 60_000.7 round-trips as
      // 60_000. If we ever switch to reject-non-integer, update this test.
      const key = await producer.upsertRepeatable({
        ...baseSpec,
        pattern: { kind: 'every', intervalMs: 60_000.7 },
      })
      const list = await producer.listRepeatable(10)
      const meta = list.find((m) => m.key === key)
      expect(meta).toBeDefined()
      expect(meta!.pattern.kind).toBe('every')
      expect(meta!.pattern.intervalMs).toBe(60_000)
      await producer.removeRepeatable(key)
    })
  })

  describe('RepeatableSpec.{limit,startAfterMs,endBeforeMs} — f64_to_u64 path', () => {
    const everyPattern = { kind: 'every', intervalMs: 60_000 }

    it('rejects negative limit', async () => {
      await expect(
        producer.upsertRepeatable({
          jobName: 'edge-job',
          payload: samplePayload(),
          pattern: everyPattern,
          limit: -1,
        }),
      ).rejects.toThrow(/limit|out of range/i)
    })

    it('rejects NaN startAfterMs', async () => {
      await expect(
        producer.upsertRepeatable({
          jobName: 'edge-job',
          payload: samplePayload(),
          pattern: everyPattern,
          startAfterMs: Number.NaN,
        }),
      ).rejects.toThrow(/startAfterMs|out of range/i)
    })

    it('rejects +Infinity endBeforeMs', async () => {
      await expect(
        producer.upsertRepeatable({
          jobName: 'edge-job',
          payload: samplePayload(),
          pattern: everyPattern,
          endBeforeMs: Number.POSITIVE_INFINITY,
        }),
      ).rejects.toThrow(/endBeforeMs|out of range/i)
    })

    it('accepts MAX_SAFE_INTEGER (boundary, valid)', async () => {
      const key = await producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: everyPattern,
        endBeforeMs: Number.MAX_SAFE_INTEGER,
      })
      expect(key).toContain('edge-job')
      await producer.removeRepeatable(key)
    })

    it('accepts MAX_SAFE_INTEGER+1 (above 2^53; documents current bound)', async () => {
      // f64_to_u64 caps on `u64::MAX as f64`, not 2^53 — so values above
      // Number.MAX_SAFE_INTEGER pass the guard. Mantissa aliasing means
      // adjacent integers in this range collapse to the same u64. The
      // engine clamps `endBeforeMs` to a sane window in practice; this
      // test pins the binding's truncation contract.
      const key = await producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: everyPattern,
        endBeforeMs: Number.MAX_SAFE_INTEGER + 1,
      })
      expect(key).toContain('edge-job')
      await producer.removeRepeatable(key)
    })
  })

  describe('MissedFiresPolicy — variant validation', () => {
    const baseSpec = {
      jobName: 'edge-mf',
      payload: samplePayload(),
      pattern: { kind: 'every', intervalMs: 60_000 },
    }

    it('rejects fire-all without maxCatchup', async () => {
      await expect(
        producer.upsertRepeatable({ ...baseSpec, missedFires: { kind: 'fire-all' } }),
      ).rejects.toThrow(/maxCatchup/i)
    })

    it('rejects fire-all with maxCatchup=0', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          missedFires: { kind: 'fire-all', maxCatchup: 0 },
        }),
      ).rejects.toThrow(/maxCatchup/i)
    })

    it('rejects unknown kind', async () => {
      await expect(
        producer.upsertRepeatable({
          ...baseSpec,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          missedFires: { kind: 'bogus' as any },
        }),
      ).rejects.toThrow(/missed-fires|unknown/i)
    })

    it('accepts skip / fire-once / fire-all w/ maxCatchup', async () => {
      const k1 = await producer.upsertRepeatable({
        ...baseSpec,
        missedFires: { kind: 'skip' },
      })
      await producer.removeRepeatable(k1)
      const k2 = await producer.upsertRepeatable({
        ...baseSpec,
        missedFires: { kind: 'fire-once' },
      })
      await producer.removeRepeatable(k2)
      const k3 = await producer.upsertRepeatable({
        ...baseSpec,
        missedFires: { kind: 'fire-all', maxCatchup: 5 },
      })
      await producer.removeRepeatable(k3)
    })
  })
})

d('NAPI edge: BackoffSpec round-trip via addWithOptions', () => {
  let producer: Producer

  beforeEach(async () => {
    producer = await Producer.connect(REDIS_URL, { queueName: freshQueue() })
  })

  it('accepts kind="fixed" with delayMs', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: { maxAttempts: 3, backoff: { kind: 'fixed', delayMs: 5_000 } },
      }),
    ).resolves.toMatch(/.+/)
  })

  it('accepts kind="exponential" with full set of fields', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: {
          maxAttempts: 5,
          backoff: {
            kind: 'exponential',
            delayMs: 1_000,
            multiplier: 2.0,
            maxDelayMs: 60_000,
            jitterMs: 100,
          },
        },
      }),
    ).resolves.toMatch(/.+/)
  })

  it('accepts kind="exponential" with only delayMs (other fields default at consumer)', async () => {
    // The binding only validates the present fields; missing optional
    // fields fall back to RetryConfig defaults at the consumer. This
    // mirrors the "let me supply just delayMs and inherit the rest"
    // ergonomics of BullMQ JobsOptions.
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: { backoff: { kind: 'exponential', delayMs: 250 } },
      }),
    ).resolves.toMatch(/.+/)
  })

  it('rejects unknown kind verbatim instead of routing through BackoffKind::Unknown', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          backoff: { kind: 'linear' as any, delayMs: 1_000 },
        },
      }),
    ).rejects.toThrow(/unknown backoff kind|expected.*fixed.*exponential/i)
  })

  it('rejects negative delayMs', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: { backoff: { kind: 'fixed', delayMs: -100 } },
      }),
    ).rejects.toThrow(/backoff\.delayMs|out of range/i)
  })

  it('rejects NaN delayMs', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: { backoff: { kind: 'fixed', delayMs: Number.NaN } },
      }),
    ).rejects.toThrow(/backoff\.delayMs|out of range/i)
  })

  it('rejects +Infinity maxDelayMs', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: {
          backoff: {
            kind: 'exponential',
            delayMs: 1_000,
            maxDelayMs: Number.POSITIVE_INFINITY,
          },
        },
      }),
    ).rejects.toThrow(/backoff\.maxDelayMs|out of range/i)
  })

  it('rejects negative jitterMs', async () => {
    await expect(
      producer.addWithOptions(samplePayload(), {
        retry: {
          backoff: { kind: 'exponential', delayMs: 1_000, jitterMs: -50 },
        },
      }),
    ).rejects.toThrow(/backoff\.jitterMs|out of range/i)
  })

  it('AddOptions without retry succeeds (retry is fully optional)', async () => {
    await expect(producer.addWithOptions(samplePayload(), {})).resolves.toMatch(/.+/)
  })
})

d('NAPI edge: RepeatPattern parsing', () => {
  let producer: Producer

  beforeEach(async () => {
    producer = await Producer.connect(REDIS_URL, { queueName: freshQueue() })
  })

  it('rejects unknown pattern kind', async () => {
    await expect(
      producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        pattern: { kind: 'hourly' as any },
      }),
    ).rejects.toThrow(/unknown pattern kind|cron.*every/i)
  })

  it('rejects cron without expression', async () => {
    await expect(
      producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: { kind: 'cron' },
      }),
    ).rejects.toThrow(/expression/i)
  })

  it('rejects every without intervalMs', async () => {
    await expect(
      producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: { kind: 'every' },
      }),
    ).rejects.toThrow(/intervalMs/i)
  })

  it('rejects invalid cron expression at upsert time', async () => {
    await expect(
      producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: { kind: 'cron', expression: 'not-a-cron' },
      }),
    ).rejects.toThrow(/cron|expression|parse/i)
  })

  it('rejects unknown timezone at upsert time', async () => {
    await expect(
      producer.upsertRepeatable({
        jobName: 'edge-job',
        payload: samplePayload(),
        pattern: { kind: 'cron', expression: '0 2 * * *', tz: 'Mars/Phobos' },
      }),
    ).rejects.toThrow(/timezone|tz|unknown/i)
  })

  it('accepts a valid IANA timezone and round-trips via listRepeatable', async () => {
    const key = await producer.upsertRepeatable({
      jobName: 'edge-job',
      payload: samplePayload(),
      pattern: { kind: 'cron', expression: '0 2 * * *', tz: 'America/New_York' },
    })
    const list = await producer.listRepeatable(10)
    const meta = list.find((m) => m.key === key)
    expect(meta).toBeDefined()
    expect(meta!.pattern.kind).toBe('cron')
    expect(meta!.pattern.expression).toBe('0 2 * * *')
    expect(meta!.pattern.tz).toBe('America/New_York')
    await producer.removeRepeatable(key)
  })

  it('accepts UTC tz and a fixed-offset tz', async () => {
    const k1 = await producer.upsertRepeatable({
      jobName: 'edge-utc',
      payload: samplePayload(),
      pattern: { kind: 'cron', expression: '0 0 * * *', tz: 'UTC' },
    })
    expect(k1).toContain('edge-utc')
    await producer.removeRepeatable(k1)

    const k2 = await producer.upsertRepeatable({
      jobName: 'edge-fixed',
      payload: samplePayload(),
      pattern: { kind: 'cron', expression: '0 0 * * *', tz: '+05:30' },
    })
    expect(k2).toContain('edge-fixed')
    await producer.removeRepeatable(k2)
  })
})

d('NAPI edge: AddOptions.name round-trips through XADD `n` field', () => {
  let producer: Producer
  let raw: IORedis

  beforeAll(() => {
    raw = new IORedis(REDIS_URL, { lazyConnect: true })
  })

  beforeEach(async () => {
    producer = await Producer.connect(REDIS_URL, { queueName: freshQueue() })
  })

  it('addWithOptions({name}) writes the `n` field on the stream entry', async () => {
    const id = await producer.addWithOptions(samplePayload(), { name: 'process-image' })
    expect(id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/) // ULID

    // XRANGE the stream and confirm the entry has both `d` (payload) and
    // `n` (name) fields. ioredis returns entries as
    // `[ [streamId, [k1, v1, k2, v2, ...]], ... ]`.
    const streamKey = producer.streamKey()
    const entries = (await raw.xrange(streamKey, '-', '+')) as Array<[string, string[]]>
    expect(entries.length).toBeGreaterThanOrEqual(1)
    const fields = entries[0]![1]
    const fieldMap = new Map<string, string | Buffer>()
    for (let i = 0; i < fields.length; i += 2) {
      fieldMap.set(fields[i]!, fields[i + 1]!)
    }
    expect(fieldMap.has('d')).toBe(true)
    expect(fieldMap.get('n')).toBe('process-image')
  })

  it('addWithOptions without name omits the `n` field entirely (legacy producers / no-op default)', async () => {
    await producer.addWithOptions(samplePayload(), {})

    const streamKey = producer.streamKey()
    const entries = (await raw.xrange(streamKey, '-', '+')) as Array<[string, string[]]>
    expect(entries.length).toBeGreaterThanOrEqual(1)
    const fields = entries[0]![1]
    const fieldKeys = new Set<string>()
    for (let i = 0; i < fields.length; i += 2) {
      fieldKeys.add(fields[i]!)
    }
    // `d` is always present; `n` must be absent when no name was supplied
    // — keeps the wire shape identical to pre-name-on-wire producers, so
    // legacy consumers reading new producers' entries don't see a phantom
    // empty `n`.
    expect(fieldKeys.has('d')).toBe(true)
    expect(fieldKeys.has('n')).toBe(false)
  })

  it('rejects a name longer than 256 bytes', async () => {
    const tooLong = 'x'.repeat(257)
    await expect(
      producer.addWithOptions(samplePayload(), { name: tooLong }),
    ).rejects.toThrow(/256|name/i)
  })

  it('accepts a name at exactly 256 bytes (boundary, valid)', async () => {
    const ok = 'x'.repeat(256)
    await expect(
      producer.addWithOptions(samplePayload(), { name: ok }),
    ).resolves.toMatch(/.+/)
  })

  it('addBulkNamed writes per-entry `n` fields', async () => {
    const ids = await producer.addBulkNamed([
      { name: 'job-a', payload: samplePayload() },
      { name: 'job-b', payload: samplePayload() },
    ])
    expect(ids).toHaveLength(2)

    const streamKey = producer.streamKey()
    const entries = (await raw.xrange(streamKey, '-', '+')) as Array<[string, string[]]>
    expect(entries.length).toBe(2)

    const namesSeen = entries
      .map(([, fields]) => {
        for (let i = 0; i < fields.length; i += 2) {
          if (fields[i] === 'n') return fields[i + 1]
        }
        return null
      })
      .filter(Boolean)
      .sort()
    expect(namesSeen).toEqual(['job-a', 'job-b'])
  })
})
