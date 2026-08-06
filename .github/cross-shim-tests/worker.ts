// Cross-shim wire-format test fixture: Node worker.
//
// Mirrors `worker.py`: drains COUNT distinct jobs from QUEUE, asserts
// each payload is `{ i: int in [0, COUNT), tag: EXPECT_TAG }`, and
// exits 0 on full coverage within TIMEOUT_SECS, else 1.
//
// EXPECT_JOB_NAME — optional. When non-empty, the handler asserts
// `job.name === EXPECT_JOB_NAME` so a regression that drops `name`
// on either shim's wire path is caught here.
//
// STORE_RESULT  — optional. When '1', the worker enables `storeResults`
//                 so the engine persists each handler's return value at
//                 `{chasqui:<QUEUE>}:result:<jobId>` for the verifier to
//                 read back. Default off.
// RESULT_VALUE  — optional, JSON-encoded. When set, the handler returns
//                 `JSON.parse(RESULT_VALUE)` instead of `undefined`.
// EMIT_PROGRESS — optional. When '1' the handler calls
//                 `job.updateProgress(75)` and appends two log lines
//                 ('step 1', 'step 2') before returning so a
//                 `verify_progress` step in the opposite shim can read
//                 them back via `Queue.getJob` / `Queue.getJobLogs`.
//                 Pair with `STORE_RESULT=1` to keep completed jobs
//                 discoverable by the introspector.
//
// LIMITER_MAX          — optional. When set (with LIMITER_DURATION_MS), the
//                        worker enables a global per-queue rate limiter of
//                        `max` jobs per `duration` ms window (shared Redis
//                        bucket). Used by the rate-limit cross-shim phase to
//                        prove the Node + Python FFI paths share ONE bucket.
// LIMITER_DURATION_MS  — optional. Window length in ms; required when
//                        LIMITER_MAX is set.

import { Worker } from '../../chasquimq-node/dist/index.js'

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const count = Number(requireEnv('COUNT'))
  const expectTag = process.env.EXPECT_TAG ?? 'node'
  const expectJobName = process.env.EXPECT_JOB_NAME ?? ''
  const timeoutSecs = Number(process.env.TIMEOUT_SECS ?? '30')
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
  const storeResults = process.env.STORE_RESULT === '1'
  const resultValueRaw = process.env.RESULT_VALUE ?? ''
  const resultValue: unknown = resultValueRaw
    ? JSON.parse(resultValueRaw)
    : undefined
  const emitProgress = process.env.EMIT_PROGRESS === '1'
  const limiterMaxRaw = process.env.LIMITER_MAX ?? ''
  const limiterDurationRaw = process.env.LIMITER_DURATION_MS ?? ''
  const limiter = limiterMaxRaw
    ? { max: Number(limiterMaxRaw), duration: Number(limiterDurationRaw) }
    : undefined

  const seen = new Set<number>()
  const errors: string[] = []
  let resolveDone: () => void
  const done = new Promise<void>((r) => {
    resolveDone = r
  })

  const worker = new Worker<{ i: number; tag: string }, unknown>(
    queueName,
    async (job) => {
      const data = job.data as unknown
      if (typeof data !== 'object' || data === null) {
        errors.push(`payload not an object: ${JSON.stringify(data)}`)
        resolveDone()
        return resultValue
      }
      const { i, tag } = data as { i?: unknown; tag?: unknown }
      if (typeof i !== 'number' || !Number.isInteger(i) || i < 0 || i >= count) {
        errors.push(`i out of range: ${JSON.stringify(i)}`)
        resolveDone()
        return resultValue
      }
      if (tag !== expectTag) {
        errors.push(`tag mismatch: got ${JSON.stringify(tag)}, want '${expectTag}'`)
        resolveDone()
        return resultValue
      }
      if (expectJobName && job.name !== expectJobName) {
        errors.push(`name mismatch: got '${job.name}', want '${expectJobName}'`)
        resolveDone()
        return resultValue
      }
      if (emitProgress) {
        try {
          await job.updateProgress(75)
          await job.log('step 1')
          await job.log('step 2')
        } catch (err) {
          errors.push(`emitProgress failed: ${(err as Error).message}`)
          resolveDone()
          return resultValue
        }
      }
      seen.add(i)
      if (seen.size >= count) {
        resolveDone()
      }
      return resultValue
    },
    {
      connection: parseConn(redisUrl),
      concurrency: 8,
      autorun: false,
      maxStalledCount: 1,
      drainDelay: 200,
      runScheduler: false,
      storeResults,
      limiter,
    },
  )

  void worker.run()

  let timedOut = false
  const timer = setTimeout(() => {
    timedOut = true
    resolveDone!()
  }, timeoutSecs * 1000)

  try {
    await done
  } finally {
    clearTimeout(timer)
    await worker.close().catch(() => {})
  }

  if (timedOut && seen.size < count) {
    console.error(
      `[node-worker] TIMEOUT after ${timeoutSecs}s — saw ${seen.size}/${count}`,
    )
    return 1
  }
  if (errors.length > 0) {
    for (const e of errors) console.error(`[node-worker] ERROR: ${e}`)
    return 1
  }
  if (seen.size !== count) {
    const missing = []
    for (let i = 0; i < count && missing.length < 10; i++) {
      if (!seen.has(i)) missing.push(i)
    }
    console.error(
      `[node-worker] coverage gap: saw ${seen.size}/${count} (missing: ${missing.join(',')}...)`,
    )
    return 1
  }

  console.log(
    `[node-worker] OK — drained ${count} distinct jobs with tag='${expectTag}' name='${expectJobName}'`,
  )
  return 0
}

function requireEnv(name: string): string {
  const v = process.env[name]
  if (v == null || v === '') {
    throw new Error(`missing required env var ${name}`)
  }
  return v
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

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err)
    process.exit(1)
  },
)
