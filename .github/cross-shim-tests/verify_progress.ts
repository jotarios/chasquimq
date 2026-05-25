// Cross-shim progress + log verifier (Node side).
//
// Reads job IDs one-per-line from JOB_IDS_FILE, then for each id asserts:
//
//   - `Queue.getJob(id).progress === EXPECT_PROGRESS`
//   - `Queue.getJobLogs(id)` deep-equals `{ logs: EXPECT_LOGS, count: EXPECT_LOGS.length }`
//
// so a regression that breaks the per-job progress STRING or log Stream
// wire format on either shim surfaces here. Exits 0 on full match, else 1.
//
// Pair with a worker run that set `EMIT_PROGRESS=1` and `STORE_RESULT=1`
// (the latter keeps the completed job discoverable by the introspector's
// result-key probe).
//
// Env vars:
//   QUEUE           — required, queue name.
//   JOB_IDS_FILE    — required, path written by the producer.
//   EXPECT_PROGRESS — optional, default '75'.
//   EXPECT_LOGS     — optional, JSON-encoded string[], default
//                      '["step 1","step 2"]'.
//   TIMEOUT_SECS    — optional, polling deadline per id (default 10).
//   REDIS_URL       — optional.

import { readFileSync } from 'node:fs'
import { Queue } from '../../chasquimq-node/dist/index.js'

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const idsFile = requireEnv('JOB_IDS_FILE')
  const expectProgress = Number(process.env.EXPECT_PROGRESS ?? '75')
  const expectLogsRaw = process.env.EXPECT_LOGS ?? '["step 1","step 2"]'
  const expectLogs = JSON.parse(expectLogsRaw) as unknown
  const timeoutSecs = Number(process.env.TIMEOUT_SECS ?? '10')
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'

  if (
    !Array.isArray(expectLogs) ||
    !expectLogs.every((s) => typeof s === 'string')
  ) {
    console.error(
      '[node-verify-progress] ERROR: EXPECT_LOGS must be a JSON array of strings',
    )
    return 1
  }

  const ids = readFileSync(idsFile, 'utf8')
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
  if (ids.length === 0) {
    console.error(`[node-verify-progress] ERROR: '${idsFile}' contains no ids`)
    return 1
  }

  const queue = new Queue(queueName, { connection: parseConn(redisUrl) })
  const progressErrors: string[] = []
  const logErrors: string[] = []
  try {
    for (const jid of ids) {
      const gotProgress = await waitForProgress(
        queue,
        jid,
        expectProgress,
        timeoutSecs * 1000,
      )
      if (gotProgress !== expectProgress) {
        progressErrors.push(
          `${jid}: progress got ${JSON.stringify(gotProgress)} want ${expectProgress}`,
        )
      }

      const { logs, count } = await waitForLogs(
        queue,
        jid,
        (expectLogs as string[]).length,
        timeoutSecs * 1000,
      )
      if (
        !arraysEqual(logs, expectLogs as string[]) ||
        count !== (expectLogs as string[]).length
      ) {
        logErrors.push(
          `${jid}: logs got (${JSON.stringify(logs)}, ${count}) ` +
            `want (${JSON.stringify(expectLogs)}, ${(expectLogs as string[]).length})`,
        )
      }
    }
  } finally {
    await queue.close()
  }

  if (progressErrors.length > 0) {
    for (const e of progressErrors.slice(0, 5)) {
      console.error(`[node-verify-progress] ERROR: ${e}`)
    }
    return 1
  }
  if (logErrors.length > 0) {
    for (const e of logErrors.slice(0, 5)) {
      console.error(`[node-verify-progress] ERROR: ${e}`)
    }
    return 1
  }

  console.log(
    `[node-verify-progress] OK — ${ids.length} jobs round-tripped ` +
      `progress=${expectProgress} logs=${JSON.stringify(expectLogs)}`,
  )
  return 0
}

async function waitForProgress(
  queue: Queue,
  jobId: string,
  expect: number,
  timeoutMs: number,
): Promise<number | null | undefined> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId)
    if (job != null && job.progress === expect) return job.progress
    await new Promise((r) => setTimeout(r, 50))
  }
  const job = await queue.getJob(jobId)
  return job?.progress
}

async function waitForLogs(
  queue: Queue,
  jobId: string,
  expectCount: number,
  timeoutMs: number,
): Promise<{ logs: string[]; count: number }> {
  const deadline = Date.now() + timeoutMs
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const res = await queue.getJobLogs(jobId)
    if (res.count >= expectCount || Date.now() >= deadline) return res
    await new Promise((r) => setTimeout(r, 50))
  }
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
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
