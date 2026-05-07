// Cross-shim result-backend verifier (Node side).
//
// Reads job IDs one-per-line from JOB_IDS_FILE, calls
// `Queue.getJobResult` for each, and asserts the returned value
// deep-equals `JSON.parse(EXPECT_RESULT)`. Exits 0 on full match,
// else 1.
//
// Used after the worker has drained, so the engine's ok-result writer
// has already persisted each result key.
//
// Env vars:
//   QUEUE         — required, queue name.
//   JOB_IDS_FILE  — required, path written by the producer.
//   EXPECT_RESULT — required, JSON-encoded expected handler return value.
//   TIMEOUT_SECS  — optional, polling deadline per id (default 10).
//   REDIS_URL     — optional.

import { readFileSync } from 'node:fs'
import { Queue } from '../../chasquimq-node/dist/index.js'

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const idsFile = requireEnv('JOB_IDS_FILE')
  const expectRaw = requireEnv('EXPECT_RESULT')
  const timeoutSecs = Number(process.env.TIMEOUT_SECS ?? '10')
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'

  const expect = JSON.parse(expectRaw)

  const ids = readFileSync(idsFile, 'utf8')
    .split('\n')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
  if (ids.length === 0) {
    console.error(`[node-verify] ERROR: '${idsFile}' contains no ids`)
    return 1
  }

  const queue = new Queue(queueName, { connection: parseConn(redisUrl) })
  const misses: string[] = []
  const mismatches: string[] = []
  try {
    for (const jid of ids) {
      const got = await waitForResult(
        () => queue.getJobResult(jid),
        timeoutSecs * 1000,
      )
      if (got === undefined || got === null) {
        misses.push(jid)
        continue
      }
      if (!deepEqual(got, expect)) {
        mismatches.push(`${jid}: got ${JSON.stringify(got)} want ${JSON.stringify(expect)}`)
      }
    }
  } finally {
    await queue.close()
  }

  if (misses.length > 0) {
    for (const jid of misses.slice(0, 5)) {
      console.error(`[node-verify] ERROR: no result for id=${jid}`)
    }
    if (misses.length > 5) {
      console.error(`[node-verify] ... and ${misses.length - 5} more missing`)
    }
    return 1
  }
  if (mismatches.length > 0) {
    for (const m of mismatches.slice(0, 5)) console.error(`[node-verify] ERROR: ${m}`)
    return 1
  }

  console.log(
    `[node-verify] OK — ${ids.length} results round-tripped, expect=${JSON.stringify(expect)}`,
  )
  return 0
}

async function waitForResult<T>(
  fn: () => Promise<T | undefined | null>,
  timeoutMs: number,
): Promise<T | undefined | null> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const v = await fn()
    if (v !== undefined && v !== null) return v
    await new Promise((r) => setTimeout(r, 50))
  }
  return undefined
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b) return false
  if (a === null || b === null) return a === b
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false
    return true
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ao = a as Record<string, unknown>
    const bo = b as Record<string, unknown>
    const ak = Object.keys(ao).sort()
    const bk = Object.keys(bo).sort()
    if (ak.length !== bk.length) return false
    for (let i = 0; i < ak.length; i++) {
      if (ak[i] !== bk[i]) return false
      if (!deepEqual(ao[ak[i]], bo[bk[i]])) return false
    }
    return true
  }
  return false
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
