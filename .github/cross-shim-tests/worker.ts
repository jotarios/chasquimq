// Cross-shim wire-format test fixture: Node worker.
//
// Mirrors `worker.py`: drains COUNT distinct jobs from QUEUE, asserts
// each payload is `{ i: int in [0, COUNT), tag: EXPECT_TAG }`, and
// exits 0 on full coverage within TIMEOUT_SECS, else 1.

import { Worker } from '../../chasquimq-node/dist/index.js'

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const count = Number(requireEnv('COUNT'))
  const expectTag = process.env.EXPECT_TAG ?? 'node'
  const timeoutSecs = Number(process.env.TIMEOUT_SECS ?? '30')
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'

  const seen = new Set<number>()
  const errors: string[] = []
  let resolveDone: () => void
  const done = new Promise<void>((r) => {
    resolveDone = r
  })

  const worker = new Worker<{ i: number; tag: string }>(
    queueName,
    async (job) => {
      const data = job.data as unknown
      if (typeof data !== 'object' || data === null) {
        errors.push(`payload not an object: ${JSON.stringify(data)}`)
        resolveDone()
        return
      }
      const { i, tag } = data as { i?: unknown; tag?: unknown }
      if (typeof i !== 'number' || !Number.isInteger(i) || i < 0 || i >= count) {
        errors.push(`i out of range: ${JSON.stringify(i)}`)
        resolveDone()
        return
      }
      if (tag !== expectTag) {
        errors.push(`tag mismatch: got ${JSON.stringify(tag)}, want '${expectTag}'`)
        resolveDone()
        return
      }
      seen.add(i)
      if (seen.size >= count) {
        resolveDone()
      }
    },
    {
      connection: parseConn(redisUrl),
      concurrency: 8,
      autorun: false,
      maxStalledCount: 1,
      drainDelay: 200,
      runScheduler: false,
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
    `[node-worker] OK — drained ${count} distinct jobs with tag='${expectTag}'`,
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
