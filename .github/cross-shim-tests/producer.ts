// Cross-shim wire-format test fixture: Node producer.
//
// Mirrors `producer.py`: pushes COUNT jobs onto QUEUE with payloads
// `{ i, tag }`. The Python worker consumes the same Redis stream
// without any translation layer.
//
// Env vars:
//   QUEUE       — required, queue name.
//   COUNT       — required, number of jobs.
//   MODE        — 'immediate' (default) | 'delayed'. 'delayed' adds a 100ms
//                  delay per job to exercise the ZSET wire format.
//   JOB_NAME    — optional. When non-empty, jobs are enqueued with this name
//                  (paired with EXPECT_JOB_NAME on the worker side to assert
//                  name round-trips through the wire format).
//   JOB_IDS_FILE — optional. When set, the resolved engine-minted job IDs
//                  are written one-per-line to this path so a downstream
//                  verifier (verify_results.{py,ts}) can read them back and
//                  assert the result-backend round-trip after the worker
//                  drains.
//   TAG, REDIS_URL — optional.

import { writeFileSync } from 'node:fs'
import { Queue } from '../../chasquimq-node/dist/index.js'

const DELAYED_MS = 100

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const count = Number(requireEnv('COUNT'))
  const jobName = process.env.JOB_NAME ?? ''
  const tag = process.env.TAG ?? 'node'
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
  const mode = (process.env.MODE ?? 'immediate').toLowerCase()
  const jobIdsFile = process.env.JOB_IDS_FILE ?? ''

  if (mode !== 'immediate' && mode !== 'delayed') {
    console.error(`[node-producer] ERROR: unknown MODE='${mode}'`)
    return 1
  }

  const ids: string[] = []
  const queue = new Queue(queueName, { connection: parseConn(redisUrl) })
  try {
    for (let i = 0; i < count; i++) {
      let job
      if (mode === 'delayed') {
        job = await queue.add(jobName, { i, tag }, { delay: DELAYED_MS })
      } else {
        job = await queue.add(jobName, { i, tag })
      }
      ids.push(job.id)
    }
  } finally {
    await queue.close()
  }

  if (jobIdsFile) {
    writeFileSync(jobIdsFile, ids.map((id) => `${id}\n`).join(''), 'utf8')
    console.log(`[node-producer] wrote ${ids.length} ids to '${jobIdsFile}'`)
  }

  console.log(
    `[node-producer] enqueued ${count} jobs to '${queueName}' ` +
      `with tag='${tag}' mode='${mode}' name='${jobName}'`,
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
