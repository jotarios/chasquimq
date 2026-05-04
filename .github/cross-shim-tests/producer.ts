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
//   TAG, JOB_NAME, REDIS_URL — optional.

import { Queue } from '../../chasquimq-node/dist/index.js'

const DELAYED_MS = 100

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const count = Number(requireEnv('COUNT'))
  const jobName = process.env.JOB_NAME ?? 'cross-shim'
  const tag = process.env.TAG ?? 'node'
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
  const mode = (process.env.MODE ?? 'immediate').toLowerCase()

  if (mode !== 'immediate' && mode !== 'delayed') {
    console.error(`[node-producer] ERROR: unknown MODE='${mode}'`)
    return 1
  }

  const queue = new Queue(queueName, { connection: parseConn(redisUrl) })
  try {
    for (let i = 0; i < count; i++) {
      if (mode === 'delayed') {
        await queue.add(jobName, { i, tag }, { delay: DELAYED_MS })
      } else {
        await queue.add(jobName, { i, tag })
      }
    }
  } finally {
    await queue.close()
  }

  console.log(
    `[node-producer] enqueued ${count} jobs to '${queueName}' ` +
      `with tag='${tag}' mode='${mode}'`,
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
