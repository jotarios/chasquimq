// Cross-shim rate-limiter verifier (Node side).
//
// Runs a rate-LIMITED worker against QUEUE for a fixed observation window
// and asserts the number of jobs drained queue-wide stays BOUNDED by the
// shared per-queue token bucket. Exits 0 when the observed drained count is
// within the expected ceiling, else 1.
//
// The strongest cross-shim proof: run this Node limited worker alongside a
// Python limited worker (worker.py with LIMITER_MAX) on the SAME queue.
// Both FFI paths land on the same engine `ConsumerConfig.rate_limit` and
// draw from ONE Redis bucket, so the COMBINED drained count over the window
// is bounded by roughly the single limiter's cap — not 2x.
//
// Env vars:
//   QUEUE               — required, queue name (already seeded by a producer).
//   LIMITER_MAX         — required, tokens per window (the shared bucket cap).
//   LIMITER_DURATION_MS — required, window length in ms.
//   OBSERVE_WINDOWS     — optional, number of windows to observe (default 2).
//   EXPECT_MAX          — required, upper bound on total jobs drained
//                          (queue-wide) within the observation window.
//   EXPECT_MIN          — optional, lower bound (default 1).
//   REDIS_URL           — optional.

import { Queue } from '../../chasquimq-node/dist/index.js'
import { Worker } from '../../chasquimq-node/dist/index.js'

async function main(): Promise<number> {
  const queueName = requireEnv('QUEUE')
  const limiterMax = Number(requireEnv('LIMITER_MAX'))
  const limiterDurationMs = Number(requireEnv('LIMITER_DURATION_MS'))
  const observeWindows = Number(process.env.OBSERVE_WINDOWS ?? '2')
  const expectMax = Number(requireEnv('EXPECT_MAX'))
  const expectMin = Number(process.env.EXPECT_MIN ?? '1')
  const redisUrl = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'

  // Snapshot pending stream depth before observing so we can compute the
  // queue-wide drained count = before - after (bounded by the shared
  // limiter regardless of how many workers drain it).
  const countsQueue = new Queue(queueName, { connection: parseConn(redisUrl) })
  let beforeWaiting: number
  try {
    const before = await countsQueue.getJobCounts()
    beforeWaiting = (before.waiting ?? 0) + (before.active ?? 0)
  } finally {
    await countsQueue.close()
  }

  let localCompleted = 0
  const worker = new Worker<{ n: number }, void>(
    queueName,
    async () => {
      localCompleted++
    },
    {
      connection: parseConn(redisUrl),
      concurrency: 8,
      autorun: false,
      drainDelay: 100,
      delayedEnabled: false,
      runScheduler: false,
      stalledDetectorEnabled: false,
      limiter: { max: limiterMax, duration: limiterDurationMs },
    },
  )

  void worker.run()
  await new Promise((r) =>
    setTimeout(r, observeWindows * limiterDurationMs),
  )
  await worker.close().catch(() => {})

  const afterQueue = new Queue(queueName, { connection: parseConn(redisUrl) })
  let afterWaiting: number
  try {
    const after = await afterQueue.getJobCounts()
    afterWaiting = (after.waiting ?? 0) + (after.active ?? 0)
  } finally {
    await afterQueue.close()
  }

  const drainedQueueWide = Math.max(beforeWaiting - afterWaiting, 0)

  console.log(
    `[node-verify-rl] localCompleted=${localCompleted} ` +
      `drainedQueueWide=${drainedQueueWide} ` +
      `(beforeWaiting=${beforeWaiting} afterWaiting=${afterWaiting}) ` +
      `limiter=${limiterMax}/${limiterDurationMs}ms ` +
      `observeWindows=${observeWindows} ` +
      `expectMin=${expectMin} expectMax=${expectMax}`,
  )

  if (drainedQueueWide < expectMin) {
    console.error(
      `[node-verify-rl] ERROR: too few drained (${drainedQueueWide} < ${expectMin}); ` +
        `limiter may have stalled the queue entirely`,
    )
    return 1
  }
  if (drainedQueueWide > expectMax) {
    console.error(
      `[node-verify-rl] ERROR: limiter breached — ${drainedQueueWide} jobs drained ` +
        `queue-wide within the window (> ${expectMax}). If a Python worker ran ` +
        `concurrently, the two FFI paths are NOT sharing one bucket.`,
    )
    return 1
  }

  console.log(
    `[node-verify-rl] OK — ${drainedQueueWide} jobs drained queue-wide, ` +
      `within [${expectMin}, ${expectMax}] for shared limiter ` +
      `${limiterMax}/${limiterDurationMs}ms`,
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
