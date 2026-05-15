import { describe, it, expect } from 'vitest'
import { Queue } from '../dist/queue.js'
import { Worker } from '../dist/worker.js'
import type { CredentialProvider } from '../dist/types.js'

// The local Redis service container (CI: `redis:8.6.2`) has no password —
// AUTH-with-wrong-password would just fail. We exercise the JS callback's
// *invocation*, not its credential value: each test asserts the callback
// was driven at least once during a successful Queue.add / Worker pass.
// The "negative" case proves a synchronous throw inside the callback
// propagates as a connect / produce error within a bounded time.
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'
const HAS_REDIS = Boolean(process.env.REDIS_URL)

const d = HAS_REDIS ? describe : describe.skip

function uniqueQueue(tag: string): string {
  return `creds-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`
}

d('Queue.add invokes credentialProvider on connect', () => {
  it('drives the callback at least once when Queue.add succeeds', async () => {
    let calls = 0
    const provider: CredentialProvider = async (host) => {
      calls += 1
      // Sanity: host is either null (initial parse) or a `host:port`
      // string. Reject anything else loudly so a wire-shape regression
      // surfaces in CI rather than as a silent test-pass.
      expect(host === null || /^[^:]+:\d+$/.test(host)).toBe(true)
      // Local Redis has no password; returning an empty pair tells fred
      // to skip AUTH for this reconnect cycle, which is exactly what we
      // want for the call-counter assertion.
      return {}
    }

    const queue = new Queue(uniqueQueue('add'), {
      connection: { url: REDIS_URL, credentialProvider: provider },
    })
    try {
      const job = await queue.add('ping', { hello: 'world' })
      expect(job.id).toMatch(/^[0-9A-HJKMNP-TV-Z]{26}$/)
      // Pool warms lazily; at least one auth round trip on the first
      // produce. `>= 1` instead of `=== 1` because fred may open more
      // than one connection (pool_size default 8) on the first call.
      expect(calls).toBeGreaterThanOrEqual(1)
    } finally {
      await queue.close()
    }
  }, 30_000)
})

d('Worker invokes credentialProvider on consumer connect', () => {
  it('drives the callback at least once when a job is processed', async () => {
    const queueName = uniqueQueue('worker')
    let workerCalls = 0
    const workerProvider: CredentialProvider = async () => {
      workerCalls += 1
      return {}
    }
    // The Queue uses its own provider so we can prove the Worker-side
    // call counter is independent of the Producer-side one.
    let queueCalls = 0
    const queueProvider: CredentialProvider = async () => {
      queueCalls += 1
      return {}
    }

    const queue = new Queue(queueName, {
      connection: { url: REDIS_URL, credentialProvider: queueProvider },
    })

    let resolveProcessed: (() => void) | undefined
    const processed = new Promise<void>((r) => {
      resolveProcessed = r
    })

    const worker = new Worker(
      queueName,
      async () => {
        resolveProcessed?.()
        return undefined
      },
      {
        connection: { url: REDIS_URL, credentialProvider: workerProvider },
        autorun: true,
        concurrency: 1,
        drainDelay: 100,
      },
    )

    try {
      await queue.add('do-thing', { n: 1 })
      await Promise.race([
        processed,
        new Promise((_, rej) =>
          setTimeout(() => rej(new Error('worker-timeout')), 8_000),
        ),
      ])
      expect(queueCalls).toBeGreaterThanOrEqual(1)
      expect(workerCalls).toBeGreaterThanOrEqual(1)
    } finally {
      await worker.close()
      await queue.close()
    }
  }, 30_000)
})

d('credentialProvider rejection surfaces to the caller', () => {
  it('a thrown error in the callback rejects the produce path', async () => {
    let calls = 0
    const provider: CredentialProvider = async () => {
      calls += 1
      // A thrown error here maps to FredErrorKind::Auth in the bridge,
      // and the engine's initial Pool::init wires that through as a
      // hard failure (not a transient reconnect) — which is what we
      // want a misconfigured provider on a real ElastiCache deployment
      // to look like: fail-loud at startup.
      throw new Error('intentional-provider-failure')
    }

    const queue = new Queue(uniqueQueue('reject'), {
      connection: { url: REDIS_URL, credentialProvider: provider },
    })

    try {
      // `Queue.add` first connects the producer pool, which runs the
      // initial AUTH cycle, which invokes our callback, which throws.
      // The whole call must reject within a bounded time (well under
      // the 5s race timeout) carrying enough context for the operator
      // to identify the root cause.
      await expect(
        Promise.race([
          queue.add('x', { a: 1 }),
          new Promise((_, rej) =>
            setTimeout(() => rej(new Error('add-timeout')), 5_000),
          ),
        ]),
      ).rejects.toThrow(/intentional-provider-failure|Authentication/i)
      expect(calls).toBeGreaterThanOrEqual(1)
    } finally {
      // Fire-and-forget close — the producer never finished
      // connecting so there's nothing live to drain.
      void queue.close().catch(() => {
        /* swallow — never-connected teardown */
      })
    }
  }, 15_000)
})
