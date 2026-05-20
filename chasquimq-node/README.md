# chasquimq (Node.js)

Node.js bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; JavaScript / TypeScript handlers process them.

> **Status:** 1.0. Prebuilt native binaries for `darwin-arm64`, `darwin-x64`, `linux-x64-gnu`, `linux-arm64-gnu`, `win32-x64-msvc`.

## Install

```bash
npm install chasquimq
```

The install resolves a prebuilt platform package (`chasquimq-<platform>-<arch>`) — no Rust toolchain or `node-gyp` required.

## Quickstart

```ts
import {
  Queue, Worker, BackoffSpec, UnrecoverableError, type Job,
} from "chasquimq"

const conn = { host: "127.0.0.1", port: 6379 }

async function main() {
  await using queue = new Queue<{ to: string }, { sentAt: number }>("emails", { connection: conn })
  await using worker = new Worker<{ to: string }, { sentAt: number }>("emails",
    async (job) => {
      const to = job.data.to
      console.log(`sending to ${to} (attempt ${job.attemptsMade + 1})`)
      if (to.includes("@unrecoverable")) {
        const err = new UnrecoverableError(`hard bounce: ${to}`)
        throw err
      }
      return { sentAt: Date.now() / 1000 }
    },
    { connection: conn, storeResults: true })

  // Plain enqueue.
  await queue.add("welcome", { to: "alice@example.com" })

  // Stable jobId — second call with the same id is a no-op (idempotent).
  await queue.addUnique("welcome", { to: "alice@example.com" }, { jobId: "welcome:alice" })

  // Per-job retry with exponential backoff.
  await queue.add("welcome", { to: "grace@flaky.example" }, {
    attempts: 3,
    backoff: BackoffSpec.exponential(100, { multiplier: 2.0, maxMs: 10_000 }),
  })

  // Delayed enqueue (milliseconds).
  await queue.add("welcome", { to: "ka@later.example" }, { delay: 2_000 })

  // Block on a single job's result.
  const job = await queue.add("welcome", { to: "ada@example.com" })
  const result = await job.waitForResult({ timeoutMs: 30_000 })
  console.log(result)

  // Drain the worker (interrupt with SIGINT in real usage).
  await worker.run()
}

main()
```

`await using` (TypeScript 5.2+) calls `worker.close()` and `queue.close()` automatically when the block exits — even if a step throws or the user `Ctrl+C`s. If you can't use `await using` yet, fall back to manual `try/finally` with `await worker.close()` and `await queue.close()`.

## What's in the box

| Surface | What it does |
|---|---|
| `Queue<DataType, ResultType, NameType>` | Producer + queue inspection. `add` / `addBulk` / `addUnique` / `getJobResult` / `peekDlq` / `replayDlq` / `cancelDelayed` / `getRepeatableJobs` / `removeRepeatableByKey` / `pause` / `resume` / `isPaused`. `[Symbol.asyncDispose]`. |
| `Worker<DataType, ResultType, NameType>` | Consumer pool. tokio-side dispatch, opt-in result storage (`storeResults: true`), `EventEmitter` events (`completed` / `failed` / `error`), `pause` / `resume` / `isPaused`. `[Symbol.asyncDispose]`. |
| `Job<DataType, ResultType, NameType>` | Read-only handle. `id`, `name`, `data`, `attemptsMade`, `waitForResult({ timeoutMs, intervalMs, signal })`. |
| `QueueEvents` | Cross-process pub/sub via the events stream. Subscribe to `completed` / `failed` / `dlq` / `retry-scheduled` / `delayed` / `drained`. `[Symbol.asyncDispose]`. |
| `BackoffSpec` | Builders: `.fixed(delayMs)` / `.exponential(initialMs, { multiplier, maxMs, jitterMs })`. |
| `UnrecoverableError` | Throw from your handler to bypass retries and route the job directly to DLQ. |
| `NotSupportedError` | Surfaces from APIs that aren't on the chasquimq roadmap (e.g. parent/child flows). |

`Queue.add(name, data, opts)` accepts: `delay` (ms), `attempts`, `backoff`, `jobId`, `repeat: { kind: 'cron' | 'every', ... }`, `missedFires: { kind: 'skip' | 'fire-once' | 'fire-all', maxCatchup? }`.

### TLS / `rediss://`

For TLS-fronted Redis (ElastiCache encryption-in-transit, or any non-cluster Redis with TLS), set `tls: true` on `connection`, or pass a `rediss://` URL directly:

```ts
const conn = { host: "my-cluster.cache.amazonaws.com", port: 6379, tls: true }
// or:
const conn = { url: "rediss://my-cluster.cache.amazonaws.com:6379" }
```

Trust roots come from the platform store via `rustls-native-certs`: keychain on macOS, the OS CA bundle on Linux (probed by `openssl-probe`), system store on Windows — so AWS Trust CA-signed endpoints work out of the box. For private CAs, point `SSL_CERT_FILE` at a PEM bundle before launching Node; that env var takes precedence over the platform store.

### Redis Cluster

For a multi-shard Redis Cluster (ElastiCache Cluster mode enabled, self-hosted Redis Cluster), set `cluster: true` on `connection`, or pass a `redis-cluster://` URL directly:

```ts
const conn = { host: "seed.cache.amazonaws.com", port: 6379, cluster: true }
// TLS + cluster:
const conn = { host: "seed.cache.amazonaws.com", port: 6379, cluster: true, tls: true }
// or an explicit URL (extra seeds via ?node=):
const conn = { url: "redis-cluster://seed:6379?node=seed2:6379" }
```

One seed node is enough — the rest of the topology is discovered automatically, and `MOVED`/`ASK` redirections plus failover are handled for you. Every key for a queue shares a `{chasqui:<queue>}` hash tag, so the queue's whole keyspace (stream, delayed, DLQ, results, locks, events) lives on a single slot and the engine's atomic operations stay correct. A queue is single-slot by design; cross-queue atomic operations are not supported on a cluster (they are not supported on single-node Redis either). An explicit `url` wins over `cluster` / `tls`, so a `rediss-cluster://` URL connects to a TLS cluster as-is.

### Rotating IAM tokens

For deployments that use short-lived auth tokens (ElastiCache IAM auth, Vault, AWS Secrets Manager, GCP Secret Manager, ...), pass an async `credentialProvider` on `connection`. The engine invokes it on every reconnect / `AUTH` cycle — never per job, so the hot path is unaffected:

```ts
import { ElastiCacheClient } from "@aws-sdk/client-elasticache"

// Sketch — swap in your own token source.
async function fetchIamToken(host: string | null) {
  // Real implementations call AWS SDK's `signer.presign` against
  // `redis://<replication-group-id>/?Action=connect&User=<user>`, or
  // hit Vault's `/database/creds/<role>`. Both return a short-lived
  // credential pair; cache + refresh on the schedule your provider
  // recommends (ElastiCache rotates every 15 minutes).
  return {
    username: "iam-user",
    password: await mintToken(host),
  }
}

const conn = {
  url: "rediss://my-cluster.cache.amazonaws.com:6379",
  credentialProvider: fetchIamToken,
}
```

A thrown error or rejected Promise from the callback maps to a fred auth error. On the **initial connect** (the first `Queue.add` / `Worker.run`), an auth failure surfaces as a hard rejection on the caller — useful for fail-loud startup of a misconfigured deployment. After the pool is up, the default `reconnect_on_auth_error` policy treats subsequent auth failures as transient and retries on the next reconnect attempt, so a brief blip in your secrets backend doesn't take the worker down. By default a permanently broken provider post-startup retry-loops inside fred forever; bound it with [`reconnectMaxAttempts`](#bounding-reconnect-attempts) so it gives up after N attempts instead.

### Bounding reconnect attempts

By default the engine reconnects forever (`reconnectMaxAttempts: 0`). That's the right behaviour for a transient network blip, but a permanently rejecting `credentialProvider` — a revoked IAM user, an expired role — will loop on reconnect indefinitely instead of surfacing the failure. Cap it with `connection.reconnectMaxAttempts`:

```ts
const conn = {
  url: "rediss://my-cluster.cache.amazonaws.com:6379",
  credentialProvider: fetchIamToken,
  // Give up after 10 failed reconnects instead of looping forever.
  reconnectMaxAttempts: 10,
}
```

Applies to both `Queue` (producer pool) and `Worker` (consumer). `0` (the default) keeps the unbounded behaviour. Pair a positive cap with alerting on reconnect churn so a bounded failure is loud, not silent.

### Capping payload size

The producer rejects any job whose encoded (MessagePack) payload exceeds `maxPayloadBytes` with an error, *before* it ever reaches Redis — the produce-side mirror of the consumer's oversize-on-read cap (which routes too-big entries to the DLQ). Both default to **1 MiB**; set both to the same value for symmetric produce/consume semantics:

```ts
import { Producer } from "chasquimq"

const producer = await Producer.connect(url, {
  queueName: "emails",
  // Reject any add* / repeatable upsert over 256 KiB before the write.
  maxPayloadBytes: 256 * 1024,
})
```

This is a native `Producer` option (same surface as `maxStreamLen` / `maxDelaySecs`). Negative values are ignored — the engine default stands. An oversize job in a bulk call fails the whole call atomically with no partial write.

### Pausing and resuming

Two levels of pause, both consumer-side: workers stop pulling new jobs, jobs already in flight finish, producers keep enqueueing.

`Worker.pause()` is **process-local** — it stops just that worker instance. Resume is instant.

```ts
const worker = new Worker("emails", handler, { connection })
void worker.run()

await worker.pause()   // this worker stops dispatching new jobs
// ...in-flight handlers still finish; queue.add() still works...
worker.resume()        // back to processing
worker.isPaused()      // => false
```

`Queue.pause()` is **durable and cross-process** — it sets a Redis flag every consumer of the queue honours, and it survives worker restarts until you `resume()`. Use it for queue-wide maintenance (a worker started while paused comes up paused).

```ts
const queue = new Queue("emails", { connection })
await queue.pause()            // every worker of "emails" parks
await queue.isPaused()         // => true
await queue.resume()           // lift it everywhere
```

The same durable flag is what the CLI's `chasqui pause <queue>` / `chasqui resume <queue>` toggle. Both surfaces are idempotent — double-pause / double-resume are no-ops.

## Power-user surface

The native engine handles ship from the same top-level package:

```ts
import { Producer, Consumer, Scheduler } from "chasquimq"
```

There is one user-facing `Job` — the high-level class returned by `Queue.add` and passed to your `Worker` processor. The native binding's `Job` value-type is internal-only and not re-exported (mirrors the Python shim).

## See also

- [Main repo README](https://github.com/jotarios/chasquimq#readme) — pitch, headline numbers, feature comparison
- [Engine internals](https://github.com/jotarios/chasquimq/blob/main/docs/engine.md) — retry semantics, delayed jobs, result backends, observability
- [Phase 3 design doc](https://github.com/jotarios/chasquimq/blob/main/docs/phase3-napi-design.md) — the NAPI-RS binding architecture

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
