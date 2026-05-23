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
| `Queue<DataType, ResultType, NameType>` | Producer + queue inspection. `add` / `addBulk` / `addUnique` / `getJob` / `getJobs` / `getJobState` / `getJobCounts` / `getJobLogs` / `getWaitingCount` / `getActiveCount` / `getDelayedCount` / `getCompletedCount` / `getFailedCount` / `count` / `getJobResult` / `peekDlq` / `replayDlq` / `cancelDelayed` / `getRepeatableJobs` / `removeRepeatableByKey` / `pause` / `resume` / `isPaused` / `remove` / `removeReport` / `drain` / `clean` / `obliterate`. `[Symbol.asyncDispose]`. |
| `Worker<DataType, ResultType, NameType>` | Consumer pool. tokio-side dispatch, opt-in result storage (`storeResults: true`), `EventEmitter` events (`ready` / `active` / `completed` / `failed` / `error` / `closing` / `closed` / `drained` / `paused` / `resumed` / `progress`), `pause` / `resume` / `isPaused`. `[Symbol.asyncDispose]`. |
| `Job<DataType, ResultType, NameType>` | Read-only handle. `id`, `name`, `data`, `attemptsMade`, `progress`, `updateProgress(n)` (Worker-side only), `log(line)` (Worker-side only), `waitForResult({ timeoutMs, intervalMs, signal })`, `waitUntilFinished(queueEvents, ttl?)`. |
| `QueueEvents` | Cross-process pub/sub via the events stream. Subscribe to `waiting` / `active` / `completed` / `failed` / `dlq` / `retry-scheduled` / `delayed` / `drained` / `retries-exhausted`, plus per-id channels (`completed:<jobId>` / `failed:<jobId>` / `active:<jobId>`) for targeted subscribers. `[Symbol.asyncDispose]`. |
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

### Subscribing to events

Two layered surfaces: per-worker `EventEmitter` events for in-process observation, and the cross-process `QueueEvents` stream for fan-out across workers, dashboards, and ops tooling.

In-process `Worker` events fire on the local worker only and use plain `EventEmitter` semantics:

```ts
const worker = new Worker("emails", handler, { connection })

worker.on("ready", () => console.log("engine loop started"))
worker.on("active", (job) => console.log("running", job.id))
worker.on("completed", (job, result) => console.log("ok", job.id, result))
worker.on("failed", (job, err) => console.error("failed", job?.id, err))
worker.on("error", (err) => console.error("engine error", err))
worker.on("drained", () => console.log("queue is empty"))
worker.on("paused", () => console.log("worker parked"))
worker.on("resumed", () => console.log("worker resumed"))
worker.on("closing", () => console.log("shutting down"))
worker.on("closed", () => console.log("shutdown complete"))
```

`drained` is the only `Worker` event that requires a cross-process subscription (the engine emits it on the events stream, since "no jobs left" is a queue-wide observation). It's lazily wired: the first `worker.on('drained', ...)` call spawns an embedded `QueueEvents` subscriber, torn down on `worker.close()`. Workers that never subscribe pay no extra Redis connections.

Cross-process `QueueEvents` listens on the events stream — every process running a `QueueEvents` instance for the same queue sees every transition, regardless of which worker emitted it:

```ts
import { QueueEvents } from "chasquimq"

const events = new QueueEvents("emails", { connection })
await events.waitUntilReady()

events.on("waiting", ({ jobId, name }) => { /* enqueued */ })
events.on("active", ({ jobId, name, attempt }) => { /* started */ })
events.on("completed", ({ jobId, name, attempt }) => { /* ok */ })
events.on("failed", ({ jobId, failedReason, attempt }) => { /* err */ })
events.on("drained", () => { /* empty */ })

// chasquimq-specific extensions
events.on("retry-scheduled", ({ jobId, attempt, backoffMs }) => { /* next try */ })
events.on("dlq", ({ jobId, reason, attempt }) => { /* moved to DLQ */ })
events.on("retries-exhausted", ({ jobId, attemptsMade, reason }) => { /* DLQ */ })

// Per-id channels — fire only for the named job. Used internally by
// Job.waitUntilFinished; useful for targeted UI updates without
// filtering every broadcast event by jobId.
events.on(`completed:${jobId}`, ({ jobId }) => { /* this job, done */ })
events.on(`failed:${jobId}`, ({ failedReason }) => { /* this job, failed */ })

await events.close()
```

`completed` payloads from the events stream do **not** carry the handler's return value (that would double-allocate the payload onto every subscriber). To read the value, pair `QueueEvents` with `Queue.getJobResult(jobId)` and run the worker with `storeResults: true`. The next section's `Job.waitUntilFinished` does this for you.

`progress` fires every time a processor calls `await job.updateProgress(n)` — the worker lazily spawns an embedded `QueueEvents` subscriber on the first `progress` listener (same zero-cost-when-unused pattern as `drained`) and re-emits the cross-process `e=progress` event as `(job, n)`. See the [Progress and logs](#progress-and-logs) section below. `stalled` is accepted on `Worker` for API stability but is currently no-op — the engine doesn't emit a stalled-detector transition yet; attach it safely, it'll start firing when the corresponding engine work lands.

### Awaiting a single job's completion

`Job.waitUntilFinished(queueEvents, ttl?)` is the event-driven completion-wait — subscribes to the per-id `completed:<jobId>` / `failed:<jobId>` channels and resolves / rejects on the first to fire:

```ts
const events = new QueueEvents("emails", { connection })
await events.waitUntilReady()

const job = await queue.add("send", { to: "ada@example.com" })

try {
  // Resolves with the handler's return value (requires
  // storeResults: true on the worker; otherwise resolves with
  // undefined). Rejects with new Error(failedReason) on failure.
  // ttl in ms — omit for an unbounded wait.
  const result = await job.waitUntilFinished(events, 30_000)
  console.log("sent:", result)
} catch (err) {
  // Either a job failure (engine reason) or a
  // WaitUntilFinishedTimeoutError (ttl elapsed with no terminal event).
  console.error(err)
} finally {
  await events.close()
}
```

Distinct from `Job.waitForResult({ timeoutMs })`: that one polls the `Queue.getJobResult` Redis key and **requires** `storeResults: true` to detect completion at all. `waitUntilFinished` is event-driven, so it detects completion even when no result key was written — but it cannot tell you a job that already finished before the call wired up. Pick `waitUntilFinished` for low-latency awaits of jobs you're about to enqueue; pick `waitForResult` for "did this id ever finish".

### Inspect jobs

Read-only introspection across the queue's stream, delayed ZSET, DLQ, and result-key surfaces. Bounded scans; no secondary index; zero impact on the producer / consumer hot paths.

```ts
const queue = new Queue("emails", { connection })

const counts = await queue.getJobCounts()
// => { waiting, active, delayed, completed, failed, paused }

const state = await queue.getJobState("job-123")
// => "waiting" | "active" | "delayed" | "completed" | "failed" | "unknown"

const job = await queue.getJob("job-123")
// => Job | undefined  (data is msgpack-decoded; engine state on attemptsMade / processedOn / finishedOn / failedReason)

const page = await queue.getJobs("waiting", 0, 50)
// => Job[]   (paginated; pass end > start to slice — start/end are inclusive indices)
```

State resolution is **live-state-first**: pending (PEL) → delayed → waiting → DLQ → result. A job that's been replayed from DLQ resolves as `"waiting"`, not `"completed"`, during the race window. A missing id resolves to `"unknown"` and `getJob` returns `undefined`.

Counts are cheap (~5 Redis round trips, one per state column). `completed` runs a bounded `SCAN` over the `result:*` keyspace under the per-queue hash tag — large keyspaces return a lower-bound figure (the cap is configurable via `CHASQUIMQ_COMPLETED_SCAN_CAP` on the engine, default 10,000).

`QueueOptions.consumerGroup` (optional, default `"default"`) configures which consumer group's PEL is read for the `active` column. Set it on the `Queue` if your `Worker` uses a non-default `group`.

```ts
const queue = new Queue("emails", { connection, consumerGroup: "primary" })
```

### Maintenance

Tear individual jobs — or a whole queue — down. All four methods are off the hot path; every scan is bounded so a single call never blocks Redis.

```ts
const queue = new Queue("emails", { connection })

// Remove one job from everywhere it lives — delayed stage, a waiting or
// in-flight stream entry, the DLQ, and the stored result. Idempotent.
const surfaces = await queue.remove("job-123")
// => number of surfaces the job was removed from (0 if not found)

const report = await queue.removeReport("job-123")
// => { delayed, stream, dlq, result } booleans

// Clear every waiting job. In-flight jobs keep running. Delayed jobs go
// too unless you pass `false`.
const drained = await queue.drain() // => count removed
await queue.drain(false) // keep scheduled future jobs

// Age- + state-filtered bulk delete. Removes up to `limit` jobs in the
// given state older than `grace` ms; returns the removed job ids.
const ids = await queue.clean(60_000, 1000, "completed")
//          clean(graceMs,  limit, "completed" | "failed" | "delayed" | "waiting")

// Nuke the entire queue keyspace — stream, DLQ, delayed, results,
// repeatable specs, the pause flag, the events stream. Returns the
// Redis key count removed.
await queue.obliterate()
```

`remove` is idempotent — a job id that exists on no surface resolves without error (count `0`). `clean` ignores `grace` for `"completed"` (a stored result has no creation timestamp; rely on result TTL for time-based expiry). `clean` with `"active"` is a no-op — use `remove` for the deliberate per-job case. `obliterate` cannot be undone.

### Progress and logs

In-handler write surface for live job state. The engine persists progress to a side-channel Redis key (`{chasqui:<queue>}:progress:<id>` STRING, TTL = `result_ttl_secs`) and appends log lines to a per-job stream (`{chasqui:<queue>}:log:<id>` STREAM with `MAXLEN ~`, TTL = `result_ttl_secs`). The msgpack `Job` envelope is untouched — wire-format-stable.

```ts
const worker = new Worker("emails", async (job) => {
  await job.updateProgress(10)
  await job.log("connecting to SMTP")

  // ... work ...
  await job.updateProgress(50)
  await job.log("envelope sent")

  // ... work ...
  await job.updateProgress(100)
  return { delivered: true }
}, { connection })
```

Read the latest progress (no extra round trip — pipelined into the existing introspector lookup) and the log lines from anywhere:

```ts
const queue = new Queue("emails", { connection })

const job = await queue.getJob("job-123")
console.log(job?.progress) // => 50

const { logs, count } = await queue.getJobLogs("job-123")
// logs: string[]   (the captured `line` field values, asc order)
// count: number    (current XLEN — not logs.length — for paginating callers)

// Page from the tail; matches BullMQ's getLogs(-100, -1) convention.
const tail = await queue.getJobLogs("job-123", -100, -1)
```

Tuning (all `WorkerOptions`):

| Option | Default | Controls |
|---|---:|---|
| `logMaxLen` | `1000` | `MAXLEN ~` cap on each per-job log stream (must be `>= 16`). |
| `logMaxLineBytes` | `4096` | Per-line byte cap; oversize lines truncate on a UTF-8 char boundary with a `[…truncated]` marker. |
| `eventsProgressEnabled` | `true` | When `false`, mutes the `e=progress` events-stream entry — the persisted progress key is still written. Useful for high-rate progress reporters that don't need cross-process fan-out. |

Subscribe to the cross-process `progress` event via `QueueEvents` (broadcast `progress`, per-id `progress:<jobId>`) or the in-process `Worker.on('progress', (job, n) => ...)` re-fan. **Read-only Job guard:** calling `updateProgress` / `log` on a Job returned by `Queue.getJob` / `Queue.add` throws `Error('Job.updateProgress() requires the Job be passed to your Worker handler; Jobs returned by Queue.getJob() are read-only')` — only Jobs handed to a `Worker` processor carry the live native handle.

> **Breaking (TS types only):** `JobProgress` narrowed from `number | object` to `number`. No runtime impact — the engine wire format never carried the object form.

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
