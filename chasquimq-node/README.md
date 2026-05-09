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
| `Queue<DataType, ResultType, NameType>` | Producer + queue inspection. `add` / `addBulk` / `addUnique` / `getJobResult` / `peekDlq` / `replayDlq` / `cancelDelayed` / `getRepeatableJobs` / `removeRepeatableByKey`. `[Symbol.asyncDispose]`. |
| `Worker<DataType, ResultType, NameType>` | Consumer pool. tokio-side dispatch, opt-in result storage (`storeResults: true`), `EventEmitter` events (`completed` / `failed` / `error`). `[Symbol.asyncDispose]`. |
| `Job<DataType, ResultType, NameType>` | Read-only handle. `id`, `name`, `data`, `attemptsMade`, `waitForResult({ timeoutMs, intervalMs, signal })`. |
| `QueueEvents` | Cross-process pub/sub via the events stream. Subscribe to `completed` / `failed` / `dlq` / `retry-scheduled` / `delayed` / `drained`. `[Symbol.asyncDispose]`. |
| `BackoffSpec` | Builders: `.fixed(delayMs)` / `.exponential(initialMs, { multiplier, maxMs, jitterMs })`. |
| `UnrecoverableError` | Throw from your handler to bypass retries and route the job directly to DLQ. |
| `NotSupportedError` | Surfaces from APIs that aren't on the chasquimq roadmap (e.g. parent/child flows). |

`Queue.add(name, data, opts)` accepts: `delay` (ms), `attempts`, `backoff`, `jobId`, `repeat: { kind: 'cron' | 'every', ... }`, `missedFires: { kind: 'skip' | 'fire-once' | 'fire-all', maxCatchup? }`.

### TLS / `rediss://`

For TLS-fronted Redis (ElastiCache encryption-in-transit, MemoryDB), set `tls: true` on `connection`, or pass a `rediss://` URL directly:

```ts
const conn = { host: "my-cluster.cache.amazonaws.com", port: 6379, tls: true }
// or:
const conn = { url: "rediss://my-cluster.cache.amazonaws.com:6379" }
```

Trust roots come from the system TLS stack (Secure Transport on macOS, OpenSSL on Linux, Schannel on Windows), so AWS Trust CA-signed endpoints work out of the box. For private CAs, install them in the system store.

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
