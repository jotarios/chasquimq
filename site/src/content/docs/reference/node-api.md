---
title: Node.js API
description: Complete reference for the chasquimq npm package — Queue, Worker, Job, QueueEvents, every option type, every method signature.
sidebar:
  order: 2
---

The Node.js shim wraps the Rust engine through a NAPI-RS binding. The
high-level surface (`Queue` / `Worker` / `Job` / `QueueEvents`) is what
application code reaches for. The unwrapped native classes (`Producer`
/ `Consumer` / `Scheduler`) are exported for power users; most callers
should not need them.

Install:

```bash
npm install chasquimq
```

Import:

```ts
import { Queue, Worker, Job, QueueEvents, BackoffSpec } from "chasquimq";
```

## On this page

- [Queue](#queue) — produce jobs, schedule recurring specs, inspect the DLQ, fetch results.
- [Worker](#worker) — consume jobs, handle retries, surface lifecycle events.
- [Job](#job) — the value type your processor receives and `Queue.add` returns.
- [QueueEvents](#queueevents) — subscribe to engine transitions across processes.
- [Option types](#option-types) — every interface that crosses a public surface.
- [Errors](#errors) — typed error classes, with full text in [error codes](/reference/error-codes/).
- [Native power-user surface](#native-power-user-surface) — the unwrapped NAPI bindings.

## Queue

```ts
class Queue<DataType = unknown, ResultType = unknown, NameType extends string = string>
```

A producer for a single queue. Construct one per logical queue;
the native producer pool is created lazily on the first call. Safe
to share across async contexts.

### `new Queue(name, opts)`

```ts
constructor(name: string, opts: QueueOptions)
```

- `name` — queue name, without the `{chasqui:...}` hash-tag wrapping.
- `opts` — connection and default-job options. See [`QueueOptions`](#queueoptions).

```ts
const queue = new Queue("emails", {
  connection: { host: "127.0.0.1", port: 6379 },
});
```

### `queue.add(name, data, opts?)`

```ts
async add(
  name: NameType,
  data: DataType,
  opts?: JobsOptions,
): Promise<Job<DataType, ResultType, NameType>>
```

Enqueue a single job. `name` is the dispatch name carried on the
stream entry's `n` field; the worker reads it back as `job.name`.
`data` is MessagePack-encoded on the way out and decoded on the way
in. Returns a `Job` whose `id` is the engine-minted ULID (or the
resolved spec key when `opts.repeat` is set).

When `opts.delay > 0`, routes through the delayed ZSET; otherwise
goes straight to the stream via `XADD`. When `opts.repeat` is set,
upserts a repeatable spec instead — see
[Repeatable jobs](/concepts/repeatable-jobs/).

**Throws** `RangeError` for non-finite or negative `delay`,
`TypeError` for whitespace-only `jobId`, `NotSupportedError` for
parent/child flows.

### `queue.addUnique(name, data, opts?)`

```ts
async addUnique(
  name: NameType,
  data: DataType,
  opts?: JobsOptions,
): Promise<Job<DataType, ResultType, NameType>>
```

Idempotent variant. Requires `opts.jobId` to be a non-empty string;
throws `TypeError` otherwise. Otherwise identical to
[`queue.add`](#queueaddname-data-opts).

Idempotency guarantees differ by path:

- **Delayed** (`delay > 0`) — strict and cross-process. A `SET NX EX`
  Lua marker on `{chasqui:<queue>}:dlid:<jobId>` gates the `ZADD`.
  The marker outlives the fire time by 1h so a producer-retry can't
  race promotion.
- **Immediate** (no `delay`) — strict within a single `Queue` instance.
  Redis 8.6 `XADD IDMP <producer_id> <jobId>` dedups at the wire
  layer, but the IDMP scope is the producer id (one per `Queue`).

For cross-process strict dedup on the immediate path, use `delay > 0`
with the same `jobId`.

### `queue.addBulk(jobs)`

```ts
async addBulk(
  jobs: Array<{ name: NameType; data: DataType; opts?: BulkJobOptions }>,
): Promise<Job<DataType, ResultType, NameType>[]>
```

Enqueue many jobs in one round trip. When all entries lack per-job
overrides (`delay`, `jobId`, `attempts`, `backoff`), routes through
the engine's pipelined `add_bulk_named` path; otherwise degrades to
a per-entry `add()` loop and loses the bulk pipelining win.

**Throws** `RangeError` on any non-finite / negative `delay`,
`NotSupportedError` if any entry sets `parent`.

### `queue.getRepeatableJobs(limit?)`

```ts
async getRepeatableJobs(limit?: number): Promise<RepeatableJobMeta[]>
```

List repeatable specs ordered by next fire time, ascending. Default
**`limit = 100`**. Payloads are intentionally not included to keep
listing thousands of specs cheap; reach for the spec hash directly
if you need them. See [`RepeatableJobMeta`](#repeatablejobmeta).

### `queue.removeRepeatableByKey(key)`

```ts
async removeRepeatableByKey(key: string): Promise<boolean>
```

Remove a repeatable spec by its resolved key. Returns `true` if a
spec was removed, `false` if no spec with that key existed.

### `queue.getJobResult(jobId)`

```ts
async getJobResult(jobId: string): Promise<ResultType | undefined>
```

Read the stored handler result. Returns `undefined` for three
indistinguishable cases: the job has not yet completed, the result
key already expired, or no result was written (handler returned
`undefined`, worker ran without `storeResults`, or job was DLQ'd).

The bytes are msgpack-decoded with the same wire format the worker
encoded them with.

### `queue.peekDlq(limit?)`

```ts
async peekDlq(limit?: number): Promise<DlqEntry[]>
```

Inspect up to `limit` DLQ entries oldest-first. Default
**`limit = 20`**. Each `DlqEntry` carries the relocated `dlqId`,
the original `sourceId`, the routing `reason`, optional `detail`,
the dispatch `name`, and the raw `payload` bytes.

### `queue.replayDlq(limit?)`

```ts
async replayDlq(limit?: number): Promise<number>
```

Atomically move up to `limit` DLQ entries back into the main stream
with their attempt counter reset. Default **`limit = 100`**. Returns
the number of entries actually replayed.

### `queue.remove(jobId)`

```ts
async remove(jobId: string): Promise<number>
```

Best-effort cancel of a delayed job. Returns `1` if the entry was
removed from the delayed ZSET, throws `NotSupportedError` otherwise
(in-stream entries can't be removed by id alone).

### `queue.pause()`, `queue.resume()`, `queue.isPaused()`

```ts
async pause(): Promise<void>
async resume(): Promise<void>
async isPaused(): Promise<boolean>
```

Durable, cross-process pause. `pause()` sets the
`{chasqui:<queue>}:paused` key (no TTL); every consumer of the queue
parks its reader at the next batch boundary while in-flight jobs
drain and producers keep enqueueing. Survives consumer restarts until
`resume()` clears the key. Idempotent. This is the queue-wide
analogue of [`worker.pause()`](#workerpause-workerresume-workerispaused);
the same key is toggled by `chasqui pause` / `chasqui resume`. See the
[Pause and resume concept](/concepts/pause-and-resume/).

### `queue.close()`

```ts
async close(): Promise<void>
```

If a producer was lazily connected, awaits the underlying pool's
`QUIT` (clean disconnect) before clearing the handle. Idempotent.
Calling without ever issuing an `add()` is a no-op. Compatible with
`await using`:

```ts
await using queue = new Queue("emails", { connection });
await queue.add("welcome", { to: "ada@example.com" });
// queue.close() runs automatically at scope exit.
```

`close()` is for clean disconnect, not flush. Every `await queue.add()`
already waits for Redis to ack the `XADD` before resolving — by the
time the call returns, the message is committed. Hosts that may be
frozen the moment your handler returns (AWS Lambda, Cloud Run) can
return immediately after `add` resolves; calling `close()` is
optional polish.

### `queue.isClosed`

```ts
get isClosed(): boolean
```

`true` after the first `close()` call.

### Stubbed methods (NotSupportedError)

These throw [`NotSupportedError`](/reference/error-codes/#cmq-100--node-feature-not-supported)
in v1: `getJob`, `getJobs`, `getJobCounts`, `getWaitingCount`,
`getActiveCount`, `getDelayedCount`, `getFailedCount`, `count`,
`drain`, `obliterate`, `clean`. Some pure-stat
methods return `0` or `'unknown'` instead. See the [options
index](/reference/options/) and [Thinking in
ChasquiMQ](/concepts/thinking-in-chasquimq/) for the engine semantics that
make these intentionally unsupported.

## Worker

```ts
class Worker<DataType = unknown, ResultType = unknown, NameType extends string = string>
  extends EventEmitter
```

Runs a user-supplied processor against a queue. The native
`Consumer` does all the scheduling, retry, DLQ, and ack work; this
class is a thin presentation layer.

### `new Worker(name, processor, opts)`

```ts
constructor(
  name: string,
  processor: Processor<DataType, ResultType, NameType>,
  opts: WorkerOptions,
)
```

- `name` — queue name to consume.
- `processor` — a `(job: Job) => Promise<ResultType>` function.
  Sandboxed processors (string / URL paths) are not supported;
  passing one throws `NotSupportedError`.
- `opts` — see [`WorkerOptions`](#workeroptions).

By default the worker calls `.run()` on the next microtask. Disable
with `opts.autorun = false` if you need to attach listeners
synchronously after construction.

### `worker.run()`

```ts
async run(): Promise<void>
```

Start the engine loop. Resolves once the engine drains (after
`close()` is called). Calling `run()` more than once returns the
same `Promise` — it does not start a second loop.

### `worker.close(force?)`

```ts
async close(force?: boolean): Promise<void>
```

Signal shutdown. The engine drains in-flight handlers up to its
configured deadline, then resolves. Idempotent; calling `close()`
more than once awaits the in-flight drain. The `force` parameter
is currently a no-op — engine-side hard-cancel is reserved.

### `worker.pause()`, `worker.resume()`, `worker.isPaused()`

```ts
async pause(doNotWaitActive?: boolean): Promise<void>
resume(): void
isPaused(): boolean
```

Process-local pause for this worker instance. `pause()` stops the
reader at the next batch boundary; jobs already in a handler run to
completion; producers keep enqueueing. `resume()` wakes the parked
reader immediately (edge-triggered, no poll latency). In-memory only —
does not write the cross-process flag and does not survive a process
restart; for queue-wide durable pause use
[`queue.pause()`](#queuepause-queueresume-queueispaused). Idempotent.
`doNotWaitActive` is accepted for BullMQ call-shape parity but is a
no-op (in-flight jobs always drain in the background).

### `worker.isClosed`, `worker.isRunning`

```ts
get isClosed(): boolean
isRunning(): boolean
```

State predicates.

### `worker.rateLimit(expireTimeMs)`

```ts
async rateLimit(expireTimeMs: number): Promise<void>
```

Throws `NotSupportedError` in v1.

### `Symbol.asyncDispose`

`Worker` implements `[Symbol.asyncDispose]`, routing through
`close()`. Use with `await using`.

### Worker events

| Event | Args | Fires when |
|---|---|---|
| `ready` | `()` | `.run()` starts the engine loop. |
| `active` | `(job, prev)` | Before each processor invocation. `prev` reserved (always `''`). |
| `completed` | `(job, result, prev)` | Processor resolves. Engine acks the job. |
| `failed` | `(job, err, prev)` | Processor rejects. Error rethrown into engine retry/DLQ path. |
| `error` | `(err)` | Engine-side error surfaced from the native loop. |
| `closing` | `(msg)` | Start of `.close()`. |
| `closed` | `()` | Shutdown completes. |

```ts
worker.on("completed", (job, result) => {
  console.log("completed", job.id, result);
});
```

## Job

```ts
class Job<DataType = unknown, ResultType = unknown, NameType extends string = string>
```

The value type a processor receives, and what `Queue.add` /
`Queue.addBulk` return. v1 does not round-trip Job state through
Redis — the engine streams via `XREADGROUP` / `XACK` and does not
persist progress, return values, or per-job state metadata.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `id` | `string` | Engine-minted ULID, or resolved spec key for repeatable upserts. |
| `name` | `NameType` | Dispatch name from the stream entry's `n` field. Empty for unnamed jobs. |
| `data` | `DataType` | The msgpack-decoded payload. |
| `opts` | `JobsOptions` | The options the job was enqueued with. |
| `attemptsMade` | `number` | 1-indexed attempt count. `0` for never-yet-run; `1` on first invocation. |
| `progress` | `JobProgress` | In-memory only; engine does not persist. |
| `returnvalue` | `ResultType?` | Set after the processor resolves. |
| `failedReason` | `string?` | Set after the processor rejects. |
| `stacktrace` | `string[]` | Reserved; empty in v1. |
| `timestamp` | `number` | Submission time, ms since epoch. Defaults to `Date.now()`. |
| `delay` | `number` | Original delay in ms. |
| `priority` | `number` | Always `0` — Streams are FIFO. |
| `processedOn` | `number?` | Reserved. |
| `finishedOn` | `number?` | Reserved. |
| `queue` | `Queue?` | Backreference for `waitForResult`. Set on producer-side jobs only. |

### `job.updateProgress(progress)`

```ts
async updateProgress(progress: JobProgress): Promise<void>
```

In-memory progress update. The engine does not persist progress;
the worker shim surfaces this via the `progress` event when called
from inside a processor.

### `job.waitForResult(opts?)`

```ts
async waitForResult(opts?: WaitForResultOptions): Promise<ResultType | undefined>
```

Poll until the engine's stored result for this job becomes
readable, until `opts.timeoutMs` elapses, or until `opts.signal`
fires. See [`WaitForResultOptions`](#waitforresultoptions).

Throws [`WaitForResultTimeoutError`](/reference/error-codes/#cmq-102--node-result-wait-timeout)
on timeout. Throws the abort reason on cancel.

:::caution[The void-handler trap]
If the worker resolved the processor with `undefined` / `void`,
*or* ran without `storeResults: true`, no result key is ever
written. The polling loop has no way to distinguish that case
from "not yet completed", so this method will time out. Mirror
`storeResults` on the consumer side before relying on
`waitForResult`. For high-fanout workloads, subscribe to
[`QueueEvents`](#queueevents) instead.
:::

### `job.toJSON()`

```ts
toJSON(): object
```

Plain-object snapshot of the job for logging / serialization.

### Stubbed methods

`log`, `getState`, `remove`, `retry`, `discard`, `update`,
`updateData`, `isCompleted`, `isFailed`, `isActive`, `isWaiting`
all throw `NotSupportedError` or return a fixed `false` in v1 —
engine-side state queries land in a future slice. `isDelayed()`
returns `delay > 0` from the in-memory option.

## QueueEvents

```ts
class QueueEvents extends EventEmitter
```

Subscribe to a queue's events stream
(`{chasqui:<queue>}:events`) across processes. Backed by `ioredis`
because the events stream is a generic Redis stream (ASCII
fields, not msgpack), so a thin pure-JS subscriber is the simplest
path.

### `new QueueEvents(name, opts)`

```ts
constructor(name: string, opts: QueueEventsOptions)
```

By default starts from `$` (only events emitted after the
subscriber is opened). Pass `opts.lastEventId = "0"` to replay
history. Auto-runs on the next microtask unless `autorun: false`.

### `queueEvents.run()`, `queueEvents.close()`, `queueEvents.waitUntilReady()`

Lifecycle. `close()` is idempotent and concurrency-safe; calling
it from multiple call sites awaits the same in-flight drain.
Implements `[Symbol.asyncDispose]`.

### Events

| Event | Args | Engine origin |
|---|---|---|
| `waiting` | `({ jobId, name }, eventId)` | Producer added (or promoter promoted) the job. |
| `active` | `({ jobId, name, prev, attempt }, eventId)` | Worker pulled the job; processor is about to run. |
| `completed` | `({ jobId, name, attempt, returnvalue }, eventId)` | Processor resolved. |
| `failed` | `({ jobId, name, failedReason, attempt }, eventId)` | Processor rejected. Fires before retry/DLQ relocation. |
| `retry-scheduled` | `({ jobId, name, attempt, backoffMs }, eventId)` | Engine atomically rescheduled the job onto the delayed ZSET. |
| `delayed` | `({ jobId, name, delay }, eventId)` | Producer enqueued with `delay > 0`. |
| `dlq` | `({ jobId, name, reason, attempt }, eventId)` | DLQ relocator wrote the entry to the DLQ stream. |
| `retries-exhausted` | `({ jobId, name, attemptsMade, reason }, eventId)` | Synthetic alias of `dlq` (chasquimq-specific). |
| `drained` | `(eventId)` | Engine drained (queue-scoped, no `jobId`). |
| `unknown` | `({ eventName, fields }, eventId)` | Forward-compat sink for unrecognized event types. |
| `error` | `(err)` | Operational error during XREAD. |

Numeric fields (`attempt`, `backoffMs`, `delay`, `duration_us`,
`ts`) are coerced to `number` at parse time so subscribers don't
have to remember which ones need an explicit cast.

## Option types

### `QueueOptions`

```ts
interface QueueOptions {
  connection: ConnectionOptions;
  prefix?: string;
  defaultJobOptions?: Partial<JobsOptions>;
}
```

- `connection` — required. See [`ConnectionOptions`](#connectionoptions).
- `prefix` — accepted; ignored. ChasquiMQ uses `{chasqui:<queue>}`
  Cluster hash tags; there is no tunable prefix.
- `defaultJobOptions` — applied as defaults on every `add()` /
  `addBulk()` call.

### `ConnectionOptions`

```ts
interface ConnectionOptions {
  host?: string;
  port?: number;
  password?: string;
  username?: string;
  db?: number;
  tls?: boolean;
  url?: string;
  [key: string]: unknown;
}
```

- `host` — **default `"127.0.0.1"`**.
- `port` — **default `6379`**.
- `password`, `username` — optional auth.
- `db` — optional logical database number.
- `tls` — when `true`, builds a `rediss://` URL (TLS) instead of plaintext. Combined with `url`, upgrades a `redis://` or schemeless input to `rediss://`. The engine negotiates TLS via fred's `enable-rustls-ring`; trust roots come from `rustls-native-certs`. For private CAs, point `SSL_CERT_FILE` at a PEM bundle before launching Node — that env var takes precedence over the platform store.
- `url` — full Redis URL. When set, overrides `host` / `port` / `password` / `username` / `db`. Use `rediss://...` for TLS, or pass `tls: true` to upgrade in place.
- Extra keys are accepted and silently ignored; the native pool
  manages its own connection lifetime.

### `WorkerOptions`

```ts
interface WorkerOptions {
  connection: ConnectionOptions;
  concurrency?: number;
  autorun?: boolean;
  drainDelay?: number;
  maxStalledCount?: number;
  removeOnComplete?: unknown;
  removeOnFail?: unknown;
  prefix?: string;
  name?: string;
  runScheduler?: boolean;
  schedulerTickMs?: number;
  storeResults?: boolean;
  resultTtlMs?: number;
}
```

- `concurrency` — max in-flight handler invocations. **Default `100`.**
- `autorun` — whether `.run()` is called on the next microtask. **Default `true`.**
- `drainDelay` — `XREADGROUP BLOCK` timeout in ms. **Default `5000`.**
- `maxStalledCount` — total attempts per job (initial + retries). Maps to engine `maxAttempts`. **Default `3`.**
- `removeOnComplete`, `removeOnFail` — accepted; no-ops. The engine `XACKDEL`s on success and DLQ-relocates on failure.
- `prefix` — accepted; no-op.
- `name` — optional consumer ID for `XREADGROUP CONSUMER`.
- `runScheduler` — auto-spawn an embedded scheduler. **Default `true`.** Set `false` when running a separate scheduler process.
- `schedulerTickMs` — scheduler tick interval. **Default `1000`.**
- `storeResults` — persist handler return values to `{chasqui:<queue>}:result:<jobId>`. **Default `false`.**
- `resultTtlMs` — TTL for stored results. **Default `3_600_000` (1h).** Rounded up to whole seconds at the FFI boundary.

### `JobsOptions`

```ts
interface JobsOptions {
  delay?: number;
  attempts?: number;
  backoff?: number | BackoffOptions;
  removeOnComplete?: boolean | number;
  removeOnFail?: boolean | number;
  priority?: number;
  jobId?: string;
  lifo?: boolean;
  timestamp?: number;
  repeat?: RepeatOptions;
  repeatJobKey?: string;
  parent?: { id: string; queue: string };
}
```

- `delay` — ms before the job becomes processable. **Default `0`.** Negative or non-finite → `RangeError`.
- `attempts` — total attempt budget for this job, overrides queue-wide `maxAttempts`. **Default queue-wide.**
- `backoff` — per-job backoff override. Either a plain `number` (fixed delay in ms) or a [`BackoffOptions`](#backoffoptions) object.
- `removeOnComplete` — accepted; no-op (engine `XACKDEL`s).
- `removeOnFail` — accepted; reserved for future DLQ trim policy.
- `priority` — accepted; ignored with a one-time console warning. Streams are FIFO.
- `jobId` — stable id for at-most-once / idempotent scheduling.
- `lifo` — accepted; ignored with a one-time console warning.
- `timestamp` — submission time in ms. **Default `Date.now()`.**
- `repeat` — schedule a recurring job. See [`RepeatOptions`](#repeatoptions).
- `repeatJobKey` — stable key for the repeat spec. Default: engine derives `<jobName>::<patternSignature>`.
- `parent` — throws `NotSupportedError`. Parent/child flows are not supported.

### `BackoffOptions`

```ts
interface BackoffOptions {
  type: "fixed" | "exponential";
  delay?: number;
  maxDelay?: number;
  multiplier?: number;
  jitterMs?: number;
}
```

- `type` — strategy. The NAPI binding rejects unknown strings up-front.
- `delay` — base delay in ms.
- `maxDelay` — cap on the computed backoff per attempt.
- `multiplier` — for `exponential`: `delay * multiplier^(attempt - 1)`. Ignored for `fixed`. **Default `2`** when built via `BackoffSpec.exponential`.
- `jitterMs` — symmetric ±jitter applied per attempt.

The `BackoffSpec` builder returns a `BackoffOptions` literal:

```ts
import { BackoffSpec } from "chasquimq";

await queue.add("send", payload, {
  attempts: 5,
  backoff: BackoffSpec.exponential(1_000, { maxDelayMs: 30_000 }),
});
```

### `RepeatOptions`

```ts
interface RepeatOptions {
  pattern?: string;
  every?: number;
  limit?: number;
  immediately?: boolean;
  startDate?: Date | string | number;
  endDate?: Date | string | number;
  tz?: string;
  jobId?: string;
  missedFires?: MissedFiresOption;
}
```

Pass exactly one of `pattern` (cron) or `every` (ms); both or
neither is rejected.

- `pattern` — cron expression. Accepts both 5-field and 6-field syntax.
- `every` — fixed ms interval. First fire lands one interval after upsert.
- `limit` — max total fires before the spec is removed.
- `immediately` — accepted; no-op in v1.
- `startDate`, `endDate` — `Date`, ms since epoch, or ISO string.
- `tz` — `"UTC"` / `"Z"`, fixed offset (`"+05:30"`), or any IANA name. IANA names are DST-aware. Ignored when `every` is set.
- `jobId` — reserved for future explicit-id-per-fire wiring.
- `missedFires` — catch-up policy. **Default `{ kind: "skip" }`.** See [`MissedFiresOption`](#missedfiresoption).

### `MissedFiresOption`

```ts
type MissedFiresOption =
  | { kind: "skip" }
  | { kind: "fire-once" }
  | { kind: "fire-all"; maxCatchup: number };
```

- `skip` — drop missed windows; resume on the first future fire. **Default.** Safe; no thundering herd.
- `fire-once` — emit one job to represent the missed window(s).
- `fire-all` — replay each missed window up to `maxCatchup` fires. `maxCatchup` must be `>= 1`.

### `RepeatableJobMeta`

```ts
interface RepeatableJobMeta {
  key: string;
  jobName: string;
  patternKind: "cron" | "every";
  pattern?: string;
  tz?: string;
  every?: number;
  nextFireMs: number;
  limit?: number;
  startAfterMs?: number;
  endBeforeMs?: number;
  missedFires?: MissedFiresOption;
}
```

Wire-compatible projection returned by
[`Queue.getRepeatableJobs`](#queuegetrepeatablejobslimit). No
payload — listing thousands of specs stays cheap.

### `QueueEventsOptions`

```ts
interface QueueEventsOptions {
  connection: ConnectionOptions;
  prefix?: string;
  autorun?: boolean;
  lastEventId?: string;
  blockingTimeout?: number;
}
```

- `prefix` — accepted; ignored.
- `autorun` — **default `true`**.
- `lastEventId` — start id. **Default `"$"`** (only new events).
- `blockingTimeout` — `XREAD BLOCK` timeout in ms. **Default `10_000`.**

### `WaitForResultOptions`

```ts
interface WaitForResultOptions {
  timeoutMs?: number;
  intervalMs?: number;
  signal?: AbortSignal;
}
```

- `timeoutMs` — total time budget. **Default `30_000`.**
- `intervalMs` — polling interval. **Default `100`.**
- `signal` — cancel the poll loop. Aborts surface as the standard `AbortError`.

### `Processor<DataType, ResultType, NameType>`

```ts
type Processor<DataType = unknown, ResultType = unknown, NameType extends string = string> =
  (job: Job<DataType, ResultType, NameType>) => Promise<ResultType>;
```

The function signature `Worker` accepts. Resolving the returned
promise acks the job; rejecting routes through the engine's retry
path.

### `JobState`, `JobType`, `JobProgress`

```ts
type JobState = "waiting" | "active" | "completed" | "failed" | "delayed" | "unknown";
type JobType = JobState | "paused" | "prioritized" | "waiting-children";
type JobProgress = number | object;
```

Type aliases for documentation. The engine itself does not
implement most of these states in v1 — see the `Queue` stub
methods.

## Errors

The shim throws typed errors so application code can branch on
`err.name`:

- [`UnrecoverableError`](/reference/error-codes/#cmq-011--handler-signaled-unrecoverable--dlq) — throw from a processor to skip retries and route to the DLQ.
- [`WaitForResultTimeoutError`](/reference/error-codes/#cmq-102--node-result-wait-timeout) — `Job.waitForResult` timed out.
- [`NotSupportedError`](/reference/error-codes/#cmq-100--node-feature-not-supported) — caller asked for a v1-stubbed feature.
- [`RateLimitError`](/reference/error-codes/#cmq-101--node-rate-limit) — reserved; `Worker.rateLimit` throws this in a future slice.

See [error codes](/reference/error-codes/) for the full table with
**When**, **Why**, **Fix**, and **See also** for each.

## Native power-user surface

The unwrapped NAPI bindings are re-exported for callers who want
to bypass the shim. Reach for these only when you have a measured
reason to skip the high-level layer.

```ts
import { Producer, Consumer, Scheduler, engineVersion } from "chasquimq";
```

- `Producer.connect(redisUrl, opts)` — async constructor.
- `new Consumer(redisUrl, opts)` — sync constructor.
- `new Scheduler(redisUrl, opts)` — sync constructor.
- `engineVersion()` — returns the engine binding crate version.

The full TypeScript surface lives in
`chasquimq-node/index.d.ts`. The low-level option types
(`ProducerOpts`, `ConsumerOpts`, `SchedulerOpts`, `RetryOpts`,
`AddOptions`, `BackoffSpec` (native), `DlqEntry`,
`JobRetryOverride`, `NamedPayload`, `RepeatPattern`,
`RepeatableSpec`, `RepeatableMeta`, `MissedFiresPolicy`) are also
exported.

These types collapse onto the engine's
[`ProducerConfig`](/reference/rust-api/#producerconfig) /
[`ConsumerConfig`](/reference/rust-api/#consumerconfig) /
[`SchedulerConfig`](/reference/rust-api/#schedulerconfig) — see
the Rust reference for the canonical field meanings.

## Utilities

### `encodePayload(data)` / `decodePayload(buf)`

```ts
function encodePayload(data: unknown): Buffer;
function decodePayload(buf: Buffer | Uint8Array): unknown;
```

The shim's MessagePack helpers, exported for parity tests across
language boundaries. `encodePayload` returns a zero-copy view of
the bytes; the native producer performs exactly one copy at the
FFI boundary into engine-managed `Bytes`.

### `engineVersion()`

```ts
function engineVersion(): string;
```

Returns the version of the binding crate. The npm package version
tracks this 1:1.
