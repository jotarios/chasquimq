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

### `queue.getJob(jobId)`

```ts
async getJob(jobId: string): Promise<Job<DataType, ResultType, NameType> | undefined>
```

Look up a single job across the four queue surfaces (stream PEL,
delayed ZSET, main stream, DLQ, result key). Bounded scan; returns
`undefined` when the id isn't found in any surface. The returned
`Job`'s `data` is msgpack-decoded; engine state surfaces via
`processedOn` / `finishedOn` / `failedReason` where applicable.

### `queue.getJobs(types?, start?, end?)`

```ts
async getJobs(
  types?: JobType | JobType[],
  start?: number,
  end?: number,
  asc?: boolean,
): Promise<Job<DataType, ResultType, NameType>[]>
```

Paginated listing within a single state. `types` is one of `"waiting"
| "active" | "delayed" | "completed" | "failed"`; `start` / `end` are
inclusive indices. Passing multiple state names throws
`NotSupportedError` — multi-state pagination would silently flatten
fundamentally different surfaces. Cursor-based multi-page sweeps go
through the lower-level native introspector; in practice for v1 the
single-page `start` / `end` API is enough.

### `queue.getJobState(jobId)`

```ts
async getJobState(jobId: string): Promise<JobState | "unknown">
```

One of `"waiting" | "active" | "delayed" | "completed" | "failed" |
"unknown"`. **Live-state-first**: a job that's been replayed from DLQ
resolves as `"waiting"` (not `"completed"`) during the race window. A
missing id resolves to `"unknown"`.

### `queue.getJobCounts(...types?)`

```ts
async getJobCounts(...types: JobType[]): Promise<Record<string, number>>
```

Per-state counts: `{ waiting, active, delayed, completed, failed,
paused }`. Pass no args for the full dict; pass one or more state
names to filter (returned dict contains only the keys you asked for).
`completed` is via bounded `SCAN` over `result:*` keys — large
keyspaces return a lower-bound figure (cap configurable via the
`CHASQUIMQ_COMPLETED_SCAN_CAP` env var on the engine).

### `queue.getWaitingCount()`, `queue.getActiveCount()`, `queue.getDelayedCount()`, `queue.getCompletedCount()`, `queue.getFailedCount()`

```ts
async getWaitingCount(): Promise<number>
async getActiveCount(): Promise<number>
async getDelayedCount(): Promise<number>
async getCompletedCount(): Promise<number>
async getFailedCount(): Promise<number>
```

Single-column convenience wrappers over `getJobCounts`.

### `queue.count()`

```ts
async count(): Promise<number>
```

`waiting + active + delayed` — the number of jobs that could still
run.

### `queue.getJobLogs(jobId, start?, end?, asc?)`

```ts
async getJobLogs(
  jobId: string,
  start?: number,
  end?: number,
  asc?: boolean,
): Promise<{ logs: string[]; count: number }>
```

Read up to `end - start + 1` lines from a job's log stream
(`{chasqui:<queue>}:log:<id>` STREAM, populated by
[`Job.log`](#joblogline) inside a processor). `start` / `end` are
inclusive entry offsets in the requested order; `end = -1` means
"to end"; negative `start` is "this many from the end"
(translated via XLEN), matching BullMQ's `Queue.getJobLogs`
convention. `asc` defaults to `true` (chronological).

Returns `{ logs, count }`:

- `logs` — captured `line` field values in the requested order.
- `count` — current XLEN of the log stream (**not** `logs.length`).
  Lets paginating callers know how many entries exist without
  walking the whole stream.

Jobs that never called `Job.log` resolve with `{ logs: [], count: 0 }`.

### `queue.remove(jobId)`, `queue.removeReport(jobId)`

```ts
async remove(jobId: string): Promise<number>
async removeReport(jobId: string): Promise<RemovalReport>
```

Remove a single job by id from every surface it could live on — the
delayed stage, a waiting or in-flight stream entry, the DLQ, and the
stored result. `remove` returns the number of distinct surfaces the job
was removed from (`0` when not found anywhere). `removeReport` returns
the full per-surface `RemovalReport { delayed, stream, dlq, result }`.

Idempotent — a job id that exists on no surface resolves without error.
The stable job id lives inside the message envelope, not the Redis
stream entry id, so the stream / DLQ branches run a bounded scan to
translate the id; a job further back than the scan window is reported
as absent on that surface. See the
[clean and obliterate guide](/guides/clean-and-obliterate/).

### `queue.drain(delayed?)`

```ts
async drain(delayed: boolean = true): Promise<number>
```

Clear every *waiting* job from the queue. In-flight (active) jobs are
left running. By default the delayed stage is also emptied; pass
`delayed = false` to keep scheduled future jobs. Returns the count of
stream + delayed entries removed.

### `queue.clean(grace, limit, type?)`

```ts
async clean(grace: number, limit: number, type?: JobType): Promise<string[]>
```

Age- and state-filtered bulk delete. Removes up to `limit` jobs in
`type` that are older than `grace` milliseconds, and returns the removed
job ids. `type` is one of `"completed"` | `"failed"` | `"delayed"` |
`"waiting"` and defaults to `"completed"`. `"active"` is a no-op —
removing an in-flight job is a footgun; use [`remove`](#queueremovejobid-queueremovereportjobid)
for the deliberate per-job case.

The age basis is the Redis stream entry id for `"waiting"` / `"failed"`
and the job's creation time for `"delayed"`. `grace` is **ignored** for
`"completed"` — a stored result has no creation timestamp; rely on the
result TTL for time-based expiry.

### `queue.obliterate(opts?)`

```ts
async obliterate(opts?: { force?: boolean; count?: number }): Promise<number>
```

Tear the entire queue down — delete every Redis key backing it: the
stream and its consumer groups, the DLQ, the delayed stage, all per-job
side-indexes and result keys, repeatable specs, the pause flag, and the
events stream. Returns the count of Redis keys removed. `opts` is
accepted for call-site compatibility; obliterate always tears the whole
queue down. **Not reversible.**

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

Parent / child job flows throw
[`NotSupportedError`](/reference/error-codes/#cmq-100--node-feature-not-supported):
passing a `parent` option to `add` / `addBulk` is rejected. See
[Thinking in ChasquiMQ](/concepts/thinking-in-chasquimq/) for the
rationale.

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
`doNotWaitActive` is accepted for call-shape stability but is a
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

The manual *per-invocation* "throttle this worker until `expireTimeMs`"
call. **Still throws `NotSupportedError`** — a different API from the
standing per-queue rate limit. For a queue-wide rate cap, set
[`WorkerOptions.limiter`](#limiteroptions) on the constructor instead;
that is shipped.

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
| `drained` | `()` | Engine observed a full→empty transition on the main stream. **Cross-process scope.** Lazily wires an embedded `QueueEvents` subscriber on the first `.on('drained', ...)` call; torn down on `close()`. |
| `paused` | `()` | `.pause()` was called. Process-local. |
| `resumed` | `()` | `.resume()` was called. Process-local. |
| `progress` | `(job, progress)` | Handler called `await job.updateProgress(n)`. The in-process re-fan finds the live `Job` in the inflight map and surfaces it with the clamped 0..=100 value. |
| `stalled` | `(jobId, prev)` | Stalled-job detector observed this entry idle past `idle_threshold_ms` for the `attempt`-th consecutive scan (under threshold; the relocate path emits `'failed'` with `reason='stalled'` via `dlq`). `prev` is always `'active'`. **Cross-process scope:** every worker on the queue receives the event. Lazily wires the internal `QueueEvents` subscriber on first attach. |

```ts
worker.on("completed", (job, result) => {
  console.log("completed", job.id, result);
});
worker.on("drained", () => {
  console.log("queue empty");
});
```

## Job

```ts
class Job<DataType = unknown, ResultType = unknown, NameType extends string = string>
```

The value type a processor receives, and what `Queue.add` /
`Queue.addBulk` return. The engine streams jobs via `XREADGROUP` /
`XACK` and does not persist return values by default (opt in with
`WorkerOptions.storeResults`). Progress and log lines *are*
persisted to side-channel keys when the handler calls
[`updateProgress`](#jobupdateprogressprogress) or
[`log`](#joblogline) — see those methods.

### Properties

| Property | Type | Meaning |
|---|---|---|
| `id` | `string` | Engine-minted ULID, or resolved spec key for repeatable upserts. |
| `name` | `NameType` | Dispatch name from the stream entry's `n` field. Empty for unnamed jobs. |
| `data` | `DataType` | The msgpack-decoded payload. |
| `opts` | `JobsOptions` | The options the job was enqueued with. |
| `attemptsMade` | `number` | 1-indexed attempt count. `0` for never-yet-run; `1` on first invocation. |
| `progress` | `JobProgress` | Latest value set via [`updateProgress`](#jobupdateprogressprogress), or read back by the introspector when this Job was returned by [`queue.getJob`](#queuegetjobjobid) / [`queue.getJobs`](#queuegetjobstypes-start-end). Default `0`. |
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

Persist a `0..=100` progress value for this job under the engine's
per-job progress key (`{chasqui:<queue>}:progress:<id>` STRING,
TTL = `result_ttl_secs`), mirror it on the local `progress` field,
and (when `WorkerOptions.eventsProgressEnabled !== false`) emit an
`e=progress` events-stream entry that `QueueEvents` re-fans onto
the broadcast `'progress'` channel and the per-id
`'progress:<jobId>'` channel.

Values outside `0..=100` are clamped to `100` at the engine
boundary (no throw; the first clamp per handle logs a single
warn-once).

**Read-only Job guard.** Throws when called on a Job returned by
[`queue.getJob`](#queuegetjobjobid) / [`queue.getJobs`](#queuegetjobstypes-start-end)
or constructed from [`queue.add`](#queueaddname-data-opts) — those
instances are synthesized from introspector / producer-side data
and carry no per-handler connection. Only Jobs handed to a
`Worker` processor have a live backref. Catch via
`err.message.startsWith('Job.updateProgress()')`.

### `job.log(line)`

```ts
async log(line: string): Promise<number>
```

Append `line` to the per-job log stream
(`{chasqui:<queue>}:log:<id>` STREAM) and return the new XLEN.
The stream is bounded by `WorkerOptions.logMaxLen` (default
`1000`) via `MAXLEN ~` and expires alongside the result key
(TTL = `result_ttl_secs`). Oversize lines (`> logMaxLineBytes`,
default `4096`) truncate on a UTF-8 char boundary with a
`[…truncated]` marker appended; first truncation per handle logs
a single warn-once.

Same read-only Job guard as [`updateProgress`](#jobupdateprogressprogress).
Read back via [`queue.getJobLogs`](#queuegetjoblogsjobid-start-end-asc).

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

### `job.waitUntilFinished(queueEvents, ttl?)`

```ts
async waitUntilFinished(
  queueEvents: QueueEvents,
  ttl?: number,
): Promise<ResultType | undefined>
```

Event-driven completion-wait. Subscribes to the
per-id `completed:<jobId>` / `failed:<jobId>` channels on the
supplied `queueEvents` and resolves / rejects on the first to fire.

- Resolves with the handler's return value when the worker ran with
  `storeResults: true`. The value is fetched via
  `Queue.getJobResult(this.id)` after the `completed` event lands.
  When `storeResults` was off (or the handler returned `undefined`),
  resolves with `undefined`.
- Rejects with `new Error(failedReason)` on the engine-reported
  failure reason (the same string surfaced on `Worker`'s `failed`
  event).
- Throws [`WaitUntilFinishedTimeoutError`](/reference/error-codes/)
  on `ttl` elapse. Omit `ttl` for an unbounded wait.

Distinct from [`waitForResult`](#jobwaitforresultopts): event-driven
(no polling), and works without `storeResults` to *detect*
completion. The two cover different races — `waitUntilFinished`
loses to a job that finished before the listeners wired up;
`waitForResult` can read a result key written before the wait
started but needs `storeResults`.

```ts
const events = new QueueEvents("emails", { connection });
await events.waitUntilReady();

const job = await queue.add("send", { to: "ada@example.com" });
const result = await job.waitUntilFinished(events, 30_000);

await events.close();
```

### `job.toJSON()`

```ts
toJSON(): object
```

Plain-object snapshot of the job for logging / serialization.

### Stubbed methods

`getState`, `remove`, `retry`, `discard`, `update`,
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
| `progress` | `({ jobId, name, progress }, eventId)` | Handler called [`job.updateProgress(n)`](#jobupdateprogressprogress). `progress` is the clamped `0..=100` value the engine persisted. |
| `drained` | `(eventId)` | Engine drained (queue-scoped, no `jobId`). |
| `unknown` | `({ eventName, fields }, eventId)` | Forward-compat sink for unrecognized event types. |
| `error` | `(err)` | Operational error during XREAD. |

In addition to the broadcast channels, three per-id channels fire
alongside the matching broadcast for events that carry a `jobId`
(naming convention: `<event>:<jobId>`):

| Event | Args | Fires alongside |
|---|---|---|
| `active:<jobId>` | `({ jobId, name, prev, attempt }, eventId)` | `active` |
| `completed:<jobId>` | `({ jobId, name, attempt, returnvalue }, eventId)` | `completed` |
| `failed:<jobId>` | `({ jobId, name, failedReason, attempt }, eventId)` | `failed` |
| `progress:<jobId>` | `({ jobId, name, progress }, eventId)` | `progress` |

Per-id channels let `Job.waitUntilFinished` (and any UI watching one
job) wire a targeted listener without paying the O(N-listeners)
broadcast dispatch cost. `returnvalue` is always `undefined` on the
wire — the events stream does not carry the handler's bytes; pair
with `Queue.getJobResult(jobId)` (and `WorkerOptions.storeResults`)
to read it back.

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
  maxAttempts?: number;
  stalledDetectorEnabled?: boolean;
  stalledInterval?: number;
  removeOnComplete?: unknown;
  removeOnFail?: unknown;
  prefix?: string;
  name?: string;
  runScheduler?: boolean;
  schedulerTickMs?: number;
  storeResults?: boolean;
  resultTtlMs?: number;
  logMaxLen?: number;
  logMaxLineBytes?: number;
  eventsProgressEnabled?: boolean;
  limiter?: LimiterOptions;
}
```

- `concurrency` — max in-flight handler invocations. **Default `100`.**
- `autorun` — whether `.run()` is called on the next microtask. **Default `true`.**
- `drainDelay` — `XREADGROUP BLOCK` timeout in ms. **Default `5000`.**
- `maxStalledCount` — **v1.4 breaking change.** Now routes to engine `max_stalled_attempts` (the stalled-detector ceiling — stall cycles past `idle_threshold_ms` before DLQ-as-`stalled`). **Default `2`** — one extra tick of headroom over BullMQ's `maxStalledCount: 1` to avoid racing the reader's CLAIM-on-read recovery path. Pre-v1.4 this field was mis-routed to `max_attempts` with a `?? 3` fallback. Migration: use `maxAttempts` for the old "cap total attempts" semantic. A one-time `WARN [chasquimq]` log fires per process when `maxStalledCount` is set without `maxAttempts`.
- `maxAttempts` — total handler attempts (initial + retries) before DLQ-as-`retries_exhausted`. Maps to engine `max_attempts`. **Default `25`** (the engine default; an undefined value flows through literally).
- `stalledDetectorEnabled` — toggle the embedded stalled-job detector. **Default `true`.** Set `false` for pure-consumer benchmarks or deployments running a separate detector process.
- `stalledInterval` — detector scan-tick interval (ms). **Default `30_000`.** The embedded spawn overrides this from `claim_min_idle_ms` to preserve the per-crash counting invariant.
- `removeOnComplete`, `removeOnFail` — accepted; no-ops. The engine `XACKDEL`s on success and DLQ-relocates on failure.
- `prefix` — accepted; no-op.
- `name` — optional consumer ID for `XREADGROUP CONSUMER`.
- `runScheduler` — auto-spawn an embedded scheduler. **Default `true`.** Set `false` when running a separate scheduler process.
- `schedulerTickMs` — scheduler tick interval. **Default `1000`.**
- `storeResults` — persist handler return values to `{chasqui:<queue>}:result:<jobId>`. **Default `false`.**
- `resultTtlMs` — TTL for stored results. **Default `3_600_000` (1h).** Rounded up to whole seconds at the FFI boundary.
- `logMaxLen` — `MAXLEN ~` cap on each per-job log stream
  (`{chasqui:<queue>}:log:<id>`). **Default `1000`.** Must be `>= 16`
  (`Consumer::run` rejects sub-minimum values — below that, the
  `MAXLEN ~` rounding can leave the stream effectively empty).
- `logMaxLineBytes` — per-line byte cap for [`Job.log`](#joblogline).
  Lines exceeding this are truncated on a UTF-8 char boundary
  with a `[…truncated]` marker appended. **Default `4096`.**
- `eventsProgressEnabled` — gates the `e=progress` events-stream
  entry emitted by [`Job.updateProgress`](#jobupdateprogressprogress).
  The persisted progress key is always written; this only mutes
  the events fan-out a `QueueEvents` subscriber would observe.
  **Default `true`.**
- `limiter` — global per-queue rate limit. Unset by default (no
  limiting). See [`LimiterOptions`](#limiteroptions) and the
  [Rate limiting concept](/concepts/rate-limiting/).

### `LimiterOptions`

```ts
interface LimiterOptions {
  max: number;
  duration: number;
  groupKey?: string;
}
```

Global per-queue token bucket: at most `max` **jobs** admitted per
`duration` window, shared across **every** worker on the queue —
one bucket in Redis (`{chasqui:<queue>}:limiter`), not one per
worker. Two workers with `{ max: 100, duration: 1000 }` process at
most 100 jobs/second *combined*.

- `max` — jobs per window. **Required; must be `>= 1`.** A fresh or
  idle bucket starts full, so the first window admits a burst up to
  `max` before settling to the `max`/`duration` steady state
  (standard token-bucket behavior).
- `duration` — window length in ms. **Required; must be `>= 1`.**
- `groupKey` — **reserved; rejected in this version.** Passing it
  throws `Error('limiter.groupKey is not supported in this version
  (global per-queue limiter only)')`. Per-key sub-buckets are a
  documented follow-up.

The bucket is evaluated once per read attempt (one `EVALSHA` before
`XREADGROUP`, never per job); a throttle delays the whole read at the
batch boundary, so FIFO is preserved. Distinct from
[`Worker.rateLimit`](#workerratelimitexpiretimems) (the manual
per-invocation call, still `NotSupportedError`).

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
- `attempts` — total attempt budget for this job, overrides queue-wide `maxAttempts`. **Default queue-wide.** When combined with `repeat`, it becomes a **per-fire** override threaded onto every job the spec fires.
- `backoff` — per-job backoff override. Either a plain `number` (fixed delay in ms) or a [`BackoffOptions`](#backoffoptions) object. When combined with `repeat`, it applies per-fire to every job the spec fires.
- `removeOnComplete` — accepted; no-op (engine `XACKDEL`s).
- `removeOnFail` — accepted; reserved for future DLQ trim policy.
- `priority` — accepted; ignored with a one-time console warning. Streams are FIFO.
- `jobId` — stable id for at-most-once / idempotent scheduling. **Throws `NotSupportedError` when combined with `repeat`** — the scheduler mints a fresh id per fire, so a stable id can't stick yet (stable-id-per-fire is a tracked follow-up). Use `repeatJobKey` / the returned spec key as the stable handle for a repeatable spec.
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
- `jobId` — **throws `NotSupportedError`** (same as the top-level `JobsOptions.jobId` on a repeatable add). The scheduler mints a fresh id per fire; explicit-id-per-fire is a tracked follow-up.
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
type JobProgress = number;
```

`JobState` is the engine's job-state classification, used by
[`getJobState`](#queuegetjobstatejobid),
[`getJobs`](#queuegetjobstypes-start-end), and
[`clean`](#queuecleangrace-limit-type). `"prioritized"` /
`"waiting-children"` are accepted on `JobType` for call-site
compatibility but have no engine semantics in v1.

`JobProgress` is `number` — the engine persists it as an ASCII
`u8` (0..=100), so any non-numeric or out-of-range value passed
to [`updateProgress`](#jobupdateprogressprogress) is clamped at
the engine boundary. **Breaking (TS types only):** previously
typed as `number | object` to mirror BullMQ; narrowed to
`number` in the progress-and-logs slice. No runtime impact —
the engine wire format never carried the object form.

### `RemovalReport`

```ts
interface RemovalReport {
  delayed: boolean;
  stream: boolean;
  dlq: boolean;
  result: boolean;
}
```

Returned by [`queue.removeReport`](#queueremovejobid-queueremovereportjobid).
Each field flags whether the job was removed from that surface — all
`false` means the id was not found anywhere (a valid result, not an
error).

## Errors

The shim throws typed errors so application code can branch on
`err.name`:

- [`UnrecoverableError`](/reference/error-codes/#cmq-011--handler-signaled-unrecoverable--dlq) — throw from a processor to skip retries and route to the DLQ.
- [`WaitForResultTimeoutError`](/reference/error-codes/#cmq-102--node-result-wait-timeout) — `Job.waitForResult` timed out.
- `WaitUntilFinishedTimeoutError` — `Job.waitUntilFinished` saw neither a `completed` nor a `failed` event within the supplied `ttl`. Distinct from a failed job: a failed job rejects with `new Error(failedReason)`; this error fires only when the events stream itself goes silent.
- [`NotSupportedError`](/reference/error-codes/#cmq-100--node-feature-not-supported) — caller asked for a v1-stubbed feature.
- [`RateLimitError`](/reference/error-codes/#cmq-101--node-rate-limit) — reserved; tied to the manual `Worker.rateLimit(expireTimeMs)` call (still `NotSupportedError`), not the shipped constructor [`limiter`](#limiteroptions). Bad `limiter` input throws a plain `Error`, not this.

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
