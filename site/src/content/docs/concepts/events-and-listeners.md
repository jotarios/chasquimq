---
title: Events and listeners
description: Two layers of events — in-process Worker listeners and the cross-process events stream — and when to reach for each.
---

ChasquiMQ exposes job-lifecycle events at two layers. Both surfaces use familiar `EventEmitter`-style names so existing application code reads naturally, but they have different scope, cost, and delivery semantics. Pick the right one for the job.

## The two layers

**In-process `Worker` events** are plain `EventEmitter` calls inside the worker that's running your processor. They fire on that worker only — no Redis traffic, no cross-process fan-out. Use them to observe what *this* worker is doing.

**Cross-process `QueueEvents`** is a subscription to the per-queue events stream (`{chasqui:<queue>}:events`). The engine `XADD`s a small ASCII entry on every transition; any process running a `QueueEvents` instance for the same queue sees every event, regardless of which worker emitted it. Use it to observe the queue as a whole, or to coordinate between processes (a producer awaiting a worker's result, a dashboard tailing transitions, a metrics sidecar).

The two surfaces share event names where it makes sense. `Worker.on('completed')` fires for jobs *this* worker processed; `queueEvents.on('completed')` fires for **every** job completed on the queue.

## What the engine emits

The events stream carries these transitions, all best-effort (a network blip on the events stream cannot delay an ack or cause a job to be retried):

| Event | Fired by | Payload highlights |
|---|---|---|
| `waiting` | Producer (`XADD` to main stream) or promoter (delayed job became eligible). | `jobId`, `name` |
| `active` | Worker, before invoking the processor. | `jobId`, `name`, `attempt` |
| `completed` | Worker, after the processor returns. | `jobId`, `name`, `attempt` |
| `failed` | Worker, after the processor raises. | `jobId`, `name`, `failedReason`, `attempt` |
| `retry-scheduled` | Retry relocator, after an atomic reschedule onto the delayed ZSET. | `jobId`, `name`, `attempt`, `backoffMs` |
| `dlq` | DLQ relocator, after writing the entry to the DLQ stream. | `jobId`, `name`, `reason`, `attempt` |
| `progress` | Worker, after the handler's `updateProgress(n)` SET succeeds. | `jobId`, `name`, `progress` |
| `stalled` | Stalled-job detector, when a PEL entry is observed idle past `idle_threshold_ms` for the `attempt`-th consecutive scan (under threshold; the relocate path emits a separate `dlq` event with `reason='stalled'`). | `jobId`, `name`, `attempt`, `prev` (always `'active'`) |
| `drained` | Reader, on a full→empty transition (not on every empty poll). | (queue-scoped) |

`retries-exhausted` is a synthetic alias of `dlq` (with the `reason` carried as the engine's `DlqReason` string) that the Node shim emits to match existing high-level-shim subscribers.

`progress` is best-effort fan-out — the persisted progress key (`{chasqui:<queue>}:progress:<id>`) is the source of truth. A failed events XADD never propagates back to the handler. High-rate progress reporters that don't need cross-process fan-out can mute the events with `WorkerOptions.eventsProgressEnabled: false` (Node) / `Worker(events_progress_enabled=False)` (Python); the persisted key is still written.

`stalled` is leader-emitted (the stalled-job detector is leader-elected per queue), so every worker on the queue receives the event regardless of which one held the stalled entry. `Worker.on('stalled', (jobId, prev) => ...)` mirrors the BullMQ two-arg payload; `prev` is always `'active'` because every stalled entry was PEL-resident when the detector saw it. See [DLQ and recovery](./dlq-and-recovery.md) for the detection + relocate semantics.

## Per-id channels

For events that carry a `jobId` (`active`, `completed`, `failed`, `progress`, `stalled`), `QueueEvents` also fans the event onto a per-id channel named `<event>:<jobId>`. Targeted subscribers (like `Job.waitUntilFinished` / `Job.wait_until_finished`) listen there directly instead of filtering every broadcast event by id — at large fan-out this is the difference between an `O(N-listeners)` dispatch tax and an `O(1)` one.

The channel naming convention is `<event>:<jobId>` (e.g. `completed:<jobId>` / `failed:<jobId>` / `progress:<jobId>`). Power users can subscribe directly:

```ts
events.on(`completed:${jobId}`, ({ jobId }) => { /* this job, done */ });
events.on(`failed:${jobId}`, ({ failedReason }) => { /* this job, failed */ });
events.on(`progress:${jobId}`, ({ progress }) => { /* this job, 0..=100 */ });
```

## The return-value choice

`completed` events from the events stream do **not** carry the handler's return bytes. Two reasons:

1. **Subscriber-fan-out cost.** A 1 MB result on a queue with 50 dashboards subscribed would push 50 MB across the events stream per job. Keeping the events stream small keeps the cross-process observation surface cheap.
2. **Result storage is opt-in.** Workers run with `storeResults: false` by default; persisting nothing is the cheapest path through the engine. Carrying the value on every event would invert that default.

The contract is: the events stream tells you *that* a job completed; if you need the value, fetch it from the result key via `Queue.getJobResult(jobId)` and run the worker with `storeResults: true`. The `Job.waitUntilFinished(queueEvents, ttl?)` helper composes both — it listens for the event and fetches the result after.

## Awaiting a single job's completion

Two helpers, both on `Job`, with different races:

- **`Job.waitForResult({ timeoutMs })`** (Node) / **`Job.wait_for_result(timeout=)`** (Python) — *polls* the result key. Requires `storeResults: true` to detect completion at all. Wins when the result key was written *before* the wait started (a job that finished a few seconds before you called).
- **`Job.waitUntilFinished(queueEvents, ttl?)`** (Node) / **`Job.wait_until_finished(queue_events, timeout=)`** (Python) — *subscribes* to the per-id channels on the events stream. Detects completion even when `storeResults` is off. Loses to a job that completed before the listeners wired up.

For low-latency awaits of jobs you just enqueued, `waitUntilFinished` is the right call (no polling tax, no need for the result backend). For "did this id ever finish" lookups, `waitForResult` is the right call. The two compose: in many deployments you set `storeResults: true` and use `waitUntilFinished` exclusively — it both detects completion via the event and fetches the persisted value.

## Lazy subscriber lifecycle

`Worker.on('drained', ...)` and `Worker.on('progress', ...)` are the only `Worker` events that require cross-process traffic. The shim lazily spawns one embedded `QueueEvents` subscriber the first time *either* listener attaches; it's torn down on `Worker.close()`. Workers that never subscribe pay no extra Redis connections — this is a strict zero-overhead-when-unused contract.

The same lazy pattern applies on the Python `Worker`. Cross-shim symmetric.

## Muting the `progress` event for high-rate handlers

`Job.updateProgress(n)` always writes the persisted progress key (`{chasqui:<queue>}:progress:<id>` STRING, TTL = `result_ttl_secs`). On top of that, by default it also emits a cross-process `e=progress` events-stream entry so subscribers see live updates.

For a handler that reports progress hundreds of times per job — a large file upload reporting per-chunk percent, a streaming media transcode, an ML training loop — the events fan-out can dominate Redis traffic without adding observability value (operators rarely watch sub-second progress). Mute the fan-out without losing the persisted state:

- Node: `new Worker(name, handler, { eventsProgressEnabled: false, ... })`
- Python: `Worker(queue_name, handler, events_progress_enabled=False, ...)`

The persisted progress key still updates on every call, so `Queue.getJob(id).progress` (introspector) returns the latest value. Only the events-stream `progress` channel (broadcast and per-id) goes quiet.

## Lost-event race

Events emitted *before* a subscriber's first `XREAD BLOCK` lands are missed. The shims minimise this race:

- The Node `Worker`'s embedded drained subscriber awaits `XREAD BLOCK` flush via a `setImmediate` yield before releasing the worker's startup gate.
- The Python `QueueEvents` exposes `await events.wait_until_ready()` — callers needing a deterministic "subscriber is listening" gate can await it before producing work.

In production this rarely matters: subscribers attach early in process startup and the events flow continuously. In tests, prefer `wait_until_ready` (Python) or the `await events.waitUntilReady()` plus a small `await new Promise(r => setImmediate(r))` (Node) to close the window.
