---
title: Result backends
description: Why result storage is opt-in, how polling vs. push compares, and the 7.1% throughput cost.
sidebar:
  order: 6
---

By default, ChasquiMQ workers ack the job and discard the handler's return value. Result storage is opt-in — you set `storeResults: true` (Node) / `store_results=True` (Python), and the engine persists the handler's return value to a Redis key with TTL.

This page explains why opt-in, what the cost is, and when to use polling vs. the events stream.

## Why opt-in

The default is no result storage because:

1. **Most jobs don't need it.** Send-email, log-event, push-notification — handlers that have side effects but no value to return. Persisting `undefined` for every job would be pure overhead.
2. **The default ack path is faster.** When `storeResults: false`, the worker pushes the entry ID into a bounded ack channel and the ack flusher pipelines a single `XACKDEL` per batch. Acks batch by default — the engine's headline `worker-concurrent` number assumes the batched-ack path.
3. **The opt-in path doesn't batch.** Result writes are per-entry `EVALSHA` (with `NOSCRIPT` fallback), one Redis round trip per ack. The 7.1% cost (see below) comes from giving up the batched-ack fast path.

If every job needed a stored result, we'd ship it on by default. Most don't.

## How it works

When `store_results=true` and the handler returns a non-`None` / non-`undefined` value:

1. The worker msgpack-encodes the return value.
2. A single Lua script atomically:
   - `XACKDEL`s the stream entry.
   - `SET`s `{chasqui:<queue>}:result:<job_id>` to the encoded bytes with `EX <ttl>`.

`Producer::get_result(job_id)` (Rust), `Queue.getJobResult` (Node), and `Queue.get_job_result` (Python) `GET` the key. `None` / `undefined` for three indistinguishable cases:

- The job has not yet completed.
- The result key already expired.
- No result was ever written (handler returned `None`, worker ran with `storeResults: false`, job was DLQ'd).

## Polling: `waitForResult` / `wait_for_result`

`Job.waitForResult({ timeoutMs })` (Node) and `job.wait_for_result(timeout=...)` (Python) are polling helpers that loop on `getJobResult` until the result is readable, the timeout fires, or (Node) the supplied `AbortSignal` aborts.

```ts
const job = await queue.add("render", spec);
const result = await job.waitForResult({ timeoutMs: 30_000, intervalMs: 100 });
```

Polling is fine for a few concurrent waiters. Each waiter issues one `GET` per `intervalMs`, so 10 concurrent waiters means 10 GETs per 100ms — manageable.

For higher fan-out, prefer the events stream.

## Push: `QueueEvents`

The engine emits a `completed` event onto `{chasqui:<queue>}:events` on every successful ack. Subscribers `XREAD BLOCK` the stream and react in real time:

```ts
import { QueueEvents } from "chasquimq";

const events = new QueueEvents("renders", { connection });
events.on("completed", (ev) => {
  console.log("done:", ev.jobId);
});
```

The advantage of push:

- **No polling tax.** Subscribers block on Redis until an event arrives.
- **Fan-out is free.** Every subscriber sees every event; no per-subscriber GET cost.
- **Lower latency.** The event fires at the moment of ack; no wait for the next polling interval.

The disadvantage:

- The `completed` event carries the job id and metadata, but **not** the return value bytes. To get the return value, subscribe + then `getJobResult`. So you save the polling cost but still pay one GET per result.

## When to use each

| Pattern | Use |
|---|---|
| Producer needs the result for one or two jobs (request/response) | `waitForResult` |
| Many concurrent waiters (>10) on the same queue | `QueueEvents` + `getJobResult` |
| You only need to know "did it complete," not the value | `QueueEvents` (skip the GET) |
| You don't need any result at all | Default (`storeResults: false`) |

## The 7.1% throughput cost

Measured on the same M3 host that produces the headline numbers. Opt-in result writes deliver **7.1% of opt-out throughput** at concurrency=100 ([`benchmarks/store-results-opt-in.md`](https://github.com/jotarios/chasquimq/blob/main/benchmarks/store-results-opt-in.md)).

The cost source is the per-entry `JOB_OK_SCRIPT` `EVALSHA` instead of the batched `XACKDEL` fast path. The script does the same `XACKDEL` (no extra Redis writes besides the result `SET`), but it runs once per ack instead of once per ack batch.

If you need both high throughput and result storage, two options:

- Use `storeResults: true` only for the queues / jobs that need it. Pure side-effect queues (email, logging) stay default.
- Reach for `QueueEvents` instead, accept the one-extra-GET cost, and keep the default ack path.

## TTL semantics

`result_ttl_ms` (Python) / `resultTtlMs` (Node) defaults to 1 hour. Behavior:

- Rounded up to whole seconds at the FFI boundary (Redis `EX` accepts integer seconds).
- Applied at write time. Results expire `result_ttl_ms` after the handler completed, not after the job was enqueued.
- Default 1h is a balance between "long enough for any reasonable polling consumer" and "short enough that DLQ'd jobs don't leave stale result keys around."

If your `waitForResult` timeout is longer than the TTL, you have a race: the handler can succeed, write the result, the result expires, and `waitForResult` times out even though the handler completed. Rule of thumb: `resultTtlMs >= timeoutMs * 2`.

## Behavior under `maxmemory` eviction

`JOB_OK_SCRIPT` carries the `flags=allow-oom` shebang and wraps the result `SET` in `redis.pcall`, so the `XACKDEL` always commits even when Redis is at the `maxmemory` ceiling.

The two policies that matter:

- **`noeviction`** — when memory is at the cap, Redis rejects new writes with OOM. The script's `XACKDEL` runs (it frees memory). The `SET` may be rejected; `pcall` swallows it and the script returns success. The job is acked; the result may be missing. `getJobResult` returns `None`.
- **`allkeys-lru` / `allkeys-lfu`** — writes succeed by evicting older keys. Result keys are eligible for eviction (no protection by hash tag); a tight cap will reap older results before their TTL. `getJobResult` returns `None` for evicted keys; the engine never observes the eviction.

What's guaranteed regardless: every accepted handler delivery either acks cleanly or reclaims via CLAIM after a worker crash. There is no "result `SET` failed and entry is stuck pending forever." What's **not** guaranteed: that the result was written. Treat `None` from `getJobResult` as ambiguous.

For deterministic completion-detection, use `QueueEvents` — the `completed` event always fires on ack, regardless of `maxmemory` policy.

For configuration: [Enable result storage](/guides/enable-result-storage/).
