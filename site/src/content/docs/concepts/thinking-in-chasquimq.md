---
title: Thinking in ChasquiMQ
description: The mental model. Producer, Consumer, Stream, Job, DLQ — one diagram, then the details.
sidebar:
  order: 1
---

If you remember one thing from this page:

> A job is a row in a Redis Stream, an ack is a delete from a pending entries list, and a DLQ is just another stream with a different name.

Everything else is detail.

## The five primitives

Five names recur through every layer of the codebase, every API, and every operational tool. Get these and you can read the engine source.

| Name | What it is |
|---|---|
| **Producer** | The thing that puts jobs into a queue. `Queue.add(...)` on the shims. |
| **Consumer** | The thing that pulls jobs out and runs your handler. `Worker(handler)` on the shims. |
| **Stream** | The Redis Stream that holds jobs. One per queue, named `{chasqui:<queue>}`. |
| **Job** | A single MessagePack-encoded entry on the stream, plus a name. The unit of work. |
| **DLQ** | A second Redis Stream where un-processable jobs land. Named `{chasqui:<queue>}:dlq`. |

The runner stays implicit. The Producer puts a message into the relay; the Consumer carries it; the Stream is the trail; the Job is the message; the DLQ is the bin where un-deliverable mail goes.

## The flow

```text
                         producer.add()
                                │
                                ▼
                    ┌───────────────────────┐
                    │   Redis Stream        │   {chasqui:<queue>}
                    │   (XADD entries)      │
                    └─────────┬─────────────┘
                              │ XREADGROUP (batched)
                              ▼
                    ┌───────────────────────┐
                    │   Consumer reader     │   one tokio task
                    │   (CLAIM idle pending)│
                    └─────────┬─────────────┘
                              │ async-channel fan-out
                              ▼
                    ┌───────────────────────┐
                    │   Worker pool         │   N tokio tasks
                    │   (your handler runs) │   N = concurrency
                    └─────────┬─────────────┘
                              │
                ┌─────────────┴──────────────┐
                ▼                            ▼
         handler returns Ok            handler returns Err
                │                            │
                ▼                            ▼
         ┌──────────────┐             ┌──────────────┐
         │  XACKDEL     │             │  retry path  │
         │  (atomic)    │             │  (Lua)       │
         └──────────────┘             └──────┬───────┘
                                             │
                            attempts < max ──┴── attempts >= max
                                  │                       │
                                  ▼                       ▼
                         ┌────────────────┐      ┌────────────────┐
                         │ delayed ZSET   │      │  DLQ stream    │
                         │ (re-promoted)  │      │ {...}:dlq      │
                         └────────────────┘      └────────────────┘
```

Five tasks: producer, reader, workers, retry relocator, DLQ relocator. The reader does only one thing — pull batches off the stream and CLAIM idle pending. The workers do only one thing — invoke your handler and report the outcome. The relocators run on dedicated tasks so handler latency doesn't block reading, and reading doesn't block acking.

## What happens on `Queue.add(name, data)`

1. The shim msgpack-encodes `data` to a `Buffer` / `bytes`.
2. The native `Producer.add(name, payload)` issues `XADD {chasqui:<queue>}` with two fields: `d` (msgpack payload bytes) and `n` (UTF-8 dispatch name).
3. Redis returns the entry ID — a millisecond timestamp plus a sequence counter (`1731072123-0`). That's your `job.id`.

Edge cases:

- `delay > 0` → instead of `XADD`, the engine writes to the delayed sorted set (`{...}:delayed`, scored by run-at-ms). The promoter task, embedded in every `Worker`, periodically `ZRANGEBYSCORE`s due entries and atomically promotes them via Lua.
- `repeat: { ... }` → no immediate enqueue. Instead, the producer upserts a *spec* into `{...}:repeat` (sorted set keyed by next fire time) plus `{...}:repeat:specs` (hash of full spec body). The scheduler — also auto-spawned by `Worker` — fires fresh jobs from the spec on each window.
- `addUnique(name, data, { jobId })` → the producer pre-pends a Lua-gated `SET NX EX` dedup marker (`{...}:dlid:<job_id>`) for delayed jobs, or uses Redis 8.6 `XADD ... IDMP <producer_id> <job_id>` for immediate jobs. Either way, a duplicate `addUnique` returns the same id, no second entry.

## What happens on the consumer side

1. The reader loop calls `XREADGROUP CLAIM ...` (Redis 8.4 idle-pending reads, one round trip for new + idle entries).
2. Each batch fans out via a bounded `async-channel` to up to `concurrency` worker tasks.
3. A worker decodes the msgpack `d` field, builds a typed `Job`, and calls your handler.
4. On `Ok`, the worker pushes the entry ID into a bounded ack channel. The ack flusher batches up to `ack_batch` IDs (default 256) or waits up to `ack_idle_ms` (default 5ms) and pipelines them as one `XACKDEL` call.
5. On `Err`, the worker writes to the retry relocator's channel. The relocator runs the retry Lua script: `XACKDEL` the original entry, increment `attempt`, `ZADD` to delayed set with backoff. The promoter will pick it up at run-at time.
6. On `Err` with `attempt + 1 >= max_attempts`, the relocator routes to DLQ instead — `XACKDEL` original, `XADD` to `{...}:dlq` with `reason: retries_exhausted`.

## The two streams

Both the main queue and the DLQ are Redis Streams. They have the same shape, the same operational primitives, the same observability story. The only differences:

- The DLQ has no consumer group. Nothing reads from it automatically.
- The DLQ is bounded (`dlq_max_stream_len`, default 100k, via `XADD MAXLEN ~ N`).
- DLQ entries carry an extra `reason` field (`retries_exhausted` / `unrecoverable` / `decode_fail` / `malformed` / `oversize`).

This symmetry is deliberate. You inspect the DLQ with the same tools you inspect the main queue (`chasqui inspect`, `XLEN`, `XRANGE`). You replay from DLQ with one Lua script (`XACKDEL` from dlq, `XADD` to main). There is no schema change between "live" and "failed."

## The retry path is a re-publish

A retry is not a CLAIM. The consumer doesn't keep the entry pending; it acks the original and writes a fresh entry to the delayed ZSET with `attempt + 1`. The promoter then promotes the fresh entry into the stream when its delay is up.

Why: the original entry is gone (`XACKDEL`'d), so the consumer group's pending list is clean. There is no "stuck retry" — every retry is a new entry from the engine's point of view.

The CLAIM path is the safety net for crashed workers. If a worker dies after reading but before the retry path runs, `XREADGROUP CLAIM` re-delivers the entry on the next read; the consumer compares the in-payload `attempt` counter against the Redis-tracked `delivery_count` to detect retry-exhaustion regardless of which path produced the count.

## Repeatable specs are not jobs

A repeatable spec is a *recipe*. The engine fires fresh jobs from it on each window — a new ULID, a new entry on the stream, a new ack/retry/DLQ lifecycle. The spec itself lives in the `:repeat` ZSET and the `:repeat:specs` hash; it never enters the stream.

This is why `chasqui repeatable list` reports specs by their resolved key (`<jobName>::<patternSignature>`), not by job id — there is no single id, just the recipe.

The `MissedFiresPolicy` (`skip` / `fire-once` / `fire-all`) decides what the scheduler does on its first tick after a window was missed. Default is `skip` — drop missed windows, no thundering herd after a deploy. See [Schedule repeatable jobs](/guides/schedule-repeatable-jobs/).

## Names on the wire

Every stream entry has two MessagePack fields plus an unencoded UTF-8 name:

| Field | What |
|---|---|
| `d` | Msgpack-encoded user payload (`data` from `Queue.add`) |
| `n` | UTF-8 dispatch name (`name` from `Queue.add`) |

The name is on the wire because it's framing-layer metadata, not part of the typed payload. The handler gets `job.name` for cheap; metrics carry `name` as a label; `chasqui events` renders it as a column. No msgpack decode required to filter by name.

This is why a Python producer and a Node worker can drain the same queue: both shims write `n` and `d` the same way; both decode `d` with the same MessagePack library on each side. The wire format is the lingua franca.

## Hash tags

Every key looks like `{chasqui:<queue>}:<suffix>`. The braces are Redis Cluster hash tags — they tell Redis "everything between the braces is the routing key, ignore the rest." This guarantees that the queue's stream, delayed ZSET, repeatable ZSET, repeatable specs hash, events stream, DLQ, and per-job result keys all live on the same cluster slot.

This is why operations like "atomically `XACKDEL` from main and `ZADD` to delayed" work — they're on the same node. A user-supplied `keyPrefix` would either break this or require a parallel layout, which is why the shim throws on `keyPrefix`.

## What ChasquiMQ does NOT do

- **No `LPUSH` / `BRPOP`.** Streams are the queue primitive. Every "I'd just use a list" pattern (per-consumer pending tracking, idempotent retries, idle-claim recovery) is a feature you'd have to invent on top of lists; Streams give it for free.
- **No JSON.** The engine's hot path is binary msgpack via `rmp-serde`. JSON is more bytes, slower to encode, and lossy on `bytes` / `Date` / `bigint`. The shims still let you pass JSON-shaped data — they encode it as msgpack at the boundary.
- **No blocking Lua scripts.** Every Lua script in the engine is short, side-effect-bounded, and never blocks. The PRD calls out blocking Lua as one of the bottlenecks ChasquiMQ exists to escape.
- **No per-job round trips.** Acks batch. Retries batch where they can. Reads batch. The only un-batchable per-job operation is the result-write path when `storeResults: true`, and that's why result storage is opt-in.
- **No exchange / routing layer.** One queue per concern. A multi-queue topology is just multiple `Worker` instances.

## The summary, again

A job is a row in a Redis Stream. An ack is a delete from a pending entries list. A DLQ is just another stream with a different name. Everything else is mechanism.

Once that frame holds, the rest of the engine reads quickly: the retry path becomes "ack and re-publish", the scheduler becomes "promoter for the spec ZSET", `MetricsSink` becomes "every transition emits one event", and `addUnique` becomes "Lua gate on the publish."

For the wire: [Redis Streams primer](/concepts/redis-streams-primer/). For the safety: [Delivery semantics](/concepts/delivery-semantics/). For the constraints: [Architecture decisions](/concepts/architecture-decisions/).
