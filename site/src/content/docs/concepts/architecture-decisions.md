---
title: Architecture decisions
description: Rust + tokio, Redis Streams, MessagePack, no Lua-blocking, pipelined acks. Why each, and what the alternative was.
sidebar:
  order: 9
---

The five load-bearing constraints. Each is a deliberate choice with a measurable consequence; each rules out a class of "obvious" alternatives.

## 1. Rust on the `tokio` async runtime

**The decision.** The whole engine is Rust. Not "Rust core with Node bindings" or "Rust hot path with JS framing" — every line of queue logic is Rust, exposed to other languages via FFI bindings (NAPI-RS for Node, PyO3 for Python).

**The alternative was.** Build the engine in Node, call it "fast Node," and trust that V8 will catch up. Or build in Go — fewer footguns than C++, decent FFI story, mature `redigo` client.

**Why Rust.** Tokio gives the best async I/O story in any language for the job-queue shape (millions of small concurrent operations). The borrow checker rules out a class of races that a single-threaded Node implementation papers over by being single-threaded. Cross-language FFI is mature: NAPI-RS and PyO3 are both production-ready. The cost — Rust expertise on the maintainer side, longer compile times — is real but a one-time cost, not a per-job cost.

**The consequence.** A handler that runs at the engine's hot path doesn't pay any GC pause. A `tokio::spawn` is ~100ns. The reader and the workers and the relocators all run as cooperative async tasks on a thread pool, not OS threads.

## 2. Redis Streams as the queue primitive

**The decision.** Every job is a Redis Stream entry. Every retry is a re-publish. Every DLQ relocation is an `XADD` on a different stream. No `LPUSH`, no `BRPOP`, no list-based queue patterns anywhere in the engine.

**The alternative was.** Use Redis lists with `BRPOP` — the legacy queue pattern, what most Redis-backed queues do. Or use a separate broker like Kafka or NATS.

**Why Streams.** Lists give you nothing for free: per-consumer pending tracking, idle-claim recovery, deterministic IDs, atomic delete-on-ack — all features you'd build yourself, badly, on top of `LPUSH`/`BRPOP`. Streams give all of them, with a stable wire protocol, with ten years of operational history.

A separate broker (Kafka, NATS) was rejected because the PRD's target audience already has Redis. ChasquiMQ's reason to exist is "the throughput of Kafka, but you don't have to deploy Kafka." If Kafka is on the table, you don't need ChasquiMQ.

**The consequence.** The engine inherits Redis's clustering, replication, and persistence stories for free. Operationally, "Redis is up" means "ChasquiMQ is up." There is no second piece of infrastructure to monitor.

## 3. MessagePack on the wire (`rmp-serde`)

**The decision.** Every job payload is MessagePack. Every retry envelope is MessagePack. Every spec body is MessagePack. JSON does not appear in the engine's hot path.

**The alternative was.** JSON. It's universal, every language has a parser, the wire format is human-readable. Or Protobuf — schema-aware, binary, with codegen.

**Why MessagePack.** Smaller (~30–50% vs JSON for typical typed payloads), faster to encode, and lossless for `bytes` / `Date` / `bigint` — all things the BullMQ-style JSON producers silently mangle. `rmp-serde` works with `serde::Serialize` derives, so user types serialize without runtime schema. No codegen.

Protobuf was rejected because schema management across heterogeneous producers (Python writing, Rust reading, Node reading, all on the same queue) means shipping `.proto` files alongside the queue. MessagePack with `serde` lets each language describe its own type and trust the wire format.

**The consequence.** A Python producer and a Node worker drain the same queue without translation. Both shims write `n` and `d` the same way; both decode `d` with the same MessagePack library. Cross-language interop is free.

## 4. No blocking Lua scripts

**The decision.** Every Lua script the engine ships is short, side-effect-bounded, and never blocks. The longest is the schedule-repeatable script (variable-length payload list, but no loops on Redis state). No `WAIT`, no `BLPOP`-equivalent, no script-level retries.

**The alternative was.** Blocking Lua. Sidekiq Pro and BullMQ both run multi-second Lua scripts under contention; Redis serializes script execution per node, so a 2-second script blocks every other operation on that slot for 2 seconds.

**Why no blocking Lua.** It's the bottleneck most "fast Redis queue" claims silently run into. The engine's Lua scripts are: retry-reschedule (atomic XACKDEL + ZADD), promote (XRANGEBYSCORE + XADD batch), DLQ replay (XACKDEL + XADD), schedule-repeatable (XADD batch + ZADD), idempotent schedule (SET NX EX + ZADD), cancel (ZREM + DEL). Each is microseconds, none blocks.

**The consequence.** Multiple workers per Redis instance contend on the script cache, not on script execution. Throughput scales linearly with Redis CPU.

## 5. Pipelined, batched `XACK` (and `XACKDEL`)

**The decision.** Acks accumulate in a bounded async channel and flush as a single Redis pipeline when either `ack_batch` jobs accumulate (default 256) or `ack_idle_ms` elapse with no new acks (default 5ms).

**The alternative was.** Ack one job at a time, the moment the handler finishes. The naive default in every queue tutorial.

**Why batch.** A handler that returns in 10ms followed by an ack that takes 1ms means the worker is 10% blocked on Redis. Multiply by N handlers in flight and N round trips per worker — Redis becomes the bottleneck before your handlers do. Batching means N handlers' acks pipeline as one round trip; the ack cost amortizes.

**The consequence.** The headline `worker-concurrent` number (~419k jobs/s on a quiet host, ~112k on a contended host) is dominated by handler dispatch latency, not ack latency. Per-job round trips are not on the hot path.

## What ChasquiMQ doesn't do

- **No PRD-listed feature gets built unless we have a benchmark for it.** The performance is the product. Adding features that move the headline number sideways isn't a free choice.
- **No fallback for older Redis.** The engine targets Redis 8.6+ (April 2026 release) for `IDMP`, `XACKDEL`, and idle-pending reads. There's no `if redis_version < 8.x` branch. Operating Redis < 8.6 is the project's "out of scope" boundary.
- **No web UI.** The PRD explicitly out-of-scopes "complex UI dashboards." The CLI is the operator surface; the events stream is the integration point.
- **No priority queue.** Streams are FIFO by construction; a parallel priority ZSET would defeat batched `XREADGROUP`. Tracked as a v1.x candidate — the design decision (interleaved reads vs. per-job reordering) needs measurement, not opinion.
- **Rate limiting shipped as a global per-queue token bucket.** The original reservation was that a consumer-side limiter adds a per-job round trip. The shipped design avoids that: the bucket is evaluated once per read attempt (one `EVALSHA` before `XREADGROUP`, never per job), the throttle delays the whole read at the batch boundary so FIFO is preserved, and a `redis.call('TIME')` shared clock lets N workers spend against one global bucket with no per-worker skew. `max` jobs per `duration` is a queue-wide ceiling, admitting a first-window burst up to `max` from a cold/idle bucket (standard token-bucket). Per-key (name-scoped) sub-buckets are **reserved for a follow-up** — the `groupKey` field is parsed but rejected today. See [Rate limiting](/concepts/rate-limiting/).

## Composing constraints

Each constraint above is independently load-bearing. The combination is more than the sum:

- **Rust + tokio** means async I/O is cheap, so we can afford **batched, pipelined acks** without blocking a worker thread per ack-flush.
- **Streams + MessagePack** means cross-language shims are wire-compatible, so we can ship native Node and Python bindings against one Rust engine.
- **No blocking Lua + Streams** means Redis is the throughput bottleneck, not the engine, so we can scale with Redis CPU.
- **Idempotent producer (Redis 8.6) + atomic ack-and-delete (Redis 8.2) + idle-pending reads (Redis 8.4)** means the engine is wire-correct against a modern Redis without inventing application-level state machines.

These are the load-bearing decisions. Everything else in the engine — the channel sizes, the relocator architecture, the metrics surface, the shim choices — is downstream of these five.

For the implementation history: [Engine internals (engine.md)](https://github.com/jotarios/chasquimq/blob/main/docs/engine.md). For the trade-offs: [Performance trade-offs](/concepts/performance-trade-offs/).
