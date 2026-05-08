---
title: Retry and backoff
description: How retries get rescheduled, the backoff math, jitter, per-job overrides, and the unrecoverable short-circuit.
sidebar:
  order: 4
---

When a handler returns `Err` (or panics, or throws), the engine retries. This page is the mechanics: how the retry actually happens, where the state lives, and what the backoff math is.

## What happens on `Err`

The worker:

1. Encodes the job with `attempt += 1` and computes `run_at_ms = now + backoff(attempt)`.
2. Atomically (via one Lua script) `XACKDEL`s the original stream entry and `ZADD`s the re-encoded job onto `{chasqui:<queue>}:delayed` (the delayed sorted set, scored by run-at-ms).
3. The promoter — embedded in every `Worker`, with `SET NX EX` leader election so only one fires per tick — `ZRANGEBYSCORE`s due entries and atomically promotes them back into the stream.
4. The next available worker reads the promoted entry. The handler sees `job.attemptsMade` (Node) / `job.attempt` (Python) incremented.

Key insight: **a retry is a re-publish, not a CLAIM**. The original entry is gone (`XACKDEL`'d), so the consumer group's pending list is clean. Every retry is a new entry from the engine's point of view.

The engine also keeps the CLAIM path as a safety net for crashed workers. If a worker dies mid-handler before the retry path runs, `XREADGROUP CLAIM` re-delivers the entry on the next read; the consumer compares the in-payload `attempt` counter against the Redis-tracked `delivery_count` to detect retry-exhaustion regardless of which path produced the count.

## Backoff math

```text
delay(attempt) = min(initial * multiplier^(attempt-1), max) + uniform(-jitter, +jitter)
```

Two strategies:

- **`fixed`** — every retry waits `delay_ms` (plus jitter).
- **`exponential`** — the delay doubles each attempt (or whatever `multiplier` you set), capped at `max_delay_ms`.

Defaults:

- `initial_backoff_ms`: 100
- `multiplier`: 2.0
- `max_backoff_ms`: 30,000
- `jitter_ms`: 100 (symmetric, ±)

So the default series is approximately:

| Attempt | Delay |
|---|---|
| 1 (initial delivery) | 0 |
| 2 (first retry) | 100ms ±100ms |
| 3 | 200ms ±100ms |
| 4 | 400ms ±100ms |
| 5 | 800ms ±100ms |
| ... | ... |
| 9+ | capped at 30s ±100ms |

## Why jitter

Without jitter, all the jobs that started failing at roughly the same time will retry at exactly the same future time. After enough attempts, every retry hits Redis (and your handler's downstream dependencies) in a synchronized burst — a thundering herd of your own making.

Jitter spreads retries uniformly within a `±jitter_ms` window. Default ±100ms is enough to break synchronization without making the retry feel sloppy. Tighten or widen as your environment dictates.

## Per-job overrides

Per-job `attempts` and `backoff` ride on the encoded `Job<T>` envelope as two optional fields. The engine's worker hot path checks per-job first and falls back to the consumer's `RetryConfig`. The retry-relocator preserves both fields when re-encoding for the next attempt — overrides survive every retry.

```ts
await queue.add(
  "expensive-render",
  data,
  {
    attempts: 10,
    backoff: { type: "exponential", delay: 1_000, maxDelay: 5 * 60_000 },
  },
);
```

Use cases for per-job overrides:

- A rare expensive job that warrants more attempts than your queue-wide default.
- A "best effort" job that should give up after one retry.
- A job that hits a stricter rate limit and needs a longer backoff floor.

## The unrecoverable short-circuit

Throwing `UnrecoverableError` from a handler bypasses the retry budget entirely. The engine routes the entry to DLQ on the same delivery, with `DlqReason::Unrecoverable`. No further attempts.

The detection is name-based: any error whose constructor (or class) is named `UnrecoverableError` triggers the short-circuit. Subclasses work — `class PoisonPill extends UnrecoverableError {}` (Node) or `class PoisonPill(UnrecoverableError)` (Python) get the same routing.

This is the right tool when:

- The payload is malformed and rerunning won't help (`schema validation failed`).
- The downstream resource is permanently gone (`user deleted, address bounces`).
- The handler is being asked to do something illegal (`exceeded plan limits`).

It is **not** the right tool for transient errors. A timeout on a downstream API should `throw new Error(...)` and let the retry path handle it.

## Panics also go to DLQ

A handler that throws an uncaught exception (panic in Rust) does not retry. The engine routes the entry to DLQ with `DlqReason::Panic`. The reasoning: a panicking handler is a code bug, not a transient failure, and retrying just thrashes.

If your handler does its own error handling and surfaces `Err` for transient failures, you get the retry path. Panics are reserved for "I don't know how to deal with this." That's the right escalation.

## What about repeatable specs?

Per-fire retry overrides on a `repeat` spec are not threaded yet. `attempts` / `backoff` on the upsert call are accepted for symmetry with `Queue.add` but ignored at the wire layer; the fired job uses queue-wide defaults.

This is a 1.x follow-up — the engine path is straightforward but the test surface (every `MissedFiresPolicy` × every retry policy) is wider than the slice budget allowed for 1.0.

## Operational notes

- **Retries observe `MetricsSink`.** Every reschedule emits a `RetryScheduled` event with `attempt` and `backoff_ms`. Useful for dashboards: a sudden spike in `RetryScheduled` is your "retries are climbing" signal.
- **The promoter is the bottleneck on retry-heavy queues.** Default tick is 100ms. If you have a tight retry loop (say, 5ms backoff between attempts), the promoter's tick interval becomes the floor on retry latency. Tighten via `PromoterConfig.poll_interval_ms` (Rust) or accept the floor.
- **`worker-retry-throughput` is one of the bench scenarios.** ChasquiMQ measures retry throughput separately — see [benchmarks](/benchmarks/the-1-0-numbers/) for the number on this hardware.

For configuration: [Configure retries](/guides/configure-retries/). For DLQ: [DLQ and recovery](/concepts/dlq-and-recovery/).
