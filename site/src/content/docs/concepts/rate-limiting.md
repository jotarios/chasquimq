---
title: Rate limiting
description: Global per-queue token bucket — one shared budget across all workers, evaluated once per read, delay-the-whole-read, and the cold-start burst allowance.
sidebar:
  order: 9
---

ChasquiMQ's rate limiter caps how many **jobs** a queue processes per time window. The limit is **global per queue** — a single token bucket in Redis, drawn down by every worker on the queue, not a per-worker budget. Set `max = 100, duration = 1000` and two workers process at most 100 jobs/second *combined*, not 200.

```ts
// Node — a WorkerOptions field.
const worker = new Worker("emails", handler, {
  connection,
  limiter: { max: 100, duration: 1000 },  // 100 jobs / 1000 ms, queue-wide
})
```

```python
# Python — flat kwargs.
worker = Worker(
    "emails", handler, redis_url=url,
    rate_limit_max=100, rate_limit_duration_ms=1000,
)
```

```rust
// Rust — ConsumerConfig.rate_limit.
let cfg = ConsumerConfig {
    rate_limit: Some(RateLimit { max: 100, duration_ms: 1000, group_key: None }),
    ..Default::default()
};
```

## One global bucket, not one per worker

The core of the design: `max` jobs per `duration` is a **queue-wide** ceiling. A single token bucket lives in Redis under `{chasqui:<queue>}:limiter` — a hash carrying the current token count and the last-refill timestamp. Every worker on the queue spends from that one bucket, so adding workers raises throughput up to the cap and no further.

Because the bucket key carries the same `{chasqui:<queue>}` hash tag as the stream, it co-locates on one Redis Cluster slot — the limiter is cluster-correct with no extra machinery.

## Evaluated once per read, never per job

A rate limiter that checked a bucket per job would add exactly the per-job Redis round trip the engine exists to avoid. Instead the bucket is evaluated **once per read attempt**: before each `XREADGROUP`, the reader runs one `EVALSHA` against a small token-bucket script. A batch of 64 jobs costs one limiter round trip, not 64. When the limiter is unset the check is a single skipped branch — zero overhead.

The script uses `redis.call('TIME')` as a **shared clock**, the same pattern the delayed-job promoter uses. Every worker refills and spends against the same server-side timestamp, so there is no per-worker clock skew to reconcile. If a token is available the script grants it and returns `0`; otherwise it returns a positive `wait_ms`.

The reply is parsed **fail-closed**: any shape the engine can't read as a non-negative integer is treated as "throttle." A limiter that failed *open* would silently over-admit — worse than no limiter — so the ambiguous case errs toward waiting.

## Delay the whole read — FIFO is preserved

When throttled, the reader sleeps `wait_ms` at the **batch boundary** and re-checks. No job is reordered, held, or requeued. The limiter gate sits *before* `XREADGROUP` (and after the pause gate — a paused queue must not spend a token), so once a token is granted the batch reads and dispatches in stream order exactly as an un-limited reader would.

The re-check after a sleep never assumes a token is ready — a concurrent worker on the same queue may have taken it — so it re-runs the script. This is what keeps the global bucket honest under concurrency.

## The cold-start burst allowance

A fresh or idle bucket **starts full**. So the first `duration` window after a queue goes live (or wakes from idle) can admit up to `max` jobs immediately, before the limiter settles to its `max`/`duration` steady state.

This is standard token-bucket behavior, not a bug — but it surprises people who expect a hard `max`-per-`duration` from the very first job. If you see up to `2 × max` jobs pass "in the first second" (a full initial bucket plus a full window's worth of refill), that is the burst allowance, not a leak. Size `max` with the burst in mind, or pre-warm the queue if a strict first-window cap matters.

## CPU cost, honestly

- **At coarse rates** (hundreds of jobs/sec), a throttled reader parks in a `wait_ms`-long sleep and wakes only when a token is due. Near-zero CPU while throttled.
- **At very high `max`/second**, the deficit-of-one wait clamps to about 1 ms, so the reader re-checks roughly every millisecond — one cheap `EVALSHA` per re-check. That is real (small) Redis load, not "zero CPU," and N throttled workers computing the same `wait_ms` wake together. A `min_poll_ms` sleep floor is a tracked follow-up if very-high-rate limiters ever need it.

## Groups are reserved, not shipped

`groupKey` (Node) / `rate_limit_group_key` (Python) / `RateLimit::group_key` (Rust) is parsed but **rejected** in this version with "not supported in this version (global per-queue limiter only)". Per-key (name-scoped) sub-buckets — a separate budget per job name — are a documented future follow-up. Reserving the field now means adding them later is additive, not a breaking API change. Don't write code that relies on groups working; today they raise an error.

## Observability

Each throttle sleep emits `MetricsSink::rate_limited_tick(RateLimitedTick { wait_ms })`; the `chasquimq-metrics` adapters expose it as `chasquimq_rate_limited_total` (counter) and `chasquimq_rate_limit_wait_seconds` (histogram).

On the events stream, `e=rate-limited` (field `wait_ms`) fires once per throttle *entry* per reader. With M workers on one queue, expect up to M events per throttle episode, and a bucket flapping around `tokens ≈ 1` may re-emit — aggregate rather than assume exactly-one.

The CLI does **not** surface the limiter. `chasqui inspect` / `watch` read Redis keys with no worker config, and the raw bucket hash is meaningless without the `max`/`duration` that live in worker config — a "limiter depth" line would read identically whether the queue is unlimited or simply idle, so it's a deliberate non-goal.

## See also

- [Options reference](/reference/options/#rate-limiting) — the per-surface field names side by side.
- [Performance trade-offs](/concepts/performance-trade-offs/#rate-limiting-without-a-per-job-round-trip) — why the design avoids a per-job round trip.
- [Architecture decisions](/concepts/architecture-decisions/) — how the limiter fits the load-bearing constraints.
- [Engine deep-dive: rate limiting](https://github.com/jotarios/chasquimq/blob/main/docs/engine.md#rate-limiting) — the token-bucket script and reader gate in detail.
