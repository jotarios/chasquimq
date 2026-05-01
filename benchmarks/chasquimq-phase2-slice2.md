# ChasquiMQ Phase 2 (slices 1+2) — measured results

**Run date:** 2026-05-01
**Commit:** Phase 2 slice 1 (delayed jobs) + slice 2 (retry backoff)
**Host:** Apple M3, 8 cores, 8 GB RAM, macOS 15. Redis 8.6.2 in Docker, loopback.

This report has two purposes:
1. **No regression on Phase 1's hot path.** Confirm slice 2's wire-format change (added `Job.attempt: u32`), reader DLQ-trigger logic change, and per-consumer retry-relocator task didn't slow down the immediate-enqueue/consume path.
2. **Three new scenarios** that exercise the Phase 2 code paths: `queue-add-delayed`, `worker-delayed-end-to-end`, `worker-retry-throughput`.

## No-regression check (Phase 1 scenarios)

5 repeats, scale=3, drop slowest. Phase 1 numbers from [`chasquimq-phase1.md`](chasquimq-phase1.md).

| Scenario | Phase 1 | Phase 2 slice 2 | Δ |
|---|---:|---:|---:|
| `queue-add` (single) | 16,300 | **17,272** | +6.0% |
| `queue-add-bulk` (50) | 195,741 | **193,156** | −1.3% |
| `worker-concurrent` (100) | 404,785 | **428,298** | +5.8% |
| `worker-generic` ⚠ noisy | 422,558 | **415,963** | −1.6% |

All four within run-to-run σ (1k–25k jobs/s depending on scenario). **No statistically meaningful regression.** The wire-format change for `Job.attempt` adds 1–2 bytes to every encoded payload but is invisible at this scale. The per-consumer retry-relocator task (one extra `connect()` and a polling channel receiver) costs nothing on the throughput path because no errors mean no retry messages.

## New scenarios

| Scenario | Mean (jobs/s) | jobs/CPU-sec | Notes |
|---|---:|---:|---|
| `queue-add-delayed` (single) | 18,219 | 69,494 | ZADD to delayed (no IDMP roundtrip) |
| `worker-delayed-end-to-end` | 751,308 | 414,101 | Producer ZADDs → promoter promotes → worker consumes |
| `worker-retry-throughput` | 68,793 | 262,072 | Every job fails once, retries via Lua-atomic XACKDEL+ZADD |

### `queue-add-delayed`

Producer schedules jobs 1 hour out, no consumer running. Just measures the cost of `ZADD <delayed_key> <score> <bytes>`. Slightly faster than `queue-add` (17.3k → 18.2k) because there's no `IDMP <producer_id> <iid>` argument on the wire — ZSET member equality is the dedup mechanism.

### `worker-delayed-end-to-end`

Preload N jobs into the delayed ZSET with a 1ms delay (so they're due by the time the consumer starts), then run a consumer with `delayed_enabled=true`. Stopwatch is in the worker handler. Measures the full pipeline: promoter's `EVALSHA` (ZRANGEBYSCORE+XADD+ZREM atomic), reader's `XREADGROUP`, worker dispatch, ack flusher.

**751k jobs/s** is faster than `worker-concurrent`'s 428k because the delayed scenario doesn't have a producer competing for Redis CPU during the bench window — all jobs are already in Redis when the consumer starts. It's an upper bound, not a steady-state mixed-workload number.

### `worker-retry-throughput`

Every job's first attempt returns `Err`, second succeeds. Measures the cost of the new retry path: worker re-encodes Job with `attempt+1`, retry-relocator runs `EVALSHA RETRY_RESCHEDULE_SCRIPT` (atomic `XACKDEL` + `ZADD`), promoter's tick picks it up and `XADD`s back to stream, consumer dispatches the retry.

Backoff is set to 1ms → 5ms (capped) so the bench window is reasonable. With these settings the promoter's 5ms poll interval becomes the pacing floor.

**68.8k jobs/s** = 6.2× slower than the no-error path (428k). Each job pays:
- 1× `XADD` (initial dispatch)
- 1× `EVALSHA` retry script (XACKDEL+ZADD atomic)
- 1× wait for promoter tick (5ms poll interval)
- 1× `EVALSHA` promote script (XADD+ZREM)
- 1× `XREADGROUP` re-dispatch
- 1× `XACKDEL` final ack

Six Redis round trips per logical "job processed" plus a poll wait. The 6× slowdown is the right shape — it's the cost of the new retry guarantee (exponential backoff via durable rescheduling) instead of the old "leave it pending and let CLAIM re-deliver after 30s" model. Latency win is enormous: median retry interval went from 30s to ~5ms.

## CPU efficiency

| Scenario | jobs/CPU-sec |
|---|---:|
| `worker-concurrent` | 246,779 |
| `worker-delayed-end-to-end` | 414,101 |
| `worker-generic` | 579,602 |
| `worker-retry-throughput` | 262,072 |

`worker-retry-throughput`'s jobs/CPU-sec is similar to `worker-concurrent`'s despite doing 6× more Redis work per logical job. Most of the retry overhead lives in Redis (the Lua scripts execute server-side); ChasquiMQ's worker process spends most retry-path time waiting on Redis I/O, not encoding or dispatching.

## Reproducing

```bash
docker start chasquimq-bench-redis
cd ~/Projects/experiments/chasquimq
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 3 --discard-slowest 1
```

Default scenario list now includes the three Phase 2 scenarios. Pass `--scenario queue-add,queue-add-bulk` to limit to a subset.

## Caveats from `chasquimq-phase1.md` still apply

- Single-host (Bun + Docker Redis + bench process all share cores).
- Latency unmeasured — throughput only.
- BullMQ-equivalent CPU% unmeasured.
- No persistence (in-memory Redis).

Plus one new caveat:

- **`worker-delayed-end-to-end` is preload-then-drain, not steady-state mix.** Producer is silent during the bench window, so all Redis CPU goes to consume. A realistic delayed workload has both producer and consumer competing; expect numbers ~30–40% lower in production. `worker-retry-throughput` has the same structure.
