---
title: The 1.0 numbers
description: Same-host re-bench, BullMQ 5.76.4 vs ChasquiMQ 1.0 on Apple M3 + Redis 8.6.2.
sidebar:
  order: 2
---

The numbers landed with the 1.0 tag. Run on a contended Mac host (load avg 1.8–4.3) on 2026-05-07. The full report lives at [`benchmarks/chasquimq-1.0.md`](https://github.com/jotarios/chasquimq/blob/main/benchmarks/chasquimq-1.0.md) in the repo; this page is the same content adapted for the docs site.

## Setup

| | |
|---|---|
| Run date | 2026-05-07 |
| Host | Apple M3, 8 logical cores, 8 GB. macOS 15. |
| Redis | `redis:8.6.2` Docker (loopback) |
| Load avg during runs | 1.82 → 4.26 (contended Mac with other agents/containers active) |
| ChasquiMQ | post-#79 main HEAD (engine + Node + Python shim with result backends) |
| BullMQ | 5.76.4, single run via `bullmq-bench` |
| ChasquiMQ harness | `--repeats 5 --scale 5 --discard-slowest 1` |
| BullMQ harness | one run per scenario (upstream default) |

This is the bench that lands the public 1.0 numbers. It is honest about host contention rather than chasing the canonical Phase 2 quiet-host upper bound.

## Results

| Scenario | BullMQ 5.76.4 | ChasquiMQ 1.0 | Ratio | PRD target |
|---|---:|---:|---:|---|
| `queue-add-bulk` (50, tiny) | 54,455 jobs/s | **188,775 jobs/s** | **3.47×** | 3× ✅ |
| `worker-concurrent` (100) | 45,643 jobs/s | **111,968 jobs/s** | 2.45× | 5× — host-load floor (see below) |
| `queue-add` (single, 10×10) | 13,245 jobs/s | 15,366 jobs/s | 1.16× | not graded |
| `worker-generic` (single) | 12,658 jobs/s | 9,517 jobs/s | 0.75× ⚠ | not graded |

ChasquiMQ distribution stats (from `--discard-slowest 1`):

| Scenario | Mean | p50 | p95 | p99 | stddev | CPU (× core) | jobs/CPU-sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `queue-add` | 15,366 | 15,242 | 16,412 | 16,493 | 846 | 0.25× | 63,354 |
| `queue-add-bulk` | 188,775 | 187,528 | 195,440 | 196,362 | 4,909 | 0.60× | 319,246 |
| `worker-concurrent` | 111,968 | 110,817 | 117,758 | 118,630 | 4,246 | 1.57× | 71,273 |
| `worker-generic` | 9,517 | 9,712 | 10,172 | 10,228 | 681 | 0.20× | 47,919 |

## Reading the numbers

### `queue-add-bulk` — 3.47×

The producer hot path bottlenecks at Redis, not on host CPU. The ratio reproduces the 3× claim under realistic load. **This is the headline producer number.**

### `worker-concurrent` — 2.45× today; 8.78× canonical

Both queues drop under host contention, but ChasquiMQ drops further (from 419k/s to 112k/s, a 73% hit) because it spawns 100 worker tasks plus a tokio thread pool that compete with the rest of the host. BullMQ drops only 4% because its single-Node-process worker pool doesn't fan out as aggressively on the host CPU.

**The engine ceiling has not moved.** The 419k/s number reproduces on a quiet host (load < 1, see `chasquimq-phase2-final.md` in the repo). To defend the canonical 8.78× claim: close other agents and containers, idle the host for a few minutes, then re-run. The host-load floor on a contended Mac caps `worker-concurrent` at ~110k–120k/s regardless of engine optimisation.

### `queue-add`, `worker-generic` — not the throughput claim

These are single-in-flight latency tests; the BullMQ baseline doc explicitly flags them as direction-only. Sub-millisecond bench windows aren't load-defensible. The `worker-generic` ⚠ ratio of 0.75× is artifact, not regression: the bench window is ~12ms at engine ceiling and stretches to ~100ms under contention, so the bench doesn't have time to converge.

## Reproduction

```bash
# Redis (one-time)
docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

# BullMQ (single run; no --repeats in upstream harness)
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts

# ChasquiMQ (5 repeats, scale=5, drop slowest)
cd chasquimq
cargo run -p chasquimq-bench --release -- \
    --repeats 5 --scale 5 --discard-slowest 1 \
    --scenario queue-add,queue-add-bulk,worker-generic,worker-concurrent
```

Raw logs are gitignored locally (`benchmarks/runs/`); only the summary `.md` files are committed.

## Verdict

The 3× headline producer claim is robust. The 5× / 8.78× consumer claim is host-quiet-only and was never claimed otherwise — the canonical `chasquimq-phase2-final.md` quiet-host run is what the marketing copy points at. Today's contended-host run is what users will actually see when they reproduce on a busy laptop.

For the 1.0 tag, **both numbers ship side-by-side** so users can calibrate against their own environment.

For the host-load gate: [Regressions and floors](/benchmarks/regressions-and-floors/). For the methodology: [Methodology](/benchmarks/methodology/).
