---
title: Performance trade-offs
description: What we measured, what hurt, and the host-load floor that caps every consumer benchmark.
sidebar:
  order: 10
---

The headline numbers (3.47× on bulk produce, up to 8.78× on concurrent consume) are real, and they are also under conditions you have to recreate. This page is the honest version: what's robust, what's host-bound, and what we chose not to do.

## Auto-pipelining is not free

The most counterintuitive lesson from running the BullMQ baseline:

> ioredis's `enableAutoPipelining` **hurts** worker throughput on loopback Redis by 38%.

[Baseline numbers](/benchmarks/methodology/) confirm it on the same M3 host. The producer paths get a tiny win (`queue-add-bulk`: +1.1%). The consumer paths drop hard (`worker-concurrent`: −38%, `worker-generic`: −5.4%).

The mechanism: auto-pipelining batches every Redis command behind a microtask. On a low-RTT (loopback) connection, the microtask delay is now in the per-job loop. Reads block waiting for the batch to flush.

**ChasquiMQ's takeaway.** Pipelining is not a free win. The engine pipelines acks (where the batch fills quickly) but does not pipeline reads (where the batch wouldn't fill before the BLOCK timeout). Every "batch X" knob trades latency for throughput; prove the trade is worth it on your scenario.

This is why ChasquiMQ does not toggle `enableAutoPipelining`-style behavior across the board. The only batched Redis call in the hot path is `XACK` / `XACKDEL`, and that's because the latency cost (5ms ack-idle window) is below human perception and below most handler durations.

## Single-host contention caps every consumer number

The `worker-concurrent` benchmark spawns 100 worker tasks plus a tokio thread pool. On a quiet 8-core M3, that's fine. On a contended Mac (load avg 1.8–4.3, browser open, other agents running), throughput drops from **419k jobs/s** to **112k jobs/s** — a 73% hit, even though the engine code didn't change.

| Run | `queue-add-bulk` | `worker-concurrent` |
|---|---:|---:|
| Quiet host (Phase 2 final, load < 1) | 196,038/s | 419,004/s |
| Contended host (today, load 1.8–4.3) | 188,775/s | 111,968/s |

The producer ratio is stable across runs (3.22× → 3.47× — basically identical) because it bottlenecks at Redis, not host CPU. The consumer ratio drops because more tokio tasks means more contention with the rest of the host.

**ChasquiMQ's takeaway.** The 8.78× consumer claim is the engine's ceiling under fair conditions. It reproduces. The 2.45× number is what you'll see on a busy laptop.

The honest framing in the README and benchmarks: both numbers ship side-by-side. `worker-concurrent` is host-load sensitive; the canonical claim is the quiet-host run; the today's-laptop run is what users will reproduce on their own contended machines.

## `worker-concurrent` is the most contention-sensitive scenario

Among the four `bullmq-bench` scenarios, `worker-concurrent` is the most sensitive to host load:

- `queue-add` — single producer, 10×10 payload. Latency-bound; not a throughput test.
- `queue-add-bulk` — bulk produce. Bottlenecks at Redis. Insensitive to host load.
- `worker-generic` — single consumer. Latency-bound; not a throughput test (bench window is too small for stable measurement, ~12ms at 419k/s).
- `worker-concurrent` — 100 consumers. Bottlenecks at host CPU. Highly sensitive to host load.

This is why benchmarks split the headlines into "producer" (stable) and "consumer" (host-dependent). Conflating them gives a misleading single number.

## CPU% is not measured (yet) for BullMQ

The PRD's secondary target is "≥50% less worker CPU." ChasquiMQ's bench harness measures CPU% per scenario; the upstream `bullmq-bench` does not.

To make the CPU claim defensible, we'd need to instrument BullMQ's bench process (with `top -pid` snapshots, or a CPU-aware bench wrapper) and compare. That hasn't been done end-to-end. The 1.0 ship deliberately doesn't claim a CPU% number against BullMQ — only ChasquiMQ's own CPU% (`jobs/CPU-sec`) is reported in `chasquimq-1.0.md`.

**Ship state.** The throughput claim is robust. The CPU claim is unverified. Both are flagged honestly in the benchmark docs.

## Latency: measured under low-rate dispatch

The bench harness now reports per-job latency for one scenario, `worker-latency` — a live producer at ~1000 jobs/sec against a `concurrency=100` consumer pool with no preloading. On a contended Apple M3 (load avg ~3): end-to-end p50 is 1,044us, p99 is 1,734us, p99.9 is 2,747us. The engine-measured handler-only distribution (no-op handler, sourced from `JobOutcome.handler_duration_us`) is sub-microsecond at p50 (1us), 2us at p99, 13us at p99.9 — most of the end-to-end envelope is Redis round trips, msgpack encode/decode, and async-channel dispatch, not engine compute.

This answers the **dispatch-overhead-on-an-idle-queue** question, not the saturated-tail one. Tail latency under heavy producer rates is a function of queue depth and consumer rate, not the engine, and is not separately reported. There is also no BullMQ comparator — the upstream `bullmq-bench` suite does not measure per-job latency, so we deliberately do not publish a "ChasquiMQ is N× lower latency than BullMQ" claim.

Full numbers, methodology, and reproducibility instructions: [`benchmarks/latency-1.x.md`](https://github.com/jotarios/chasquimq/blob/main/benchmarks/latency-1.x.md).

## What's not on the roadmap

These are conscious omissions, not "we ran out of time":

- **Priority queues.** Streams are FIFO by construction. A parallel priority ZSET would either require interleaving reads (defeats batched `XREADGROUP`) or per-job round trips back into priority order. Either way, the engine's headline performance lever — batched reads — is gone. Better to stay BullMQ-on-priority than ship a slow ChasquiMQ-on-priority.
- **Rate limiting on the queue.** A leaky-bucket on the consumer side adds per-job round trips. Better implemented at the handler level (token bucket per downstream API).
- **Pause/resume.** Implementable at the consumer side (gate dispatch with a flag) but the engine continues to read. Without a way to pause the engine itself, "pause" is an illusion. v1.x.
- **DAG flows / parent-child jobs.** Streams aren't the right primitive for DAG semantics. A separate workflow store + ChasquiMQ as the execution layer is the right shape; that's a separate project, not a ChasquiMQ feature.

## What we did do because it was measured

- **Pipelined acks.** Measured: 38% improvement on `worker-concurrent` vs. per-ack round trips. Shipped on by default.
- **`XACKDEL` over `XACK + XDEL`.** Measured: same code path, one round trip instead of two. Shipped Redis-version-required.
- **MessagePack over JSON.** Measured: ~20–30% smaller wire, ~30% faster encode. Shipped, no fallback.
- **Tokio multi-receiver dispatch.** Measured: per-batch fanout via `async-channel` is faster than a shared `Mutex<Receiver>`. Shipped.
- **`Arc<str>` on hot-path strings.** Measured: ~5% reduction in allocations on the dispatch path. Shipped.

The pattern: measure, decide, ship. Performance lessons go in `benchmarks/`; opinions don't.

## How to reproduce

The whole point of the benchmark methodology is reproducibility. To re-run on your own M3-class host:

```bash
docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

cd ~/chasquimq
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Numbers within ±10% of the reported figures are within run-to-run variance on the same host class. Numbers wildly off mean either the host is contended, or the engine has regressed; the [host-load gate](/benchmarks/regressions-and-floors/) is the rule.

For the numbers themselves: [The 1.0 numbers](/benchmarks/the-1-0-numbers/). For the host-load story: [Regressions and floors](/benchmarks/regressions-and-floors/).
