---
title: Benchmarks
description: Measured throughput and CPU numbers for ChasquiMQ.
sidebar:
  order: 0
---

ChasquiMQ's headline claim is "the fastest open-source message broker for Redis." This section is the evidence behind that claim, and the honest caveats around it.

## Headline numbers

Apple M3, Redis 8.6.2 (Docker, loopback), same host as the BullMQ baseline. Two snapshots:

- **Quiet-host canonical** — the engine's ceiling under fair conditions (load < 1).
- **Today's contended-host run** (2026-05-07, load avg 1.8–4.3) — what reproduces on a working laptop.

| Scenario | Quiet host | Contended host | Ratio (best) |
|---|---:|---:|---:|
| `queue-add-bulk` (50, tiny) | **196,038 jobs/s** | **188,775 jobs/s** | up to **3.47×** |
| `worker-concurrent` (100) | **419,004 jobs/s** | 111,968 jobs/s | up to **8.78×** |
| `queue-add` (single, 10×10) | 16,548/s | 15,366/s | 1.16× |

The producer ratio is stable across runs (3.22× → 3.47× — essentially identical) because it bottlenecks at Redis, not host CPU. The consumer ratio drops sharply under host contention: ChasquiMQ spawns 100 worker tasks plus a tokio thread pool that compete with everything else on the box. The engine ceiling itself hasn't moved — the 419k number reproduces on a quiet host.

## What "fastest" means

The two scenarios that matter for the headline claim are:

- **`queue-add-bulk`** — bulk produce. The `XADD MAXLEN ~ N` hot path. **3.47× BullMQ on the same host.**
- **`worker-concurrent`** — 100 concurrent workers. **Up to 8.78× BullMQ on a quiet host.**

`queue-add` and `worker-generic` are latency-bound (single in-flight op), not throughput tests. They aren't the claim.

## Detailed reports

- [Methodology](/benchmarks/methodology/) — how we measure. Same host, same Redis, same scenarios.
- [The 1.0 numbers](/benchmarks/the-1-0-numbers/) — the same-host re-bench that landed with 1.0.
- [Regressions and floors](/benchmarks/regressions-and-floors/) — the host-load gate, the engine ceiling, when contention is a valid explanation.
- [Latency under low-rate dispatch](https://github.com/jotarios/chasquimq/blob/main/benchmarks/latency-1.x.md) — `worker-latency` p50/p99/p99.9 for handler and end-to-end, contended-Mac numbers.

The raw `.md` reports live in [`benchmarks/`](https://github.com/jotarios/chasquimq/tree/main/benchmarks) in the repo. The committed reports are the canonical record; raw run logs are gitignored.

## Reproducing

Requires Redis 8.6+ on `127.0.0.1:6379` (loopback).

```bash
# Redis (one-time)
docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

# ChasquiMQ — canonical run (5 repeats, scale=5, drop slowest)
cd chasquimq
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

For the BullMQ comparison run, see [Methodology](/benchmarks/methodology/) — the upstream `bullmq-bench` repo lives separately and is not vendored.

## Methodology limitations

The numbers are defensible for *this hardware* and *this setup*. Open caveats:

- **Latency is measured under low-rate dispatch only.** The `worker-latency` scenario reports end-to-end p50 ~1 ms / p99.9 < 3 ms and engine-side handler dispatch p99.9 ~13 µs on a contended Mac. Saturated-tail latency (i.e. latency at the throughput ceiling) is a separate question, not yet measured. Methodology and caveats: [Latency under low-rate dispatch](https://github.com/jotarios/chasquimq/blob/main/benchmarks/latency-1.x.md).
- **Same-host bench.** Bench process and Redis share cores. Apples-to-apples vs. BullMQ on the same host; not directly comparable to BullMQ's published cross-host numbers.
- **Worker CPU vs. BullMQ unmeasured.** ChasquiMQ's CPU is instrumented; the upstream `bullmq-bench` doesn't measure it. The PRD's "≥50% less worker CPU" target needs parallel CPU measurement before we can claim a number.
- **No persistence.** Redis runs default in-memory config — no AOF, no RDB. Production-realistic numbers would be lower for both queues.
