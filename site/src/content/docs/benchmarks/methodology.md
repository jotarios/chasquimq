---
title: Methodology
description: How we measure. Same M3 host, Redis 8.6.2, MessagePack 256B payloads, no auto-pipelining.
sidebar:
  order: 1
---

The benchmark methodology mirrors BullMQ's upstream `bullmq-bench` so the numbers are directly comparable. This page lays out the host, the scenarios, the harness flags, and the BullMQ reference run.

## Stack

| | Version |
|---|---|
| Rust | 1.85+ (2024 edition) |
| ChasquiMQ engine | 1.0 (post-#79 main HEAD) |
| ChasquiMQ bench harness | `chasquimq-bench` (in-repo) |
| BullMQ | 5.76.4 (latest at measurement time) |
| BullMQ bench harness | `bullmq-bench` (upstream, not vendored) |
| Bun | 1.2.2 (used to run BullMQ bench) |
| Redis | 8.6.2 (Docker `redis:8.6`, default config, single-instance, no AOF / no RDB) |
| Host | Apple M3, 8 logical cores, 8 GB RAM, macOS 15 (Darwin 24.6.0) |

Redis runs in Docker on the same machine as the bench client. The bench process and Redis share CPU. BullMQ's published methodology uses a beefier client box than Redis box specifically to avoid this. **Numbers in this section are upper-bounded by single-host contention and should not be compared to BullMQ's published cross-host blog numbers.** They are valid as our internal A/B because we run ChasquiMQ on the same host with the same Redis.

## The scenarios

Four mirror `bullmq-suite.ts` from `bullmq-bench` and form the head-to-head comparison:

| Scenario | Warmup | Bench jobs | Bulk size | Payload | Worker concurrency |
|---|---|---|---|---|---|
| `queue-add` | 1,000 | 1,000 | — (single `add`) | 10×10 nested UUIDs | n/a |
| `queue-add-bulk` | 1,000 | 10,000 | 50 | 1×1 tiny | n/a |
| `worker-generic` | 1,000 | 1,000 | n/a | 1×1 tiny | default (1) |
| `worker-concurrent` | 1,000 | 10,000 | n/a | 1×1 tiny | 100 |

`queue-add-bulk` and `worker-concurrent` are the two scenarios that matter for the headline throughput claim. `queue-add` and `worker-generic` are latency-bound (single in-flight op) — useful for direction-only comparison, not for a 3× claim.

One ChasquiMQ-only scenario reports per-job latency rather than throughput:

| Scenario | Warmup | Bench jobs | Producer | Worker concurrency | What it measures |
|---|---|---|---|---|---|
| `worker-latency` | 200 | 10,000 | paced (~1 ms inter-arrival) | 100 | end-to-end p50/p99/p99.9 + engine-measured handler-only |

The producer paces deliberately so the consumer is never saturated; the goal is dispatch overhead on an idle queue, not the throughput-ceiling tail. `bullmq-bench` does not measure per-job latency, so this scenario has no BullMQ comparator. Full methodology in [Latency under low-rate dispatch](https://github.com/jotarios/chasquimq/blob/main/benchmarks/latency-1.x.md).

## ChasquiMQ harness flags

```bash
cargo run -p chasquimq-bench --release -- \
    --repeats 5 --scale 5 --discard-slowest 1 \
    --scenario queue-add,queue-add-bulk,worker-generic,worker-concurrent
```

- `--repeats 5` — run each scenario 5 times.
- `--scale 5` — multiply the harness defaults' job count by 5 (so `queue-add-bulk` writes 50,000 jobs per run, not 10,000).
- `--discard-slowest 1` — drop the slowest run before computing the mean. Reduces tail influence.

The harness reports mean, p50, p95, p99, stddev, CPU% (× core), and `jobs/CPU-sec` — see [The 1.0 numbers](/benchmarks/the-1-0-numbers/) for the full distribution table.

## BullMQ reference run

```bash
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts
```

The upstream harness runs each scenario once per invocation. We run it once per side-by-side measurement. The harness pre-loads `warmupJobsNum + benchmarkJobsNum` jobs, starts the stopwatch when the warmup count is hit, stops at the total, and reports `1000 * jobsTotal / time_ms` as `jobs/sec`.

Note: the upstream `bullmq-bench` repo's `package.json` says `"bullmq": "latest"` but the lockfile pinned an older 4.x. Run `bun add bullmq@latest` after cloning if you re-baseline against current BullMQ.

## Side-by-side conditions

Both runs use:

- Loopback Redis (no network).
- No `enableAutoPipelining` on the BullMQ side (it hurts the worker scenarios by 38% on loopback — see [Performance trade-offs](/concepts/performance-trade-offs/)).
- No persistence (default in-memory Redis).
- The same M3 host, ideally with the same load avg.
- The bench process pinned to the same cores as Redis (i.e., not pinned at all — the M3 OS scheduler places both freely).

## On host load

`worker-concurrent` is the most contention-sensitive scenario in the suite. The engine ceiling reproduces only on a quiet host (load avg < 1). On a contended Mac (browser, other agents, multiple Docker containers), the ceiling drops by 73% even though the engine code didn't change.

The honest pattern: report quiet-host and contended-host numbers side by side. The quiet-host run is the engine ceiling — the marketing-defensible number. The contended-host run is what users will see when they reproduce on a busy laptop. Calibrating against your own environment matters.

The [host-load gate](/benchmarks/regressions-and-floors/) formalizes this: a "host contention" explanation for a `worker-concurrent` regression is valid only when the engine code didn't change between runs.

## On Redis pinning

We tested pinning Redis to cores 0–3 with `docker run --cpuset-cpus="0-3" ...` to see if separating Redis from the bench client would push throughput up. It hurt by 10–15% on the throughput-bound scenarios. The OS scheduler still places Bun on cores 0–3 (the cpuset constrains the Redis container, not other processes), so the result was sharing 4 cores between bench client and Redis instead of 8 — strictly worse.

To genuinely separate bench client from Redis on this host, we'd need to also pin Bun (e.g., `taskset -c 4-7`). On macOS that's clumsy. The unpinned baseline is the realistic single-host configuration.

## Numbers we don't yet have

One open caveat remains for the head-to-head claim:

- **Worker CPU% vs. BullMQ.** ChasquiMQ's bench measures its own CPU%. `bullmq-bench` does not. To claim "≥50% less worker CPU" defensibly, we'd need to instrument BullMQ's bench process with `top -pid` or a CPU-aware wrapper. Not done end-to-end.

The 1.0 ship deliberately doesn't claim a CPU% delta against BullMQ — only ChasquiMQ's own CPU% (`jobs/CPU-sec`) is reported.

Latency under low-rate dispatch is measured (see the `worker-latency` row above and the [latency report](https://github.com/jotarios/chasquimq/blob/main/benchmarks/latency-1.x.md)). Saturated-tail latency at the throughput ceiling is a separate question and not yet reported.

For the actual numbers: [The 1.0 numbers](/benchmarks/the-1-0-numbers/). For the host-load reasoning: [Regressions and floors](/benchmarks/regressions-and-floors/).
