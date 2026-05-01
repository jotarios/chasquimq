# BullMQ baseline (the number we have to beat)

This is the baseline ChasquiMQ has to outperform — measured on this developer's hardware so the comparison is apples-to-apples. The goal from the PRD is **3–5× throughput** at **≥50% less worker CPU**. These numbers are the "1×" reference.

## How it was run

- **Suite:** [`taskforcesh/bullmq-bench`](https://github.com/taskforcesh/bullmq-bench) (commit on `master` as of 2026-04-30) — the official BullMQ benchmark harness.
- **Reproduction:** `git clone https://github.com/taskforcesh/bullmq-bench.git && bun install && BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts`.
- **Methodology:** for each scenario, the harness pre-loads `warmupJobsNum + benchmarkJobsNum` jobs, starts the stopwatch when the warmup count is hit, stops when the total is hit, and reports `1000 * jobsTotal / time_ms` as `jobs/sec`.
- **Variance:** 3 repeats per configuration. Means and ranges below.

### Stack

| | Version |
|---|---|
| BullMQ | 5.76.4 (current `latest` on npm; bench repo's lockfile pinned 4.12.0, which I bumped) |
| Redis | 8.6.2 (Docker `redis:8.6`, default config, single-instance, no AOF/RDB tuning) |
| Bun | 1.2.2 |
| Host | Apple M3, 8 cores, 8 GB RAM, macOS 15 (Darwin 24.6.0) |
| Docker | 28.3.2 (linux/arm64 VM) |

Redis ran in Docker on the same machine as Bun. That means the benchmark client and Redis share CPU — BullMQ's published methodology uses a beefier client box than Redis box specifically to avoid this. **Numbers below are upper-bounded by single-host contention and should not be compared to BullMQ's blog numbers.** They are valid as our internal baseline because we'll run ChasquiMQ on the same host.

### The 4 scenarios

All from [`bullmq-suite.ts`](https://github.com/taskforcesh/bullmq-bench/blob/master/src/bullmq-suite.ts):

| Scenario | Warmup | Bench jobs | Bulk size | Payload (W×D) | Worker concurrency |
|---|---|---|---|---|---|
| `queue-add` | 1,000 | 1,000 | — (single `add`) | 10×10 (large nested UUIDs) | n/a |
| `queue-add-bulk` | 1,000 | 10,000 | 50 | 1×1 (tiny) | n/a |
| `worker-generic` | 1,000 | 1,000 | n/a | 1×1 | default (1) |
| `worker-concurrent` | 1,000 | 10,000 | n/a | 1×1 | 100 |

## Results

### No pipelining (default)

| Scenario | Mean (jobs/s) | Range | 3× target | 5× target |
|---|---:|---|---:|---:|
| `queue-add`         | **13,961** | 13,699 – 14,388 | 41,883  | 69,805  |
| `queue-add-bulk`    | **60,828** | 56,410 – 63,218 | 182,484 | 304,140 |
| `worker-generic`    | **13,250** | 12,346 – 13,889 | 39,750  | 66,250  |
| `worker-concurrent` | **47,707** | 44,898 – 49,550 | 143,121 | 238,535 |

### `BULLMQ_BENCH_ENABLE_PIPELINE=1` (ioredis auto-pipelining)

| Scenario | Mean (jobs/s) | Range | Δ vs. no-pipeline |
|---|---:|---|---|
| `queue-add`         | **14,465** | 14,184 – 14,925 | +3.6% |
| `queue-add-bulk`    | **61,499** | 57,292 – 64,706 | +1.1% |
| `worker-generic`    | **12,528** | 12,346 – 12,658 | -5.4% |
| `worker-concurrent` | **29,364** | 28,871 – 29,650 | **-38%** |

Auto-pipelining slightly helps the producer paths and **noticeably hurts** the worker paths on this hardware. Likely cause: ioredis batches the worker's `XACK`/state-update commands behind a microtask, which adds latency to the per-job loop on a low-RTT (loopback) connection. Worth knowing — we should not assume "pipelining always wins." Default config is the more representative baseline.

## Pinned-Redis baseline (cpuset 0-3)

Re-ran on 2026-04-30 with Redis pinned to cores 0–3 (`docker run --cpuset-cpus="0-3" ...`) to test the hypothesis that contention between Bun and Redis was capping throughput. 3 alternating no-pipeline / pipelined runs (6 total).

### No pipelining

| Scenario | Mean (jobs/s) | Range | Δ vs. unpinned |
|---|---:|---|---|
| `queue-add`         | **14,434** | 13,889 – 14,815 | +3.4% |
| `queue-add-bulk`    | **54,440** | 47,826 – 58,201 | **−10.5%** |
| `worker-generic`    | **12,475** | 12,346 – 12,579 | −5.8% |
| `worker-concurrent` | **40,791** | 38,062 – 42,802 | **−14.5%** |

### `BULLMQ_BENCH_ENABLE_PIPELINE=1`

| Scenario | Mean (jobs/s) | Range | Δ vs. unpinned-pipelined |
|---|---:|---|---|
| `queue-add`         | **14,231** | 13,793 – 14,815 | −1.6% |
| `queue-add-bulk`    | **53,933** | 50,459 – 58,201 | −12.3% |
| `worker-generic`    | **11,775** | 11,494 – 12,270 | −6.0% |
| `worker-concurrent` | **26,578** | 26,005 – 27,160 | −9.5% |

**Conclusion:** pinning Redis to 4 cores **hurts** throughput on this 8-core M3, by ~10–15% on the throughput-bound scenarios. The OS scheduler still happily places Bun on cores 0–3 (cpuset on the Redis container does not exclude other processes from those cores), so we ended up sharing 4 cores between the bench client and Redis instead of 8 — strictly worse than the unpinned baseline. To genuinely separate them on this host, we'd need to also restrict Bun (e.g., `taskset -c 4-7`) or run on different physical hosts. Treating the **unpinned baseline as the canonical 1×** because it represents the most realistic single-host configuration we can actually operate, and the unpinned numbers are also what ChasquiMQ will run against.

Raw logs: `runs/baseline-pinned-redis-run{1..6}-*.log`.

## What ChasquiMQ has to clear

Using the **unpinned** no-pipeline means as 1× (since pinning hurts on this host and unpinned is what ChasquiMQ will run against):

- **Producer hot path (`queue-add-bulk`):** beat ~61k jobs/s → 3× = ~182k, 5× = ~304k.
- **Concurrent worker (`worker-concurrent`):** beat ~48k jobs/s → 3× = ~143k, 5× = ~238k.
- **Single producer / single worker** scenarios are dominated by per-call overhead, not throughput — useful for latency comparisons but not the headline win condition.

The PRD also calls for **≥50% less worker CPU** under load. We need to capture CPU% during the worker scenarios in future runs (not measured here — bullmq-bench doesn't instrument it). When we run ChasquiMQ's equivalent suite, both runs need `time` / `top -pid` snapshots so we can compare.

## Caveats

1. **Single-host contention.** Bun, Docker, and Redis all share 8 M3 cores. A 2-machine setup (or at least a dedicated CPU pin for Redis) would push every number up. Re-baseline before publishing any external comparison.
2. **No persistence.** Redis is running with default in-memory config — no AOF, no RDB pressure. Production-realistic numbers would be lower.
3. **Small sample.** 3 repeats catches gross variance but not tail behavior. For final claims, increase `benchmarkJobsNum` to ~50k+ per scenario (BullMQ's own published methodology uses 50k for statistically-meaningful runtimes).
4. **`bullmq-bench` is the single-process suite.** For multi-queue, multi-worker comparisons (where Dragonfly's 30× claim came from), we'd separately want to run [`bullmq-concurrent-bench`](https://github.com/taskforcesh/bullmq-concurrent-bench).

## Raw data

Raw run logs are local-only (`benchmarks/runs/` is gitignored). The summary numbers above are the committed record. The `bullmq-bench` checkout lives at `~/Projects/experiments/bullmq-bench` (sibling to this repo, not vendored).

## Re-running

```bash
# start Redis
docker start chasquimq-bench-redis  # or: docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

# baseline
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts

# pipelined variant
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 BULLMQ_BENCH_ENABLE_PIPELINE=1 bun src/index.ts
```
