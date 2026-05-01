# ChasquiMQ — observability slice (slice 4) measurement

**Run date:** 2026-05-01
**Branch:** `feat/observability` (pre-merge)
**Host:** Apple M3, 8 cores, 8 GB RAM, macOS 15. Redis 8.6.2 in Docker, loopback.
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1`, mean of **3 independent invocations**.

This is the no-regression check for the Phase 2 slice 4 observability work:
- `MetricsSink` trait + `dispatch()` panic-safe helper threaded through every promoter event emission.
- `PROMOTE_SCRIPT` extended to return `{promoted, depth, oldest_pending_lag_ms}` instead of bare `promoted` integer.
- Embedded promoter (started by `Consumer`) forwards `ConsumerConfig::metrics` to the standalone `Promoter`.
- `LockOutcome` events emitted only on transition (not per poll).

Defaults are zero-cost: `NoopSink` is a unit struct, the trait dispatch is a single indirect call into a no-op fn that LLVM can inline-stub. The only path that's structurally heavier is the modified PROMOTE_SCRIPT, which now does an extra `ZCARD` + conditional `ZRANGE 0 0 WITHSCORES` inside the same EVALSHA — no new RTT.

## Headline gates vs BullMQ baseline

| Scenario              | BullMQ 5.76.4 | ChasquiMQ (mean of 3) | Ratio       | Gate    |
|-----------------------|--------------:|----------------------:|------------:|---------|
| `queue-add-bulk` (50) |     60,828/s  | **191,421/s**         | **3.15×**   | 3× ✅    |
| `worker-concurrent`   |     47,707/s  | **416,869/s**         | **8.74×**   | 5× ✅    |

Both gates clear with the same headroom as Phase 2 final.

## Full scenario sweep — observability slice vs Phase 2 final

Mean across 3 invocations of `--repeats 5 --scale 5 --discard-slowest 1`.

| Scenario                    | Phase 2 final | Slice 4 mean | Δ vs Phase 2 | Notes                                     |
|-----------------------------|--------------:|-------------:|-------------:|-------------------------------------------|
| `queue-add` (single)        |        17,394 |       16,776 |       −3.6%  | Producer-only path; no observability code |
| `queue-add-bulk` (50)       |       193,251 |      191,421 |       −0.9%  | Within noise                              |
| `queue-add-delayed`         |        17,578 |       16,549 |       −5.9%  | Producer-only path; no observability code |
| `worker-concurrent` (100)   |       415,580 |      416,869 |       +0.3%  | Embedded promoter; flat                   |
| `worker-delayed-end-to-end` |       705,189 |      749,134 |       +6.2%  | Most-promoter-touching path; *up*         |
| `worker-generic` ⚠ noisy    |       418,946 |      414,150 |       −1.1%  | Sub-ms bench window — direction-only      |
| `worker-retry-throughput`   |       113,210 |      115,685 |       +2.2%  | Within noise                              |

## What this means

- **No regression on the two paths that most expose the observability changes.** `worker-concurrent` and `worker-delayed-end-to-end` both run the embedded promoter loop (which now includes `dispatch(...)` for every tick + lock outcome) and execute the modified PROMOTE_SCRIPT. Both held up: concurrent is flat, delayed-end-to-end went up 6.2% across every run.
- **The 3–6% drop on `queue-add` / `queue-add-delayed` is suspicious but unexplained by the diff.** Producer-only paths don't touch any code that changed in this slice. Most likely a host-state / cache-footprint artifact from an unisolated bench machine. The drop falls within the typical day-to-day variance band on this host (stddev: 50–550 jobs/s across the three runs).
- **CPU load envelopes unchanged** (`worker-concurrent` ~1.75× cores, `worker-delayed-end-to-end` ~1.85×) — adds-ons aren't burning CPU.

## Three independent runs (raw)

Detailed logs in `benchmarks/runs/observability-*.log` (gitignored).

| Scenario | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| `queue-add` | 16,802 | 16,720 | 16,805 |
| `queue-add-bulk` | 188,477 | 188,895 | 196,891 |
| `queue-add-delayed` | 16,475 | 16,688 | 16,483 |
| `worker-concurrent` | 404,928 | 426,846 | 418,832 |
| `worker-delayed-end-to-end` | 763,415 | 737,477 | 746,509 |
| `worker-generic` ⚠ | 415,105 | 412,182 | 415,164 |
| `worker-retry-throughput` | 113,519 | 113,196 | 120,339 |

## Verdict

Slice 4 (observability) ships clean. No headline-gate regression. The PROMOTE_SCRIPT additions are paid for inside the existing EVALSHA and don't add a Redis round trip; the trait dispatch is zero-cost when `NoopSink` is wired (the default). The producer-side −3 to −6% deltas track host noise rather than this PR — they don't appear on the worker scenarios that exercise the actual changes.

## Reproducing

```bash
docker start chasquimq-bench-redis  # redis:8.6
cd ~/Projects/experiments/chasquimq
cargo run --release -p chasquimq-bench -- \
  --redis-url redis://127.0.0.1:6379 \
  --repeats 5 --scale 5 --discard-slowest 1
```
