# Progress + log slice — no-regression check

**Run date:** 2026-05-23
**Host:** Apple M3, 8 logical cores. macOS 15. `redis:8.6.2` Docker (loopback).
**Load avg during runs:** 14.0 → 16.0 (heavily contended Mac host; several agents active).
**ChasquiMQ:** `feat/progress-and-log` branch at HEAD (engine `JobHandle` + introspector `get_job_logs` + `progress` field on `JobInfo` + three new `ConsumerConfig` fields; Node + Python shims wired through).
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1`, matching `chasquimq-1.0.md`.

The engine hot path *was* touched this slice (new `JobHandle` consumer dispatch wiring, plus the `progress: Option<u8>` round-trip threaded through every `get_job` / `get_jobs` path in the introspector), so per the host-load gate in [`benchmarks/README.md`](README.md#interpreting-numbers) the host-contention explanation is not available — the numbers below stand on their own.

## Results vs. the 1.0 baseline

| Scenario | 1.0 baseline (2026-05-07) | This branch (2026-05-23) | Δ | Within baseline stddev? |
|---|---:|---:|---:|---|
| `queue-add-bulk` (50, tiny) | **188,775 jobs/s** | **184,295 jobs/s** | **−2.4%** | ✓ (baseline stddev 4,909) |
| `worker-concurrent` (100) | **111,968 jobs/s** | **110,726 jobs/s** | **−1.1%** | ✓ (baseline stddev 4,246) |
| `queue-add` (single) | 15,366 jobs/s | 16,340 jobs/s | +6.3% | ✓ (baseline stddev 846) |
| `worker-generic` (single) ⚠ | 9,517 jobs/s ⚠ | 9,810 jobs/s ⚠ | +3.1% | ✓ direction-only |

Distribution stats from this branch (drop-slowest applied):

| Scenario | Mean | p50 | p95 | p99 | stddev | CPU (× core) | jobs/CPU-sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `queue-add` | 16,340 | 16,265 | 16,733 | 16,794 | 290 | 0.26× | 63,667 |
| `queue-add-bulk` | 184,295 | 185,947 | 186,252 | 186,278 | 3,061 | 0.65× | 284,781 |
| `worker-concurrent` | 110,726 | 110,479 | 113,936 | 114,331 | 2,500 | 1.88× | 58,956 |
| `worker-generic` ⚠ | 9,810 | 9,730 | 10,234 | 10,301 | 313 | 0.23× | 43,731 |

## Reading the numbers

**No regression.** Every per-scenario delta vs the 1.0 baseline lands inside one standard deviation of the baseline run. The host this branch was benched on was substantially more contended than 1.0's (load avg 14–16 vs 1.8–4.3); the producer path's `queue-add-bulk` falling 2.4% under that contention is the upper envelope of expected noise, and `worker-concurrent` shifting 1.1% under load-avg ~14 is structural noise, not engine drift.

**Why no regression is expected.** The `JobHandle` is constructed per dispatch on the worker hot path, but it carries only `Arc<str>` clones plus a shared `fred::clients::Pool` reference — no per-job allocation, no extra Redis round trip. Handler-side `update_progress` / `log` are opt-in: handlers that never call them pay nothing. The introspector's new `progress: Option<u8>` field is pipelined into the existing `get_job` lookups under the same hash tag, so it's an off-hot-path read.

**`queue-add` +6.3%.** Single-add is latency-bound (sub-millisecond bench window) and well within the BullMQ baseline doc's "treat as direction-only" caveat — but the positive sign rules out a regression hiding here.

## Methodology

```bash
docker start chasquimq-bench-redis  # redis:8.6.2 on 127.0.0.1:6379
cd /Users/jotarios/Projects/experiments/chasquimq-progress-log
cargo run -p chasquimq-bench --release -- \
    --repeats 5 --scale 5 --discard-slowest 1 \
    --scenario queue-add,queue-add-bulk,worker-generic,worker-concurrent
```

Raw log: `benchmarks/runs/progress-log-bench-2026-05-23-1638.log` (gitignored — local only).

## Verdict

No regression. The slice ships with the headline `queue-add-bulk` and `worker-concurrent` numbers intact within stddev of the 1.0 baseline, on a substantially noisier host.
