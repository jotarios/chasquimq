# ChasquiMQ — observability slice 5 (consumer/retry/DLQ hooks) measurement

**Run date:** 2026-05-02
**Branch:** `feat/observability-extension` (pre-merge)
**Host:** Apple M3, 8 cores, 8 GB RAM, macOS 15. Redis 8.6.2 in Docker, loopback.
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1`, mean of **2 independent invocations**.

This is the no-regression check for the Phase 2 slice 5 work that
extends `MetricsSink` to the consumer hot path:

- `JobOutcome { kind, attempt, handler_duration_us }` per handler invocation, with `Instant::now()` pair around the await + `dispatch()` panic-safe sink call.
- `RetryScheduled { attempt, backoff_ms }` from the retry relocator, gated on `RETRY_RESCHEDULE_SCRIPT` returning `1` (the script return value is now parsed defensively across `Value::Integer` / `Value::String` / `Value::Bytes`; previously discarded).
- `DlqRouted { reason, attempt }` after the DLQ relocator's atomic XADD+XACKDEL succeeds.
- `ReaderBatch { size, reclaimed }` per non-empty `XREADGROUP` (raw size including entries about to be DLQ-routed; reclaimed = `delivery_count > 1`).
- `DlqReason` promoted from `pub(crate)` to public (`Copy`-derived) so it can ride on `DlqRouted`.
- `chasquimq-metrics` adapter gets a `QueueLabeled<S>` wrapper that adds a `queue` label using `Arc<str>.clone()` into `metrics-rs` `SharedString` (atomic refcount, no per-event String allocation).
- Adapter histogram naming follows Prometheus base-unit convention: `chasquimq_handler_duration_seconds`, `chasquimq_retry_backoff_seconds`. Engine events keep micros / ms internally; the adapter divides at the boundary.

The hot-path concern: `JobOutcome` fires once per handler invocation, so on `worker-concurrent` (~420k jobs/s) the new code runs ~420k times per second. Defaults stay zero-cost — `NoopSink` is a unit struct, the trait dispatch is a single indirect call into a no-op fn that LLVM can inline-stub, and the `Instant::now()` pair adds ~20ns. The expectation going in: zero measurable regression.

## Headline gates vs BullMQ baseline

| Scenario              | BullMQ 5.76.4 | ChasquiMQ slice 5 (mean of 2) | Ratio       | Gate    |
|-----------------------|--------------:|------------------------------:|------------:|---------|
| `queue-add-bulk` (50) |     60,828/s  | **196,038/s**                 | **3.22×**   | 3× ✅    |
| `worker-concurrent`   |     47,707/s  | **419,004/s**                 | **8.78×**   | 5× ✅    |

Both gates clear with the same headroom as slice 4.

## Full scenario sweep — slice 5 vs slice 4

Mean across 2 invocations of `--repeats 5 --scale 5 --discard-slowest 1`.

| Scenario                    | Slice 4 mean | Slice 5 mean | Δ vs slice 4 | Notes                                     |
|-----------------------------|-------------:|-------------:|-------------:|-------------------------------------------|
| `queue-add` (single)        |       16,776 |       16,548 |       −1.4%  | Producer-only path; no consumer hooks     |
| `queue-add-bulk` (50)       |      191,421 |      196,038 |       +2.4%  | Producer-only; well within noise          |
| `queue-add-delayed`         |       16,549 |       16,618 |       +0.4%  | Producer-only path                        |
| `worker-concurrent` (100)   |      416,869 |      419,004 |       +0.5%  | Per-job `JobOutcome` fires here ~420k×/s  |
| `worker-delayed-end-to-end` |      749,134 |      755,034 |       +0.8%  | Reader+worker+promoter all instrumented   |
| `worker-generic` ⚠ noisy    |      414,150 |      414,010 |       −0.0%  | Sub-ms bench window — direction-only      |
| `worker-retry-throughput`   |      115,685 |      116,542 |       +0.7%  | New `RetryScheduled` event fires here     |

## What this means

- **No regression on the headline scenarios.** `worker-concurrent` runs the per-job `JobOutcome` event ~420k times per second; `worker-retry-throughput` additionally fires `RetryScheduled` once per retry round-trip; `worker-delayed-end-to-end` exercises everything (reader → worker → retry → promoter). All three are flat-to-up within noise. The decision to use `Instant::now()` unconditionally (rather than gating on sink identity) was the right tradeoff — the ~20ns/job cost is invisible in practice, and the simpler call site is worth more than a micro-optimization that wouldn't measurably register.
- **Per-scenario stddev on slice 5 ranges 76–39,132 jobs/s.** Every delta in the table is well inside one stddev for both runs. The two scenarios with the largest swings (`worker-delayed-end-to-end` ±37k between runs, `worker-concurrent` ±14k) are noise-dominated — the run-to-run variance is comparable to (and in some cases larger than) the slice-4-vs-slice-5 delta.
- **CPU envelopes unchanged** (`worker-concurrent` ~1.78× cores, `worker-delayed-end-to-end` ~1.88× — same as slice 4).
- **`queue-add` −1.4% follows the same noise pattern as slice 4** (slice 4 saw −3.6% on the same producer-only scenario vs Phase 2 final). These paths don't touch any code that changed in either slice — host-state / cache-footprint artifact.

## Two independent runs (raw)

Detailed logs in `benchmarks/runs/slice5-fair-*.log` (gitignored).

| Scenario | Run 1 (00:24:14) | Run 2 (00:29:21) |
|---|---:|---:|
| `queue-add` | 16,795 | 16,302 |
| `queue-add-bulk` | 196,244 | 195,833 |
| `queue-add-delayed` | 16,781 | 16,455 |
| `worker-concurrent` | 411,959 | 426,049 |
| `worker-delayed-end-to-end` | 773,497 | 736,571 |
| `worker-generic` ⚠ | 415,593 | 412,427 |
| `worker-retry-throughput` | 113,465 | 119,620 |

## Verdict

Slice 5 ships clean. The new per-job sink hooks are free with the
default `NoopSink`: virtual call into an empty function inside
`catch_unwind`, plus one `Instant::now()` pair, fires ~420k times per
second on `worker-concurrent` and produces no measurable throughput
delta vs slice 4. The deferred test items (reclaimed-from-CLAIM, race-lost
retry script) are tracked as separate follow-ups; their absence doesn't
change the perf picture.

## Reproducing

```bash
docker start chasquimq-bench-redis  # redis:8.6
cd ~/Projects/experiments/chasquimq
cargo run --release -p chasquimq-bench -- \
  --redis-url redis://127.0.0.1:6379 \
  --repeats 5 --scale 5 --discard-slowest 1
```
