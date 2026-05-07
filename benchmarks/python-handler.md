# Python-handler-in-loop bench

**Scope.** Measure the throughput of a ChasquiMQ `Worker` whose handler
is a no-op Python coroutine. This isolates the cost of the PyO3 dispatch
path — `pyo3-async-runtimes::into_future_with_locals` punching every job
across the tokio↔asyncio seam — from any user-handler work. Closes the
"Python-handler-in-loop scenario" gap [`CLAUDE.md`](../CLAUDE.md) flagged
post-Phase 4.

The Rust-only `worker-concurrent` scenario in `chasquimq-bench` measures
the engine ceiling. This script measures what's left after the Python
seam takes its cut. The delta is the ceiling on what a Python user can
get out of the engine without rewriting their handler in Rust.

## Run

```bash
# 1. Build the wheel against this checkout, in a venv:
cd chasquimq-py
python3 -m venv .venv-bench && source .venv-bench/bin/activate
pip install msgpack redis maturin
maturin develop --release
cd ..

# 2. Run the bench (defaults: 100k jobs after 10k warmup, concurrency=100,
#    3 repeats, payload ~100B):
python benchmarks/scripts/python_handler_bench.py
```

Override defaults:

```bash
python benchmarks/scripts/python_handler_bench.py \
    --redis-url redis://127.0.0.1:6379 \
    --jobs 100000 --warmup 10000 \
    --concurrency 100 --payload-bytes 100 --repeats 3
```

## Method

* **Pre-load via the native producer.** Jobs are XADD'd to the stream
  via `chasquimq._native.Producer` (the same Rust `Producer<RawBytes>`
  that `chasquimq-bench` drives) before the consumer starts. Producer
  noise stays out of the consumer measurement window.
* **No-op handler.** The Python coroutine is `async def handler(_job):
  pass` semantics — no I/O, no compute. Anything the handler does is
  throughput tax on top of these numbers.
* **Stopwatch starts after warmup.** First N jobs (default 10k) drain
  before the timer starts, so the bench window is steady-state.
* **CPU measured via `getrusage(RUSAGE_SELF)`.** Same-process CPU
  attributable to the bench (Python interpreter + tokio runtime + any
  redis-py preload churn).
* **Dispatch-gap latency.** Per-handler `time.perf_counter()` taken at
  handler entry; sampled every ~24 jobs to keep overhead negligible.
  Reported as p50/p99 of the inter-handler gap. With concurrency=100
  this is the *aggregate* dispatch interval (interleaved across worker
  tasks) — the per-task interval is 100× larger.

## Baseline

**Run date:** 2026-05-07
**Host:** Apple M3, 8 logical cores, macOS 15. Redis 8.6 (Docker, loopback).
**Bench:** `--jobs 100000 --warmup 10000 --concurrency 100 --repeats 3`.
Host load: ~3 (concurrent agent processes; same conditions as the Phase 4 bench-guard re-run).

| Run | jobs/s | elapsed (s) | CPU % | p50 dispatch gap | p99 dispatch gap |
|----:|-------:|------------:|------:|-----------------:|-----------------:|
|   1 | 53,135 |       1.882 | 201.6 |            429us |          1,356us |
|   2 | 43,388 |       2.305 | 185.5 |            505us |          1,894us |
|   3 | 43,697 |       2.288 | 197.8 |            524us |          1,613us |

**Mean:** 46,740 jobs/s (stddev 4,523); CPU 195.0% (stddev 6.9%).
**Dispatch gap (handler-to-handler interval):** p50 mean 486us, p99 mean 1,621us.

## Interpretation

`worker-concurrent` (engine, no handler) on this host today: 116,745
jobs/s. Python-handler-in-loop on the same host: 46,740 jobs/s — **~40%
of the degraded host-load ceiling**, not the canonical engine ceiling.
The Phase 2 final canonical `worker-concurrent` is 415,580 jobs/s; the
~116k figure is what reproduces on this host today under load avg ~3
(see [`post-1.0-bench-baseline.md`](post-1.0-bench-baseline.md)). Both
numbers are measured under the same host conditions, so the ratio is
the stable signal even if the absolute ceiling is depressed. The other
60% is the PyO3 dispatch seam: GIL acquire, `into_future_with_locals`
setup, asyncio scheduler step, task creation, awaitable resolution,
exception path check.

CPU at ~195% means the bench saturates ~2 cores: roughly one for the
asyncio loop (single-threaded by GIL, but the engine reader and
ack-flusher tasks run on tokio worker threads in parallel, and
`getrusage` rolls all of them into one number) and one for the engine.
The remaining headroom is GIL-serialized — adding more concurrency past
~100 will not help.

This is the regression-trackable number for the Python track. A future
engine change that drops it below ~40k/s on this host (under similar
load) is a Python-side regression worth investigating; a change that
raises it above ~55k/s on this host is a Python-side win worth
celebrating.

## Reproducing the engine baseline alongside

To confirm the engine ceiling on the same host conditions:

```bash
cargo run -p chasquimq-bench --release -- \
    --repeats 3 --scale 5 --discard-slowest 1 \
    --scenario queue-add-bulk,worker-concurrent
```

The ratio `worker-concurrent : python-handler-in-loop` is the
"PyO3 dispatch tax" on this host. The absolute numbers move with host
load; the ratio is the stable signal.
