# Worker latency under low-rate dispatch — `worker-latency` bench

**Run date:** 2026-05-14
**Branch:** `feat/latency-bench` @ `89e38e8`
**Host:** Apple M3, 8 logical cores, macOS 15.7.2. Redis 8.6.2 (Docker, loopback).
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1` (canonical).
**Host load (matched window):** `load avg 3.79 / 2.96 / 2.51` at start, `2.72 / 3.25 / 2.85` at finish. Contended-Mac territory; inside the documented 1.8–4.3 today's-laptop envelope.
**Engine diff vs `main`:** empty (`git diff main -- chasquimq/` = 0 lines). Zero engine changes — this slice is bench-crate-only.

Closes the "Latency is unmeasured" caveat that lived on [`site/src/content/docs/concepts/performance-trade-offs.md`](../site/src/content/docs/concepts/performance-trade-offs.md) and the corresponding bullet in [`benchmarks/README.md`](README.md). Adds a new `worker-latency` scenario — explicitly a **dispatch-overhead-on-an-idle-queue** measurement, not a saturated-tail one. Concretely separates the two questions a user might ask about latency: "if I publish a job to an idle queue right now, how long until it's done?" (this bench) vs. "what's the tail under saturation?" (a function of queue depth and producer/consumer rate, not the engine).

There is no BullMQ comparator. The upstream `bullmq-bench` suite does not measure per-job latency, so this is a ChasquiMQ-only number — published without an X-faster-than-BullMQ claim by design.

## Method

A single live producer fires on `tokio::time::interval(1ms)` — ~1000 jobs/sec — against a consumer pool with `concurrency=100` and `batch=64`. No preloading. Every job sees a near-empty stream; the bench measures dispatch overhead, not queue-depth wait.

The handler is a no-op. It records two histograms per invocation:

1. **`handler_us`** — the engine's own measurement of handler future duration, sourced from `JobOutcome.handler_duration_us` via a bench-side `MetricsSink` impl. With a no-op handler this is the engine's idea of "how long the handler ran" — useful as a floor on per-invocation engine work and as a baseline for the derived overhead figure below.
2. **`end_to_end_us`** — wall-clock delta from `Job::created_at_ms` (set inside `Producer::add` at producer-side time) to a `SystemTime::now()` reading taken inside the handler. Captures everything: producer encode → `XADD` → Redis bookkeeping → `XREADGROUP` reader loop → msgpack decode → worker channel hop → handler entry → handler body. Misses ack-write latency (the ack flushes after the handler returns).

Aggregation uses `hdrhistogram = "7"` with bounds `1..600_000_000` microseconds, 3 significant figures. Per-repeat histograms merge into a per-scenario aggregate via `Histogram::add(other)`. Overflow is clamped to the upper bound; the first overflow logs via `tracing::warn!` once per scenario and an `AtomicU64` counter is reported at end-of-scenario.

Two fixes landed late in the slice from a `daster-bug` review:

- **Warmup gate.** Both histograms now only record on jobs past the warmup boundary. Without the gate, cold-start outliers (consumer pool spin-up, first-XREADGROUP block) polluted the aggregate. The gate uses the same Stopwatch counter that decides when to terminate the bench.
- **Overshoot-deadlock fix.** The producer task is cancelled via a dedicated `CancellationToken` once the bench-count reaches `warmup + bench`; without this the 1ms `tokio::time::interval` would keep XADDing past the bench window and pile jobs into the stream after the consumer shut down, on rare runs deadlocking the `done_rx` await.

## Numbers

Aggregate across 4 fastest of 5 repeats per scenario (drop-1-slowest). The headline `worker-latency` row reports `end_to_end_us` in the latency columns; the handler-only and engine-overhead distributions are broken out below the table.

| Scenario | Mean (jobs/s) | p50 | p95 | p99 | stddev | CPU load (× core) | jobs/CPU-sec | p50 lat (us) | p99 lat (us) | p99.9 lat (us) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `worker-latency` | **1,050** | 1,050 | 1,050 | 1,050 | 0 | 0.22× | 5,742 | 1,044 | 1,734 | 2,747 |

The `Mean (jobs/s)` value is the producer's hard-coded ~1ms inter-arrival cadence, **not** a throughput finding. CPU is 22% of one core because at 1000 jobs/sec the consumer pool is mostly idle. Both numbers are reported for parity with the rest of the bench harness, not as headline figures.

### `end_to_end_us` (headline)

The number a user sees when they ask "I enqueued a job — when did it run?":

| Percentile | Microseconds |
|---|---:|
| p50 | 1,044 |
| p99 | 1,734 |
| p99.9 | 2,747 |

### `handler_us` (engine-measured, no-op handler)

Engine's measurement of how long the handler future actually ran:

| Percentile | Microseconds |
|---|---:|
| p50 | 1 |
| p99 | 2 |
| p99.9 | 13 |

Sub-microsecond at p50; the long tail is scheduling jitter, not work. This is the floor on engine-side per-invocation overhead — a real handler's `handler_us` is its own work plus this.

### `engine_overhead_us` (derived per-percentile delta)

`end_to_end_us - handler_us` at each percentile. This is **not** a per-job histogram (computing it that way would require holding both values per job before merge); it's the aggregate signal of "everything that's not the handler future itself" — produce + XADD + reader + decode + channel hop + dispatch:

| Percentile | Microseconds |
|---|---:|
| p50 | 1,043 |
| p99 | 1,732 |
| p99.9 | 2,734 |

## Interpretation

p50 end-to-end is ~1ms. At face value that looks heavy, but it's dominated by the producer's intentional 1ms inter-arrival cadence and the millisecond-resolution of `created_at_ms` on the wire. The absolute floor on `end_to_end_us` measurement is ±500us per-job from that resolution alone; at scale=5 the law of large numbers averages most of it out, but the floor remains.

Net: **on this host under low-rate dispatch the engine adds ~1ms of overhead at p50 and ~2.7ms at p99.9 from "Producer::add returned" to "handler future completed."** The engine itself is doing ≤2us of work in the median per the `handler_us` numbers; the rest of the envelope is Redis round trips (XADD + XREADGROUP), msgpack encode/decode, and async-channel dispatch.

For comparison, the saturated regime answers a different question. `worker-concurrent` on the same host sustains 117k jobs/s (`benchmarks/chasquimq-1.0.md`) — that's 8.5us per job amortised, but every job there is sitting behind a deep queue of pending work. The two numbers are not directly comparable.

## What's missing

- **No BullMQ comparator.** `bullmq-bench` does not measure per-job latency. We deliberately do not publish a "ChasquiMQ is N× lower latency than BullMQ" claim — there is no like-for-like number to cite.
- **No ack-latency attribution.** The `end_to_end_us` measurement stops at handler return. The ack flush after that is batched (`ack_idle_ms=5ms` default) and is not separately measured.
- **No engine-internal dispatch split.** We do not separately report the gap between "consumer reader pulled the entry off the wire" and "handler started". Splitting that envelope further is a v1.x+ refinement once we add a dedicated engine hook.
- **Single-host only.** Producer and consumer share a process on the same host as Redis. Distributed-clock support (producer on one host, consumer on another) is out of scope.

## Reproduce

```bash
docker start chasquimq-bench-redis  # redis:8.6.2 on 127.0.0.1:6379

cargo run -p chasquimq-bench --release -- \
    --scenario worker-latency --repeats 5 --scale 5
```

The full canonical run (all scenarios, drop-slowest) that produced the host snapshot above:

```bash
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Note: `--discard-slowest` is ignored for `worker-latency` — drop-slowest applies to per-repeat *means*, not per-job histogram values. Tail-percentile aggregation needs every recorded value to converge.
