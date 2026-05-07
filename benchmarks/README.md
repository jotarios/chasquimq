# Benchmarks

ChasquiMQ's headline claim is **"the fastest open-source message broker for Redis"** — this directory is the evidence behind that claim, and the honest caveats around it.

## Headline numbers

Measured on Apple M3, Redis 8.6 (Docker, loopback), single host. BullMQ 5.76.4 baseline run on the same machine. Latest committed run is slice 5 (5 repeats × scale=5, drop-slowest, mean of 2 invocations) — slices 6 (idempotent delayed scheduling) and 7 (`cancel_delayed` + promoter side-index cleanup) are not on the throughput hot path the bench measures, so the slice 5 numbers are still the canonical snapshot. A re-bench after each future hot-path change is the convention.

| Scenario                    | BullMQ 5.76.4 | ChasquiMQ        | Ratio       |
|-----------------------------|--------------:|-----------------:|------------:|
| `queue-add-bulk` (50)       |     60,828/s  | **196,038/s**    | **3.22×**   |
| `worker-concurrent` (100)   |     47,707/s  | **419,004/s**    | **8.78×**   |
| `worker-delayed-end-to-end` |          n/a  |     **755,034/s**|         n/a |
| `worker-retry-throughput`   |          n/a  |     **116,542/s**|         n/a |
| `queue-add` (single)        |     13,961/s  |       16,548/s   |     1.19×   |
| `queue-add-delayed`         |          n/a  |       16,618/s   |         n/a |
| `worker-generic` ⚠          |     13,250/s  |      414,010/s   |    31.2×    |

`queue-add` and `worker-generic` are latency-bound (single in-flight op) — they aren't the throughput claim. The two scenarios that matter for "fastest broker on Redis" are `queue-add-bulk` and `worker-concurrent`; both clear the 3× gate, and `worker-concurrent` clears 5× comfortably.

⚠ `worker-generic`'s bench window is too small for stable measurement (~12ms at 419k/s); treat the ratio as direction-only.

`worker-delayed-end-to-end` and `worker-retry-throughput` are Phase 2 paths (no BullMQ counterpart in `bullmq-bench`).

## Detailed reports

- [`baseline-bullmq.md`](baseline-bullmq.md) — full BullMQ baseline methodology, raw numbers, lessons from running the suite (notably: `enableAutoPipelining` *hurts* on loopback).
- [`chasquimq-phase1.md`](chasquimq-phase1.md) — full ChasquiMQ Phase 1 results, post-critique iterations, and harness improvements (distribution stats, `--scale` flag, slowest-discard).
- [`chasquimq-phase2-slice2.md`](chasquimq-phase2-slice2.md) — Phase 2 slices 1 (delayed jobs) and 2 (retry backoff). No-regression check on Phase 1 hot-path scenarios + three new scenarios for the delayed and retry paths.
- [`chasquimq-phase2-final.md`](chasquimq-phase2-final.md) — Final Phase 2 (slices 1–3) ship-readiness measurement. All three slices (delayed jobs, retry backoff, DLQ tooling) + the daster-bug review fixes. 5 repeats × scale=5, drop-slowest applied. Both headline gates clear: `queue-add-bulk` 3.18×, `worker-concurrent` 8.71×.
- [`observability.md`](observability.md) — **Slice 4 (observability) no-regression check.** `MetricsSink` trait + `dispatch()` panic-safe wrapper + extended PROMOTE_SCRIPT returning `{promoted, depth, oldest_pending_lag_ms}`. Mean of 3 invocations: gates still clear (`queue-add-bulk` 3.15×, `worker-concurrent` 8.74×), no regression on the worker paths that actually exercise the changes.
- [`observability-slice5.md`](observability-slice5.md) — **Slice 5 (consumer/retry/DLQ observability) no-regression check.** Adds per-job `JobOutcome` (with `Instant::now()` pair + `dispatch()` per handler invocation), `RetryScheduled`, `DlqRouted`, `ReaderBatch`, plus a `QueueLabeled<S>` adapter wrapper using `Arc<str>` clones. Mean of 2 invocations: gates still clear (`queue-add-bulk` 3.22×, `worker-concurrent` 8.78×), every per-scenario delta vs slice 4 within one stddev — the per-job hot-path hooks are free with `NoopSink` (the default).
- [`phase4-bench-guard.md`](phase4-bench-guard.md) — **Phase 4 bench guard.** Producer/consumer bindings + CLI added in Phase 4 are isolated from the engine hot path (zero diff in `chasquimq/` or `chasquimq-bench/src/`); a re-run on the same host shows `queue-add-bulk` reproducing within +3.1% and `worker-concurrent` falling to 120k due to host load (4× higher than the Phase 2 final run).
- [`python-handler.md`](python-handler.md) — **Python-handler-in-loop bench.** Throughput ceiling for a no-op Python coroutine handler (~46.7k jobs/s on this host today, ~40% of the engine's `worker-concurrent` ceiling under the same host load). Closes the Python-handler bench gap flagged in `CLAUDE.md`. Run script: [`benchmarks/scripts/python_handler_bench.py`](scripts/python_handler_bench.py).
- [`ffi-buffer-copy.md`](ffi-buffer-copy.md) — **FFI buffer-copy gate.** Criterion microbench isolating the Rust-side cost of moving msgpack payloads across the Node and Python FFI seams. ~15ns inbound + ~30ns outbound for a realistic 256B payload — ~1.9% of one core at the engine's 420k jobs/s ceiling. Source: [`chasquimq-bench/benches/ffi_buffer_copy.rs`](../chasquimq-bench/benches/ffi_buffer_copy.rs).
- [`post-1.0-bench-baseline.md`](post-1.0-bench-baseline.md) — **Post-1.0 polish slice 3 baseline + no-regression check.** Adds the two bench scenarios above and confirms `queue-add-bulk` reproduces Phase 2 final to within 0.2%; `worker-concurrent` matches the Phase 4 bench-guard re-run (host-load-explained, no engine code changed).

## Reproducing

Requires Redis 8.6+ on `127.0.0.1:6379` (loopback). The BullMQ baseline lives in a sibling repo cloned at `~/Projects/experiments/bullmq-bench` (not vendored).

```bash
# Redis (one-time)
docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

# BullMQ baseline
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts

# ChasquiMQ — canonical run (5 repeats, scale=5, drop slowest)
cd ~/Projects/experiments/chasquimq
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Raw run logs land in `benchmarks/runs/` (gitignored — local only). The committed `.md` files in this directory are the canonical record.

## Interpreting numbers

> **Host-load floor gate.** A "host-load floor" / "host contention" /
> "concurrent agents" explanation for a `worker-concurrent` regression
> only applies when `git diff <previous-baseline> -- chasquimq/` is
> empty. If engine code changed, that explanation is forfeited; the
> regression must be re-run on a quiet host (`load avg < 1.0`) before
> being accepted as no-regression.

## When to rerun

The bench is the headline claim — re-measure when it could move:

- Each tagged release.
- Whenever the engine hot path is touched
  (`chasquimq/src/{consumer,producer,redis}/`).
- At least once per major refactor, even if the diff looks
  hot-path-adjacent rather than hot-path itself.

Rebench cadence is per-PR judgment for changes outside those
directories; small tweaks to bindings, the CLI, or docs do not need a
fresh run.

## Methodology limitations

The numbers above are defensible for *this hardware* and *this setup*. Open caveats, tracked in `TODOS.md`:

- **Latency unmeasured.** Throughput only — no dispatch-to-ack p99 yet.
- **Same-host bench.** Bench process and Redis share cores. Apples-to-apples vs BullMQ on the same host; not directly comparable to BullMQ's published cross-host numbers.
- **Worker CPU vs BullMQ unmeasured.** ChasquiMQ's CPU is instrumented; BullMQ's isn't (the `bullmq-bench` suite doesn't measure it). The PRD's "≥50% less worker CPU" target requires building a parallel CPU measurement against BullMQ before we can claim it.
- **No persistence.** Redis is in-memory default config — no AOF, no RDB. Production-realistic numbers would be lower for both queues.
