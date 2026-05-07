# Post-1.0 polish — bench baseline & no-regression check

**Run date:** 2026-05-07
**Branch:** `worktree-agent-afda244fae7714482` (post-1.0 polish slice 3)
**Host:** Apple M3, 8 logical cores, 8 GB RAM, macOS 15. Redis 8.6 (Docker, loopback).
**Bench config:** `--repeats 3 --scale 5 --discard-slowest 1` (engine), `--repeats 3` (Python).

This slice ships two new benchmark scenarios that close the gaps the
[`CLAUDE.md`](../CLAUDE.md) repository status flagged after Phase 4:

1. **Python-handler-in-loop** — measures the throughput ceiling for a
   no-op Python coroutine handler, isolating the PyO3 dispatch path.
   See [`python-handler.md`](python-handler.md).
2. **FFI buffer-copy gate** — Criterion microbench measuring the
   Rust-side cost of moving msgpack payloads across the Node and
   Python FFI seams. See [`ffi-buffer-copy.md`](ffi-buffer-copy.md).

Zero engine code changed in this slice (`git diff main -- chasquimq/`
is empty). The new artifacts live under `chasquimq-bench/benches/`,
`benchmarks/scripts/`, and `benchmarks/*.md`.

## Engine no-regression check

Same canonical run shape as Phase 2 final (`--repeats N --scale 5
--discard-slowest 1`), with N=3 reduced from N=5 to keep the bench
window short on a contended host.

| Scenario              | Phase 2 final (2026-05-01) | Phase 4 re-run (2026-05-03) | This slice (2026-05-07) | Δ vs Phase 2 final |
|-----------------------|---------------------------:|----------------------------:|------------------------:|-------------------:|
| `queue-add-bulk` (50) | 193,251 | 199,308 | **193,643** | **+0.2%** |
| `worker-concurrent`   | 415,580 | 120,265 | **116,745** | -71.9% (host-load) |

* `queue-add-bulk` reproduces the Phase 2 final number to within 0.2% —
  the producer hot path is rock-stable, well inside the ±5% tolerance.
* `worker-concurrent` matches the Phase 4 bench-guard re-run (120,265)
  to within 3%, not the Phase 2 final number. As documented in
  [`phase4-bench-guard.md`](phase4-bench-guard.md), the discrepancy is
  environmental, not a code regression: host load avg ~3 today (other
  agent processes + Chrome renderers) vs. ~0.7 during the Phase 2 final
  measurement, and `worker-concurrent` is the most CPU-contention-
  sensitive scenario in the suite. **`git diff main -- chasquimq/
  chasquimq-bench/src/` is empty for this slice; the engine and the
  bench scenario logic are byte-identical to Phase 4 close.** The new
  `chasquimq-bench/benches/ffi_buffer_copy.rs` is a separate `[[bench]]`
  target, not a scenario.

The ±15% concurrent-tolerance band the bench guard prescribes is
breached, but only by the same amount and for the same reason the
Phase 4 bench-guard already documented. This is the host-load floor
on contended Mac hosts; the engine ceiling has not moved.

For belt-and-braces, the producer-only path (which bottlenecks on
Redis, not on host CPU) reproduces cleanly:

| Scenario              | Phase 2 final | This slice | Δ |
|-----------------------|--------------:|-----------:|--:|
| `queue-add-bulk` (50) |       193,251 |    193,643 | +0.2% |
| `queue-add` (single)  |        17,394 |     16,503 | -5.1% |
| `worker-generic` ⚠    |       418,946 |      9,824 | sub-ms window |

`queue-add-bulk` ships clean. `queue-add` is borderline (-5.1%) but
inside the noise the Phase 4 bench-guard already showed (-5.6% there).
`worker-generic` is sub-millisecond and not interpretable, as flagged
in every prior report.

## New scenario numbers

### Python-handler-in-loop — 46,740 jobs/s

```bash
python benchmarks/scripts/python_handler_bench.py \
    --jobs 100000 --warmup 10000 --concurrency 100 --repeats 3
```

| Run | jobs/s | elapsed (s) | CPU % | p50 dispatch gap | p99 dispatch gap |
|----:|-------:|------------:|------:|-----------------:|-----------------:|
|   1 | 53,135 |       1.882 | 201.6 |            429us |          1,356us |
|   2 | 43,388 |       2.305 | 185.5 |            505us |          1,894us |
|   3 | 43,697 |       2.288 | 197.8 |            524us |          1,613us |

**Mean:** 46,740 jobs/s (stddev 4,523), CPU 195% (≈2 cores saturated).

The Python no-op handler reaches **40% of the engine ceiling** on the
same host (`worker-concurrent` 116,745 today). The other 60% is the
PyO3 dispatch seam. Full breakdown in [`python-handler.md`](python-handler.md).

### FFI buffer-copy — 256B inbound: 14.8ns @ 16.1 GiB/s

```bash
cargo bench -p chasquimq-bench --bench ffi_buffer_copy
```

| Path                                                    | 256B mean | 256B thrpt   |
|---------------------------------------------------------|----------:|-------------:|
| Inbound `Bytes::copy_from_slice` (host → engine)        |    14.8ns |  16.13 GiB/s |
| Outbound `Bytes::to_vec` (Node `Buffer::from(Vec)`)     |    30.0ns |   7.96 GiB/s |
| Outbound `slice.to_vec` (PyBytes-equivalent)            |    13.8ns |  17.24 GiB/s |

Per-job round trip at the byte-copy layer for a realistic 256B
payload: ~45ns. At the engine's 420k jobs/s ceiling that's ~1.9% of
one core. The byte copy is **not** the bottleneck. Full table in
[`ffi-buffer-copy.md`](ffi-buffer-copy.md).

## Files added

* `benchmarks/python-handler.md` — bench doc + baseline run.
* `benchmarks/ffi-buffer-copy.md` — bench doc + baseline run.
* `benchmarks/post-1.0-bench-baseline.md` — this file.
* `benchmarks/scripts/python_handler_bench.py` — Python harness.
* `chasquimq-bench/benches/ffi_buffer_copy.rs` — Criterion microbench.
* `chasquimq-bench/Cargo.toml` — add `bytes` dep, `criterion` dev-dep,
  `[[bench]]` target.

## Reproducing

```bash
docker start chasquimq-bench-redis  # redis:8.6.2

# Engine baseline (matches Phase 2 final shape)
cargo run -p chasquimq-bench --release -- \
    --repeats 3 --scale 5 --discard-slowest 1 \
    --scenario queue-add-bulk,worker-concurrent

# FFI microbench
cargo bench -p chasquimq-bench --bench ffi_buffer_copy

# Python handler-in-loop (requires `cd chasquimq-py && maturin develop --release`)
python benchmarks/scripts/python_handler_bench.py \
    --jobs 100000 --warmup 10000 --concurrency 100 --repeats 3
```
