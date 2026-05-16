# Pause / resume — bench regression check

**Date:** 2026-05-16. **Host:** Apple M3, contended Mac (this repo's documented host), load avg ~2.4–2.7 at run time (settled below the 1.8–4.3 envelope). **Redis:** `redis:8.6.2` in Docker. **Flags:** `--repeats 5 --scale 5 --discard-slowest 1`.

The pause/resume slice touches the **consumer reader hot path** (`reader_loop` gained a batch-boundary pause gate). It does **not** touch the producer path. Per the host-load gate in `benchmarks/README.md`, because engine code on the consume path changed, contention is *not* a valid hand-wave for any regression — so this is a same-session A/B: `main` (`f4a0abd`) vs `feat/pause-resume`, both binaries built this session, run back-to-back on the same settled host.

## Headline gates

| Scenario | main `f4a0abd` | `feat/pause-resume` | Δ | Gate | Verdict |
|---|---:|---:|---:|---|---|
| `queue-add-bulk` (bulk 50, tiny) | 186,732 jobs/s | 189,459 jobs/s | **+1.46%** | ±5% (producer untouched, must be flat) | ✅ |
| `worker-concurrent` (concurrency 100) | 104,076 jobs/s | 108,297 jobs/s | **+4.06%** | ±15% (reader hot path touched) | ✅ |

Both gates pass with margin. Both scenarios measured slightly *faster* on the branch — within run-to-run noise (a third same-session run of the main binary measured 194,862 / 115,739, confirming the spread is ≈±5%). There is no regression in either direction beyond noise.

- **`queue-add-bulk` flat (+1.46%).** The producer path is byte-unchanged; the new `Producer::pause/resume/is_paused` are not on the `add` path. Confirms zero producer-side cost.
- **`worker-concurrent` no regression (+4.06%).** The pause gate's not-paused cost is one atomic `watch::Receiver::borrow()` plus one `Option<Instant>` comparison per batch — never per-job, never a Redis round trip when not paused. As predicted in the engineering review, this is strictly cheaper than the per-iteration `shutdown.is_cancelled()` already in the reader loop, so it is unmeasurable against run-to-run noise. The fallback plan (move the cross-process check off the hot path) was not needed.

## Secondary scenarios (branch, no regression)

| Scenario | branch | Documented baseline (`chasquimq-1.0.md`) | Note |
|---|---:|---:|---|
| `queue-add` (single, 10×10) | 16,421 jobs/s | 15,366 jobs/s | latency-bound, not a throughput gate; ≥ baseline |
| `worker-generic` (single consumer) ⚠ | 9,407 jobs/s | 9,517 jobs/s | flagged-noisy / latency-bound in the baseline; within its band |

## Reproduce

```bash
docker start chasquimq-bench-redis
cargo run -p chasquimq-bench --release -- \
  --scenario queue-add-bulk,worker-concurrent \
  --repeats 5 --scale 5 --discard-slowest 1
```

The bench harness is in `chasquimq-bench/`; raw logs land in `benchmarks/runs/` (gitignored). Only this summary is committed.
