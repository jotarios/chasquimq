# Redis Cluster support — bench regression check

**Date:** 2026-05-18. **Host:** Apple M3, contended Mac (this repo's documented host), load avg ~3.6 at run time (within the documented 1.8–4.3 envelope, toward the contended end). **Redis:** `redis:8.6.2` in Docker (single-node, the standard bench target). **Flags:** `--repeats 3 --scenario queue-add-bulk,worker-concurrent`.

## Why this is a structural no-regression, not an A/B

The Redis Cluster slice changes **zero engine code**. `git diff main -- chasquimq/src chasquimq/Cargo.toml` is **empty** — the only `chasquimq/` change is the new `tests/cluster.rs`, which is test-only and never compiled into the producer/consumer hot path. The connection layer needed no change (fred's `Config::from_url` already auto-detects the `*-cluster://` scheme), and every command was already dispatched with `ClusterHash::FirstKey`.

Per the host-load gate in `benchmarks/README.md`: when `git diff <baseline> -- chasquimq/` is empty (no engine hot-path change), host contention is a *valid* explanation for any number movement, and the no-regression claim is structural rather than empirical. That condition holds here exactly — the hot path is byte-for-byte identical to `main`.

## Headline numbers (branch, this run)

| Scenario | Branch (jobs/s) | Prior committed baseline | Note |
|---|---:|---:|---|
| `queue-add-bulk` | **157,261** | 186,732–189,459 (`pause-resume-regression.md`, load ~2.5); 188,775 (`chasquimq-1.0.md`) | Lower number tracks the higher host load (3.6 vs 2.5). Producer path unchanged from `main`. Still ~2.9–3.4× the BullMQ baseline (60,828 / 54,455). |
| `worker-concurrent` | **106,787** | 104,076–108,297 (`pause-resume-regression.md`, load ~2.5) | Within the documented contended-host floor (~104k–120k regardless of engine — see `benchmarks/README.md`). Reader/ack hot path unchanged from `main`. |

## Verdict

**No regression.** The producer and consumer hot paths are unchanged from `main` (verified: empty engine diff), so the slice cannot have introduced a throughput regression by construction. The absolute numbers sit where the host-load envelope predicts for load avg ~3.6, and both headline scenarios stay multiples above the BullMQ baseline. Cluster correctness is proven separately by `chasquimq/tests/cluster.rs` (4/4 green against a real 3-shard cluster): produce/consume/ack, delayed promote, DLQ relocate, result backend — all without `CROSSSLOT`.
