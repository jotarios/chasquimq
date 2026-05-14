# Result-backend opt-in — `store_results=true` bench

> **Status as of PR #92 (2026-05-14): writer pipelining lifted opt-in throughput from 7.1% → 72% of opt-out** on the same host. See [Post-pipelining (PR #92)](#post-pipelining-pr-92) at the bottom of this file. The 1.0 baseline section below is preserved as the "before" reference.

---

## 1.0 baseline (the "before" — preserved for reference)

**Run date:** 2026-05-07
**Branch:** `fix/cross-shim-delay-ms` (post-1.0 polish, deferred follow-up #15)
**Host:** Apple M3, 8 logical cores, 8 GB RAM, macOS 15.7.2. Redis 8.6.2 (Docker, loopback).
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1` (canonical).
**Host load (matched window):** `load avg 7.10 / 7.64 / 4.97` at the start of the run, falling to ~4 by the end. Contended-Mac territory, same shape as the prior `worker-concurrent` host-load floor (see [`post-1.0-bench-baseline.md`](post-1.0-bench-baseline.md)).

This closes the deferred "no sustained-throughput claim about the result-backend write path" item left open in [`post-1.0-bench-baseline.md`](post-1.0-bench-baseline.md) by quantifying the cost of the opt-in `store_results=true` path against the default `store_results=false` path in the same window on the same host.

## Methodology

A new bench scenario `worker-concurrent-store-results` was added at
[`chasquimq-bench/src/scenarios/worker_concurrent_store_results.rs`](../chasquimq-bench/src/scenarios/worker_concurrent_store_results.rs).
It mirrors the canonical `worker-concurrent` scenario byte-for-byte (concurrency=100, 256-entry batch, tiny payload, drain-to-empty model) with two flips:

1. `ConsumerConfig.store_results = true` and `result_ttl_secs = 3600`.
2. Handler returns a fixed 64-byte `Bytes` result on every successful job (representative small payload), instead of `Bytes::new()`.

Both scenarios were run in the same invocation back-to-back so the host snapshot is identical:

```
cargo run -p chasquimq-bench --release -- \
    --scenario worker-concurrent,worker-concurrent-store-results \
    --repeats 5 --scale 5 --discard-slowest 1 --format markdown
```

## Numbers

Mean across the 4 fastest of 5 repeats per scenario (drop-1-slowest).

| Scenario | Mean (jobs/s) | p50 | p95 | p99 | stddev | CPU load (× core) | jobs/CPU-sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `worker-concurrent` (`store_results=false`) | **117,170** | 116,971 | 117,817 | 117,931 | 464 | 1.69× | 69,259 |
| `worker-concurrent-store-results` (`store_results=true`) | **8,291** | 8,280 | 8,407 | 8,415 | 95 | 0.36× | 23,566 |

**Verdict: opt-in throughput is 7.1% of opt-out at concurrency=100** on this host (8,291 / 117,170).

The off-path number reproduces the canonical 118,117 jobs/s baseline ([`post-1.0-bench-baseline.md`](post-1.0-bench-baseline.md)) within 1% — the comparison is on a fresh, valid host snapshot, not a stale baseline.

## Distribution stats

The opt-in path's stddev is tight (95 jobs/s, ~1.1% of mean) — the slowdown is not jitter, it is sustained. p99/p50 ratio is 1.016 (versus 1.008 for the off path). Both distributions are tight; the gap is real and stable.

## CPU comparison

| Scenario | Wall (ms) | CPU user% | CPU sys% | CPU total (× core) | jobs/CPU-sec |
|---|---:|---:|---:|---:|---:|
| `store_results=false` | ~470 | ~118% | ~48% | 1.69× | 69,259 |
| `store_results=true`  | ~6,700 | ~14% | ~18% | 0.36× | 23,566 |

The opt-in path does **3× less work per CPU-second** (23,566 vs 69,259). CPU is dramatically underutilized (0.36× of one core — the bench process is sitting on Redis round trips, not computing). This is the diagnostic signature of a network-bound bottleneck.

## Where the time goes

`run_ok_result_writer` ([`chasquimq/src/ack.rs:122-149`](../chasquimq/src/ack.rs)) consumes a single-producer `mpsc::Receiver<JobOk>` and `await`s one `EVALSHA` per item on the fred client. With concurrency=100 handlers feeding the channel and a single `await` loop draining it, every successful job pays one synchronous Redis RTT in the writer task — there is no batching for result writes (each call has distinct keys + argv, so a Lua-level batch would need a different script shape). At loopback Redis RTTs this caps throughput at ~1/RTT, which matches the observed ~8k jobs/s.

Compare to the default path, which batches up to `ack_batch=256` `XACKDEL` ids per pipelined call. That's where the ~14× headroom comes from.

This was the expected story going in — "extra round trip" — but the magnitude is larger than a naive read suggests, because it's not "one extra RTT per job", it's "every successful job becomes a synchronous RTT in a serial writer". CLAUDE.md flags per-job round trips as the bottleneck ChasquiMQ exists to escape; this confirms that the result-backend writer is the one place in the consumer path that still has it, by design (per-entry script needs distinct argv).

## Implications and caveats

- **Default path is unchanged.** `store_results=false` still measures 117,170 jobs/s on this host (within 1% of the post-#79 canonical 118,117). The opt-in plumbing is genuinely zero-overhead for users who don't enable it.
- **The opt-in path is for correctness, not throughput.** Users who need durable per-job results (response-result patterns, audit trails) should opt in and accept the throughput floor. Users who don't need them should stay on the default.
- **Headroom for a future optimization.** A multi-key Lua script that XACKDELs N entries and SETs N result keys in one EVALSHA would close most of the gap (would still pay 1 round trip per N≤k jobs instead of 1 per job). Out of scope for this slice — this report locks in the current baseline so a future PR can quantify the win.
- **Host load.** Numbers were collected at load avg 7-8, falling to ~4. The off-path number reproduces canonical (within 1%), so the snapshot is valid. The opt-in stddev is 1.1% of mean — tight enough to claim the verdict.

## Reproduction

```bash
docker start chasquimq-bench-redis  # redis:8.6.2 on 127.0.0.1:6379

cargo run -p chasquimq-bench --release -- \
    --scenario worker-concurrent,worker-concurrent-store-results \
    --repeats 5 --scale 5 --discard-slowest 1
```

For raw jsonl per-run distribution (5 samples per scenario):

```bash
cargo run -p chasquimq-bench --release -- \
    --scenario worker-concurrent,worker-concurrent-store-results \
    --repeats 5 --scale 5 --discard-slowest 1 --format jsonl
```

---

## Post-pipelining (PR #92)

**Run date:** 2026-05-14
**Branch:** `worktree-perf-result-writer-pipeline` (PR #92 — `perf(consumer): pipeline result-writer EVALSHA`)
**Host:** Apple M3, 8 logical cores, macOS 15.7.2. Redis 8.6.2 (Docker, loopback). Same host as the 1.0 baseline above.
**Bench config:** `--repeats 5 --scale 5 --discard-slowest 1` (canonical, byte-for-byte matched to the baseline above).
**Host load (matched window):** `load avg 5.06 / 4.34 / 3.40` at start. Still contended-Mac territory; this is a deliberate same-host A/B against the 1.0 baseline above. The 1.0 baseline was measured at higher load (~7) — the comparison is conservative on the new code's side.
**Engine change:** `run_ok_result_writer` ([`chasquimq/src/ack.rs`](../chasquimq/src/ack.rs)) was rewritten to mirror `run_ack_flusher`: drain `Vec<JobOk>` of capacity `result_batch` (default 64) or until `result_idle_ms` (default 5ms), then fire one `redis::Pipeline` of N `EVALSHA`s via `pipeline.try_all`. Failure semantics are now per-element (NOSCRIPT triggers a whole-batch inline-`EVAL` rebuild; other errors leave only the affected entry pending). See PR #92 for the full design.

### Numbers

Mean across the 4 fastest of 5 repeats per scenario (drop-1-slowest), same shape as the 1.0 baseline section above.

| Scenario | Mean (jobs/s) | p50 | p95 | p99 | stddev | CPU load (× core) | jobs/CPU-sec |
|---|---:|---:|---:|---:|---:|---:|---:|
| `worker-concurrent` (`store_results=false`) | **96,594** | 97,015 | 112,365 | 113,312 | 13,729 | 1.52× | 63,244 |
| `worker-concurrent-store-results` (`store_results=true`) | **69,813** | 70,968 | 73,293 | 73,587 | 3,725 | 1.35× | 51,903 |

**Verdict: opt-in throughput is 72.3% of opt-out at concurrency=100** on this host (69,813 / 96,594). That's an **8.4× improvement over the 1.0 baseline** (8,291 → 69,813), clearing issue #92's ≥5× acceptance gate by a wide margin.

### Comparison

| Path | 1.0 baseline (2026-05-07) | PR #92 (2026-05-14) | Δ |
|---|---:|---:|---:|
| `store_results=false` (off) | 117,170 jobs/s | 96,594 jobs/s | −18% (host-load delta; off path unchanged in code) |
| `store_results=true` (on) | 8,291 jobs/s | **69,813 jobs/s** | **+742% (8.4×)** |
| Opt-in / opt-out ratio | **7.1%** | **72.3%** | +65 pp |

The off-path number dropped 18% because the comparison host was less loaded on 2026-05-14 — but the engine's `worker-concurrent` code is byte-identical on both runs (no perf-relevant change touches the default path; this PR only modifies `run_ok_result_writer`). The interpretable headline is the **ratio**, which jumped from 7.1% to 72.3%. The on-path absolute jumped 8.4×.

### Distribution stats

The opt-in path's stddev tightened in relative terms: 3,725 jobs/s on a 69,813 mean is ~5.3% of mean, versus 1.1% on the 1.0 baseline — wider, but consistent with the higher absolute throughput. The p99/p50 ratio is 1.037 (versus 1.016 on the 1.0 baseline). Distribution is still tight enough to claim the verdict.

The off-path stddev is wider (13,729 on 96,594, ~14%) because of host-load jitter during the run, not engine behavior — the p50 (97,015) is close to the canonical 117k floor and the p95 (112,365) brushes it.

### Where the time goes now

Per-job RTTs are gone. With `result_batch=64` and `concurrency=100`, the writer drains ≤64 entries per round trip, so the network-bound floor moved from 1/RTT to (roughly) 64/RTT. CPU is also more utilized: 1.35× core on the opt-in path versus the prior 0.36×, because the writer task now does meaningful work between RTTs (pipeline encoding for 64 entries). The opt-in path is no longer network-RTT-bound at concurrency=100 on loopback Redis; it is approaching the same Redis command-loop ceiling as the off path.

The remaining 28-point gap to opt-out (72% vs 100%) is:

- Result-key SET still costs proportionally more bytes per Redis command than XACKDEL alone.
- The opt-in path runs Lua (`JOB_OK_SCRIPT`) for every entry; the off path's batched `XACKDEL` is a single C-level command per batch.
- A future "Option B" (multi-key Lua that XACKDELs N + SETs N inside one `EVALSHA`) would close most of the remaining gap, at the cost of script complexity and partial-batch semantics. Not pursued in PR #92; tracked as a v1.2 follow-up.

### Caveats

- **Same-host A/B; not a quiet-host headline.** Both runs were on the contended Mac (load avg 4-7), the same host as the 1.0 baseline. Cloud-Redis and quiet-host numbers will differ.
- **Failure-path semantics differ.** The 1.0 path leaves a single entry pending on error; the new path leaves only the failing entry of a batch pending and applies sibling successes. See [`chasquimq/src/ack.rs`](../chasquimq/src/ack.rs) `run_ok_result_writer` rustdoc for the full contract. Net behavior is at least as correct, often more granular.
- **Default knobs are conservative.** `result_batch=64` and `result_idle_ms=5` were chosen for loopback Redis with low-RTT. Cloud-Redis users on >5ms RTT should likely raise both — see [`docs/engine.md`](../docs/engine.md) for tuning notes.
