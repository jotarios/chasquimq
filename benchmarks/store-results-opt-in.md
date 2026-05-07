# Result-backend opt-in — `store_results=true` bench

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
