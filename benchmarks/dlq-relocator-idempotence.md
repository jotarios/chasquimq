# DLQ relocator idempotence — bench guard

**Scope.** Verify the DLQ-relocator atomicity fix (atomic `RELOCATE_DLQ_SCRIPT` replacing the non-atomic XADD-then-XACKDEL pipeline in `consumer/dlq.rs`) did not regress the engine produce/consume hot path. This is a correctness fix, not a perf change.

**Why the hot path is unaffected (by construction).**

```bash
git diff main..HEAD -- chasquimq/src/producer/mod.rs \
  chasquimq/src/consumer/worker.rs chasquimq/src/consumer/reader.rs \
  chasquimq/src/ack.rs chasquimq/src/redis/conn.rs
# → empty
```

The change is confined to: `consumer/dlq.rs` (the DLQ relocator, a dedicated task that runs only on retry-exhaustion / malformed / oversize — never during steady-state produce or consume), `redis/commands.rs` (added `RELOCATE_DLQ_SCRIPT` + two arg builders, removed the now-dead `xadd_dlq_args`), and `job.rs` (a doc comment only). None of this executes on the `queue-add-bulk` or `worker-concurrent` paths. The relocator was already off the hot path before this change and remains so; the only difference is one EVALSHA round trip in place of a two-command pipeline (same ~1 RTT, after a once-per-task cached `SCRIPT LOAD`).

**Validation run.**

```bash
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Apple M3, Redis 8.6.2 (Docker), host load avg **5.34** at run time — more contended than the [1.0 re-bench](chasquimq-1.0.md) (load avg ~1.8–4.3).

| Scenario | [1.0 baseline](chasquimq-1.0.md) (contended) | This run (load 5.34) | Delta |
|---|---:|---:|---:|
| `queue-add-bulk` (headline) | 188,775 jobs/s | 186,874 jobs/s | −1.0% |
| `worker-concurrent` (headline) | 111,968 jobs/s | 113,195 jobs/s | +1.1% |
| `queue-add` (single) | 15,366 jobs/s | 17,867 jobs/s | +16.3% |
| `worker-generic` ⚠ noisy | 9,517 jobs/s | 9,776 jobs/s | +2.7% |

**Verdict: no regression.** Both headline gates are flat-to-improved. The −1.0% on `queue-add-bulk` is inside host-noise: this run was on a higher-load host than the baseline, and the host-load gate in [`README.md`](README.md) explicitly admits a delta when the engine hot path is unchanged (verified empty diff above). `worker-concurrent` improved despite higher host load. Per the project's correctness-first rule there was no perf/correctness tradeoff to adjudicate — the hot path is byte-identical to `main`.

Full numbers and the relocator-specific integration coverage that proves the fix (`tests/dlq_relocator_idempotence.rs`: crash-window idempotence, concurrent relocators, gate-lost) are in the PR and `docs/history.md`.
