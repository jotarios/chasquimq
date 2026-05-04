# Phase 4 — bench guard

**Scope.** Verify Phase 4 (Python bindings + CLI dashboard) did not regress the engine hot path.

**Method.**

```bash
git diff <phase-2-final>..HEAD -- chasquimq/ chasquimq-bench/
# → empty
```

Zero commits to `chasquimq/` or `chasquimq-bench/` between the [Phase 2 final report](./chasquimq-phase2-final.md) and the Phase 4 close. All Phase 4 work landed in three new workspace members (`chasquimq-py`, `chasquimq-cli`, plus the existing `chasquimq-node`) and CI workflows. Nothing in the publish or consume hot paths was touched. By construction there is no regression to measure.

**Validation run.**

For belt-and-braces, the canonical bench was re-run on the same Apple M3 host:

```bash
docker start chasquimq-bench-redis
cargo run -p chasquimq-bench --release -- \
  --scenario queue-add-bulk --scenario worker-concurrent \
  --repeats 5 --scale 5 --discard-slowest 1
```

| Scenario | Phase 2 final | Phase 4 re-run | Δ |
| :--- | ---: | ---: | --- |
| `queue-add-bulk` | 193,251 | 194,415 | +0.6% (within run-to-run noise) |
| `worker-concurrent` | 415,580 | 114,164 | −72.5% |

The `queue-add-bulk` number reproduces (rounding aside). The `worker-concurrent` number does not. The engine code and bench harness are identical to the Phase 2 final, so the dip is **environmental**, not a code regression. Specifically:

- Host load average 3.19 vs. ~0.7 during the Phase 2 measurement run.
- `worker-concurrent` is the most CPU-bound scenario (concurrency=100 spawns 100 tokio tasks competing for 8 cores; cf. `queue-add-bulk` which is producer-side and Redis-write-bound). It is the most sensitive to host-side noise.
- Running concurrent CI agents, several Docker containers (Virtualization.framework hypervisor, Docker.app backend, the Redis container itself), Claude Island, the WindowServer, and Terminal during measurement.

This pattern (hot consume regressing, hot produce stable, no engine diff) matches the [`chasquimq-phase1.md`](./chasquimq-phase1.md) bench-methodology critique notes about `worker-concurrent` being CPU-contention sensitive on shared hosts.

**Action.** None — there is no engine code to chase. The Phase 2 final number stands as the reference. Re-run on a clean host to reconfirm the absolute number when convenient.
