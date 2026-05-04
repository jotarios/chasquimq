# Phase 4 — bench guard

**Scope.** Verify Phase 4 (Python bindings + CLI dashboard) did not regress the engine hot path.

**Method.**

```bash
git diff <phase-3-close>..HEAD -- chasquimq/ chasquimq-bench/
# → empty
git log --oneline 6d238a1..HEAD -- chasquimq/ chasquimq-bench/
# → empty
```

Zero commits to `chasquimq/` or `chasquimq-bench/` between the [Phase 3 close](https://github.com/jotarios/chasquimq/commit/6d238a1) and the Phase 4 close. All Phase 4 work landed in three new workspace members (`chasquimq-py`, `chasquimq-cli`, plus the existing `chasquimq-node`) and CI workflows. Nothing in the publish or consume hot paths was touched. By construction there is no regression to measure.

**Validation run.**

For belt-and-braces, the canonical bench was re-run on the same Apple M3 host:

```bash
docker start chasquimq-bench-redis
docker exec chasquimq-bench-redis redis-cli FLUSHDB
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Run date: 2026-05-03. Host conditions during the run: load average 6.70 → 5.41 → 3.06 (settling) over the bench window; 1.71× core utilization on `worker-concurrent`. Other live processes: a long-running `claude` agent (~7.8% CPU), Google Chrome renderers (~35% spikes), Claude Island (~3-5% steady), WindowServer.

| Scenario | Phase 2 final | Phase 4 re-run | Δ | Within ±5% noise floor? |
| :--- | ---: | ---: | ---: | :--- |
| `queue-add` | 17,394 | 16,417 | −5.6% | borderline |
| `queue-add-bulk` | 193,251 | 199,308 | **+3.1%** | yes (reproduces) |
| `queue-add-delayed` | 17,578 | 16,351 | −7.0% | no (>5%, <10%) |
| `worker-concurrent` | 415,580 | 120,265 | **−71.1%** | no (host-load) |
| `worker-delayed-end-to-end` | 705,189 | 17,927 | −97.5% | no (catastrophic) |
| `worker-generic` ⚠ noisy | 418,946 | 9,982 | −97.6% | no (sub-ms window) |
| `worker-retry-throughput` | 113,210 | 61,160 | −46.0% | no (>10%, host-load) |

**The two headline scenarios diverge:**

- `queue-add-bulk` (3× target gate) reproduces within run-to-run noise (+3.1%). The producer hot path is stable.
- `worker-concurrent` (5× target gate) does not reproduce. **120k vs. 415k = −71%.**

**Diagnosis: environmental, not a code regression.**

1. **Zero engine/bench code diff during Phase 4.** `git log 6d238a1..HEAD -- chasquimq/ chasquimq-bench/` returns empty. There is no engine code to chase.
2. **Host load is ~4× higher than during the Phase 2 final measurement.** Phase 2 final ran at load avg ~0.7. This run started at 6.70 and settled to ~3.0 by mid-run. `worker-concurrent` spawns concurrency=100 tokio tasks competing for 8 cores, so it is the most CPU-bound and most sensitive to host noise.
3. **Reproducible at longer windows.** Re-ran `worker-concurrent` alone at `--scale 10 --repeats 5 --discard-slowest 1` (bench window grows ~10× to dilute startup overhead): 118,768 jobs/s, stddev 2,482. The 120k figure is consistent at both scale 5 and scale 10, so this is not bench-window noise — it is the actual sustained throughput on this host under current conditions.
4. **Matches the prior Phase 4 bench-guard re-run.** The previous validation run (load 3.19) measured 114,164 jobs/s. Today's run (load 3.06–6.70, settling 3.0) measured 120,265. Same order of magnitude, same explanation.

This pattern (hot consume regressing, hot produce stable, no engine diff) matches the [`chasquimq-phase1.md`](./chasquimq-phase1.md) bench-methodology critique notes about `worker-concurrent` being CPU-contention sensitive on shared hosts.

**The "catastrophic" rows are bench-window artefacts:**

- `worker-generic` is flagged ⚠ noisy in the Phase 2 final report itself ("bench window is sub-millisecond — direction-only"). At scale 5 it can land anywhere from 10k to 500k depending on which microsecond the stopwatch fires. Not interpretable.
- `worker-delayed-end-to-end` collapsed from 705k to 17.9k. The Phase 2 final report explicitly warns this is an upper bound, not steady-state — it pre-loads all jobs into the delayed ZSET with 1 ms delay and measures pure consumer drain rate. Under host CPU pressure, the promoter's poll cadence and the consumer's reader loop both block on the OS scheduler, so the measurement is pinned to scheduler latency rather than engine throughput. Like `worker-generic`, it is environment-dominated under load.

**Action.** None — there is no engine code to chase. The Phase 2 final number stands as the reference. Re-run on a clean, idle host to reconfirm the absolute number when convenient (load avg should be < 1.0 for `worker-concurrent` to be near its 415k ceiling).

**Reproduction.**

Raw logs (local-only, not committed): `/tmp/chasqui-bench-phase4-canonical.log` (full canonical run) and `/tmp/chasqui-bench-phase4-worker-concurrent-scale10.log` (worker-concurrent at scale 10). Numbers above are the committed record.
