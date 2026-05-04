# Post-Phase 4 fix-up bench guard

**Scope.** Verify the three fix-up PRs that landed after the Phase 4 canonical bench run did not regress the engine hot path.

**PRs covered:**

- [#48](https://github.com/jotarios/chasquimq/pull/48) `fix(cli): validate watch interval + guard inspect pipeline length` — CLI only.
- [#49](https://github.com/jotarios/chasquimq/pull/49) `fix(py)!: runtime redis dep + worker shutdown race + concurrency default` — Python shim only.
- [#50](https://github.com/jotarios/chasquimq/pull/50) `fix(py,node,docs): post-#49 review follow-ups` — Python + Node shims, no engine.

**Engine isolation.**

```bash
git log 7a914f6..HEAD -- chasquimq/ chasquimq-bench/ --oneline
# → empty
git log f04e673~1..HEAD -- chasquimq/ chasquimq-bench/ --oneline
# → empty
```

Zero diff in `chasquimq/` (engine) or `chasquimq-bench/` (harness) since the [Phase 4 canonical run](https://github.com/jotarios/chasquimq/commit/7a914f6). All three fix-up PRs land in `chasquimq-cli/`, `chasquimq-py/`, `chasquimq-node/`, or docs. By construction there is no engine regression to measure — this run is belt-and-braces.

**Validation run.**

```bash
docker exec chasquimq-bench-redis redis-cli FLUSHDB
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Run date: 2026-05-04. Redis 8.6.2 in Docker (loopback). Same host (Apple M3, 8 cores, 8 GB) as the Phase 4 canonical run.

Host conditions: `uptime` reported load average **2.70 / 2.90 / 3.25** during the run; ~67 MB free pages, ~1.2 GB inactive (memory pressure mild). Docker 28.3.2. Other live processes during the window: long-running Claude agents (concurrent worktree), Chrome renderers — i.e. the same shared-host noise pattern that drove the Phase 4 canonical run's `worker-concurrent` dip.

**Comparison vs Phase 4 canonical.**

| Scenario | Phase 4 (jobs/s) | This run (jobs/s) | Δ% | Status |
| :--- | ---: | ---: | ---: | :--- |
| `queue-add` | 16,417 | 17,875 | +8.9% | improvement (within run-to-run noise) |
| `queue-add-bulk` | 199,308 | 195,938 | −1.7% | parity (within ±2%) |
| `queue-add-delayed` | 16,351 | 17,933 | +9.7% | improvement (within run-to-run noise) |
| `worker-concurrent` | 120,265 | 103,634 | −13.8% | host-load (same regime as Phase 4) |
| `worker-delayed-end-to-end` | 17,927 | 18,018 | +0.5% | parity (within ±2%) |
| `worker-generic` ⚠ noisy | 9,982 | 10,217 | +2.4% | parity (sub-ms window, direction-only) |
| `worker-retry-throughput` | 61,160 | 56,698 | −7.3% | host-load (within Phase 4 envelope) |

**Verdict: no engine regression.**

- The headline `queue-add-bulk` (3× target gate) reproduces within ±2% (−1.7%). The producer hot path is stable.
- `worker-concurrent` (5× target gate) lands at 104k — same host-CPU-bound regime as the Phase 4 canonical run (120k) and the prior Phase 4 bench-guard re-run (114k). All three are far below the Phase 2 final 415k ceiling for the same reason: shared-host load. Diagnosis is identical to Phase 4: `git log 7a914f6..HEAD -- chasquimq/ chasquimq-bench/` is empty, so there is no engine code to chase. Reconfirm the absolute number on a clean idle host (load avg < 1.0) when convenient.
- `worker-retry-throughput` slips 7% but stays well within the Phase 4-era envelope (Phase 4 canonical: 61k; Phase 2 final: 113k under load < 1.0). Same environmental story.
- The two fully delayed scenarios (`queue-add-delayed`, `worker-delayed-end-to-end`) and the single-add latency-bound scenario (`queue-add`) all reproduce or improve, confirming the producer / promoter / consumer paths are unchanged.

**Reproduction.**

Raw log (gitignored, local-only): `benchmarks/runs/post-phase4-fixups-20260504-182912.log`. Numbers above are the committed record.
