---
title: Regressions and floors
description: The host-load gate, the engine ceiling, and when contention is a valid explanation.
sidebar:
  order: 3
---

ChasquiMQ's headline numbers reproduce on the same hardware they were measured on. They do **not** reproduce on a contended host — `worker-concurrent` drops from 419k/s to ~112k/s under load avg 1.8–4.3, even though the engine code didn't change.

This page is the rule for telling regression from contention.

## The host-load floor gate

> A "host-load floor" / "host contention" / "concurrent agents" explanation for a `worker-concurrent` regression only applies when `git diff <previous-baseline> -- chasquimq/` is empty.
>
> If engine code changed, that explanation is forfeited; the regression must be re-run on a quiet host (`load avg < 1.0`) before being accepted as no-regression.

Source: [`benchmarks/README.md`](https://github.com/jotarios/chasquimq/blob/main/benchmarks/README.md).

The reasoning: host contention is a real explanation, but it's also the easiest one to reach for when an engine change actually regressed. The gate forces a re-run under fair conditions before accepting "the host was busy."

## What "the engine ceiling" means

The engine ceiling is the highest measured throughput on the cleanest host we've recorded:

| Scenario | Quiet-host ceiling | Where measured |
|---|---:|---|
| `queue-add-bulk` (50, tiny) | **196,038 jobs/s** | Phase 2 final, load avg < 1 |
| `worker-concurrent` (100) | **419,004 jobs/s** | Phase 2 final, load avg < 1 |
| `worker-generic` (single) | 414,010 jobs/s | Phase 2 final, load avg < 1 |
| `queue-add` (single, 10×10) | 16,548 jobs/s | Phase 2 final, load avg < 1 |
| `worker-delayed-end-to-end` | 755,034 jobs/s | Phase 2 final |
| `worker-retry-throughput` | 116,542 jobs/s | Phase 2 final |

These reproduce on the same M3 host when the load is below 1. Pages and pages of bench reports in the repo confirm it.

The contended-host floor is roughly 25–30% of the ceiling on `worker-concurrent` — that's the band you'll see on a working laptop with browser, Slack, Docker, multiple coding agents, and ChasquiMQ all running.

## Why we accept 2.45× on contended hosts

The 2.45× ratio is below the PRD's 5× consumer target. We ship the 1.0 anyway because:

- **The engine ceiling itself is unchanged.** Every quiet-host run since slice 3 reproduces 8.78× ratio on `worker-concurrent`. The 2.45× is host-bound, not engine-bound.
- **The 3× producer target is met under load.** `queue-add-bulk` reproduces 3.47× even on a contended host, because the producer hot path bottlenecks at Redis. The producer claim is robust regardless of host load.
- **Calibration > marketing.** Reporting both numbers side-by-side teaches users that consumer throughput is host-load-sensitive — which is true for any concurrent-tasks-on-loopback-Redis workload, including BullMQ. We'd rather under-promise on the contended-host number than over-promise on the quiet-host number.

The honest framing in the README:

> 3.47× sustained on bulk produce, up to 8.78× on concurrent consume on a quiet host. The 8.78× drops to 2.45× under host contention; both numbers ship.

## When to suspect a real regression

The gate fires when:

1. The bench reports a number more than ~10% below the previous baseline.
2. `git diff <previous-baseline> -- chasquimq/` is non-empty.
3. The host load is comparable to the previous run (i.e., not visibly contended).

If all three apply, you have a real regression. Reasonable diagnostic steps:

- **Re-run on a quiet host.** Close everything except your terminal and the bench. Run with `--repeats 10 --scale 5 --discard-slowest 2` for tighter stats.
- **Bisect.** `git bisect` between the last good baseline and HEAD. The bench harness is fast enough (one full sweep ~3 minutes) that this is feasible.
- **Profile the hot path.** `cargo flamegraph` against the bench binary. The reader (`consumer/read_loop`), the worker pool dispatch (`consumer/worker.rs`), and the ack flusher (`ack.rs`) are the three places to look first.

## When NOT to suspect a regression

- **`worker-concurrent` is in the 100k–150k band on a contended Mac.** That's the floor. Don't chase it.
- **`queue-add` is in the 10k–20k band.** It's latency-bound by design; per-call overhead dominates. Not a throughput test.
- **`worker-generic` reports wildly varying numbers under load.** The bench window is too small (~12ms at engine ceiling) for stable measurement. Treat as direction-only.

## When a regression is real, the path forward

ChasquiMQ has shipped multiple no-regression checks per slice. The pattern:

1. Take a baseline before the change.
2. Run the bench on the same host, same conditions.
3. Compare. Anything within ±5% is run-to-run noise. Anything beyond, investigate.
4. If a regression is real, decide: revert, or accept and document the trade-off in the slice's `benchmarks/<slice>.md`.

The committed reports in `benchmarks/` are the audit trail. A reader six months from now should be able to ask "did slice X regress the bulk-produce path?" and find the answer in one place.

## On the 8.78× claim

The 8.78× consumer ratio is the highest we've measured. It reproduces on the M3 with load avg < 1. The marketing pitch in the README points at this number with explicit honesty:

> ~419k jobs/s for an 8.78× ratio on a quiet host (load < 1).

If a future change moves the quiet-host ceiling materially, that number changes too. The benchmark methodology is designed to detect that.

For the methodology: [Methodology](/benchmarks/methodology/). For the underlying trade-offs: [Performance trade-offs](/concepts/performance-trade-offs/).
