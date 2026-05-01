# Benchmarks

ChasquiMQ's headline claim is **"the fastest open-source message broker for Redis"** — this directory is the evidence behind that claim, and the honest caveats around it.

## Headline numbers

Measured on Apple M3, Redis 8.6 (Docker, loopback), single host. BullMQ 5.76.4 baseline run on the same machine.

| Scenario              | BullMQ 5.76.4 | ChasquiMQ        | Ratio       |
|-----------------------|--------------:|-----------------:|------------:|
| `queue-add-bulk` (50) |     60,828/s  | **194,394/s**    | **3.20×**   |
| `worker-concurrent`   |     47,707/s  | **437,683/s**    | **9.17×**   |
| `queue-add` (single)  |     13,961/s  |       16,476/s   |     1.18×   |
| `worker-generic` ⚠    |     13,250/s  |      431,291/s   |    32.6×    |

`queue-add` and `worker-generic` are latency-bound (single in-flight op) — they aren't the throughput claim. The two scenarios that matter for "fastest broker on Redis" are `queue-add-bulk` and `worker-concurrent`; both clear the 3× gate, and `worker-concurrent` clears 5× comfortably.

⚠ `worker-generic`'s bench window is too small for stable measurement (~12ms at 430k/s); treat the ratio as direction-only. See methodology notes in [`chasquimq-phase1.md`](chasquimq-phase1.md).

## Detailed reports

- [`baseline-bullmq.md`](baseline-bullmq.md) — full BullMQ baseline methodology, raw numbers, lessons from running the suite (notably: `enableAutoPipelining` *hurts* on loopback).
- [`chasquimq-phase1.md`](chasquimq-phase1.md) — full ChasquiMQ Phase 1 results, post-critique iterations, and harness improvements (distribution stats, `--scale` flag, slowest-discard).

## Reproducing

Requires Redis 8.6+ on `127.0.0.1:6379` (loopback). The BullMQ baseline lives in a sibling repo cloned at `~/Projects/experiments/bullmq-bench` (not vendored).

```bash
# Redis (one-time)
docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6

# BullMQ baseline
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts

# ChasquiMQ — canonical run (5 repeats, scale=5, drop slowest)
cd ~/Projects/experiments/chasquimq
cargo run -p chasquimq-bench --release -- --repeats 5 --scale 5 --discard-slowest 1
```

Raw run logs land in `benchmarks/runs/` (gitignored — local only). The committed `.md` files in this directory are the canonical record.

## Methodology limitations

The numbers above are defensible for *this hardware* and *this setup*. Open caveats, tracked in `TODOS.md`:

- **Latency unmeasured.** Throughput only — no dispatch-to-ack p99 yet.
- **Same-host bench.** Bench process and Redis share cores. Apples-to-apples vs BullMQ on the same host; not directly comparable to BullMQ's published cross-host numbers.
- **Worker CPU vs BullMQ unmeasured.** ChasquiMQ's CPU is instrumented; BullMQ's isn't (the `bullmq-bench` suite doesn't measure it). The PRD's "≥50% less worker CPU" target requires building a parallel CPU measurement against BullMQ before we can claim it.
- **No persistence.** Redis is in-memory default config — no AOF, no RDB. Production-realistic numbers would be lower for both queues.
