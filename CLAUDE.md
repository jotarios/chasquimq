# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

Phase 2 in progress. Phase 1 (MVP) shipped — Cargo workspace with `chasquimq` (engine) and `chasquimq-bench` (harness). Phase 2 slice 1 has landed delayed jobs (`add_in` / `add_at` / `add_in_bulk` on `Producer`, plus a standalone `Promoter` with `SET NX EX` leader election and a Lua promote script that uses `redis.call('TIME')` for clock-skew immunity). Key format migrated to Redis Cluster hash-tag form (`{chasqui:<queue>}:<suffix>`). API is pre-1.0 — breaking changes allowed but must be flagged in the commit (see [Commit conventions](#commit-conventions) below).

Key files for context:

- `README.md` — public-facing pitch, headline numbers, quickstart, feature comparison.
- `CONTRIBUTING.md` — dev setup, PR workflow, commit conventions, in/out of scope.
- `prd/prd.md` — product requirements, source of truth for product intent.
- `benchmarks/README.md` — index for all bench reports (numbers, methodology, reproduction).
- `benchmarks/baseline-bullmq.md` — measured BullMQ baseline on this host. **The numbers ChasquiMQ has to beat live here.** Read it before making any perf-related design choice.
- `benchmarks/chasquimq-phase1.md` — ChasquiMQ Phase 1 measured results, post-critique iterations, harness improvements.
- `benchmarks/runs/` (gitignored) — raw logs land here locally; only the summary `.md` files are committed.

When updating user-facing docs, keep all four (`README.md`, `CONTRIBUTING.md`, `benchmarks/README.md`, this file) in sync. Don't duplicate content across them — link instead.

The upstream BullMQ benchmark suite is **not vendored** — it's cloned at `~/Projects/experiments/bullmq-bench` (sibling to this repo). Treat it as external; don't edit it.

## Product

ChasquiMQ is a Redis-backed message broker / background job queue. Pitch: "the fastest open-source message broker for Redis." Goal is 3–5× throughput and ≥50% less worker CPU vs. Node.js queues (BullMQ, etc.) on the same Redis instance.

## Architecture (load-bearing constraints)

These are not preferences — they're the product's reason to exist. Do not silently swap them out.

- **Language:** Rust on the `tokio` async runtime. The whole core engine is Rust; other-language support comes later via FFI bindings (NAPI-RS for Node in Phase 3, PyO3 for Python in Phase 4), not by rewriting logic in those languages.
- **Datastore:** Redis 8.6+ (latest stable, April 2026). The PRD originally said "5.0+"; this project targets the latest tech, so use the modern Streams feature set. Don't add fallback paths for older Redis.
- **Queue primitive:** Redis Streams (`XADD` produce, `XREADGROUP` consume, `XACK` acknowledge). Do **not** reach for `LPUSH`/`BRPOP` or other list-based patterns — bypassing those is a core differentiator.
- **Delayed jobs:** Redis Sorted Sets (`ZADD` with score = run-at timestamp, `ZRANGEBYSCORE` to promote due jobs into the stream). Phase 2.
- **Serialization:** MessagePack via `rmp-serde`. Job payloads are binary, not JSON. JSON anywhere on the hot path is a regression.
- **Network strategy:** Aggressive connection multiplexing and pipelined acks. Batch `XACK` calls; don't ack one job at a time.
- **Anti-patterns to avoid:** blocking Lua scripts, human-readable JSON payloads, per-job round trips. The PRD calls these out explicitly as the bottlenecks ChasquiMQ exists to escape.

### Modern Streams features to use (Redis 8.x)

These changed how a queue should be built on Streams; prefer them over the older idioms:

- **Idempotent producer (8.6):** Use `XADD ... IDMPAUTO` (or `IDMP <id>`) for at-most-once delivery so producer retries after network failures don't double-publish. Reach for this before inventing application-level dedup.
- **Atomic delete-on-ack (8.2):** `XACKDEL` acks and removes a message in one round trip; `XDELEX` deletes with consumer-group awareness. Both replace ack-then-delete sequences and reduce Redis round trips.
- **Idle-pending reads (8.4):** Consumers can fetch new and idle pending messages in one call — relevant for retry/recovery without a separate `XPENDING`+`XCLAIM` dance.

### Rust client choice

Pick one client and stick with it across the engine. `redis-rs` (with `tokio-comp` + `connection-manager`) is the conventional choice; `fred` is the alternative if you need first-class pipelining/clustering ergonomics. Whichever is chosen, it must support pipelining and Streams commands natively — confirm before building on it.

## Phased scope

Stay inside the current phase unless the user asks to expand. Building Phase 2 features while Phase 1 is incomplete is scope creep.

- **Phase 1 (MVP):** Producer (`XADD` of MessagePack-serialized `Job` struct), tokio-based consumer pool (`XREADGROUP` batches dispatched to async workers), batched pipelined `XACK`. Out of scope: delayed jobs, retries, any non-Rust SDK.
- **Phase 2:** Delayed jobs (sorted sets), automatic retries with exponential backoff, dead-letter queue.
- **Phase 3:** Node.js bindings via NAPI-RS — JS workers process jobs pulled by the Rust engine.
- **Phase 4:** Python bindings via PyO3, CLI monitoring dashboard.

## Commit conventions

This repo uses **[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)**. Full reference and examples in `CONTRIBUTING.md`; the short version for day-to-day work:

```
<type>(<optional scope>): <subject ≤72 chars, imperative, no trailing period>
```

Types in use: `feat`, `fix`, `perf`, `refactor`, `bench`, `docs`, `test`, `build`, `chore`. Common scopes: `producer`, `consumer`, `ack`, `dlq`, `redis`, `bench`, `config`.

- **Pick the most specific type.** A bench-harness change is `bench:`, not `chore:`. A throughput improvement is `perf:`, not `refactor:` — and `perf:` commits should include before/after numbers in the body.
- **Breaking changes:** mark with `!` after the type/scope **and** a `BREAKING CHANGE:` footer. Until 1.0, breakage is allowed without a major bump but must be flagged this way so the changelog catches it.
- **Don't include `Co-Authored-By: Claude` trailers** (per user preference).

## Success metrics drive design choices

Performance is the product. When two implementations are close, prefer the one with fewer allocations, fewer Redis round trips, and less serialization work. Benchmark against a real Redis (8.6+) instance, not mocks — claims of 3–5× throughput need to be defensible on the same hardware as the comparison queue.

### The numbers to beat (BullMQ 5.76.4, Redis 8.6.2, Apple M3, no pipelining)

From `benchmarks/baseline-bullmq.md` — these are the 1× reference. Re-measure on the same host before claiming any win.

| Scenario | BullMQ mean | 3× target | 5× target |
|---|---:|---:|---:|
| `queue-add` (single producer, 10×10 payload) | 13,961 jobs/s | 41,883 | 69,805 |
| `queue-add-bulk` (bulk 50, tiny payload) | 60,828 jobs/s | 182,484 | 304,140 |
| `worker-generic` (single consumer) | 13,250 jobs/s | 39,750 | 66,250 |
| `worker-concurrent` (concurrency=100) | 47,707 jobs/s | 143,121 | 238,535 |

The two scenarios that matter most for the headline claim: **bulk produce (~61k)** and **concurrent consume (~48k)**. Single-add and single-worker are latency-bound, not throughput tests.

### Lessons from running the baseline

- **`enableAutoPipelining` hurts the worker scenarios on loopback** (-38% on `worker-concurrent`). Pipelining is not a free win; prove it per scenario before turning it on by default in our engine.
- **CPU% is not measured** by `bullmq-bench`. The PRD's "≥50% less worker CPU" target needs us to instrument it ourselves when we build our equivalent harness.
- **Single-host contention** (Bun + Docker Redis on the same M3) caps every number. Fine for our internal A/B against BullMQ on the same host, **not** comparable to BullMQ's published blog numbers.

### Reproducing the baseline

```bash
docker start chasquimq-bench-redis  # or: docker run -d --name chasquimq-bench-redis -p 6379:6379 redis:8.6
cd ~/Projects/experiments/bullmq-bench
BULLMQ_BENCH_REDIS_HOST=127.0.0.1 bun src/index.ts
```

Note: `bullmq-bench`'s `package.json` says `"bullmq": "latest"` but the lockfile pinned an older 4.x. Run `bun add bullmq@latest` after cloning if you re-baseline.
