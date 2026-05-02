# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository status

Phase 2 complete. Phase 1 (MVP) shipped. Phase 2 slice 1 landed delayed jobs (`add_in` / `add_at` / `add_in_bulk` on `Producer`, plus a standalone `Promoter` with `SET NX EX` leader election and a Lua promote script that uses `redis.call('TIME')` for clock-skew immunity). Slice 2 landed exponential retry backoff via delayed-ZSET re-scheduling — handler errors ack-and-reschedule atomically with `attempt+1` carried in the encoded payload, eliminating the fixed-30s `claim_min_idle_ms` retry interval; the CLAIM path remains as the safety net for crashed workers. Slice 3 landed DLQ tooling: `Producer::peek_dlq` (inspect) + `Producer::replay_dlq` (atomic XADD+XDEL via Lua, resets `attempt` so replayed jobs get a fresh retry budget) + `dlq_max_stream_len` cap on the relocator. Slice 4 landed promoter observability: `MetricsSink` trait (no-op default, in-memory testing sink) wired into `PromoterConfig` / `ConsumerConfig`; the promote script now returns `{promoted, depth, oldest_pending_lag_ms}` so depth and lag are observed in the same Redis round trip with no extra ZCARD/ZRANGE calls. `LockOutcome` events fire transition-only, not per-tick. Slice 5 extended `MetricsSink` to the consumer hot path: `ReaderBatch` (per non-empty `XREADGROUP`, with reclaimed-from-CLAIM count), `JobOutcome` (per handler invocation, 1-indexed attempt, microsecond `handler_duration_us`, Ok/Err/Panic kinds), `RetryScheduled` (only when `RETRY_RESCHEDULE_SCRIPT` returns 1 — gate-correct, no over-counting on lost races), and `DlqRouted` (with `DlqReason` promoted to public; attempt count carried). `DlqRelocate` carries `attempt`; `RetryRelocate` carries `attempt` + `backoff_ms`; the retry script return value (`1` rescheduled vs `0` race lost) is now parsed defensively across `Value::Integer` / `Value::String` / `Value::Bytes`. All four config structs (`WorkerWiring`, `ReadState`, `RetryRelocatorConfig`, `DlqRelocatorConfig`) carry `Arc<dyn MetricsSink>` threaded by `consumer/mod.rs` from `cfg.metrics`. The engine crate itself carries zero observability dependencies; the separate `chasquimq-metrics` workspace crate ships `MetricsFacadeSink` (bridges into the `metrics-rs` facade for Prometheus/OTel/StatsD users), a `QueueLabeled<S>` wrapper that adds a `queue` label using `Arc<str>.clone()` into `metrics-rs` `SharedString` (atomic refcount, no per-event String allocation), plus `chasquimq-metrics/examples/{facade,prometheus}_sink.rs` (canonical `metrics-rs` route + hand-rolled `prometheus` integration with a working `/metrics` HTTP endpoint). Adapter histogram names follow Prometheus base-unit convention (`chasquimq_handler_duration_seconds`, `chasquimq_retry_backoff_seconds`); engine events keep micros/ms internally. All `prometheus`/`tiny_http`/`metrics-util` deps live under `chasquimq-metrics` so end users of `chasquimq` never pay for observability infra they didn't ask for. Slice 6 closed the at-least-once gap on delayed adds: `add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids` accept a stable `JobId`, gated by `SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT` (Lua `SET NX EX` on `{chasqui:<queue>}:dlid:<job_id>` with TTL = `delay_secs + DEDUP_MARKER_GRACE_SECS` (3600s) so a delayed producer-retry can't race a successful promotion); same script also writes a side-index `{chasqui:<queue>}:didx:<job_id>` → encoded ZSET member, so cancel can `ZREM` precisely without scanning. Encoded ZSET member format unchanged from slice 1, so promoter and consumer paths are untouched. Slice 7 added `Producer::cancel_delayed(&JobId) -> bool` and `cancel_delayed_bulk(&[JobId]) -> Vec<bool>` (`CANCEL_DELAYED_SCRIPT`: `GET` side-index → `ZREM` exact member → `DEL` side-index + dedup marker; cancel-vs-promote race is serialized at Redis under the shared hash tag, with the only outcomes being `(removed=true, never delivered)` or `(removed=false, delivered)`). The promoter was extended in the same slice to clean up `:didx:<id>` for each promoted member (`PROMOTE_SCRIPT` now returns a 4th element `promoted_members`; the Rust caller decodes each `JobId` via `Job<IgnoredAny>` and pipelines a single `DEL` per non-empty tick) — the `:dlid:<id>` marker is deliberately preserved on promote because its remaining TTL is the post-promote idempotence guard for slice 6. `parse_lua_int` parses the script's int reply via `str::parse::<i64>` defensively across `Value::Integer` / `Value::String` / `Value::Bytes` (matching the `consumer/retry.rs::script_returned_one` pattern). Key format migrated to Redis Cluster hash-tag form (`{chasqui:<queue>}:<suffix>`). Cargo workspace: `chasquimq` (engine), `chasquimq-bench` (harness), `chasquimq-metrics` (Prometheus/OTel/StatsD adapter). CI lives at `.github/workflows/ci.yml` (rustfmt, clippy `--all-targets --workspace -- -D warnings`, `cargo test --workspace -- --include-ignored` against a `redis:8.6.2` service container) — runs on push to `main` and every PR. API is pre-1.0 — breaking changes allowed but must be flagged in the commit (see [Commit conventions](#commit-conventions) below).

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
- **Phase 2:** Delayed jobs (sorted sets) ✅, automatic retries with exponential backoff ✅, richer DLQ tooling ✅, observability (`MetricsSink` + `chasquimq-metrics` adapter) ✅, idempotent delayed scheduling + cancel ✅.
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
