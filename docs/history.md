# Engineering history

A slice-by-slice record of how ChasquiMQ got to 1.0. Preserved for future-reader context. The current state lives in `CLAUDE.md` (orientation), `README.md` (user-facing), `docs/engine.md` (engine internals), and the PRD; this file is the long-form changelog.

## Phase 1 — MVP

Producer (`XADD` of MessagePack-serialized `Job` struct), tokio-based consumer pool (`XREADGROUP` batches dispatched to async workers), batched pipelined `XACK`. DLQ stream + graceful shutdown. Phase 2+ is everything below.

## Phase 2 — delayed jobs, retries, observability

**Slice 1: delayed jobs.** `add_in` / `add_at` / `add_in_bulk` on `Producer`, plus a standalone `Promoter` with `SET NX EX` leader election and a Lua promote script that uses `redis.call('TIME')` for clock-skew immunity.

**Slice 2: exponential retry backoff** via delayed-ZSET re-scheduling. Handler errors ack-and-reschedule atomically with `attempt+1` carried in the encoded payload, eliminating the fixed-30s `claim_min_idle_ms` retry interval. The CLAIM path remains as the safety net for crashed workers.

**Slice 3: DLQ tooling.** `Producer::peek_dlq` (inspect) + `Producer::replay_dlq` (atomic `XADD+XDEL` via Lua, resets `attempt` so replayed jobs get a fresh retry budget) + `dlq_max_stream_len` cap on the relocator.

**Slice 4: promoter observability.** `MetricsSink` trait (no-op default, in-memory testing sink) wired into `PromoterConfig` / `ConsumerConfig`. The promote script returns `{promoted, depth, oldest_pending_lag_ms}` so depth and lag are observed in the same Redis round trip with no extra ZCARD/ZRANGE calls. `LockOutcome` events fire transition-only, not per-tick.

**Slice 5: consumer / retry / DLQ observability.** `MetricsSink` extended to the consumer hot path — `ReaderBatch` (per non-empty `XREADGROUP`, with reclaimed-from-CLAIM count), `JobOutcome` (per handler invocation, 1-indexed attempt, microsecond `handler_duration_us`, `Ok` / `Err` / `Panic` kinds), `RetryScheduled` (only when `RETRY_RESCHEDULE_SCRIPT` returns 1 — gate-correct, no over-counting on lost races), `DlqRouted` (with `DlqReason` promoted to public; attempt count carried). All four config structs (`WorkerWiring`, `ReadState`, `RetryRelocatorConfig`, `DlqRelocatorConfig`) carry `Arc<dyn MetricsSink>` threaded by `consumer/mod.rs` from `cfg.metrics`. `parse_lua_int` parses script int replies defensively across `Value::Integer` / `Value::String` / `Value::Bytes`.

The engine itself carries zero observability dependencies; the separate `chasquimq-metrics` workspace crate ships `MetricsFacadeSink` (bridges into the `metrics-rs` facade) and `QueueLabeled<S>` (adds a `queue` label using `Arc<str>.clone()` into `metrics-rs` `SharedString` — atomic refcount, no per-event String allocation), plus working examples for `metrics-exporter-prometheus` and a hand-rolled `prometheus`-crate sink. Adapter histogram names follow Prometheus base-unit convention (`chasquimq_handler_duration_seconds`, `chasquimq_retry_backoff_seconds`); engine events keep micros/ms internally.

**Slice 6: at-least-once gap closed on delayed adds.** `add_in_with_id` / `add_at_with_id` / `add_in_bulk_with_ids` accept a stable `JobId`, gated by `SCHEDULE_DELAYED_IDEMPOTENT_SCRIPT` (Lua `SET NX EX` on `{chasqui:<queue>}:dlid:<job_id>` with TTL = `delay_secs + DEDUP_MARKER_GRACE_SECS` (3600s) so a delayed producer-retry can't race a successful promotion). Same script writes a side-index `{chasqui:<queue>}:didx:<job_id>` → encoded ZSET member, so cancel can `ZREM` precisely without scanning. Encoded ZSET member format unchanged from slice 1, so promoter and consumer paths are untouched.

**Slice 7: cancellation.** `Producer::cancel_delayed(&JobId) -> bool` and `cancel_delayed_bulk(&[JobId]) -> Vec<bool>` (`CANCEL_DELAYED_SCRIPT`: `GET` side-index → `ZREM` exact member → `DEL` side-index + dedup marker). The cancel-vs-promote race is serialized at Redis under the shared hash tag, with the only outcomes being `(removed=true, never delivered)` or `(removed=false, delivered)`. The promoter was extended in the same slice to clean up `:didx:<id>` for each promoted member (`PROMOTE_SCRIPT` returns a 4th element `promoted_members`; the Rust caller decodes each `JobId` via `Job<IgnoredAny>` and pipelines a single `DEL` per non-empty tick) — the `:dlid:<id>` marker is deliberately preserved on promote because its remaining TTL is the post-promote idempotence guard for slice 6. Key format migrated to Redis Cluster hash-tag form (`{chasqui:<queue>}:<suffix>`).

## Phase 3 — Node.js bindings

Slice b scaffolded `chasquimq-node` (workspace member, `napi-rs` build setup, `src-ts/` for the TypeScript shim, `__test__/` Vitest harness). Slice c shipped native NAPI bindings exposing `Producer` / `Consumer` / `Promoter` / `Scheduler` to Node — `Consumer` dispatches handler invocations through a tokio-side TSFN (`ThreadsafeFunction`) so JS handlers run on the Node event loop without blocking the engine's reader. Slice d shipped the high-level shim (`Queue` / `Job` / `Worker` / `QueueEvents`, plus `NotSupportedError` / `UnrecoverableError`) — wraps the bindings, MessagePack-encodes payloads via `@msgpack/msgpack` on the JS side, surfaces `EventEmitter` events backed by the engine's `MetricsSink`. `QueueEvents` reads from the events stream over `ioredis` for cross-process fan-out.

Engine slices 8/9/10 wired the prerequisites:

- **Slice 8 (PR #14):** per-job retry overrides on the producer. Wire-format deploy-order requirement: `Job::retry = Some(...)` encodes as a 5-element msgpack array that pre-slice-8 consumers can't decode, so roll new consumers out before producers that emit overrides.
- **Slice 9 (PR #15):** events stream emitted by the engine.
- **Slice 10 (PR #16):** repeatable-jobs scheduler.

Node track shipped via PRs #13 (scaffold), #17 (native bindings), #24 (high-level shim).

IANA timezone names supported on repeatable cron specs (`tz: Some("America/New_York")` resolves via `chrono-tz` (default-features off) and is DST-aware). `parse_tz` returns a `TzKind { Fixed | Named }` and `next_cron_after` dispatches once at the entry point so the hot path stays monomorphized. Catch-up policy for missed cron windows is configurable per spec via `RepeatableSpec::missed_fires` (`Skip` (default) drops missed windows after scheduler downtime; `FireOnce` emits one job; `FireAll { max_catchup }` replays each missed window up to a cap). `SCHEDULE_REPEATABLE_SCRIPT` accepts a variable-length list of `(fire_at_ms, payload)` pairs so all catch-up fires + the `next_fire_ms` ZADD happen in one atomic round trip. `skip_serializing_if = "is_default_missed_fires_policy"` keeps pre-existing encoded specs decoding unchanged.

`HandlerError::unrecoverable()` carries a flag the consumer's retry path checks before scheduling a retry — handlers that signal unrecoverable bypass `max_attempts` and per-job overrides and route directly to the DLQ with `DlqReason::Unrecoverable` (panics keep the existing recoverable path with `DlqReason::Panic`).

Repeatable / cron native exposure shipped via PR #33: `Queue.add(name, data, { repeat })` upserts via the native `Producer.upsertRepeatable`. The shim's `Worker` originally auto-spawned an embedded native `Scheduler`; that auto-spawn moved into the engine in PR #64. NAPI value types `NativeRepeatPattern` / `NativeMissedFiresPolicy` / `NativeRepeatableSpec` / `NativeRepeatableMeta` round-trip via tagged-`kind` strings; `f64`→`u64` boundary casts are guarded against non-finite / negative / out-of-range floats.

Per-job retry exposure shipped via PR #34: `Queue.add(name, payload, { attempts, backoff })` honored end-to-end. JS `Error.name === 'UnrecoverableError'` maps to `HandlerError::unrecoverable(...)`.

## Phase 4 — Python bindings + CLI

**Track A — Python.** Slice A1 (PR #37) scaffolded `chasquimq-py` (pyo3 0.28 + pyo3-async-runtimes 0.28, `abi3-py39` so one wheel covers Python 3.9+, src layout, maturin build backend). Slice A2 (PR #40) shipped the native `Producer` with the full producer surface — all async via `pyo3_async_runtimes::tokio::future_into_py`, kind-tagged dicts for backoff and patterns with `f64→u64` validation at the FFI boundary.

Slice A3 (PR #41) shipped the native `Consumer`. Load-bearing piece: `TaskLocals::with_running_loop(py)?.copy_context(py)?` captured **once** at the entry to `run()` and reused per invocation via `into_future_with_locals(&locals, ...)` — otherwise the user's coroutine is dropped without ever being awaited, because the engine-side closure runs on tokio worker threads with no asyncio loop attached. JS-style `Error.name == 'UnrecoverableError'` mapping is `e.get_type(py).name() == "UnrecoverableError"`. `consumer.shutdown()` trips a `tokio_util::sync::CancellationToken`.

Slice A4 (PR #44) shipped the high-level Python shim: `Queue` / `Worker` / `Job` (frozen dataclass) / `QueueEvents` (asyncio iterator over the events stream via `redis-py` XREAD-BLOCK), plus `RepeatPattern` / `BackoffSpec` / `RepeatableMeta` / `NotSupportedError` / `UnrecoverableError`. Wire format mirrors the Node shim exactly — payloads are msgpack-encoded **user data only**, so a Python producer and a Node worker drain the same Redis stream without translation.

Slice A5 (PR #42) shipped `.github/workflows/py-ci.yml`: 5-platform matrix (linux x86_64 + aarch64, macOS x86_64 + aarch64, windows x86_64) via `PyO3/maturin-action@v1` with `manylinux: auto` (linux-aarch64 cross-builds via QEMU). Test job runs the **published wheel** through `pytest` against a `redis:8.6.2` service container. Publish job uses PyPI **Trusted Publishing** via OIDC, gated on `chore(release):` commit prefix on `main`.

**Track B — CLI.** `chasquimq-cli` workspace member with the `chasqui` binary:

- Slice B1 (PR #38): `chasqui inspect <queue>` — one-shot snapshot via a single fred `Pipeline`.
- Slice B2 (PR #39): `chasqui dlq peek/replay` and `chasqui repeatable list/remove`. Generic over `Producer<rmpv::Value>` so the engine `Producer`'s replay path can decode/modify/re-encode any user's msgpack payload without knowing the schema.
- Slice B3 (PR #43): `chasqui watch` (crossterm, no alternate screen / no raw mode — operators see final state on exit; `tokio::select!` Ctrl+C beats the next tick) and `chasqui events <queue>` (XREAD-BLOCK on the events stream).
- Slice B4: cargo-dist tag-based release pipeline.

## Post-Phase-4 polish (name-on-wire + engine refactors)

Five connected changes after Phase 4 closed; design doc: [`docs/name-on-wire-design.md`](name-on-wire-design.md).

- **Slice 1 (PR #56, breaking):** engine `Job<T>::name` field with `#[serde(skip)]` (framing-layer metadata, not envelope) and an `n` field on stream entries via `XADD ... d <bytes> n <utf8-name>`. `AddOptions::name`, `Producer::add_with_options`, `Producer::add_bulk_named`, `DlqEntry::name`, and `replay_dlq` preserve `n` end-to-end. Old consumers reading new producer entries are graceful — the existing `[k, v, ...]` parser walks unknowns.
- **Slice 2 (PR #57):** both shims surface `Queue.add(name, data)` plumbing and `Job.name` on the worker side; `add_bulk_named` shim API.
- **Slice 3 (PR #59, breaking):** delayed-ZSET member encoding becomes `name_len:u32_le + name_utf8 + msgpack_payload`; `PROMOTE_SCRIPT` and `SCHEDULE_REPEATABLE_SCRIPT` parse the prefix.
- **Slice 4 (PR #61):** cross-shim CI fixtures gained `JOB_NAME` / `EXPECT_JOB_NAME` env vars and assert `name` round-trip across all four directions.
- **Slice 5 (PR #58, breaking):** events stream + `MetricsSink::JobOutcome / RetryScheduled / DlqRouted` carry `name`; `chasqui events` renders the column; Prometheus / OTel adapters add a `name` label.

**Engine `Consumer` auto-embeds `Scheduler` (PR #64, breaking).** `Consumer::run` auto-spawns a `Scheduler` task alongside the reader / promoter / relocators when `run_scheduler=true` (default). Both Python and Node `Worker` shims dropped their auto-spawn-scheduler logic (~74 LOC removed).

**`Native` prefix dropped on binding classes (PR #62, breaking).** `chasquimq._native.NativeProducer` → `chasquimq._native.Producer` (and `Consumer`, `Job`, `Scheduler`); same on the Node side. The user-facing high-level surface unchanged.

## 1.0 polish (post-#62, in shipped order)

Eleven PRs landed after the post-Phase-4 polish above; together they close the three PRD-listed 1.0 blockers.

- **PR #68 (breaking)** — Node `chasquimq/native` subpath dropped; native classes (`Producer` / `Consumer` / `Promoter` / `Scheduler`) re-exported from the package root, with the native binding `Job` re-exported as `NativeJob`.
- **PR #69 (breaking)** — Python `chasquimq._native` flattened the same way; `from chasquimq import Producer, Consumer, Scheduler` now works.
- **PR #70** — Bench coverage closing both gaps: `chasquimq-bench/benches/ffi_buffer_copy.rs` Criterion microbench and `benchmarks/scripts/python_handler_bench.py`. Closes the third 1.0 blocker.
- **PR #71 (breaking)** — `MissedFiresPolicy` exposed on both shims. `RepeatableMeta` gains a `missed_fires` field on the engine list-repeatable surface (admin path, not a hot-path field). `fire-all` requires `max_catchup >= 1` at both the shim builder and the FFI boundary.
- **PR #72** — `Queue.addUnique` / `add_unique` for stable-id idempotent enqueue. Delayed path is strict / cross-process via slice-6 `SET-NX-EX` dedup marker; immediate path is strict within a single Producer instance via Redis 8.6 `XADD ... IDMP <producer_id> <job_id>`. No engine changes. Closes the first 1.0 blocker.
- **PR #73** — Python smoke-test docstring fix.
- **PR #74** — 40 NAPI low-level binding edge tests in `__test__/native-edges.test.ts`.
- **PR #75 (breaking)** — Engine result-backend (slice 5a): `JOB_OK_SCRIPT` (atomic `XACKDEL` + conditional `SET` of result bytes); `ConsumerConfig.store_results` (default `false`) + `result_ttl_secs` (default 3600); `Producer::get_result` / `get_result_bulk`; `run_ok_result_writer` flusher in `ack.rs` (per-entry EVALSHA with NOSCRIPT-fallback). Result-writer task only spawned when `store_results=true` so the worker's match always takes the existing batched-ack fast path with `ok_result_tx=None` — zero overhead for users who don't opt in. **Handler signature changed** from `Result<(), HandlerError>` to `Result<Bytes, HandlerError>`.
- **PR #76** — Python result-backend plumbing: `Worker(store_results=, result_ttl_ms=)`; native dispatch closure captures the user coroutine's resolution value; `Queue.get_job_result(id)` / `get_job_result_bulk(ids)`.
- **PR #77** — Node result-backend plumbing: `WorkerOptions { storeResults, resultTtlMs }`; `Queue.getJobResult<ResultType>(jobId) → ResultType | undefined` is generic over the user's `Queue<DataType, ResultType, NameType>`. Non-Buffer / non-nullish handler returns silently collapse to ack-only with a `tracing::warn!`, mirroring the Python shim.
- **PR #78** — `Job.waitForResult({ timeoutMs, intervalMs, signal })` (Node) and `job.wait_for_result(timeout=...)` (Python) polling helpers. Node honors `AbortSignal.throwIfAborted`; Python wraps `asyncio.wait_for` so `TimeoutError` / `CancelledError` propagate unchanged. Worker-side jobs intentionally have no queue ref — calling `waitForResult` on those raises a clear "Queue reference required" error. Closes the second 1.0 blocker.
- **PR #79** — Cross-shim CI Phases 5–8 covering `storeResults`/`getJobResult` round-trip across all four directions. Producer fixtures grew `JOB_IDS_FILE`; workers grew `STORE_RESULT=1` and `RESULT_VALUE=<json>`. New `verify_results.{py,ts}` runs after the worker drains, polls per id and asserts deep-equality with `EXPECT_RESULT`.

**Same-host 1.0 re-bench (2026-05-07):** today's contended-host (load avg ~1.8–4.3) `queue-add-bulk` reproduces 3.47× BullMQ; `worker-concurrent` reproduces 2.45× under host contention. Quiet-host canonical Phase 2 final stays at 3.22× / 8.78× and is the upper bound the marketing copy points at. Engine ceiling unchanged across the slice; the contended-host floor is what users will see on a busy laptop.

## Post-1.0 polish

- **Public surface aligned across both shims (2026-05-08, partial breaking).** Final shape: `Queue` / `Worker` / `Job` (the ergonomic high-level path) plus `Producer` / `Consumer` / `Scheduler` (engine handles for power users). All six are exported from the package root on both shims. The native PyO3 wire-format pyclass is now `chasquimq._native._Job` — underscore-prefixed, internal-only, not part of any public surface (`Job` → `_Job` rename is the breaking part on the Python side). On the Node side, the `NativeJob` re-export was dropped: there is one user-facing `Job` on each shim. The raw `Consumer.run(handler)` path keeps working for backward compat but is undocumented; users should use `Worker`. Landed across three commits: a `Job` consolidation, a (later-reversed) surface minimization, and the final restoration of top-level engine-handle exports.

## Slice 11 — AWS Lambda prerequisites (cloud-Redis polish)

Triggered by an AWS Lambda doc feasibility eval (2026-05-09): the existing engine could not honestly support a Lambda producer guide because four pieces were missing — TLS, TCP keepalive + reconnect, an explicit flush guarantee, and a credential-rotation hook for ElastiCache IAM auth. Four PRs landed as a chain.

- **PR #114 — TLS via `rediss://` (Phase 1).** Flipped `chasquimq/Cargo.toml` to pull fred with `enable-rustls-ring`. Both shims gained a `tls` flag (Node `connection.tls: boolean`; Python `Queue/Worker/QueueEvents(tls=True)`) plus Node's `connection.url` for hand-rolled URLs. A small `_url.apply_tls` helper in `chasquimq-py` (with 5 unit tests) handles the corner cases the AWS console exposes — schemeless `host:port` endpoints, case-insensitive scheme prefixes, already-`rediss://` URLs. Cross-compile ran into ring's ARM-assembly missing `__ARM_ARCH`; resolved by injecting `CFLAGS_aarch64_unknown_linux_gnu=-D__ARM_ARCH=8` into both `node-ci.yml` and `py-ci.yml` (per briansmith/ring#1728/1789), not by swapping crypto backends. Two earlier feature attempts (`enable-rustls` aws-lc-rs, then `enable-native-tls`) had different cross-compile failure modes; both reverted. Trust roots come from `rustls-native-certs` (platform store), with `SSL_CERT_FILE` taking precedence for private CAs.
- **PR #115 — TCP keepalive + reconnect via `ConnectionTuning` (Phase 2).** New `ConnectionTuning` struct in `chasquimq::config` exposes fred's `PerformanceConfig` / `ConnectionConfig` / `ReconnectPolicy` slots. `ProducerConfig`, `ConsumerConfig`, `PromoterConfig`, and `SchedulerConfig` each carry `connection: ConnectionTuning`; embedded promoter/scheduler inherit from the parent `Consumer`. Defaults: 60s TCP keepalive (well under AWS NAT's 350s idle cutoff), exponential reconnect (unbounded attempts, 100ms→30s, base 2, ±50ms jitter), 10s connection timeout, `reconnect_on_auth_error: true`. Bench impact within sample noise (keepalive is OS-level once the socket is set up). Shim-side overrides intentionally deferred — most users want the defaults.
- **PR #116 — `Producer::shutdown` + flush-guarantee docs (Phase 3).** The flush guarantee already held: `Producer::xadd` does `client.custom().await`, which waits for Redis's `XADD` response. So by the time `Producer::add` resolves, bytes are committed (not just queued). PR added explicit documentation, plus a new `Producer::shutdown` (engine + napi-rs + pyo3) that calls `Pool::quit` for clean disconnect. Both shim `Queue.close()` calls were rewired to await `producer.shutdown()` instead of just dropping the cached promise. New `add_then_shutdown_preserves_committed_write` integration test pins the contract via a separate admin client. Lambda hosts can return from the handler the moment `add` resolves; calling `close()` is optional polish.
- **PR #117 — Rotating IAM tokens via `CredentialProvider` (Phase 4).** Enabled fred's `credential-provider` feature; added `ConnectionTuning::credential_provider: Option<Arc<dyn fred::types::config::CredentialProvider>>`. Wired into `redis/conn.rs::connect`/`connect_pool` immediately after `Config::from_url`. fred calls `fetch()` before every `AUTH` / `HELLO`, so a long-lived pool stays usable across ElastiCache's ~15-min IAM token rotations. Two unit tests in `redis/conn.rs` cover the rotation primitive and `Arc::clone` preservation through `ConnectionTuning::clone()`. **Out of scope:** cross-FFI async-callback wiring for the Node and Python shims (deferred — non-trivial through napi-rs `ThreadsafeFunction` and pyo3 async-callback machinery). Lambda producers in Node/Python on ElastiCache IAM auth currently need either the Rust API or `REDIS_URL` rotation + `Queue` reconstruction.
- **PR #118 — doc-sync checklist in `CLAUDE.md`.** A meta-PR with no feature changes, triggered by realizing the four feature PRs above shipped engine surface that never reached the Starlight docs site. Added a "Doc surfaces — keep in sync" section listing all eleven user-facing surfaces (root README, `docs/engine.md`, this file, both shim READMEs, four Starlight subtrees, `astro.config.mjs` sidebar, `CLAUDE.md`), what triggers an update on each, and the slip-ups to actively guard against (asymmetric shim READMEs, missed history entries, unmentioned config knobs in `reference/options.md`).

The original AWS Lambda doc that motivated this slice is now actually buildable — engine supports it end-to-end for Rust producers, and the producer-side story for Node/Python on ElastiCache IAM is documented honestly with workarounds. Lands as a separate doc PR (Diátaxis: how-to + explanation + reference).

**Release operator note (1.2.0).** When cutting a release PR, **squash-merge** so the head commit's subject is the PR title (which starts with `chore(release):`). The publish gate on `node-ci.yml` and `py-ci.yml` checks the head commit's subject; a regular merge commit titled "Merge pull request #N from ..." does not match the gate and silently skips both publishes. v1.2.0's first merge attempt (PR #121) used `gh pr merge --merge`, skipped the publishes, and required a follow-up `chore(release):`-prefixed commit on `main` to re-fire the gate. For future releases: `gh pr merge <N> --squash` is the right invocation.

## Latency bench scenario (post-slice-11)

**Date:** 2026-05-14. **Branch:** `feat/latency-bench` (six commits ahead of `main`; engine code byte-identical to `main`).

A new bench-crate-only scenario `worker-latency` measures dispatch overhead on an idle queue — the question "if I publish a job to an idle queue right now, how long until it's done?" — which the throughput-only bench harness was silent on. Implemented as a single live producer on `tokio::time::interval(1ms)` against a consumer pool with `concurrency=100`, `batch=64`. Two histograms recorded per invocation via `hdrhistogram = "7"`: `handler_us` (engine-measured handler future duration, sourced from `JobOutcome.handler_duration_us` via a bench-side `MetricsSink` impl) and `end_to_end_us` (wall-clock delta from `Job::created_at_ms` to a `SystemTime::now()` reading inside the handler). A derived `engine_overhead_us` per-percentile delta (`end_to_end - handler`) is rendered as a footnote.

Headline numbers on a contended Apple M3 (load avg ~3, in the documented 1.8–4.3 today's-laptop envelope), `--repeats 5 --scale 5 --discard-slowest 1`:

| Histogram | p50 (us) | p99 (us) | p99.9 (us) |
|---|---:|---:|---:|
| `end_to_end_us` | 1,044 | 1,734 | 2,747 |
| `handler_us` (no-op handler, engine-measured) | 1 | 2 | 13 |
| `engine_overhead_us` (derived) | 1,043 | 1,732 | 2,734 |

Why this matters: the previous bench harness reported throughput only, leaving the "latency is unmeasured" caveat live on the Starlight `performance-trade-offs` page and the `benchmarks/README.md` methodology limitations list. This scenario closes that gap with an honest, dispatch-overhead-shaped measurement. There is no BullMQ comparator (their bench does not measure per-job latency), so the report deliberately does not publish a "N× lower latency than BullMQ" claim. The p50 ~1ms reading is bounded from below by the producer's 1ms inter-arrival cadence and the millisecond-resolution of `created_at_ms` on the wire (~500us floor per-job); both are documented in the report.

Two MEDIUM fixes from the late `daster-bug` review landed in the same branch: a **warmup gate** (both histograms now only record on jobs past the `warmup` boundary — without it, cold-start outliers from consumer pool spin-up and the first XREADGROUP block polluted the aggregate), and an **overshoot-deadlock fix** (the producer task is cancelled via a dedicated `CancellationToken` once the bench-count reaches `warmup + bench`; without it, the 1ms `tokio::time::interval` kept XADDing past the bench window and on rare runs deadlocked the `done_rx` await). Both fixes held on the canonical run.

Full report with raw numbers, distribution stats, and reproducibility instructions: [`benchmarks/latency-1.x.md`](../benchmarks/latency-1.x.md).

## Deferred follow-ups for 1.x

- **Opt-in result-write bench scenario.** The PR #75 bench guard locked in the no-overhead-when-off claim (`store_results=false` regresses 0%). The opt-in path (`store_results=true` under sustained load) is not yet measured.
- **`maxmemory` eviction-behavior verification.** The result keys' TTL contract is documented but the engine-level behavior under Redis `maxmemory-policy: allkeys-lru` / `volatile-lru` eviction has not been exercised end-to-end.
- **Cross-FFI credential-provider callback (Phase 4 follow-up).** Wire async `fetch()` callbacks through napi-rs `ThreadsafeFunction` and pyo3 async-callback machinery so Node/Python users can plug in IAM-token-fetching callbacks directly, instead of dropping to the Rust API or rotating `REDIS_URL` externally.
