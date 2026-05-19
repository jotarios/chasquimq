# Engine deep-dive

Internals and operational details for the Rust core. The Node and Python shims wrap this engine — most of what's here applies through them too, with the API surfaces remapped to JS/TS and Python idioms.

## Retry semantics

When a handler returns `Err` (or panics), the worker:

1. Encodes the job with `attempt += 1` and computes `run_at_ms = now + backoff(attempt)`.
2. Atomically (via Lua) `XACKDEL`s the original stream entry and `ZADD`s the re-encoded job onto the queue's delayed set.
3. The promoter promotes it back into the stream when due. Next handler invocation sees `job.attempt` incremented.

If `next_attempt >= max_attempts`, the entry goes straight to DLQ instead. Backoff is `min(initial * multiplier^(attempt-1), max) + jitter`. Defaults: `initial=100ms`, `multiplier=2`, `max=30s`, `jitter=100ms`. Configure via `ConsumerConfig::retry: RetryConfig`.

The classic `XREADGROUP CLAIM` mechanism (Redis 8.4 idle-pending reads) remains the safety net: if a worker dies mid-handler before the retry path runs, CLAIM re-delivers the entry on the next read, and the reader compares the in-payload `attempt` counter against `delivery_count` to detect retry-exhaustion regardless of which path produced the count.

## Delayed jobs

`Producer::add_in(delay, payload)` and `Producer::add_at(when, payload)` schedule jobs to fire later. Bulk variant is `Producer::add_in_bulk`. A `delay` of zero (or `add_at` in the past) fast-paths straight to the stream.

By default any `Consumer` with `delayed_enabled = true` (the default) runs an embedded promoter that moves due jobs from the delayed sorted set into the stream. Multiple consumers coordinate via a per-queue lock so only one promotes per tick. For producer-only deployments where no consumer runs locally, run a standalone [`Promoter`](../chasquimq/examples/standalone_promoter.rs).

`max_delay_secs` on `ProducerConfig` (default 30 days) caps how far in the future jobs can be scheduled. Set to `0` to disable the cap.

### Idempotent variants

`add_in_with_id(id, delay, payload)` / `add_at_with_id(id, when, payload)` / `add_in_bulk_with_ids(delay, items)` accept a stable caller-supplied `JobId` and are safe under producer-driven retries. A single Lua script atomically `SET NX EX`s a dedup marker (`{chasqui:<queue>}:dlid:<job_id>`, TTL = `delay + 1h grace`) and only then `ZADD`s the encoded job — a retry after a network failure that already reached Redis is a no-op returning the same id. The marker grace covers the post-promote window so a delayed retry can't race a successful promotion. The plain `add_in` / `add_at` / `add_in_bulk` calls remain available and use a fresh ULID per call (at-least-once under caller retry).

### Cancellation

`Producer::cancel_delayed(&id) -> bool` removes a previously scheduled delayed job. Returns `true` only when the entry was atomically `ZREM`'d from the delayed ZSET; `false` covers "never scheduled", "side-index expired", and "promoter already moved it to the stream" (the cancel-vs-promote race lost). `cancel_delayed_bulk(&[id])` pipelines many. Both schedule and cancel paths execute as Lua under the queue's `{chasqui:<queue>}` hash tag, so they serialize at Redis — `(cancel returned true, job still delivered)` is impossible.

## DLQ tooling

`Producer::peek_dlq(limit)` reads up to N DLQ entries with their failure metadata (`source_id`, `reason`, optional `detail`, raw payload bytes) without removing them — the inspection API.

`Producer::replay_dlq(limit)` moves up to N DLQ entries back into the main stream atomically. Each entry's `attempt` counter is reset to 0 before re-`XADD` so the replayed job gets a full retry budget (otherwise it'd land in DLQ again on first dispatch). The fix-the-bug-and-requeue workflow.

DLQ growth is capped via `ConsumerConfig::dlq_max_stream_len` (default 100,000). `XADD MAXLEN ~ N` is approximate so a runaway error rate may overshoot temporarily but won't grow unboundedly.

## Result backends

Opt-in per-job result storage. `ConsumerConfig::store_results = true` enables it; `result_ttl_secs` (default 3600) controls expiry. When the handler returns `Ok(bytes)`, a Lua script atomically `XACKDEL`s the stream entry and `SET`s `{chasqui:<queue>}:result:<job_id>` with the bytes and TTL.

Result-writes are batched, not per-job: completed entries accumulate in a bounded channel and flush as a single pipelined round trip, mirroring the batched `XACKDEL` ack-flusher. `result_batch` (default 64) caps the flush size; `result_idle_ms` (default 5) bounds latency when the batch isn't full. A `NOSCRIPT` mid-batch rebuilds the whole flush as inline `EVAL`; a per-element error leaves only that entry pending so `CLAIM` reclaims it. Tune `result_batch` up for throughput, down for lower result-visibility latency.

`Producer::get_result(&JobId) -> Result<Option<Bytes>>` and `get_result_bulk(&[JobId])` read it back. `None` collapses three indistinguishable cases: not yet completed, expired, never written. For at-most-once result semantics, prefer `QueueEvents` subscription over `get_result`.

Default-config consumers (`store_results = false`) skip the writer task entirely — zero overhead vs. the no-result-backend path. On opt-in workloads the batched writer lifts `worker-concurrent-store-results` 8.4× over the per-entry PR #75 path (see [`benchmarks/store-results-opt-in.md`](../benchmarks/store-results-opt-in.md)).

### Behavior under `maxmemory` eviction

`JOB_OK_SCRIPT` carries the `#!lua flags=allow-oom` shebang and wraps the result `SET` in `redis.pcall`, so the XACKDEL always commits even when Redis is at the `maxmemory` ceiling. The two policies that matter:

- **`noeviction`**: when `used_memory >= maxmemory`, Redis rejects new writes with `OOM`. The script's XACKDEL still runs (it frees memory). The `SET` may be rejected or accepted depending on whether the freed bytes leave headroom — either way, `pcall` swallows the rejection and the script returns success. The job is acked, the result may be missing. `get_result` returns `None`, which is the documented "indistinguishable from expired / never written" case.
- **`allkeys-lru` / `allkeys-lfu`**: writes succeed by evicting older keys to make room. Result-keys are eligible for eviction (no protection by hash tag); a tight cap will reap older results before their TTL. `get_result` returns `None` for evicted keys; the engine never observes the eviction.

What the engine guarantees regardless of policy: every accepted handler delivery either acks cleanly or reclaims via CLAIM after a worker crash. There is no scenario where `JOB_OK_SCRIPT`'s SET failure leaves an entry pending forever — the integration test in `chasquimq/tests/maxmemory.rs` (gated behind `CHASQUIMQ_RUN_MAXMEMORY_TEST=1` because it mutates Redis CONFIG) exercises both policies end-to-end. What is **not** guaranteed: that the result was written. Treat `get_result` returning `None` as ambiguous and use `QueueEvents` for deterministic completion-detection.

## Pause / resume

Consumer-side stop-dispatch. When paused, the reader stops issuing `XREADGROUP` and stops handing jobs to the worker pool at the **next batch boundary**; jobs already dispatched to handlers drain to completion; producers (and the promoter/scheduler/relocators) are unaffected — the stream backlog grows while paused, by design.

Two independent signals, both observed by the reader only at batch boundaries — never per-job, never on the produce path:

- **In-process `PauseControl`** (process-local). `Consumer::pause_control()` returns a shared `Arc<PauseControl>` (same sharing model as the shutdown `CancellationToken`) with `pause()` / `resume()` / `is_paused()`. Backed by a `tokio::sync::watch::channel(bool)` so a resume wakes the parked reader immediately (edge-triggered, no poll latency) and the lost-wakeup race is eliminated by construction. Double-pause / double-resume are idempotent and produce no spurious wake. This is the `Worker.pause()` path on the shims.
- **Cross-process `{chasqui:<queue>}:paused` key** (durable). Set/cleared by `Producer::pause()` / `resume()` (and thus `Queue.pause()` / `chasqui pause <queue>`). Every consumer of the queue observes it; no TTL, so it survives consumer restarts — a fresh consumer parks before its first `XREADGROUP`. The reader checks the key with a single `EXISTS` time-gated by `ConsumerConfig::pause_poll_ms` (default 250 ms): the not-paused hot path is one atomic `watch::borrow()` plus one `Instant` comparison per batch, never a Redis round trip. Cross-process pause/resume is observed within `pause_poll_ms`; the in-process path is instant. An `EXISTS` error retains the last known cross-process state (debug-logged, never flipped); the in-process switch is unaffected.

Shutdown signalled while the reader is parked still drains cleanly. `worker-concurrent` throughput is unchanged — the gate adds one atomic load and one time comparison per batch when not paused, strictly cheaper than the per-iteration shutdown check already in the loop.

## Observability

Every load-bearing engine subsystem emits structured events through the single `chasquimq::MetricsSink` trait:

| Event | Source | Carries |
|:---|:---|:---|
| `PromoterTick` | promoter (per tick) | `promoted`, `depth`, `oldest_pending_lag_ms` |
| `LockOutcome` | promoter (transition-only) | `Acquired` / `Held` |
| `ReaderBatch` | consumer reader (per non-empty `XREADGROUP`) | `size`, `reclaimed` (CLAIM-recovery count) |
| `JobOutcome` | worker (per handler invocation) | `kind: Ok\|Err\|Panic`, 1-indexed `attempt`, `handler_duration_us` |
| `RetryScheduled` | retry relocator (only when the script gate fires) | 1-indexed `attempt`, `backoff_ms` |
| `DlqRouted` | DLQ relocator (after the relocate succeeds) | `reason: DlqReason`, `attempt` |

Operator identity: `chasquimq_jobs_completed_total + chasquimq_jobs_failed_total` = handler invocations. Reader-side DLQ paths (malformed entry / oversize payload / decode failure / retries-exhausted-on-arrival) emit `DlqRouted` only — the handler never ran, so they carry `attempt: 0`. Total inbound jobs = handler invocations + reader-DLQ.

Plug your own sink in via `PromoterConfig::metrics` or `ConsumerConfig::metrics`. The default is a zero-cost no-op sink. `chasquimq::metrics::testing::InMemorySink` is provided for integration tests with derived rollup accessors (`jobs_completed()`, `dlq_count(reason)`, `total_retries()`, `last_handler_duration_us()`, etc.).

The engine itself has zero observability dependencies — the trait and the no-op default are all `chasquimq` ships. Two opt-in paths in the separate [`chasquimq-metrics`](../chasquimq-metrics/) workspace crate:

- **`metrics-rs` facade route (recommended):** `MetricsFacadeSink` bridges into the [`metrics`](https://docs.rs/metrics) facade. Wrap with `QueueLabeled::new(sink, "<queue>")` for a per-queue label (composes — stack wrappers for `tenant`, `region`, …). Install any `metrics_exporter_*` recorder. Working example with `metrics-exporter-prometheus`: [`chasquimq-metrics/examples/facade_sink.rs`](../chasquimq-metrics/examples/facade_sink.rs).
- **Direct Prometheus route:** [`chasquimq-metrics/examples/prometheus_sink.rs`](../chasquimq-metrics/examples/prometheus_sink.rs) shows a hand-rolled `prometheus`-crate sink + `tiny_http` `/metrics` endpoint.

Adapter metric names follow Prometheus base-unit convention: durations are exposed as `chasquimq_handler_duration_seconds` and `chasquimq_retry_backoff_seconds` (engine events keep micros/ms internally; the adapter divides at the boundary).

## Operational notes

- **Stream MAXLEN trim is approximate.** Phase 1 stream and the delayed-job promoter use `XADD MAXLEN ~ N`. If consumers fall sustainedly behind producers, entries near the cap can be trimmed before they are read. Monitor `XLEN` against your consume rate; the silent failure mode is "job vanished."
- **`cancel_delayed` only works for jobs scheduled via the `_with_id` API surface.** Cancel looks up the exact ZSET member through a side-index (`{chasqui:<queue>}:didx:<job_id>`) that is written only by the idempotent schedule script. Plain `add_in` / `add_at` / `add_in_bulk` calls don't populate the index, so cancel by id is a no-op (returns `false`) for those.
- **Key format uses Redis Cluster hash tags.** Every chasqui key looks like `{chasqui:<queue>}:<suffix>` so the queue's keyspace co-locates on a single Redis Cluster slot.
- **Result-backend `None` is ambiguous.** `Producer::get_result` returns `None` for "not yet completed", "expired", and "never written" alike. For deterministic completion-detection, subscribe to the events stream via `QueueEvents`.
- **A durable pause has no TTL.** `chasqui pause <queue>` / `Queue.pause()` set `{chasqui:<queue>}:paused` with no expiry — it persists across consumer restarts until an explicit `resume`. A queue left paused stays paused; that is the intended operator semantics, not a leak. `Worker.pause()` is process-local instead and never touches Redis.
- **TLS via `rediss://`.** Pass `rediss://host:port` and the engine negotiates TLS via fred's `enable-rustls-ring` feature; trust roots come from the platform store via `rustls-native-certs` (keychain on macOS, OS CA bundle on Linux probed by `openssl-probe`, system store on Windows). `SSL_CERT_FILE`, when set, takes precedence over the platform store — point it at a PEM bundle to trust private CAs. Plain `redis://` is unaffected — the TLS connector is only constructed when the URL scheme demands it.
- **Connection tuning defaults via `ConnectionTuning`.** Every `*Config` (`Producer`, `Consumer`, `Promoter`, `Scheduler`) carries a `connection: ConnectionTuning` field with sensible defaults: TCP keepalive on (60s probe interval, 10s between probes), exponential reconnect (unbounded attempts, 100ms→30s with base 2 and 50ms jitter), 10s connection timeout, reconnect on auth error. The keepalive matters for environments where idle TCP gets dropped silently — most notably AWS NAT Gateways (350s idle cutoff) and some load balancers. Override per-component by constructing the config explicitly: `ProducerConfig { connection: ConnectionTuning { tcp_keepalive_secs: 30, ..Default::default() }, ..Default::default() }`. Set `tcp_keepalive_secs: 0` to disable keepalive entirely.
- **`Producer::add` resolves only after Redis acks the XADD.** Every producer write — `add`, `add_with_id`, `add_with_options`, `add_bulk`, `add_in`, `add_at`, and the idempotent `_with_id` variants — awaits the server response before the future resolves. There is no buffering layer between the call and Redis: by the time `await` returns, the bytes are not just on the wire but committed to the stream (or scheduled in the delayed ZSET, for the timed variants). `Producer::shutdown` calls fred's `Pool::quit` to disconnect cleanly; it does not wait for in-flight commands because there are none — every `add` is already drained when it returns. This matters for hosts that may be frozen or terminated immediately after the call resolves (AWS Lambda, Cloud Run, fly.io machines): you can safely return from your handler the moment `add` resolves without an explicit flush.
- **Rotating credentials via `ConnectionTuning::credential_provider`.** For Redis deployments that use short-lived auth tokens (most notably ElastiCache IAM auth, where tokens expire ~15 min), set `connection.credential_provider = Some(Arc::new(MyProvider))` where `MyProvider` implements `fred::types::config::CredentialProvider`. fred calls `fetch()` before every `AUTH` / `HELLO` command — initial connect, every reconnect (paired with the unbounded `reconnect_max_attempts` default and `reconnect_on_auth_error: true`), so a long-lived pool stays authenticated through token rotation without rebuilding. Both shims now expose this: Node via `connection.credentialProvider` (a callback returning `{ username?, password? }`), Python via the `credential_provider` keyword arg (an async callable returning a `(username, password)` tuple) — see the [Node](../chasquimq-node/README.md) and [Python](../chasquimq-py/README.md) shim READMEs, or the [options reference](https://chasquimq.io/reference/options/#connection). `reconnect_max_attempts` is exposed on both shims too (Node `connection.reconnectMaxAttempts`, Python `reconnect_max_attempts=`), so a callback that always rejects can be bounded instead of looping forever on reconnect — `0` (the default) keeps the unbounded behaviour.

## Why the design choices

The bottlenecks ChasquiMQ exists to escape, and what it does instead — at engine depth:

- **Redis Streams over `LPUSH`/`BRPOP`.** Consumer groups give per-consumer pending lists, idle-claim recovery, and deterministic IDs without inventing them in user space.
- **MessagePack payloads via `rmp-serde`.** Binary, schema-flexible, smaller and faster than JSON on every hop.
- **Batched, pipelined `XACK`.** Acks accumulate in a bounded channel and flush as a single pipelined batch (`ack_batch` jobs or `ack_idle_ms` idle, whichever first).
- **`XACKDEL` (Redis 8.2).** Atomic ack-and-delete in one round trip — no ack-then-delete dance.
- **Atomic DLQ relocation.** Routing a poisoned entry into the DLQ is a single Lua script (`RELOCATE_DLQ_SCRIPT`): `XACKDEL` the source entry from the consumer group first, then `XADD` it into the DLQ only if the ack actually removed it. The whole move is one server-side invocation, so a crash or dropped connection can never leave the entry both in the DLQ and still pending on the main stream (the duplicate-on-retry window the old non-atomic pipeline had). Idempotent under client retry: a retried relocate after a lost reply finds nothing to ack and skips the second `XADD`. Same gate-then-side-effect shape as the retry-reschedule path.
- **`IDMP` idempotent `XADD` (Redis 8.6).** The DLQ relocate's `XADD` still carries `IDMP <producer_id> <source_id>` as defense-in-depth on top of the atomic gate. On the immediate produce path, a stable `jobId` scopes dedup to the producer instance via Redis `IDMP <producer_id> <job_id>`, so producer retries after network blips don't double-publish.
- **Tokio multi-receiver dispatch.** `async-channel` fans batches to N workers without a shared `Mutex` on the receiver. Per-job work stays off the reader's hot path; DLQ moves run on a dedicated relocator task.
- **`Arc<str>` everywhere on the hot path.** Stream entry IDs and consumer/producer IDs are reference-counted, not cloned as `String`.

Anti-patterns we don't reach for: blocking Lua scripts, JSON payloads, per-job round trips.
