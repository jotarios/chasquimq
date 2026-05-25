# Engine deep-dive

Internals and operational details for the Rust core. The Node and Python shims wrap this engine — most of what's here applies through them too, with the API surfaces remapped to JS/TS and Python idioms.

## Retry semantics

When a handler returns `Err` (or panics), the worker:

1. Encodes the job with `attempt += 1` and computes `run_at_ms = now + backoff(attempt)`.
2. Atomically (via Lua) `XACKDEL`s the original stream entry and `ZADD`s the re-encoded job onto the queue's delayed set.
3. The promoter promotes it back into the stream when due. Next handler invocation sees `job.attempt` incremented.

If `next_attempt >= max_attempts`, the entry goes straight to DLQ instead. Backoff is `min(initial * multiplier^(attempt-1), max) + jitter`. Defaults: `initial=100ms`, `multiplier=2`, `max=30s`, `jitter=100ms`. Configure via `ConsumerConfig::retry: RetryConfig`.

The classic `XREADGROUP CLAIM` mechanism (Redis 8.4 idle-pending reads) remains the safety net: if a worker dies mid-handler before the retry path runs, CLAIM re-delivers the entry on the next read, and the reader compares the in-payload `attempt` counter against `delivery_count` to detect retry-exhaustion regardless of which path produced the count.

## Stalled-job detector

CLAIM-on-read recovers a *single* mid-handler crash, but it can't bound a *loop* of worker crashes against the same entry — every CLAIM redelivery resets the idle clock, the `delivery_count` rises forever, and the entry never escapes the PEL. The stalled-job detector is the active sibling of that passive recovery: a leader-elected background task spawned alongside the promoter and scheduler that scans the consumer group's PEL on a tick, INCRs a per-job stall counter for entries idle past the threshold, and atomically relocates them to the DLQ as `DlqReason::Stalled` at `max_stalled_attempts`.

**What counts as stalled:** every detector tick at which the entry is observed sitting in the PEL idle longer than `idle_threshold_ms`. The counter is per-job (`{chasqui:<queue>}:stalls:<job_id>`, sliding TTL = `idle_threshold_ms * max_stalled_attempts * 2`); it's `DEL`'d on successful ack (inside `JOB_OK_SCRIPT`) and on DLQ replay (inside `REPLAY_DLQ_SCRIPT`), so a one-off stall followed by success starts a fresh streak.

**Race-isolation invariant.** The reader's `XREADGROUP ... CLAIM <claim_min_idle_ms>` is still the only path that bumps `delivery_count`; the detector reads idle-ms but never `XCLAIM`s. To keep the per-crash counting invariant ("one INCR per crash cycle"), the embedded spawn forces `stalled_detector.tick_interval_ms == idle_threshold_ms == claim_min_idle_ms` so reader and detector move in lockstep: reader CLAIM resets idle → handler crashes → idle climbs back past threshold → next detector tick INCRs once. `ConsumerConfig::validate()` rejects `tick_interval_ms < idle_threshold_ms` to keep operators who override these explicitly from breaking it.

**Threshold-hit relocate path.** When `n >= max_stalled_attempts`, `STALLED_SCAN_SCRIPT` `XACKDEL`s the entry out of the PEL inside the same Lua call (atomic with the INCR) and signals Rust to enqueue a DLQ relocate via the existing `dlq_tx` channel. The relocator uses a sibling `RELOCATE_DLQ_PRE_ACKED_SCRIPT` (XADD-only, no XACKDEL gate) since the entry is already acked — the IDMP marker on the XADD is the dedup guard. The eventual `e=dlq` event carries `reason="stalled"` so DLQ subscribers can distinguish handler-failure loops (`retries_exhausted`) from worker-crash loops (`stalled`).

**Configuration:** `ConsumerConfig::stalled_detector_enabled` (default `true`) and `ConsumerConfig::stalled_detector: StalledDetectorConfig` — `max_stalled_attempts` (default `1`, matches BullMQ's `maxStalledCount`), `scan_batch` (default `256`, caps `XPENDING ... IDLE - + N`), `lock_ttl_secs` (default `90`, sized to outlive a full `tick_interval_ms` sleep so the sleeping leader doesn't lose its lock to a replica every tick — bump in lockstep when raising `tick_interval_ms`). `tick_interval_ms` and `idle_threshold_ms` default to `30_000` but are overridden from `claim_min_idle_ms` at spawn time on the embedded path. Observability: `MetricsSink::stalled_tick(StalledTick { scanned, incremented, relocated })` per leader tick, plus the existing `dlq_routed` event with `reason: DlqReason::Stalled`.

**Active-state observability:** `JobInfo.stalled_count` is populated by the introspector only for `Active`-state lookups. The counter is `DEL`'d on every terminal transition (ack / DLQ-relocate / DLQ-replay), so it's never live outside the active window — probing in other states would always return `nil` and burn one extra round trip on every admin lookup.

## Delayed jobs

`Producer::add_in(delay, payload)` and `Producer::add_at(when, payload)` schedule jobs to fire later. Bulk variant is `Producer::add_in_bulk`. A `delay` of zero (or `add_at` in the past) fast-paths straight to the stream.

By default any `Consumer` with `delayed_enabled = true` (the default) runs an embedded promoter that moves due jobs from the delayed sorted set into the stream. Multiple consumers coordinate via a per-queue lock so only one promotes per tick. For producer-only deployments where no consumer runs locally, run a standalone [`Promoter`](../chasquimq/examples/standalone_promoter.rs).

`max_delay_secs` on `ProducerConfig` (default 30 days) caps how far in the future jobs can be scheduled. Set to `0` to disable the cap.

### Idempotent variants

`add_in_with_id(id, delay, payload)` / `add_at_with_id(id, when, payload)` / `add_in_bulk_with_ids(delay, items)` accept a stable caller-supplied `JobId` and are safe under producer-driven retries. A single Lua script atomically `SET NX EX`s a dedup marker (`{chasqui:<queue>}:dlid:<job_id>`, TTL = `delay + 1h grace`) and only then `ZADD`s the encoded job — a retry after a network failure that already reached Redis is a no-op returning the same id. The marker grace covers the post-promote window so a delayed retry can't race a successful promotion. The plain `add_in` / `add_at` / `add_in_bulk` calls remain available and use a fresh ULID per call (at-least-once under caller retry).

### Cancellation

`Producer::cancel_delayed(&id) -> bool` removes a previously scheduled delayed job. Returns `true` only when the entry was atomically `ZREM`'d from the delayed ZSET; `false` covers "never scheduled", "side-index expired", and "promoter already moved it to the stream" (the cancel-vs-promote race lost). `cancel_delayed_bulk(&[id])` pipelines many. Both schedule and cancel paths execute as Lua under the queue's `{chasqui:<queue>}` hash tag, so they serialize at Redis — `(cancel returned true, job still delivered)` is impossible.

## DLQ tooling

`Producer::peek_dlq(limit)` reads up to N DLQ entries with their failure metadata (`source_id`, `reason`, optional `detail`, raw payload bytes) without removing them — the inspection API. `reason` values: `retries_exhausted` (handler failed enough times), `decode_failed` (msgpack envelope didn't parse), `malformed { reason }` (stream entry shape wrong), `oversize_payload`, `unrecoverable` (handler raised `UnrecoverableError`), `stalled` (stalled-detector relocated a worker-crash loop — see [Stalled-job detector](#stalled-job-detector)).

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

## Job introspection

`chasquimq::Introspector` is the read-only counterpart to the producer/consumer hot paths. Open one per `(queue, consumer_group)` pair (`Introspector::connect`); it keeps a small dedicated pool (size 2) so introspection bursts don't compete with the producer's connections.

Surface:

- **`get_job_counts() -> JobCounts`** — `{ waiting, active, delayed, completed, failed, paused, completed_is_capped }` in ~5 round trips (`XLEN`, `XPENDING`, `ZCARD`, `XLEN dlq`, `EXISTS paused`, plus a bounded `SCAN result:*`). `completed_is_capped = true` when the SCAN hit `CHASQUIMQ_COMPLETED_SCAN_CAP` (default 10,000) before exhausting; treat `completed` as a lower bound when capped.
- **`get_job_state(id) -> JobState`** — live-state-first: pending (PEL) → delayed → waiting → DLQ → result. A job replayed from DLQ resolves as `Waiting`, not `Completed`, during the race window — what matters for callers is the work the next worker tick is about to do.
- **`get_job(id) -> Option<JobInfo>`** — bounded XRANGE + ZRANGEBYSCORE + DLQ scan + result-key probe. `JobInfo::payload` is the opaque msgpack bytes (the engine doesn't decode). An entry whose envelope didn't decode surfaces with `decode_failed = true` instead of panicking or silently skipping.
- **`get_jobs(state, offset, limit, cursor) -> JobsPage { jobs, next_cursor }`** — paginated listing. Cursor encoding per state: stream entry id (waiting / failed); `score:offset_into_score` (delayed — fixed against tied-fire-ms members being dropped at the page boundary); raw `SCAN` cursor (completed); none / `None`-after-first-page (active, the PEL window is already bounded).

Operational notes:

- **NOGROUP on a fresh queue is swallowed.** `XPENDING` against a never-opened consumer group errors with `NOGROUP`; `get_job_counts` reports `active = 0`. The sticky `consumer_group` constructor option on both shims (Node `QueueOptions.consumerGroup`, Python `Queue(consumer_group=...)`) lets the introspector and the workers agree on which group's PEL counts as "active."
- **`completed` is approximate.** It SCANs the `result:*` keyspace under the per-queue hash tag; very large keyspaces return the cap. Tighten or widen via `CHASQUIMQ_COMPLETED_SCAN_CAP`.
- **No hot-path cost.** The producer / consumer / promoter / scheduler paths are byte-for-byte unchanged by introspection — verified by the no-regression bench in [`benchmarks/chasquimq-1.0.md`](../benchmarks/chasquimq-1.0.md).

## Job maintenance

`Producer` carries four maintenance methods for tearing jobs — or a whole queue — down. They are off the hot path (no `XADD` / `XREADGROUP` / `XACK` change) and every scan is bounded so a single call can never block Redis. All four take the consumer-group name (for stream acks); the shims pass their configured group, defaulting to `"default"`.

Surface:

- **`remove(job_id, group) -> RemovalReport`** — delete one job everywhere it could live: the delayed ZSET (plus its `didx` / `dlid` side-indexes), a waiting or active main-stream entry, the DLQ, the per-job result key, and the per-job [progress + log keys](#progress-and-logs). Idempotent — a `job_id` on no surface returns an all-`false` `RemovalReport { delayed, stream, dlq, result }`, not an error. The stable `JobId` lives inside the msgpack envelope, not the Redis stream entry id, so the stream / DLQ branches run a bounded `XRANGE` scan to translate the job id to an entry id before the atomic `XACKDEL` / `XDEL`. A job past the bounded scan window reports as "not on this surface" — pair with the introspection API to find jobs deep in a very large stream. The delayed branch reuses `CANCEL_DELAYED_SCRIPT` verbatim.
- **`drain(group, DrainOptions) -> u64`** — clear every *waiting* job (main-stream entries not in any consumer-group PEL) and, by default, the delayed ZSET. In-flight (pending) jobs are left running. `DrainOptions { delayed: false }` keeps scheduled future jobs. A ChasquiMQ stream mixes waiting and active entries on one Redis Stream, so `DRAIN_STREAM_SCRIPT` subtracts the `XPENDING` set from an `XRANGE` page and `XDEL`s the complement; the drain runs in bounded passes until a pass deletes nothing. Returns the total stream + delayed count removed.
- **`clean(group, grace_ms, limit, state) -> Vec<String>`** — age- and state-filtered bulk delete; removes up to `limit` jobs in `state` older than `now - grace_ms` and returns the removed job ids. Supported states: `Waiting`, `Failed` (DLQ), `Delayed`, `Completed`. `Active` is a deliberate no-op (removing an in-flight job mid-execution is a footgun — use `remove`). Age basis: the stream entry id's millisecond prefix for `Waiting` / `Failed`; the job's `created_at_ms` for `Delayed`; `grace_ms` is **ignored** for `Completed` (a result key has no creation timestamp — its own `result_ttl_secs` handles age-out, so `clean(Completed, …)` is limit-only). Per removed job the bulk path matches `remove`'s semantics: the [per-job progress + log keys](#progress-and-logs) are unlinked in the same pipeline.
- **`obliterate(group) -> u64`** — tear the entire `{chasqui:<queue>}` keyspace down: the main stream and its consumer groups, the DLQ, the delayed ZSET, every `didx` / `dlid` side-index, every result key, every progress + log key, all repeatable specs, the durable paused flag, the events stream, and the promoter / scheduler locks. Implemented as a batched `SCAN` + `UNLINK` (async reclaim, so a multi-GB stream never stalls Redis). Not atomic — but obliterate is a destructive admin op and a crash mid-teardown is fully recoverable by re-running (the next `SCAN` finds the remainder). Returns the count of Redis keys removed.

Operational notes:

- **`remove` / `clean` scans are bounded.** They walk a single `XRANGE … COUNT 1024` window. A job further back than that is reported as absent on the stream / DLQ surface — not an error, just out of the convenience-scan window. For a job deep in a very large stream, locate it via `get_jobs` pagination first.
- **`obliterate` drops every consumer group.** It deletes the stream key wholesale, so all consumer groups on the queue go with it. The `group` argument is taken for signature symmetry only.
- **`clean(Completed)` is limit-only.** No `grace_ms` filtering — result keys carry no creation timestamp. Rely on `result_ttl_secs` for time-based result expiry; use `clean(Completed, …)` only to reclaim result keys eagerly.
- **The CLI exposes `clean` and `obliterate`.** `chasqui clean <queue> --state <s> --grace-ms <ms> --limit <n>` and `chasqui obliterate <queue>`; both are destructive and prompt for confirmation unless `--yes` is passed.

## Progress and logs

Per-handler write surface for in-flight job state. Attached to `Job<T>::handle` as `Option<JobHandle>` immediately before the user handler runs; absent on Jobs returned by `Introspector::get_job` / `get_jobs` (which throws a read-only error if a caller tries to write).

Two side-channel Redis keys under the queue's existing `{chasqui:<queue>}` hash tag (single-slot on Redis Cluster):

- **`{chasqui:<q>}:progress:<id>`** — STRING. ASCII-decimal `u8` (so any shim reads it with `parseInt` / `int(str(...))` without a msgpack dependency). TTL = `result_ttl_secs` so it disappears alongside the result key after a successful completion. Written by `JobHandle::update_progress(n)`; read by the introspector and surfaced on `JobInfo::progress: Option<u8>`. Values `> 100` clamp to 100 (warn-once per handle).
- **`{chasqui:<q>}:log:<id>`** — STREAM. One entry per `JobHandle::log(line)` call, under field `line`. `MAXLEN ~ log_max_stream_len` keeps the stream bounded. Each `log()` also pipelines `EXPIRE log_key result_ttl_secs` so the stream key disappears alongside the result key after job completion — `MAXLEN ~` caps entries but not the key itself, so without this an orphan one-shot log line would leak the stream indefinitely. Oversize lines (`> log_max_line_bytes`) truncate on a UTF-8 char boundary with a `[…truncated]` marker (warn-once per handle). Read back via `Introspector::get_job_logs(id, start, end, asc) -> (Vec<String>, u64)` — `start = -N` means "N from the end" via XLEN (matches BullMQ's `getLogs` convention); the trailing `u64` is the current XLEN.

Three new `ConsumerConfig` fields (also configurable through both shims):

- `log_max_stream_len: u64 = 1000` — `MAXLEN ~` cap on each per-job log stream. Validated `≥ 16` at `Consumer::run` (below the minimum, the `MAXLEN ~` rounding leaves the stream effectively empty between writes).
- `log_max_line_bytes: usize = 4096` — per-line byte cap before truncation.
- `events_progress_enabled: bool = true` — gates the `e=progress` events-stream entry emitted after a successful `update_progress` SET. The persisted progress key is always written regardless; this only mutes the events fan-out, so a high-rate progress handler can opt out of the events flood while keeping introspector-visible state.

Connection budget: `JobHandle` borrows a **shared 2–8-sized `fred::clients::Pool`** sized to consumer concurrency — never a client per worker. Handlers that never call `update_progress` / `log` pay nothing.

Both keys are reaped on the existing maintenance paths: `Producer::remove(id)` unlinks them in the same pipeline as the result key; `Producer::clean(state, ...)` mirrors the same per-removed-job tail; `Producer::obliterate()` already deletes the whole `{chasqui:<queue>}` keyspace.

**`progress` event on the events stream.** When `events_progress_enabled = true` (default), every `update_progress` call emits an `e=progress` entry on `{chasqui:<queue>}:events` after the SET succeeds (best-effort — a failed XADD never propagates back to the handler; the persisted key is the source of truth). The Node and Python shims fan this onto `Worker.on('progress', (job, n) => ...)` (via a lazy embedded `QueueEvents` subscriber, same zero-cost-when-unused pattern as `drained`) plus broadcast `'progress'` and per-id `'progress:<jobId>'` channels on `QueueEvents`.

**Read-only Job guard.** Jobs returned by `Queue.getJob` / `getJobs` / `Queue.add` (and any other introspector- or producer-side path) carry no per-handler `JobHandle`. Calling `updateProgress` / `log` on those raises a clear "read-only Job" error on both shims (`Error` with the marker message on Node, `RuntimeError` on Python). Only Jobs handed to a `Worker` processor have a live backref.

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
- **Producer-side payload cap is symmetric with the consumer's.** `ProducerConfig::max_payload_bytes` (default 1 MiB, identical to `ConsumerConfig::max_payload_bytes`) rejects any `add*` / `upsert_repeatable` whose encoded MessagePack payload exceeds it with `Error::Config`, *before* any Redis write — so an oversize job never reaches the stream/ZSET (vs. the consumer cap, which routes oversize-on-read to the DLQ). Lowering the cap below an already-stored repeatable spec only bites on the next `upsert_repeatable` for that key; specs already in the repeat hash keep firing until re-upserted or cancelled.
- **`cancel_delayed` only works for jobs scheduled via the `_with_id` API surface.** Cancel looks up the exact ZSET member through a side-index (`{chasqui:<queue>}:didx:<job_id>`) that is written only by the idempotent schedule script. Plain `add_in` / `add_at` / `add_in_bulk` calls don't populate the index, so cancel by id is a no-op (returns `false`) for those.
- **Redis Cluster: connect with a `redis-cluster://` URL.** Pass `redis-cluster://seed-host:port` (or `rediss-cluster://…` for TLS, `valkey-cluster://` / `valkeys-cluster://` for Valkey). fred treats the host as one seed node and discovers the rest of the topology via `CLUSTER SLOTS`; add more seeds with `?node=host:port` query params. No feature flag, no config field — the scheme is the switch, and `ConnectionTuning` (keepalive, reconnect, `credential_provider`) applies to a clustered connection unchanged. MOVED/ASK redirection and topology refresh are handled inside fred. Correctness rests on two invariants the engine already holds: every key for one queue carries the `{chasqui:<queue>}` hash tag (so the queue's whole keyspace — stream, delayed ZSET, DLQ, result/dedup/lock/events/repeat keys — lands on **one** slot), and every command and multi-key Lua script (`PROMOTE`, `RETRY_RESCHEDULE`, `RELOCATE_DLQ`, `JOB_OK`, `CANCEL_DELAYED`, `SCHEDULE_*`) is dispatched with `ClusterHash::FirstKey` so it routes to that slot — never `CROSSSLOT`. A single queue is therefore single-slot by construction; cross-slot fan-out of one queue and cross-queue atomic operations are out of scope (the latter do not exist on single-node either). On a cluster `SCRIPT LOAD` lands on one node, so the first `EVALSHA` against another slot-owning node returns `NOSCRIPT`; the engine's existing inline-`EVAL` fallback runs the body and caches it on that node, so the path self-heals after one fallback per script per node and the steady state is pure `EVALSHA`.
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
