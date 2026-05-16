---
title: Rust API
description: Reference for the chasquimq engine crate — Producer, Consumer, Promoter, Scheduler, every config struct, every observability event, the error enum.
sidebar:
  order: 4
---

The Rust crate is the engine. The Node and Python shims are NAPI-RS
and PyO3 bindings on top of the same binary; everything documented
here is what those shims call into.

```toml
[dependencies]
chasquimq = "1.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For the always-current rustdoc once published, see
[https://docs.rs/chasquimq](https://docs.rs/chasquimq). The notes
below mirror the in-source documentation but live alongside the
language shims so cross-references resolve.

## On this page

- [Configs](#configs) — `ProducerConfig`, `ConsumerConfig`, `PromoterConfig`, `SchedulerConfig`, `RetryConfig`, `ConnectionTuning`.
- [Job types](#job-types) — `Job<T>`, `JobId`, `JobRetryOverride`, `AddOptions`.
- [Producer](#producer) — every method.
- [Consumer](#consumer) — constructor and `run`.
- [Promoter](#promoter) — standalone delayed-job promoter.
- [Scheduler](#scheduler) — standalone repeatable-spec scheduler.
- [Repeatable jobs](#repeatable-jobs) — `RepeatPattern`, `MissedFiresPolicy`, `RepeatableSpec`, `RepeatableMeta`.
- [Backoff](#backoff) — `BackoffSpec`, `BackoffKind`.
- [Errors](#errors) — `Error`, `HandlerError`, `Result`.
- [Observability](#observability) — `MetricsSink` trait and every event type.
- [DLQ](#dlq) — `DlqEntry`, `DlqReason`.

## Configs

### `ProducerConfig`

```rust
pub struct ProducerConfig {
    pub queue_name: String,
    pub pool_size: usize,
    pub max_stream_len: u64,
    pub max_delay_secs: u64,
    pub connection: ConnectionTuning,
}
```

- `queue_name` — used to compute every `{chasqui:<queue>}:*` key. **Default `"default"`.**
- `pool_size` — `fred` connection pool size. **Default `8`.**
- `max_stream_len` — `XADD MAXLEN ~` cap on the main stream. **Default `1_000_000`.**
- `max_delay_secs` — reject `add_in` / `add_at` calls whose delay exceeds this. **Default `30 * 24 * 3600` (30 days).**
- `connection` — TCP keepalive, reconnect policy, and credential-rotation hook. See [`ConnectionTuning`](#connectiontuning).

### `ConsumerConfig`

```rust
pub struct ConsumerConfig {
    pub queue_name: String,
    pub group: String,
    pub consumer_id: String,
    pub batch: usize,
    pub block_ms: u64,
    pub claim_min_idle_ms: u64,
    pub concurrency: usize,
    pub max_attempts: u32,
    pub ack_batch: usize,
    pub ack_idle_ms: u64,
    pub shutdown_deadline_secs: u64,
    pub max_payload_bytes: usize,
    pub dlq_inflight: usize,
    pub dlq_max_stream_len: u64,
    pub retry: RetryConfig,
    pub retry_inflight: usize,
    pub delayed_enabled: bool,
    pub delayed_poll_interval_ms: u64,
    pub delayed_promote_batch: usize,
    pub delayed_max_stream_len: u64,
    pub delayed_lock_ttl_secs: u64,
    pub events_enabled: bool,
    pub events_max_stream_len: u64,
    pub run_scheduler: bool,
    pub scheduler: SchedulerConfig,
    pub store_results: bool,
    pub result_ttl_secs: u64,
    pub pause_poll_ms: u64,
    pub metrics: Arc<dyn MetricsSink>,
    pub connection: ConnectionTuning,
}
```

- `queue_name` — **default `"default"`**.
- `group` — XREADGROUP consumer-group name. **Default `"default"`.**
- `consumer_id` — XREADGROUP consumer name. **Default `format!("c-{}", Uuid::new_v4())`.**
- `batch` — `XREADGROUP COUNT`. **Default `64`.**
- `block_ms` — `XREADGROUP BLOCK`. **Default `5000`.**
- `claim_min_idle_ms` — recovery threshold for the `XREADGROUP ... CLAIM` safety net. **Default `30_000`.**
- `concurrency` — max in-flight handler invocations. **Default `100`.**
- `max_attempts` — total attempts per job (initial + retries). **Default `3`.**
- `ack_batch` — `XACK` flush size. **Default `256`.**
- `ack_idle_ms` — max idle time before flushing a partial ack batch. **Default `5`.**
- `shutdown_deadline_secs` — max time the engine waits for in-flight handlers on shutdown. **Default `30`.**
- `max_payload_bytes` — entries above this go straight to the DLQ as `OversizePayload`. **Default `1_048_576` (1 MiB).**
- `dlq_inflight` — bounded channel size for the DLQ relocator. **Default `32`.**
- `dlq_max_stream_len` — `MAXLEN ~` cap on the DLQ stream. **Default `100_000`.**
- `retry` — see [`RetryConfig`](#retryconfig).
- `retry_inflight` — bounded channel size for the retry relocator. **Default `64`.**
- `delayed_enabled` — auto-spawn the promoter task. **Default `true`.**
- `delayed_poll_interval_ms` — promoter tick interval. **Default `100`.**
- `delayed_promote_batch` — max promotions per tick. **Default `256`.**
- `delayed_max_stream_len` — `MAXLEN ~` on the promote-path `XADD`. **Default `1_000_000`.**
- `delayed_lock_ttl_secs` — promoter leader-lock TTL. **Default `5`.**
- `events_enabled` — write to `{chasqui:<queue>}:events`. **Default `true`.**
- `events_max_stream_len` — `MAXLEN ~` cap on the events stream. **Default `100_000`.**
- `run_scheduler` — auto-spawn an embedded scheduler. **Default `true`.**
- `scheduler` — embedded scheduler config when `run_scheduler == true`. The `queue_name` field is overridden at spawn time. **Default `SchedulerConfig::default()`.**
- `store_results` — opt-in result backend. **Default `false`.**
- `result_ttl_secs` — TTL for stored results when `store_results == true`. **Default `3600` (1h).**
- `pause_poll_ms` — how often a consumer re-checks the durable cross-process pause key (`{chasqui:<queue>}:paused`), and the worst-case latency for a cross-process pause/resume to be observed. Not on the per-job hot path: when not paused the reader pays one atomic load + one time comparison per batch, no Redis round trip. **Default `250`.**
- `metrics` — `Arc<dyn MetricsSink>` for the embedded promoter / scheduler / hot-path subsystems. **Default `crate::metrics::noop_sink()`.**
- `connection` — TCP keepalive, reconnect policy, and credential-rotation hook. The embedded promoter and scheduler inherit this through their parent `ConsumerConfig`. See [`ConnectionTuning`](#connectiontuning).

### `PromoterConfig`

```rust
pub struct PromoterConfig {
    pub queue_name: String,
    pub poll_interval_ms: u64,
    pub promote_batch: usize,
    pub max_stream_len: u64,
    pub lock_ttl_secs: u64,
    pub holder_id: String,
    pub events_enabled: bool,
    pub events_max_stream_len: u64,
    pub metrics: Arc<dyn MetricsSink>,
    pub connection: ConnectionTuning,
}
```

- `poll_interval_ms` — tick frequency. **Default `100`.**
- `promote_batch` — max promotions per tick. **Default `256`.**
- `max_stream_len` — `MAXLEN ~` on the promote-path `XADD`. **Default `1_000_000`.**
- `lock_ttl_secs` — leader-lock TTL. **Default `5`.**
- `holder_id` — unique lock holder id. **Default `format!("p-{}", Uuid::new_v4())`.**
- `events_enabled` — write `waiting` events for promoted jobs. **Default `true`.**
- `events_max_stream_len` — **default `100_000`**.
- `metrics` — **default `noop_sink()`**.
- `connection` — TCP keepalive, reconnect policy, credential-rotation hook. See [`ConnectionTuning`](#connectiontuning).

### `SchedulerConfig`

```rust
pub struct SchedulerConfig {
    pub queue_name: String,
    pub tick_interval_ms: u64,
    pub batch: usize,
    pub max_stream_len: u64,
    pub lock_ttl_secs: u64,
    pub holder_id: String,
    pub metrics: Arc<dyn MetricsSink>,
    pub connection: ConnectionTuning,
}
```

- `tick_interval_ms` — tick frequency. **Default `1000`.** Lower bound on per-spec fire jitter is roughly this interval.
- `batch` — max specs hydrated per tick. **Default `256`.**
- `max_stream_len` — `MAXLEN ~` on the immediate-dispatch path. **Default `1_000_000`.**
- `lock_ttl_secs` — leader-lock TTL on `{chasqui:<queue>}:scheduler:lock`. **Default `5`.**
- `holder_id` — **default `format!("s-{}", Uuid::new_v4())`**.
- `metrics` — **default `noop_sink()`**.
- `connection` — TCP keepalive, reconnect policy, credential-rotation hook. See [`ConnectionTuning`](#connectiontuning).

### `RetryConfig`

```rust
pub struct RetryConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub multiplier: f64,
    pub jitter_ms: u64,
}
```

The queue-wide retry curve. Per-job overrides via
[`JobRetryOverride`](#jobretryoverride) take precedence.

- `initial_backoff_ms` — base delay for the first retry. **Default `100`.**
- `max_backoff_ms` — cap on the computed backoff per attempt. **Default `30_000`.**
- `multiplier` — exponential growth factor. **Default `2.0`.**
- `jitter_ms` — symmetric ±jitter added per retry. **Default `100`.**

### `ConnectionTuning`

```rust
pub struct ConnectionTuning {
    pub tcp_keepalive_secs: u64,
    pub tcp_keepalive_interval_secs: u64,
    pub reconnect_max_attempts: u32,
    pub reconnect_min_delay_ms: u32,
    pub reconnect_max_delay_ms: u32,
    pub reconnect_backoff_base: u32,
    pub reconnect_jitter_ms: u32,
    pub connection_timeout_ms: u64,
    pub credential_provider: Option<Arc<dyn fred::types::config::CredentialProvider>>,
}
```

Fred-side connection knobs threaded through every `*Config`. The
embedded promoter and scheduler that `Consumer::run` spawns inherit
this from the parent `ConsumerConfig`, so a single override at the
consumer level reaches every subsystem.

- `tcp_keepalive_secs` — idle interval before TCP keepalive probes start. **Default `60`.** Set to `0` to disable. Matters for environments that drop idle TCP silently — most notably AWS NAT Gateways (350s idle cutoff).
- `tcp_keepalive_interval_secs` — spacing between probes after the first. **Default `10`.**
- `reconnect_max_attempts` — max reconnect attempts on transient failure. **Default `0`** (unbounded with exponential backoff per fred convention).
- `reconnect_min_delay_ms` — first reconnect delay. **Default `100`.**
- `reconnect_max_delay_ms` — cap on the exponential reconnect backoff. **Default `30_000`.**
- `reconnect_backoff_base` — exponential growth factor. **Default `2`.**
- `reconnect_jitter_ms` — ±jitter on each reconnect; decorrelates fleets. **Default `50`.**
- `connection_timeout_ms` — per-attempt deadline for TCP+TLS+AUTH handshake. **Default `10_000`.**
- `credential_provider` — fred-side hook called before every `AUTH` / `HELLO`. **Default `None`.** Use for ElastiCache IAM auth (15-min token rotation): implement `fred::types::config::CredentialProvider`, wrap in `Arc::new`, set this field. Combined with the unbounded `reconnect_max_attempts` default and `reconnect_on_auth_error: true` (built into the engine), the pool stays usable indefinitely across token rotations. The Node and Python shims do not yet expose a callback surface for this.

`reconnect_on_auth_error: true` is hard-coded into the engine's
`ConnectionConfig` builder (not user-tunable) so IAM-auth-rotated
tokens have a path forward.

## Job types

### `Job<T>`

```rust
pub struct Job<T> {
    pub id: JobId,
    pub payload: T,
    pub created_at_ms: u64,
    pub attempt: u32,
    pub retry: Option<JobRetryOverride>,
    pub name: String,
}

impl<T> Job<T> {
    pub fn new(payload: T) -> Self;
    pub fn with_id(id: JobId, payload: T) -> Self;
    pub fn with_retry(self, retry: JobRetryOverride) -> Self;
    pub fn with_name(self, name: impl Into<String>) -> Self;
}
```

The msgpack-encoded envelope on the wire. `name` is `#[serde(skip)]`
on the encode path — the dispatch name travels as a separate `n`
field on the stream entry, not inside the msgpack body — and the
read path populates `Job::name` from that field. See
[wire format](/reference/wire-format/) for the byte layout.

### `JobId`

```rust
pub type JobId = String;
```

A stable string id for a single job. Defaults to a fresh ULID
when the producer mints one.

### `JobRetryOverride`

```rust
pub struct JobRetryOverride {
    pub max_attempts: Option<u32>,
    pub backoff: Option<BackoffSpec>,
}
```

Per-job override of the queue-wide [`ConsumerConfig::max_attempts`]
and [`ConsumerConfig::retry`]. `None` means "fall back to the
queue-wide default."

### `AddOptions`

```rust
pub struct AddOptions {
    pub id: Option<JobId>,
    pub retry: Option<JobRetryOverride>,
    pub name: String,
}

impl AddOptions {
    pub fn new() -> Self;
    pub fn with_id(self, id: JobId) -> Self;
    pub fn with_retry(self, retry: JobRetryOverride) -> Self;
    pub fn with_name(self, name: impl Into<String>) -> Self;
}
```

Per-job options for the `*_with_options` family of producer methods.
`name` is capped at 256 bytes; oversize names are rejected with
`Error::Config` at the producer boundary.

## Producer

```rust
pub struct Producer<T>;
```

Generic over the payload type `T: Serialize + DeserializeOwned`.
Cheap to clone — the underlying `fred::Pool` is reference-counted.

### `Producer::connect`

```rust
pub async fn connect(redis_url: &str, config: ProducerConfig) -> Result<Self>
```

Open the connection pool and validate the queue config.

### Add (immediate)

```rust
pub async fn add(&self, payload: T) -> Result<JobId>;
pub async fn add_with_id(&self, id: JobId, payload: T) -> Result<JobId>;
pub async fn add_with_options(&self, payload: T, opts: AddOptions) -> Result<JobId>;
pub async fn add_bulk(&self, payloads: Vec<T>) -> Result<Vec<JobId>>;
pub async fn add_bulk_with_options(&self, payloads: Vec<T>, opts: AddOptions) -> Result<Vec<JobId>>;
pub async fn add_bulk_named(&self, items: Vec<(String, T)>) -> Result<Vec<JobId>>;
```

`add` is one `XADD`. `add_bulk*` pipelines one `XADD` per payload
on a single connection — the one-shot bulk-produce ratio is
measured against this path.

`add_bulk_with_options` shares one `AddOptions` instance across
all entries; setting `opts.id` with multiple payloads is rejected
with `Error::Config`. Use `add_in_bulk_with_ids` (delayed) or
loop `add_with_id` (immediate) when you need per-entry ids.

`add_bulk_named` accepts per-entry `(name, payload)` so each job
can carry its own dispatch name. Names are validated against the
256-byte cap before any `XADD`.

### Add (delayed)

```rust
pub async fn add_in(&self, delay: Duration, payload: T) -> Result<JobId>;
pub async fn add_at(&self, run_at: SystemTime, payload: T) -> Result<JobId>;
pub async fn add_in_bulk(&self, delay: Duration, payloads: Vec<T>) -> Result<Vec<JobId>>;
pub async fn add_in_with_options(&self, delay: Duration, payload: T, opts: AddOptions) -> Result<JobId>;
pub async fn add_at_with_options(&self, run_at: SystemTime, payload: T, opts: AddOptions) -> Result<JobId>;
pub async fn add_in_with_id(&self, id: JobId, delay: Duration, payload: T) -> Result<JobId>;
pub async fn add_at_with_id(&self, id: JobId, run_at: SystemTime, payload: T) -> Result<JobId>;
pub async fn add_in_bulk_with_ids(&self, delay: Duration, items: Vec<(JobId, T)>) -> Result<Vec<JobId>>;
```

`add_in` / `add_at` `ZADD` into the delayed ZSET; the promoter
moves due jobs into the main stream.

The `*_with_id` and `*_with_options` (with `opts.id` set) variants
route through the idempotent path: a `SET NX EX` Lua marker on
`{chasqui:<queue>}:dlid:<jobId>` gates the `ZADD`, so a producer
retry of the same logical schedule is a no-op rather than a
duplicate. The marker TTL is `seconds_until_run + 3600`.

### `Producer::cancel_delayed`

```rust
pub async fn cancel_delayed(&self, id: &JobId) -> Result<bool>;
pub async fn cancel_delayed_bulk(&self, ids: &[JobId]) -> Result<Vec<bool>>;
```

Atomically remove a delayed entry by id. Returns `true` if the
ZSET still held the entry. The (delivered=true, cancel=true)
outcome is impossible — cancel and promote serialize at Redis
under the same hash tag.

### `Producer::peek_dlq` / `Producer::replay_dlq`

```rust
pub async fn peek_dlq(&self, limit: usize) -> Result<Vec<DlqEntry>>;
pub async fn replay_dlq(&self, limit: usize) -> Result<usize>
where
    T: Serialize + DeserializeOwned;
```

`peek_dlq` returns up to `limit` entries oldest-first without
removing them. `replay_dlq` atomically moves up to `limit`
entries back to the main stream with the attempt counter reset
to zero. See [`DlqEntry`](#dlqentry).

### `Producer::upsert_repeatable` / `list_repeatable` / `remove_repeatable`

```rust
pub async fn upsert_repeatable(&self, spec: RepeatableSpec<T>) -> Result<String>
where
    T: Serialize;
pub async fn list_repeatable(&self, limit: usize) -> Result<Vec<RepeatableMeta>>;
pub async fn remove_repeatable(&self, spec_key: &str) -> Result<bool>;
```

Repeatable / cron specs. `upsert_repeatable` returns the resolved
spec key (auto-derived from `<job_name>::<pattern_signature>` when
`spec.key` is empty). Re-upserting with the same key overwrites
the spec and re-anchors the next fire time. `list_repeatable`
omits payloads to keep listing thousands of specs cheap; see
[`RepeatableMeta`](#repeatablemeta).

### `Producer::get_result` / `get_result_bulk`

```rust
pub async fn get_result(&self, id: &JobId) -> Result<Option<Bytes>>;
pub async fn get_result_bulk(&self, ids: &[JobId]) -> Result<Vec<Option<Bytes>>>;
```

Read the stored handler return value for `id` (one `GET`). Returns
`None` for three indistinguishable cases: not yet completed, key
expired, or no result was written. The shims call this from
`Queue.getJobResult` / `Queue.get_job_result` and from
`Job.waitForResult` / `Job.wait_for_result`.

### `Producer::pause` / `resume` / `is_paused`

```rust
pub async fn pause(&self) -> Result<()>;
pub async fn resume(&self) -> Result<()>;
pub async fn is_paused(&self) -> Result<bool>;
```

Durable, cross-process pause. `pause` `SET`s `{chasqui:<queue>}:paused`
(no TTL); `resume` `DEL`s it. Every consumer of the queue parks its
reader at the next batch boundary while paused — in-flight jobs drain,
producers keep enqueueing. Survives consumer restarts. Idempotent.
This is the engine surface behind `Queue.pause()` and
`chasqui pause <queue>`. For process-local pause of a single consumer
without touching Redis, use [`Consumer::pause_control`](#consumer).
See the [Pause and resume concept](/concepts/pause-and-resume/).

### `Producer::shutdown`

```rust
pub async fn shutdown(&self) -> Result<()>;
```

Calls fred's `Pool::quit` to disconnect cleanly. Does not wait for
in-flight commands because there are none — every `Producer::add`
already awaits the Redis `XADD` response before resolving, so by the
time `add` returns the bytes are committed (not just queued). This
matters for hosts that may be frozen the moment your handler
returns (AWS Lambda, Cloud Run, fly.io machines): you can return
immediately after `add` resolves; calling `shutdown` is optional
polish for long-lived servers and integration tests that want to
free file descriptors deterministically.

### Key accessors

```rust
pub fn producer_id(&self) -> &str;
pub fn stream_key(&self) -> &str;
pub fn delayed_key(&self) -> &str;
pub fn dlq_key(&self) -> &str;
```

For introspection / direct Redis CLI work.

## Consumer

```rust
pub struct Consumer<T>;

impl<T: Serialize + DeserializeOwned + Clone + Send + 'static> Consumer<T> {
    pub fn new(redis_url: impl Into<String>, cfg: ConsumerConfig) -> Self;
    pub fn with_pause_control(
        redis_url: impl Into<String>,
        cfg: ConsumerConfig,
        pause: Arc<PauseControl>,
    ) -> Self;
    pub fn pause_control(&self) -> Arc<PauseControl>;
    pub async fn run<H, Fut>(self, handler: H, shutdown: CancellationToken) -> Result<()>
    where
        H: Fn(Job<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Bytes, HandlerError>> + Send + 'static;
}

pub struct PauseControl;

impl PauseControl {
    pub fn new() -> Self;
    pub fn pause(&self);
    pub fn resume(&self);
    pub fn is_paused(&self) -> bool;
}
```

The consumer owns the reader, worker pool, ack flusher, retry
relocator, and DLQ relocator. When `cfg.delayed_enabled` is `true`
it auto-spawns an embedded [`Promoter`](#promoter); when
`cfg.run_scheduler` is `true` it auto-spawns a
[`Scheduler<T>`](#scheduler).

The `handler` future returns `Result<Bytes, HandlerError>`. An
`Ok(Bytes::new())` is the "ack-only" path — empty bytes resolve
to no result-key write even when `cfg.store_results = true`. Any
non-empty `Bytes` is forwarded to the engine's result-writer
gated on the same `XACKDEL` round trip.

`shutdown` is a `tokio_util::sync::CancellationToken`; firing it
trips the engine's drain path. `run` resolves once the engine
finishes draining (up to `cfg.shutdown_deadline_secs`).

`pause_control()` returns a shared `Arc<PauseControl>` (same sharing
model as the shutdown token — clone it before `run()` or hand it to
another task). `PauseControl::pause()` parks the reader at its next
batch boundary; in-flight handlers run to completion, producers and
the embedded promoter/scheduler are unaffected. `resume()` wakes the
parked reader immediately (edge-triggered, backed by a `tokio::sync::
watch` channel). `pause()`/`resume()` are idempotent. This is
process-local and never touches Redis — distinct from the durable
`{chasqui:<queue>}:paused` key (`Producer::pause`), which the reader
*also* honours independently. `with_pause_control` lets a caller own
the `Arc` (the FFI bindings construct it at their wrapper's `new()`
so `pause()` is callable before `run()`); `new` creates one
internally. See the [Pause and resume concept](/concepts/pause-and-resume/).

## Promoter

```rust
pub struct Promoter;

impl Promoter {
    pub fn new(redis_url: impl Into<String>, cfg: PromoterConfig) -> Self;
    pub async fn run(self, shutdown: CancellationToken) -> Result<()>;
}
```

Standalone delayed-job promoter. Polls
`{chasqui:<queue>}:delayed` every `poll_interval_ms` and atomically
moves due entries into the main stream via a single Lua round trip.
Leader-elected via `SET NX EX` on `{chasqui:<queue>}:promoter:lock`,
so multiple replicas can hot-spare without double-promotion.

Use this when you run promotion in a separate process. The
[`Consumer`](#consumer) auto-embeds a promoter when
`delayed_enabled = true`.

## Scheduler

```rust
pub struct Scheduler<T>;

impl<T: Serialize + DeserializeOwned + Send + 'static> Scheduler<T> {
    pub fn new(redis_url: impl Into<String>, cfg: SchedulerConfig) -> Self;
    pub async fn run(self, shutdown: CancellationToken) -> Result<()>;
}
```

Standalone repeatable-spec scheduler. Tails
`{chasqui:<queue>}:repeat` every `tick_interval_ms`, materializes
one fire of each due spec, schedules the resulting job
(immediately or to the delayed ZSET), and updates the spec's
next-fire score in the same Lua round trip. Leader-elected via
`SET NX EX` on `{chasqui:<queue>}:scheduler:lock`.

Generic over `T` so the scheduler can decode the per-spec stored
payload type. The [`Consumer`](#consumer) auto-embeds a scheduler
when `run_scheduler = true`.

## Repeatable jobs

### `RepeatPattern`

```rust
pub enum RepeatPattern {
    Cron { expression: String, tz: Option<String> },
    Every { interval_ms: u64 },
}

impl RepeatPattern {
    pub fn signature(&self) -> String;
}
```

`signature()` is the stable string used to derive a default
spec key when the user doesn't supply one
(`<job_name>::<pattern_signature>`).

The `tz` field on `Cron` accepts:

- `None` or `"UTC"` / `"Z"` → UTC.
- Fixed offsets `"+HH:MM"` / `"-HH:MM"` / `"+HHMM"` / `"-HHMM"`.
- Any IANA name (`"America/New_York"`, `"Europe/London"`, `"US/Eastern"`).

IANA names are DST-aware: `0 2 * * *` in `America/New_York` fires
at 02:00 local on both sides of spring-forward / fall-back. On
fall-back ambiguous local times the **earlier** UTC instant is
selected.

### `MissedFiresPolicy`

```rust
pub enum MissedFiresPolicy {
    Skip,                              // default
    FireOnce,
    FireAll { max_catchup: u32 },
}
```

Catch-up policy when the scheduler's `next_fire_ms` is more than
one cadence in the past. **Default `Skip`** — drop missed windows;
no thundering herd on restart. `FireAll { max_catchup }` is bounded
by `max_catchup` (`>= 1`) to avoid pathological replays.

### `RepeatableSpec<T>`

```rust
pub struct RepeatableSpec<T> {
    pub key: String,
    pub job_name: String,
    pub pattern: RepeatPattern,
    pub payload: T,
    pub limit: Option<u64>,
    pub start_after_ms: Option<u64>,
    pub end_before_ms: Option<u64>,
    pub missed_fires: MissedFiresPolicy,
}

impl<T> RepeatableSpec<T> {
    pub fn new(job_name: impl Into<String>, pattern: RepeatPattern, payload: T) -> Self;
    pub fn with_key(self, key: impl Into<String>) -> Self;
    pub fn with_limit(self, limit: u64) -> Self;
    pub fn with_start_after_ms(self, ms: u64) -> Self;
    pub fn with_end_before_ms(self, ms: u64) -> Self;
    pub fn with_missed_fires(self, policy: MissedFiresPolicy) -> Self;
    pub fn resolved_key(&self) -> String;
}
```

A recurring job spec. Use the chainable setters for forward
compatibility — adding new fields stays source-compatible with
callers that build via `new(...).with_*(...)`.

### `RepeatableMeta`

```rust
pub struct RepeatableMeta {
    pub key: String,
    pub job_name: String,
    pub pattern: RepeatPattern,
    pub next_fire_ms: u64,
    pub limit: Option<u64>,
    pub start_after_ms: Option<u64>,
    pub end_before_ms: Option<u64>,
    pub missed_fires: MissedFiresPolicy,
}
```

Lightweight projection returned by
[`Producer::list_repeatable`](#producer-upsert_repeatable--list_repeatable--remove_repeatable).
Omits the payload to keep listing thousands of specs cheap.

## Backoff

### `BackoffSpec`

```rust
pub struct BackoffSpec {
    pub kind: BackoffKind,
    pub delay_ms: u64,
    pub max_delay_ms: Option<u64>,
    pub multiplier: Option<f64>,
    pub jitter_ms: Option<u64>,
}
```

Per-job backoff spec. `max_delay_ms`, `multiplier`, `jitter_ms`
default to `RetryConfig::max_backoff_ms`, `RetryConfig::multiplier`,
`RetryConfig::jitter_ms` when `None`.

### `BackoffKind`

```rust
#[non_exhaustive]
pub enum BackoffKind {
    Fixed,
    Exponential,
    Unknown,        // forward-compat sink, decodes to Exponential math
}
```

Unit-only; serializes as the lowercase string `"fixed"` /
`"exponential"`. Unknown variants from a future SDK decode as
`Unknown` and are routed through the `Exponential` math at the
consumer.

## Errors

### `Error`

```rust
#[derive(thiserror::Error, Debug)]
pub enum Error {
    Redis(fred::error::Error),    // CMQ-001
    Encode(rmp_serde::encode::Error), // CMQ-002
    Decode(rmp_serde::decode::Error), // CMQ-003
    Config(String),                // CMQ-004
    Shutdown,                      // CMQ-005
}
```

The crate-wide error type. Each variant has a stable
[`CMQ-*` code](/reference/error-codes/) for cross-language
diagnosis.

### `HandlerError`

```rust
pub struct HandlerError { /* opaque */ }

impl HandlerError {
    pub fn new<E>(err: E) -> Self
    where E: std::error::Error + Send + Sync + 'static;
    pub fn unrecoverable<E>(err: E) -> Self
    where E: std::error::Error + Send + Sync + 'static;
    pub fn is_unrecoverable(&self) -> bool;
    pub fn source_err(&self) -> &(dyn std::error::Error + Send + Sync);
    pub fn into_source(self) -> Box<dyn std::error::Error + Send + Sync>;
}
```

Returned from a handler to signal failure. `HandlerError::new` is
recoverable — the engine retries with backoff up to
`max_attempts`. `HandlerError::unrecoverable` short-circuits the
retry budget and routes the job straight to the DLQ with
[`DlqReason::Unrecoverable`](#dlqreason).

The Node and Python shims map a thrown `UnrecoverableError`
(by `error.name === 'UnrecoverableError'` / by MRO-aware
`issubclass`) onto `HandlerError::unrecoverable` automatically.

### `Result`

```rust
pub type Result<T> = std::result::Result<T, Error>;
```

## Observability

### `MetricsSink` trait

```rust
pub trait MetricsSink: Send + Sync + 'static {
    fn promoter_tick(&self, _tick: PromoterTick) {}
    fn promoter_lock_outcome(&self, _outcome: LockOutcome) {}
    fn reader_batch(&self, _batch: ReaderBatch) {}
    fn job_outcome(&self, _outcome: JobOutcome) {}
    fn retry_scheduled(&self, _retry: RetryScheduled) {}
    fn dlq_routed(&self, _dlq: DlqRouted) {}
}
```

Receiver of engine-internal observability events. **All methods
have default no-op bodies**, so adding new events does not break
downstream implementations.

:::caution[Hot-path constraints]
Implementations must be cheap and non-blocking. Events fire on
hot loops — promoter ticks, every handler invocation, every
retry/DLQ transition — and any latency added here directly raises
end-to-end scheduling lag and lowers throughput. Safe: atomic
counters, channel pushes, histogram observations. Unsafe:
synchronous network I/O, `block_on`, anything that allocates per
event in the millions.
:::

### `NoopSink`

```rust
pub struct NoopSink;
impl MetricsSink for NoopSink {}

pub fn noop_sink() -> Arc<dyn MetricsSink>;
```

Default sink — drops every event. The configs default to this;
swap in your own implementation to bridge to Prometheus,
OpenTelemetry, etc. The `chasquimq-metrics` crate ships an
opt-in adapter to `metrics-rs` / Prometheus.

### Event types

#### `PromoterTick`

```rust
pub struct PromoterTick {
    pub promoted: u64,
    pub depth: u64,
    pub oldest_pending_lag_ms: u64,
}
```

Emitted on every promoter tick. `oldest_pending_lag_ms` is `0` in
the steady state — positive only when a backlog forms and an
overdue entry is still in the ZSET.

#### `LockOutcome`

```rust
pub enum LockOutcome { Acquired, Held }
```

Emitted on transitions only — a non-leader replica does NOT spam
this on every poll.

#### `ReaderBatch`

```rust
pub struct ReaderBatch {
    pub size: u64,        // raw stream-entry count
    pub reclaimed: u64,   // subset whose delivery_count > 1
}
```

Shape of the most recent non-empty `XREADGROUP` response. Empty
responses are not emitted.

#### `JobOutcome` / `JobOutcomeKind`

```rust
pub struct JobOutcome {
    pub kind: JobOutcomeKind,
    pub attempt: u32,
    pub handler_duration_us: u64,
    pub name: String,
}

pub enum JobOutcomeKind { Ok, Err, Panic }
```

Per-handler-invocation outcome. `name` is the dispatch name from
the source stream entry's `n` field — adapters typically render
this as a `name="..."` Prometheus label so handler duration can be
sliced by job kind.

`handler_duration_us` is wall-clock microseconds (not ms — most
queue workloads complete sub-millisecond, and recording in ms
would pile every call at zero).

#### `RetryScheduled`

```rust
pub struct RetryScheduled {
    pub attempt: u32,
    pub backoff_ms: u64,
    pub name: String,
}
```

Emitted only when the retry relocator atomically removed the
in-flight stream entry and re-added it to the delayed ZSET. When
the script returns `0` (CLAIM race lost), no event fires.

#### `DlqRouted`

```rust
pub struct DlqRouted {
    pub reason: DlqReason,
    pub attempt: u32,
    pub name: String,
}
```

Emitted after the DLQ relocator atomically moved an entry to the
DLQ stream. See [`DlqReason`](#dlqreason).

## DLQ

### `DlqEntry`

```rust
pub struct DlqEntry {
    pub dlq_id: String,
    pub source_id: String,
    pub reason: String,
    pub detail: Option<String>,
    pub payload: Bytes,
    pub name: String,
}
```

Returned by [`Producer::peek_dlq`](#producer-peek_dlq--producer-replay_dlq).
`name` is preserved verbatim from the source entry's `n` field
and re-emitted by `replay_dlq`.

### `DlqReason`

```rust
#[derive(Clone, Copy)]
pub enum DlqReason {
    RetriesExhausted,                     // CMQ-020
    DecodeFailed,                         // CMQ-021
    Malformed { reason: &'static str },   // CMQ-022
    OversizePayload,                      // CMQ-023
    Unrecoverable,                        // CMQ-024
}

impl DlqReason {
    pub fn as_str(&self) -> &'static str;
    pub fn detail(&self) -> Option<&'static str>;
}
```

Carries the reason an entry landed in the DLQ. `as_str()` returns
a snake_case key (`"retries_exhausted"`, etc.) for use as a
Prometheus label. See [error codes](/reference/error-codes/) for
the recovery actions per reason.
