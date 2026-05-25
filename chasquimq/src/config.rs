#[derive(Clone)]
pub struct ConnectionTuning {
    pub tcp_keepalive_secs: u64,
    pub tcp_keepalive_interval_secs: u64,
    pub reconnect_max_attempts: u32,
    pub reconnect_min_delay_ms: u32,
    pub reconnect_max_delay_ms: u32,
    pub reconnect_backoff_base: u32,
    pub reconnect_jitter_ms: u32,
    pub connection_timeout_ms: u64,
    pub credential_provider: Option<std::sync::Arc<dyn fred::types::config::CredentialProvider>>,
}

impl std::fmt::Debug for ConnectionTuning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionTuning")
            .field("tcp_keepalive_secs", &self.tcp_keepalive_secs)
            .field(
                "tcp_keepalive_interval_secs",
                &self.tcp_keepalive_interval_secs,
            )
            .field("reconnect_max_attempts", &self.reconnect_max_attempts)
            .field("reconnect_min_delay_ms", &self.reconnect_min_delay_ms)
            .field("reconnect_max_delay_ms", &self.reconnect_max_delay_ms)
            .field("reconnect_backoff_base", &self.reconnect_backoff_base)
            .field("reconnect_jitter_ms", &self.reconnect_jitter_ms)
            .field("connection_timeout_ms", &self.connection_timeout_ms)
            .field(
                "credential_provider",
                &self
                    .credential_provider
                    .as_ref()
                    .map(|_| "<dyn CredentialProvider>"),
            )
            .finish()
    }
}

impl Default for ConnectionTuning {
    fn default() -> Self {
        Self {
            tcp_keepalive_secs: 60,
            tcp_keepalive_interval_secs: 10,
            reconnect_max_attempts: 0,
            reconnect_min_delay_ms: 100,
            reconnect_max_delay_ms: 30_000,
            reconnect_backoff_base: 2,
            reconnect_jitter_ms: 50,
            connection_timeout_ms: 10_000,
            credential_provider: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub queue_name: String,
    pub pool_size: usize,
    pub max_stream_len: u64,
    pub max_delay_secs: u64,
    /// Producer-side ingress cap on the encoded (MessagePack) byte length
    /// of a single job payload. Any `add*` / `upsert_repeatable` call whose
    /// encoded payload exceeds this is rejected with [`crate::Error::Config`]
    /// *before* anything is written to Redis. Mirrors
    /// [`ConsumerConfig::max_payload_bytes`] (the consumer-side egress cap
    /// that routes oversize-on-read to the DLQ) so an operator setting both
    /// to the same value gets symmetric produce/consume semantics. Default
    /// `1_048_576` (1 MiB), identical to the consumer default.
    pub max_payload_bytes: usize,
    pub connection: ConnectionTuning,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            pool_size: 8,
            max_stream_len: 1_000_000,
            max_delay_secs: 30 * 24 * 3600,
            max_payload_bytes: 1_048_576,
            connection: ConnectionTuning::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub multiplier: f64,
    pub jitter_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            multiplier: 2.0,
            jitter_ms: 100,
        }
    }
}

#[derive(Clone)]
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
    /// Whether the engine writes to the per-queue events stream
    /// (`{chasqui:<queue>}:events`). Sibling to `MetricsSink`: events fire on
    /// the same hot-path occurrences but cross-process subscribers can
    /// observe them with a plain `XREAD`. Default `true` so the BullMQ
    /// `QueueEvents` class works against a default-config consumer; set
    /// `false` to skip every events-stream `XADD`.
    pub events_enabled: bool,
    /// `MAXLEN ~` cap applied to the events stream's `XADD`. Trim is
    /// approximate (the `~`) so Redis does it cheaply; expect actual length
    /// to oscillate up to a few hundred entries above the cap.
    pub events_max_stream_len: u64,
    /// Whether `Consumer::run` auto-spawns an embedded [`crate::Scheduler`]
    /// task alongside the reader / promoter / relocators. Mirrors
    /// `delayed_enabled` for the [`crate::Promoter`]: a worker process that
    /// loads the consumer also gets repeatable / cron firing for free,
    /// without the user managing a second task. Default `true`. Set to
    /// `false` for deployments that run a separate scheduler process —
    /// the standalone [`crate::Scheduler`] API is unaffected. Multiple
    /// in-process schedulers cooperate via the engine's existing leader
    /// election (`SET NX EX` on `{chasqui:<queue>}:scheduler:lock`).
    pub run_scheduler: bool,
    /// Configuration for the embedded scheduler when `run_scheduler` is
    /// `true`. The `queue_name` field is overridden from
    /// `ConsumerConfig::queue_name` at spawn time; everything else
    /// (`tick_interval_ms`, `batch`, `max_stream_len`, `lock_ttl_secs`,
    /// `holder_id`, `metrics`) is forwarded as-is. Defaults to
    /// [`SchedulerConfig::default`].
    pub scheduler: SchedulerConfig,
    /// Opt-in result backend. When `true`, the engine writes each handler's
    /// non-empty `Bytes` return value to a per-job result key
    /// (`{chasqui:<queue>}:result:<job_id>`) with TTL `result_ttl_secs`,
    /// readable via [`crate::Producer::get_result`]. The write is gated on
    /// the same XACKDEL inside a single Lua round trip — no orphan results
    /// when CLAIM removed the entry first. Default `false` so the BullMQ-
    /// style "discard handler return" default holds and the hot path stays
    /// a plain batched XACKDEL for users who never call `get_result`.
    pub store_results: bool,
    /// TTL applied to result keys when `store_results = true`. Default
    /// `3600` (one hour). `Producer::get_result` returns `None` for
    /// expired keys (indistinguishable from "never existed" / "not yet
    /// completed"), so set this to comfortably exceed any
    /// `wait_for_result` polling timeout your shim uses.
    pub result_ttl_secs: u64,
    /// Max `JobOk` entries batched into a single pipelined `EVALSHA`
    /// flush by the opt-in result-writer (mirror of `ack_batch` for the
    /// store-results path). Larger batches amortize the Redis round trip
    /// at the cost of result-key visibility latency. Only consulted when
    /// `store_results = true`. Default `64`.
    pub result_batch: usize,
    /// Idle deadline in ms before a partial result-writer batch flushes
    /// even when it has not reached `result_batch` (mirror of
    /// `ack_idle_ms`). Caps the worst-case wait for a single trailing
    /// `JobOk` to land in Redis under low concurrency. Default `5`.
    pub result_idle_ms: u64,
    /// How often (ms) a paused reader re-checks the cross-process pause
    /// key (`{chasqui:<queue>}:paused`) set by `chasqui pause` /
    /// `Queue.pause()`. Also bounds the worst-case latency for a
    /// cross-process pause/resume to be observed by an actively-draining
    /// consumer. The in-process pause path (`Worker.pause()`) is
    /// edge-triggered and observes instantly regardless of this value.
    /// Not consulted on the per-job hot path: when not paused the reader
    /// pays one `Instant` comparison per batch, no Redis round trip.
    /// Default `250`.
    pub pause_poll_ms: u64,
    /// `MAXLEN ~` cap applied to each per-job log stream by
    /// [`crate::JobHandle::log`]. Approximate trim (the `~`) so Redis
    /// can do it cheaply; expect actual length to oscillate a few
    /// entries above the cap. **Must be ≥ 16** — anything smaller and
    /// the `MAXLEN ~` rounding can leave the stream effectively empty
    /// (rejected at `Consumer::run` with `Error::Config`). Default
    /// `1000`.
    pub log_max_stream_len: u64,
    /// Per-line byte cap applied by [`crate::JobHandle::log`]. Lines
    /// exceeding this are truncated on a UTF-8 char boundary with a
    /// `"[…truncated]"` marker appended; the truncate fires a single
    /// warn-once per handle. Default `4096`.
    pub log_max_line_bytes: usize,
    /// Toggle for the per-progress `e=progress` event emission.
    /// `update_progress` always SETs the persisted progress key; this
    /// only gates the events-stream `XADD` so a high-rate handler can
    /// opt out of the events flood while keeping persisted progress.
    /// Default `true`.
    pub events_progress_enabled: bool,
    /// Whether `Consumer::run` auto-spawns an embedded
    /// [`crate::StalledDetector`] alongside the reader / promoter /
    /// scheduler. Mirrors `delayed_enabled` for the promoter: a worker
    /// process that loads the consumer also gets stalled-job detection
    /// for free, without the operator managing a separate detector
    /// process. Default `true`. Set to `false` for deployments running a
    /// separate detector process or for pure-consumer benchmarks. The
    /// standalone [`crate::StalledDetector`] API is unaffected.
    pub stalled_detector_enabled: bool,
    /// Configuration for the embedded stalled-job detector when
    /// `stalled_detector_enabled` is `true`. The `queue_name`,
    /// `tick_interval_ms`, and `idle_threshold_ms` fields are overridden
    /// from the parent `ConsumerConfig` at spawn time (`queue_name` from
    /// `ConsumerConfig::queue_name`; both timing fields from
    /// `claim_min_idle_ms`), so the counter-semantic invariant
    /// `tick_interval_ms == idle_threshold_ms == claim_min_idle_ms`
    /// holds without the operator having to mirror three fields. The
    /// `metrics` and `connection` fields are also forwarded from the
    /// parent at spawn time. The other fields (`max_stalled_attempts`,
    /// `scan_batch`, `lock_ttl_secs`, `holder_id`) are honored as
    /// configured. Defaults to [`StalledDetectorConfig::default`].
    pub stalled_detector: StalledDetectorConfig,
    /// Forwarded to the inline promoter the consumer spawns when
    /// `delayed_enabled` is true. Defaults to [`crate::metrics::NoopSink`].
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
    pub connection: ConnectionTuning,
}

impl std::fmt::Debug for ConsumerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerConfig")
            .field("queue_name", &self.queue_name)
            .field("group", &self.group)
            .field("consumer_id", &self.consumer_id)
            .field("batch", &self.batch)
            .field("block_ms", &self.block_ms)
            .field("claim_min_idle_ms", &self.claim_min_idle_ms)
            .field("concurrency", &self.concurrency)
            .field("max_attempts", &self.max_attempts)
            .field("ack_batch", &self.ack_batch)
            .field("ack_idle_ms", &self.ack_idle_ms)
            .field("shutdown_deadline_secs", &self.shutdown_deadline_secs)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("dlq_inflight", &self.dlq_inflight)
            .field("dlq_max_stream_len", &self.dlq_max_stream_len)
            .field("retry", &self.retry)
            .field("retry_inflight", &self.retry_inflight)
            .field("delayed_enabled", &self.delayed_enabled)
            .field("delayed_poll_interval_ms", &self.delayed_poll_interval_ms)
            .field("delayed_promote_batch", &self.delayed_promote_batch)
            .field("delayed_max_stream_len", &self.delayed_max_stream_len)
            .field("delayed_lock_ttl_secs", &self.delayed_lock_ttl_secs)
            .field("events_enabled", &self.events_enabled)
            .field("events_max_stream_len", &self.events_max_stream_len)
            .field("run_scheduler", &self.run_scheduler)
            .field("scheduler", &self.scheduler)
            .field("store_results", &self.store_results)
            .field("result_ttl_secs", &self.result_ttl_secs)
            .field("result_batch", &self.result_batch)
            .field("result_idle_ms", &self.result_idle_ms)
            .field("pause_poll_ms", &self.pause_poll_ms)
            .field("log_max_stream_len", &self.log_max_stream_len)
            .field("log_max_line_bytes", &self.log_max_line_bytes)
            .field("events_progress_enabled", &self.events_progress_enabled)
            .field("stalled_detector_enabled", &self.stalled_detector_enabled)
            .field("stalled_detector", &self.stalled_detector)
            .field("metrics", &"<dyn MetricsSink>")
            .field("connection", &self.connection)
            .finish()
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            group: "default".to_string(),
            consumer_id: format!("c-{}", uuid::Uuid::new_v4()),
            batch: 64,
            block_ms: 5_000,
            claim_min_idle_ms: 30_000,
            concurrency: 100,
            // Default tuned to match the FFI shims (Python kwarg default,
            // Node docs) and BullMQ. A handler that always fails will run
            // 25 times before being DLQ'd as `retries_exhausted`. Override
            // per-queue via `ConsumerConfig::max_attempts` or per-job via
            // `JobRetryOverride::max_attempts` on `Producer::add_with_options`.
            max_attempts: 25,
            ack_batch: 256,
            ack_idle_ms: 5,
            shutdown_deadline_secs: 30,
            max_payload_bytes: 1_048_576,
            dlq_inflight: 32,
            dlq_max_stream_len: 100_000,
            retry: RetryConfig::default(),
            retry_inflight: 64,
            delayed_enabled: true,
            delayed_poll_interval_ms: 100,
            delayed_promote_batch: 256,
            delayed_max_stream_len: 1_000_000,
            delayed_lock_ttl_secs: 5,
            events_enabled: true,
            events_max_stream_len: 100_000,
            run_scheduler: true,
            scheduler: SchedulerConfig::default(),
            store_results: false,
            result_ttl_secs: 3600,
            result_batch: 64,
            result_idle_ms: 5,
            pause_poll_ms: 250,
            log_max_stream_len: 1_000,
            log_max_line_bytes: 4_096,
            events_progress_enabled: true,
            stalled_detector_enabled: true,
            stalled_detector: StalledDetectorConfig::default(),
            metrics: crate::metrics::noop_sink(),
            connection: ConnectionTuning::default(),
        }
    }
}

impl ConsumerConfig {
    /// Minimum acceptable `log_max_stream_len`. Caps below this defeat
    /// the `MAXLEN ~` rounding inside Redis and can leave the per-job
    /// log stream effectively empty between writes.
    pub const MIN_LOG_MAX_STREAM_LEN: u64 = 16;

    /// Validate the config. Called once at the start of `Consumer::run`
    /// so a misconfigured field surfaces at startup rather than as a
    /// silent data-loss bug after the first `JobHandle::log` call.
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.log_max_stream_len < Self::MIN_LOG_MAX_STREAM_LEN {
            return Err(crate::Error::Config(format!(
                "log_max_stream_len must be >= {} (got {})",
                Self::MIN_LOG_MAX_STREAM_LEN,
                self.log_max_stream_len
            )));
        }
        if self.stalled_detector_enabled {
            if self.stalled_detector.max_stalled_attempts == 0 {
                return Err(crate::Error::Config(
                    "stalled_detector.max_stalled_attempts must be >= 1 (a value of 0 would \
                     relocate every observed pending entry on first scan)"
                        .into(),
                ));
            }
            if self.stalled_detector.scan_batch == 0 {
                return Err(crate::Error::Config(
                    "stalled_detector.scan_batch must be >= 1".into(),
                ));
            }
            // The counter-semantic invariant: a faster tick than the idle
            // threshold INCRs more than once per crash. The embedded spawn
            // overrides both fields from `claim_min_idle_ms` so the default
            // path is always valid; only matters when a user overrides
            // `stalled_detector.tick_interval_ms` / `idle_threshold_ms`
            // explicitly.
            if self.stalled_detector.tick_interval_ms < self.stalled_detector.idle_threshold_ms {
                return Err(crate::Error::Config(format!(
                    "stalled_detector.tick_interval_ms ({}) must be >= \
                     stalled_detector.idle_threshold_ms ({}) — faster ticks INCR more than \
                     once per crash and break per-crash counting",
                    self.stalled_detector.tick_interval_ms, self.stalled_detector.idle_threshold_ms,
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct PromoterConfig {
    pub queue_name: String,
    pub poll_interval_ms: u64,
    pub promote_batch: usize,
    pub max_stream_len: u64,
    pub lock_ttl_secs: u64,
    pub holder_id: String,
    /// Mirrors [`ConsumerConfig::events_enabled`]: when `true` the promoter
    /// writes a `waiting` event to `{chasqui:<queue>}:events` for each job
    /// it just promoted from the delayed ZSET into the stream. When the
    /// promoter is spawned by `Consumer::run`, this field is forwarded from
    /// `ConsumerConfig::events_enabled`.
    pub events_enabled: bool,
    /// `MAXLEN ~` cap for events-stream `XADD`s. Forwarded from
    /// `ConsumerConfig::events_max_stream_len` by the embedded promoter.
    pub events_max_stream_len: u64,
    /// Receiver for promoter tick / lock-outcome events. Defaults to
    /// [`crate::metrics::NoopSink`]; swap in your own [`MetricsSink`] to
    /// bridge into Prometheus, OpenTelemetry, etc.
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
    pub connection: ConnectionTuning,
}

impl std::fmt::Debug for PromoterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PromoterConfig")
            .field("queue_name", &self.queue_name)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field("promote_batch", &self.promote_batch)
            .field("max_stream_len", &self.max_stream_len)
            .field("lock_ttl_secs", &self.lock_ttl_secs)
            .field("holder_id", &self.holder_id)
            .field("events_enabled", &self.events_enabled)
            .field("events_max_stream_len", &self.events_max_stream_len)
            .field("metrics", &"<dyn MetricsSink>")
            .field("connection", &self.connection)
            .finish()
    }
}

/// Configuration for the standalone [`crate::Scheduler`] (slice 10).
///
/// The scheduler tails the per-queue repeat ZSET (`{chasqui:<queue>}:repeat`)
/// at `tick_interval_ms`, materializes one fire of each due spec, schedules
/// the resulting job (immediately to the stream or to the delayed ZSET if
/// dispatch should still wait), and updates the spec's next-fire score in
/// the same Lua round trip. Leader-elected via `SET NX EX` on
/// `{chasqui:<queue>}:scheduler:lock` so multiple replicas can hot-spare
/// without double-firing.
#[derive(Clone)]
pub struct SchedulerConfig {
    pub queue_name: String,
    /// How often the leader drains due specs from the repeat ZSET. Default
    /// 1000ms — the lower bound on per-spec fire jitter is roughly this
    /// interval (a spec scheduled for 100ms-from-now still has to wait for
    /// the next tick to be picked up).
    pub tick_interval_ms: u64,
    /// Max specs hydrated per tick. Specs beyond this batch wait for the
    /// next tick — keeps a single fat tick from monopolizing the leader.
    pub batch: usize,
    /// `MAXLEN ~` cap forwarded to the script's XADD on the immediate-
    /// dispatch path.
    pub max_stream_len: u64,
    pub lock_ttl_secs: u64,
    pub holder_id: String,
    /// Forwarded into ack of metrics for tick / lock-outcome events.
    /// Defaults to [`crate::metrics::NoopSink`]. The scheduler currently
    /// emits the same `LockOutcome` events as the promoter; spec-level
    /// metrics (fires per tick, exhaustion events) are intentionally
    /// reserved for a follow-up slice.
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
    pub connection: ConnectionTuning,
}

impl std::fmt::Debug for SchedulerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerConfig")
            .field("queue_name", &self.queue_name)
            .field("tick_interval_ms", &self.tick_interval_ms)
            .field("batch", &self.batch)
            .field("max_stream_len", &self.max_stream_len)
            .field("lock_ttl_secs", &self.lock_ttl_secs)
            .field("holder_id", &self.holder_id)
            .field("metrics", &"<dyn MetricsSink>")
            .field("connection", &self.connection)
            .finish()
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            tick_interval_ms: 1_000,
            batch: 256,
            max_stream_len: 1_000_000,
            lock_ttl_secs: 5,
            holder_id: format!("s-{}", uuid::Uuid::new_v4()),
            metrics: crate::metrics::noop_sink(),
            connection: ConnectionTuning::default(),
        }
    }
}

impl Default for PromoterConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            poll_interval_ms: 100,
            promote_batch: 256,
            max_stream_len: 1_000_000,
            lock_ttl_secs: 5,
            holder_id: format!("p-{}", uuid::Uuid::new_v4()),
            events_enabled: true,
            events_max_stream_len: 100_000,
            metrics: crate::metrics::noop_sink(),
            connection: ConnectionTuning::default(),
        }
    }
}

/// Configuration for the standalone [`crate::StalledDetector`] (slice 12).
///
/// The detector is leader-elected (via `SET NX EX` on
/// `{chasqui:<queue>}:stalled:lock`) and scans the consumer group's PEL
/// every `tick_interval_ms`, INCR'ing a per-job stall counter for every
/// entry that has sat idle past `idle_threshold_ms`. When a job's stall
/// counter reaches `max_stalled_attempts`, the entry is atomically
/// relocated to the DLQ with `DlqReason::Stalled` (distinct from
/// `RetriesExhausted` — handler-failure loops vs worker-crash loops).
///
/// Defaults inherit cleanly from `ConsumerConfig::claim_min_idle_ms` when
/// the detector is embedded (`spawn_stalled_detector`) — operators
/// running the detector standalone own the
/// `tick_interval_ms >= idle_threshold_ms` invariant.
#[derive(Clone)]
pub struct StalledDetectorConfig {
    pub queue_name: String,
    /// How often the leader runs `XPENDING ... IDLE`. Default 30_000ms.
    /// **Must be >= `idle_threshold_ms`** — a faster tick INCRs more than
    /// once per crash and breaks the per-crash counting invariant. The
    /// embedded spawn overrides both from `ConsumerConfig::claim_min_idle_ms`
    /// so the invariant is automatic on the common path.
    pub tick_interval_ms: u64,
    /// Idle threshold passed to `XPENDING ... IDLE`. Default 30_000ms.
    /// See `tick_interval_ms` for the lockstep invariant. The embedded
    /// spawn forwards `ConsumerConfig::claim_min_idle_ms` so the detector
    /// scans the same entries the reader's CLAIM safety net is already
    /// re-delivering — one INCR per crash cycle.
    pub idle_threshold_ms: u64,
    /// Stall counter ceiling. When `n >= max_stalled_attempts`, the entry
    /// is atomically relocated to the DLQ as `DlqReason::Stalled`. Default
    /// `1` — matches BullMQ's `maxStalledCount` default; one missed scan
    /// is enough to call it stalled. Set higher for noisy workloads where
    /// brief CLAIM redeliveries shouldn't count. Validation rejects `0`.
    pub max_stalled_attempts: u32,
    /// `XPENDING ... - + <count>` cap. Bounds the scan size so a giant
    /// stuck PEL can't block the leader on one tick. Default `256`.
    pub scan_batch: usize,
    /// Leader-election lock TTL (seconds). Must comfortably exceed
    /// `tick_interval_ms` — the detector sleeps for one full tick
    /// between scans, and the lock must not expire while the sleeping
    /// leader holds it or another replica will steal leadership every
    /// tick (and the original leader will reacquire on wake, causing
    /// thrash). Default `90` — `tick_interval_ms` defaults to `30_000`
    /// (30s), so `90s = 3× tick` leaves enough headroom for tick jitter
    /// and short Redis hiccups. Operators overriding `tick_interval_ms`
    /// upward should bump this in lockstep (rule of thumb: at least
    /// `2 × (tick_interval_ms / 1000) + 5s`).
    pub lock_ttl_secs: u64,
    /// Detector-instance id (value of the leader lock). Default a fresh
    /// `format!("sd-{uuid}")`.
    pub holder_id: String,
    /// Receiver of `stalled_tick` events. Defaults to
    /// [`crate::metrics::NoopSink`]. The embedded spawn forwards the
    /// parent `ConsumerConfig::metrics` so dashboards see the same sink.
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
    pub connection: ConnectionTuning,
}

impl std::fmt::Debug for StalledDetectorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StalledDetectorConfig")
            .field("queue_name", &self.queue_name)
            .field("tick_interval_ms", &self.tick_interval_ms)
            .field("idle_threshold_ms", &self.idle_threshold_ms)
            .field("max_stalled_attempts", &self.max_stalled_attempts)
            .field("scan_batch", &self.scan_batch)
            .field("lock_ttl_secs", &self.lock_ttl_secs)
            .field("holder_id", &self.holder_id)
            .field("metrics", &"<dyn MetricsSink>")
            .field("connection", &self.connection)
            .finish()
    }
}

impl Default for StalledDetectorConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            tick_interval_ms: 30_000,
            idle_threshold_ms: 30_000,
            max_stalled_attempts: 1,
            scan_batch: 256,
            // Must outlive `tick_interval_ms` (the leader sleeps for one
            // full tick between scans and will lose its lock to a
            // replica otherwise — see field doc). `90s` covers the
            // default `30_000ms` tick with 3× headroom.
            lock_ttl_secs: 90,
            holder_id: format!("sd-{}", uuid::Uuid::new_v4()),
            metrics: crate::metrics::noop_sink(),
            connection: ConnectionTuning::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_config_defaults_match_documented_values() {
        let cfg = ConsumerConfig::default();
        assert_eq!(cfg.log_max_stream_len, 1_000);
        assert_eq!(cfg.log_max_line_bytes, 4_096);
        assert!(cfg.events_progress_enabled);
    }

    #[test]
    fn validate_rejects_log_max_stream_len_below_minimum() {
        let mut cfg = ConsumerConfig {
            log_max_stream_len: ConsumerConfig::MIN_LOG_MAX_STREAM_LEN - 1,
            ..ConsumerConfig::default()
        };
        assert!(cfg.validate().is_err());
        cfg.log_max_stream_len = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_log_max_stream_len_at_minimum() {
        let cfg = ConsumerConfig {
            log_max_stream_len: ConsumerConfig::MIN_LOG_MAX_STREAM_LEN,
            ..ConsumerConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn default_config_passes_validation() {
        // The defaults always satisfy the stalled-detector invariants:
        // tick_interval_ms == idle_threshold_ms (both 30_000ms),
        // max_stalled_attempts == 1 (>= 1), scan_batch == 256 (>= 1).
        let cfg = ConsumerConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_max_stalled_attempts_zero() {
        let cfg = ConsumerConfig {
            stalled_detector: StalledDetectorConfig {
                max_stalled_attempts: 0,
                ..StalledDetectorConfig::default()
            },
            ..ConsumerConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("max_stalled_attempts=0 must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("max_stalled_attempts"),
            "error must name the field: {msg}"
        );
    }

    #[test]
    fn validate_rejects_zero_scan_batch() {
        let cfg = ConsumerConfig {
            stalled_detector: StalledDetectorConfig {
                scan_batch: 0,
                ..StalledDetectorConfig::default()
            },
            ..ConsumerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_tick_below_idle_threshold() {
        // tick=5s, idle=30s — a 5s tick would INCR 6x per 30s crash cycle,
        // breaking the per-crash counting invariant.
        let cfg = ConsumerConfig {
            stalled_detector: StalledDetectorConfig {
                tick_interval_ms: 5_000,
                idle_threshold_ms: 30_000,
                ..StalledDetectorConfig::default()
            },
            ..ConsumerConfig::default()
        };
        let err = cfg.validate().expect_err("tick < idle must reject");
        let msg = format!("{err}");
        assert!(
            msg.contains("tick_interval_ms"),
            "error must name the field: {msg}"
        );
        assert!(
            msg.contains("idle_threshold_ms"),
            "error must name the field: {msg}"
        );
    }

    #[test]
    fn validate_accepts_disabled_detector_with_garbage_settings() {
        // When the detector is disabled, the embedded path won't spawn it,
        // so invariants on the inner struct are not enforced.
        let cfg = ConsumerConfig {
            stalled_detector_enabled: false,
            stalled_detector: StalledDetectorConfig {
                max_stalled_attempts: 0,
                tick_interval_ms: 100,
                idle_threshold_ms: 100_000,
                ..StalledDetectorConfig::default()
            },
            ..ConsumerConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }
}
