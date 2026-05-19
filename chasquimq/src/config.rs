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
            max_attempts: 3,
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
            metrics: crate::metrics::noop_sink(),
            connection: ConnectionTuning::default(),
        }
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
