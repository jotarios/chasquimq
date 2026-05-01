#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub queue_name: String,
    pub pool_size: usize,
    pub max_stream_len: u64,
    pub max_delay_secs: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            pool_size: 8,
            max_stream_len: 1_000_000,
            max_delay_secs: 30 * 24 * 3600,
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
    /// Forwarded to the inline promoter the consumer spawns when
    /// `delayed_enabled` is true. Defaults to [`crate::metrics::NoopSink`].
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
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
            .field("metrics", &"<dyn MetricsSink>")
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
            metrics: crate::metrics::noop_sink(),
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
    /// Receiver for promoter tick / lock-outcome events. Defaults to
    /// [`crate::metrics::NoopSink`]; swap in your own [`MetricsSink`] to
    /// bridge into Prometheus, OpenTelemetry, etc.
    pub metrics: std::sync::Arc<dyn crate::metrics::MetricsSink>,
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
            .field("metrics", &"<dyn MetricsSink>")
            .finish()
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
            metrics: crate::metrics::noop_sink(),
        }
    }
}
