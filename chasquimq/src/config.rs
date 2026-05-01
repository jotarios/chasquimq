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

#[derive(Debug, Clone)]
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
        }
    }
}

#[derive(Debug, Clone)]
pub struct PromoterConfig {
    pub queue_name: String,
    pub poll_interval_ms: u64,
    pub promote_batch: usize,
    pub max_stream_len: u64,
    pub lock_ttl_secs: u64,
    pub holder_id: String,
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
        }
    }
}
