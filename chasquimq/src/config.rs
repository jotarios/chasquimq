#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub queue_name: String,
    pub pool_size: usize,
    pub max_stream_len: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            queue_name: "default".to_string(),
            pool_size: 8,
            max_stream_len: 1_000_000,
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
        }
    }
}
