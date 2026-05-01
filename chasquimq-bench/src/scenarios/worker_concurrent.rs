use super::ScenarioReport;
use super::preload::preload_jobs;
use super::scaled_params;
use super::worker_generic::drive_worker_scenario;
use crate::sample::{Payload, generate_sample};
use chasquimq::config::ConsumerConfig;

pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(1_000, 10_000, scale);
    let total = params.warmup + params.bench;
    let payload: Payload = generate_sample(1, 1);

    preload_jobs(redis_url, queue, 4, &payload, total).await;

    let consumer_cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "bench".to_string(),
        consumer_id: "w1".to_string(),
        batch: 256,
        block_ms: 100,
        claim_min_idle_ms: 30_000,
        concurrency: 100,
        max_attempts: 3,
        ack_batch: 256,
        ack_idle_ms: 2,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 64,
        delayed_enabled: false,
        ..Default::default()
    };

    drive_worker_scenario(
        redis_url,
        consumer_cfg,
        params.warmup,
        params.bench,
        "worker-concurrent",
    )
    .await
}
