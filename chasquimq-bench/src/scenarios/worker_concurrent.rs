use super::ScenarioReport;
use super::worker_generic::drive_worker_scenario;
use crate::sample::{Payload, generate_sample};
use chasquimq::Producer;
use chasquimq::config::{ConsumerConfig, ProducerConfig};

pub async fn run(redis_url: &str, queue: &str) -> ScenarioReport {
    let warmup: u64 = 1_000;
    let bench: u64 = 10_000;
    let total = warmup + bench;
    let payload: Payload = generate_sample(1, 1);

    let producer: Producer<Payload> = Producer::connect(
        redis_url,
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size: 4,
            max_stream_len: 1_000_000,
        },
    )
    .await
    .expect("connect producer");

    let mut emitted: u64 = 0;
    while emitted < total {
        let remaining = (total - emitted) as usize;
        let n = remaining.min(100);
        let payloads: Vec<Payload> = (0..n).map(|_| payload.clone()).collect();
        producer.add_bulk(payloads).await.expect("preload");
        emitted += n as u64;
    }

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
    };

    drive_worker_scenario(redis_url, consumer_cfg, warmup, bench, "worker-concurrent").await
}
