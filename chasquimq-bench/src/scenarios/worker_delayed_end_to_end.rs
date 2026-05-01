use super::ScenarioReport;
use super::scaled_params;
use super::worker_generic::drive_worker_scenario;
use crate::sample::{Payload, generate_sample};
use chasquimq::Producer;
use chasquimq::config::{ConsumerConfig, ProducerConfig};
use std::time::Duration;

/// End-to-end delayed-job throughput: producer ZADDs jobs that are already
/// due, the promoter (running inside the consumer) drains the delayed ZSET
/// into the stream, and the worker consumes them. Stopwatch is in the worker
/// handler, same as worker-generic.
pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(1_000, 1_000, scale);
    let total = params.warmup + params.bench;
    let payload: Payload = generate_sample(1, 1);

    preload_delayed(redis_url, queue, &payload, total).await;

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
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        delayed_promote_batch: 1024,
        delayed_max_stream_len: 1_000_000,
        delayed_lock_ttl_secs: 5,
        ..Default::default()
    };

    drive_worker_scenario(
        redis_url,
        consumer_cfg,
        params.warmup,
        params.bench,
        "worker-delayed-end-to-end",
    )
    .await
}

async fn preload_delayed(redis_url: &str, queue: &str, payload: &Payload, total: u64) {
    let producer: Producer<Payload> = Producer::connect(
        redis_url,
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size: 4,
            max_stream_len: 1_000_000,
            ..Default::default()
        },
    )
    .await
    .expect("connect producer");

    // 1ms in the past so they are immediately due. Score is i64 ms; using a
    // tiny negative offset via add_in is awkward, so use add_at with a past
    // SystemTime — but add_at fast-paths to immediate XADD when run_at <= now.
    // Instead bypass via add_in_bulk with a 1ms delay so they all land in the
    // ZSET; by the time the consumer starts ~ms later they are due.
    let mut emitted: u64 = 0;
    while emitted < total {
        let remaining = (total - emitted) as usize;
        let n = remaining.min(500);
        let payloads: Vec<Payload> = (0..n).map(|_| payload.clone()).collect();
        producer
            .add_in_bulk(Duration::from_millis(1), payloads)
            .await
            .expect("preload add_in_bulk");
        emitted += n as u64;
    }

    // Make sure they're all due before the bench starts; the harness creates
    // queue, preloads, then runs the consumer scenario, so a small sleep here
    // is enough.
    tokio::time::sleep(Duration::from_millis(5)).await;
}
