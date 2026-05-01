use super::{ScenarioReport, Stopwatch, scaled_params};
use crate::sample::{Payload, generate_sample};
use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::{Consumer, Job, Producer};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(1_000, 1_000, scale);
    let total = params.warmup + params.bench;
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
        batch: 64,
        block_ms: 100,
        claim_min_idle_ms: 30_000,
        concurrency: 1,
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
    };

    drive_worker_scenario(
        redis_url,
        consumer_cfg,
        params.warmup,
        params.bench,
        "worker-generic",
    )
    .await
}

pub(crate) async fn drive_worker_scenario(
    redis_url: &str,
    consumer_cfg: ConsumerConfig,
    warmup: u64,
    bench: u64,
    name: &'static str,
) -> ScenarioReport {
    let sw = Arc::new(Mutex::new(Stopwatch::new(warmup, bench)));
    let (done_tx, done_rx) = oneshot::channel::<super::ScenarioOutcome>();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let consumer: Consumer<Payload> = Consumer::new(redis_url, consumer_cfg);
    let join = tokio::spawn(async move {
        consumer
            .run(
                {
                    let sw = sw.clone();
                    let done_tx = done_tx.clone();
                    let shutdown = shutdown_clone.clone();
                    move |_: Job<Payload>| {
                        let sw = sw.clone();
                        let done_tx = done_tx.clone();
                        let shutdown = shutdown.clone();
                        async move {
                            let outcome = {
                                let mut guard = sw.lock().await;
                                guard.tick()
                            };
                            if let Some(outcome) = outcome
                                && let Some(tx) = done_tx.lock().await.take()
                            {
                                let _ = tx.send(outcome);
                                shutdown.cancel();
                            }
                            Ok(())
                        }
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let outcome = done_rx.await.expect("scenario must finish");
    shutdown.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), join).await;
    outcome.into_report(name)
}
