use super::preload::preload_jobs;
use super::{ScenarioReport, Stopwatch, scaled_params};
use crate::sample::{Payload, generate_sample};
use chasquimq::config::ConsumerConfig;
use chasquimq::{Consumer, Job};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(1_000, 1_000, scale);
    let total = params.warmup + params.bench;
    let payload: Payload = generate_sample(1, 1);

    preload_jobs(redis_url, queue, 4, &payload, total).await;

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
