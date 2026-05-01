use super::ScenarioReport;
use super::preload::preload_jobs;
use super::scaled_params;
use crate::sample::{Payload, generate_sample};
use chasquimq::config::{ConsumerConfig, RetryConfig};
use chasquimq::{Consumer, Job};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Throughput of the retry path: every job fails its first attempt then
/// succeeds the second. Measures the cost of the XACKDEL+ZADD-via-Lua
/// reschedule plus the promoter pulling it back into the stream. Backoffs
/// are intentionally tiny so the bench window stays measurable.
pub async fn run(redis_url: &str, queue: &str, scale: u32) -> ScenarioReport {
    let params = scaled_params(500, 500, scale);
    let total = params.warmup + params.bench;
    let payload: Payload = generate_sample(1, 1);

    preload_jobs(redis_url, queue, 4, &payload, total).await;

    let consumer_cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "bench".to_string(),
        consumer_id: "w1".to_string(),
        batch: 256,
        block_ms: 50,
        claim_min_idle_ms: 30_000,
        concurrency: 100,
        max_attempts: 5,
        ack_batch: 256,
        ack_idle_ms: 2,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 64,
        dlq_max_stream_len: 100_000,
        retry: RetryConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        retry_inflight: 256,
        delayed_enabled: true,
        delayed_poll_interval_ms: 5,
        delayed_promote_batch: 1024,
        delayed_max_stream_len: 1_000_000,
        delayed_lock_ttl_secs: 5,
    };

    drive_retry_scenario(
        redis_url,
        consumer_cfg,
        params.warmup,
        params.bench,
        "worker-retry-throughput",
    )
    .await
}

async fn drive_retry_scenario(
    redis_url: &str,
    consumer_cfg: ConsumerConfig,
    warmup: u64,
    bench: u64,
    name: &'static str,
) -> ScenarioReport {
    let sw = Arc::new(Mutex::new(super::Stopwatch::new(warmup, bench)));
    let (done_tx, done_rx) = oneshot::channel::<super::ScenarioOutcome>();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let calls_seen: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    let consumer: Consumer<Payload> = Consumer::new(redis_url, consumer_cfg);
    let calls_seen_h = calls_seen.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                {
                    let sw = sw.clone();
                    let done_tx = done_tx.clone();
                    let shutdown = shutdown_clone.clone();
                    move |job: Job<Payload>| {
                        let sw = sw.clone();
                        let done_tx = done_tx.clone();
                        let shutdown = shutdown.clone();
                        let calls = calls_seen_h.clone();
                        async move {
                            calls.fetch_add(1, Ordering::Relaxed);
                            // Fail first attempt, succeed on retry. job.attempt
                            // is 0 on the original delivery, 1 on the retry.
                            if job.attempt == 0 {
                                return Err(chasquimq::HandlerError::new(std::io::Error::other(
                                    "bench-retry",
                                )));
                            }
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

    let outcome = done_rx.await.expect("retry scenario must finish");
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
    outcome.into_report(name)
}
