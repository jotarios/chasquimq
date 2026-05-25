//! `progress_throughput`: measure the cost of calling
//! `JobHandle::update_progress` from inside a handler.
//!
//! Three sibling scenarios, one per K (progress calls per job):
//! `progress-throughput-1`, `progress-throughput-10`,
//! `progress-throughput-100`. Concurrency matches `worker-concurrent`
//! (100) so the headline jobs/sec column is apples-to-apples with the
//! no-progress baseline.
//!
//! Handler does nothing except call `update_progress` K times. The
//! progress value cycles 0..=100 so the engine's clamp short-circuit
//! never fires and we measure the steady-state SET cost.
//!
//! `events_progress_enabled` is wired through from a CLI flag (default
//! `false`) so users can isolate "just the progress key write" from
//! "progress key write + events XADD".
use super::ScenarioReport;
use super::preload::preload_jobs;
use super::scaled_params;
use super::worker_generic::drive_worker_scenario_with_handle_handler;
use crate::sample::{Payload, generate_sample};
use chasquimq::config::ConsumerConfig;

pub async fn run(
    redis_url: &str,
    queue: &str,
    scale: u32,
    calls_per_job: u32,
    events_progress_enabled: bool,
) -> ScenarioReport {
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
        run_scheduler: false,
        events_progress_enabled,
        ..Default::default()
    };

    let name: &'static str = match calls_per_job {
        1 => "progress-throughput-1",
        10 => "progress-throughput-10",
        100 => "progress-throughput-100",
        _ => "progress-throughput-custom",
    };

    drive_worker_scenario_with_handle_handler(
        redis_url,
        consumer_cfg,
        params.warmup,
        params.bench,
        name,
        calls_per_job,
    )
    .await
}
