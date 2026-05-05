//! `MetricsFacadeSink`: emits unlabeled metrics via the `metrics-rs` facade.
//!
//! For per-queue (or any other) labels, wrap with
//! [`crate::QueueLabeled`] — that's the canonical pattern and the one
//! demonstrated in `examples/`.

use chasquimq::{
    DlqRouted, JobOutcome, JobOutcomeKind, LockOutcome, MetricsSink, PromoterTick, ReaderBatch,
    RetryScheduled,
};
use metrics::{counter, gauge, histogram};

/// `MetricsSink` impl that emits via the global `metrics` facade. Construct
/// once and pass into `PromoterConfig::metrics` / `ConsumerConfig::metrics`.
///
/// Emits metrics **without labels**.
///
/// # Why per-call macros (not cached handles)
///
/// Each event resolves the handle through `counter!()` / `gauge!()` /
/// `histogram!()` rather than caching a `Counter`/`Gauge`/`Histogram` at
/// construction. Two reasons:
///
/// 1. **Late-recorder safety.** A user who constructs the sink before
///    installing their global recorder (very natural in `main()` setup
///    code) would otherwise freeze a no-op handle that keeps dropping
///    events even after the real recorder is installed.
/// 2. **Per-call cost is one HashMap lookup + Arc clone** in real
///    recorders (verified for `metrics-exporter-prometheus`'s
///    `FrozenRecorder` and `FreezableRecorder`). At promoter tick rates
///    (50ms+ default), the lookup is well under 1% of the per-tick budget.
///    On the per-job path, the lookup runs ~3× per job; still negligible
///    relative to handler time.
///
/// If you measure macro overhead as a real bottleneck in your deployment,
/// you can wrap your own `MetricsSink` that caches handles after recorder
/// install — but do it deliberately, not by default.
pub struct MetricsFacadeSink;

impl Default for MetricsFacadeSink {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsFacadeSink {
    pub const fn new() -> Self {
        Self
    }
}

impl MetricsSink for MetricsFacadeSink {
    fn promoter_tick(&self, tick: PromoterTick) {
        counter!("chasquimq_promoter_promoted_total").increment(tick.promoted);
        gauge!("chasquimq_delayed_zset_depth").set(tick.depth as f64);
        gauge!("chasquimq_promoter_oldest_pending_lag_ms").set(tick.oldest_pending_lag_ms as f64);
    }

    fn promoter_lock_outcome(&self, outcome: LockOutcome) {
        match outcome {
            LockOutcome::Acquired => {
                counter!("chasquimq_promoter_lock_acquired_total").increment(1);
            }
            LockOutcome::Held => {
                counter!("chasquimq_promoter_lock_lost_total").increment(1);
            }
        }
    }

    fn reader_batch(&self, batch: ReaderBatch) {
        histogram!("chasquimq_consumer_batch_size").record(batch.size as f64);
        if batch.reclaimed > 0 {
            counter!("chasquimq_consumer_reclaimed_total").increment(batch.reclaimed);
        }
    }

    fn job_outcome(&self, outcome: JobOutcome) {
        // `name` is rendered as a Prometheus / OTel label so operators can
        // slice handler-duration histograms and failure counters by job kind
        // without msgpack-decoding payload bytes — the architectural payoff
        // of putting `name` at the Streams framing layer (Option B in
        // `docs/name-on-wire-design.md`).
        histogram!("chasquimq_handler_duration_seconds", "name" => outcome.name.clone())
            .record(outcome.handler_duration_us as f64 / 1_000_000.0);
        match outcome.kind {
            JobOutcomeKind::Ok => {
                counter!("chasquimq_jobs_completed_total", "name" => outcome.name).increment(1);
            }
            JobOutcomeKind::Err => {
                counter!(
                    "chasquimq_jobs_failed_total",
                    "kind" => "error",
                    "name" => outcome.name,
                )
                .increment(1);
            }
            JobOutcomeKind::Panic => {
                counter!(
                    "chasquimq_jobs_failed_total",
                    "kind" => "panic",
                    "name" => outcome.name,
                )
                .increment(1);
            }
        }
    }

    fn retry_scheduled(&self, retry: RetryScheduled) {
        counter!("chasquimq_retries_scheduled_total", "name" => retry.name).increment(1);
        histogram!("chasquimq_retry_backoff_seconds").record(retry.backoff_ms as f64 / 1_000.0);
    }

    fn dlq_routed(&self, dlq: DlqRouted) {
        counter!(
            "chasquimq_dlq_routed_total",
            "reason" => dlq.reason.as_str(),
            "name" => dlq.name,
        )
        .increment(1);
    }
}
