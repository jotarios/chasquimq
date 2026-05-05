//! `QueueLabeled<S>`: wraps any `MetricsSink` to add a `queue` label.
//!
//! The canonical labeling pattern for `chasquimq-metrics` output. Composes
//! with any `MetricsSink` (not just [`crate::MetricsFacadeSink`]). To stack
//! additional labels (`tenant`, `region`, …), write a similar wrapper
//! following this pattern — it's intentionally small.

use chasquimq::{
    DlqRouted, JobOutcome, JobOutcomeKind, LockOutcome, MetricsSink, PromoterTick, ReaderBatch,
    RetryScheduled,
};
use metrics::{counter, gauge, histogram};
use std::sync::Arc;

/// Wrapper that adds a `queue` label to every metric emitted by the inner
/// sink. The canonical way to label `chasquimq-metrics` output.
///
/// ```no_run
/// use chasquimq::ConsumerConfig;
/// use chasquimq_metrics::{MetricsFacadeSink, QueueLabeled};
/// use std::sync::Arc;
///
/// let cfg = ConsumerConfig {
///     metrics: Arc::new(QueueLabeled::new(MetricsFacadeSink::new(), "orders")),
///     ..Default::default()
/// };
/// ```
///
/// `queue` is stored as `Arc<str>` so cloning the wrapper into multiple
/// consumers (or threading it through several configs) doesn't keep
/// re-allocating the label string.
pub struct QueueLabeled<S: MetricsSink> {
    inner: S,
    queue: Arc<str>,
}

impl<S: MetricsSink> QueueLabeled<S> {
    pub fn new(inner: S, queue: impl Into<Arc<str>>) -> Self {
        Self {
            inner,
            queue: queue.into(),
        }
    }
}

// `metrics::Label` is built from `(impl Into<SharedString>, impl Into<SharedString>)`,
// where `SharedString` is `metrics::Cow<'static, str>` and `Cow` has a `From<Arc<T>>`
// impl. So we pass `self.queue.clone()` (one atomic increment) instead of
// `self.queue.as_ref().to_string()` (one allocation). On the per-job path
// (`job_outcome` fires per handler invocation), this is the difference between
// 50k allocs/sec and 50k atomic increments/sec at the project's headline target.
impl<S: MetricsSink> MetricsSink for QueueLabeled<S> {
    fn promoter_tick(&self, tick: PromoterTick) {
        counter!("chasquimq_promoter_promoted_total", "queue" => self.queue.clone())
            .increment(tick.promoted);
        gauge!("chasquimq_delayed_zset_depth", "queue" => self.queue.clone())
            .set(tick.depth as f64);
        gauge!("chasquimq_promoter_oldest_pending_lag_ms", "queue" => self.queue.clone())
            .set(tick.oldest_pending_lag_ms as f64);
        self.inner.promoter_tick(tick);
    }

    fn promoter_lock_outcome(&self, outcome: LockOutcome) {
        match outcome {
            LockOutcome::Acquired => {
                counter!("chasquimq_promoter_lock_acquired_total", "queue" => self.queue.clone())
                    .increment(1);
            }
            LockOutcome::Held => {
                counter!("chasquimq_promoter_lock_lost_total", "queue" => self.queue.clone())
                    .increment(1);
            }
        }
        self.inner.promoter_lock_outcome(outcome);
    }

    fn reader_batch(&self, batch: ReaderBatch) {
        histogram!("chasquimq_consumer_batch_size", "queue" => self.queue.clone())
            .record(batch.size as f64);
        if batch.reclaimed > 0 {
            counter!("chasquimq_consumer_reclaimed_total", "queue" => self.queue.clone())
                .increment(batch.reclaimed);
        }
        self.inner.reader_batch(batch);
    }

    fn job_outcome(&self, outcome: JobOutcome) {
        // Render as seconds (Prometheus convention) so adapter buckets line up
        // with standard handler-duration dashboards. Engine event keeps micros
        // for honest precision; the conversion happens here at the adapter
        // boundary. `name` is added as a label so operators can slice metrics
        // by job kind without msgpack-decoding payload bytes — the
        // observability payoff of slice 5.
        histogram!(
            "chasquimq_handler_duration_seconds",
            "queue" => self.queue.clone(),
            "name" => outcome.name.clone(),
        )
        .record(outcome.handler_duration_us as f64 / 1_000_000.0);
        match outcome.kind {
            JobOutcomeKind::Ok => {
                counter!(
                    "chasquimq_jobs_completed_total",
                    "queue" => self.queue.clone(),
                    "name" => outcome.name.clone(),
                )
                .increment(1);
            }
            JobOutcomeKind::Err => {
                counter!(
                    "chasquimq_jobs_failed_total",
                    "queue" => self.queue.clone(),
                    "kind" => "error",
                    "name" => outcome.name.clone(),
                )
                .increment(1);
            }
            JobOutcomeKind::Panic => {
                counter!(
                    "chasquimq_jobs_failed_total",
                    "queue" => self.queue.clone(),
                    "kind" => "panic",
                    "name" => outcome.name.clone(),
                )
                .increment(1);
            }
        }
        self.inner.job_outcome(outcome);
    }

    fn retry_scheduled(&self, retry: RetryScheduled) {
        counter!(
            "chasquimq_retries_scheduled_total",
            "queue" => self.queue.clone(),
            "name" => retry.name.clone(),
        )
        .increment(1);
        // Render as seconds (Prometheus convention), matching
        // `chasquimq_handler_duration_seconds`. Engine event keeps ms because
        // it's what `RetryConfig` is configured in.
        histogram!(
            "chasquimq_retry_backoff_seconds",
            "queue" => self.queue.clone(),
            "name" => retry.name.clone(),
        )
        .record(retry.backoff_ms as f64 / 1_000.0);
        self.inner.retry_scheduled(retry);
    }

    fn dlq_routed(&self, dlq: DlqRouted) {
        counter!(
            "chasquimq_dlq_routed_total",
            "queue" => self.queue.clone(),
            "reason" => dlq.reason.as_str(),
            "name" => dlq.name.clone(),
        )
        .increment(1);
        self.inner.dlq_routed(dlq);
    }
}
