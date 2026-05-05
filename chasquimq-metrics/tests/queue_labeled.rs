//! Adapter regression: every metric emitted via `QueueLabeled` carries
//! the `queue` label. This is the test that protects the queue-label
//! rollout — if someone forgets a label on a new event in future, this
//! test fails.
//!
//! Lives in `tests/` rather than `#[cfg(test)] mod tests` so the global
//! `metrics-rs` recorder install happens in its own process — no risk of
//! collision with other tests in the crate.

use chasquimq::{
    DlqReason, DlqRouted, JobOutcome, JobOutcomeKind, LockOutcome, MetricsSink, NoopSink,
    PromoterTick, ReaderBatch, RetryScheduled,
};
use chasquimq_metrics::QueueLabeled;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use std::collections::HashMap;

type Labels = Vec<(String, String)>;
type CounterMap = HashMap<String, Vec<(Labels, u64)>>;
type GaugeMap = HashMap<String, Vec<(Labels, f64)>>;
type Snapshot = (CounterMap, GaugeMap);

fn install_debug_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install recorder");
    snapshotter
}

/// Snapshot once and bucket the values. `key.labels()` exposes the
/// label set so we can assert that `queue=...` is present where
/// expected. Some recorder implementations reset counter handles
/// between snapshots — one snapshot per assertion run.
fn collect(snapshotter: &Snapshotter) -> Snapshot {
    let mut counters: CounterMap = HashMap::new();
    let mut gauges: GaugeMap = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        let name = key.key().name().to_string();
        let labels: Labels = key
            .key()
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        match value {
            DebugValue::Counter(n) => {
                counters.entry(name).or_default().push((labels, n));
            }
            DebugValue::Gauge(n) => {
                gauges
                    .entry(name)
                    .or_default()
                    .push((labels, n.into_inner()));
            }
            _ => {}
        }
    }
    (counters, gauges)
}

fn label_match(actual: &[(String, String)], want: &[(&str, &str)]) -> bool {
    if actual.len() != want.len() {
        return false;
    }
    want.iter()
        .all(|(k, v)| actual.iter().any(|(ak, av)| ak == k && av == v))
}

fn counter_value(counters: &CounterMap, name: &str, want_labels: &[(&str, &str)]) -> Option<u64> {
    counters
        .get(name)?
        .iter()
        .find(|(labels, _)| label_match(labels, want_labels))
        .map(|(_, n)| *n)
}

#[test]
fn queue_labeled_wraps_all_events() {
    let snapshotter = install_debug_recorder();
    let sink = QueueLabeled::new(NoopSink, "orders");

    // Promoter
    sink.promoter_tick(PromoterTick {
        promoted: 7,
        depth: 42,
        oldest_pending_lag_ms: 1500,
    });
    sink.promoter_lock_outcome(LockOutcome::Acquired);
    sink.promoter_lock_outcome(LockOutcome::Held);
    sink.promoter_lock_outcome(LockOutcome::Held);

    // Reader
    sink.reader_batch(ReaderBatch {
        size: 10,
        reclaimed: 2,
    });

    // Worker
    sink.job_outcome(JobOutcome {
        kind: JobOutcomeKind::Ok,
        attempt: 1,
        handler_duration_us: 12,
        name: "send-email".to_string(),
    });
    sink.job_outcome(JobOutcome {
        kind: JobOutcomeKind::Err,
        attempt: 2,
        handler_duration_us: 5,
        name: "send-email".to_string(),
    });
    sink.job_outcome(JobOutcome {
        kind: JobOutcomeKind::Panic,
        attempt: 1,
        handler_duration_us: 3,
        name: String::new(),
    });

    // Retry
    sink.retry_scheduled(RetryScheduled {
        attempt: 2,
        backoff_ms: 200,
        name: "send-email".to_string(),
    });
    sink.retry_scheduled(RetryScheduled {
        attempt: 3,
        backoff_ms: 400,
        name: "send-email".to_string(),
    });

    // DLQ — one of each reason variant.
    sink.dlq_routed(DlqRouted {
        reason: DlqReason::RetriesExhausted,
        attempt: 3,
        name: "send-email".to_string(),
    });
    sink.dlq_routed(DlqRouted {
        reason: DlqReason::DecodeFailed,
        attempt: 0,
        name: String::new(),
    });
    sink.dlq_routed(DlqRouted {
        reason: DlqReason::Malformed { reason: "test" },
        attempt: 0,
        name: String::new(),
    });
    sink.dlq_routed(DlqRouted {
        reason: DlqReason::OversizePayload,
        attempt: 0,
        name: String::new(),
    });

    let (counters, _gauges) = collect(&snapshotter);

    // Promoter counters carry queue label.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_promoter_promoted_total",
            &[("queue", "orders")]
        ),
        Some(7),
        "promoter promoted_total: {counters:?}"
    );
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_promoter_lock_acquired_total",
            &[("queue", "orders")]
        ),
        Some(1),
    );
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_promoter_lock_lost_total",
            &[("queue", "orders")]
        ),
        Some(2),
    );

    // Reader: reclaimed > 0 fired the counter.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_consumer_reclaimed_total",
            &[("queue", "orders")]
        ),
        Some(2),
    );

    // Worker: jobs_completed_total has queue + name labels.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_jobs_completed_total",
            &[("queue", "orders"), ("name", "send-email")]
        ),
        Some(1),
    );
    // jobs_failed_total is split by `kind` label.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_jobs_failed_total",
            &[
                ("queue", "orders"),
                ("kind", "error"),
                ("name", "send-email")
            ]
        ),
        Some(1),
    );
    // The Panic outcome was emitted with an empty name above, so the label
    // is rendered as an empty string. This pins the contract that empty
    // name renders as an empty label rather than dropping the metric.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_jobs_failed_total",
            &[("queue", "orders"), ("kind", "panic"), ("name", "")]
        ),
        Some(1),
    );

    // Retry counter has queue + name labels.
    assert_eq!(
        counter_value(
            &counters,
            "chasquimq_retries_scheduled_total",
            &[("queue", "orders"), ("name", "send-email")]
        ),
        Some(2),
    );

    // DLQ counter is split by `reason` + `name` labels, all carrying queue.
    let dlq_cases: &[(&str, &str, u64)] = &[
        ("retries_exhausted", "send-email", 1),
        ("decode_failed", "", 1),
        ("malformed", "", 1),
        ("oversize_payload", "", 1),
    ];
    for (reason, name, want) in dlq_cases {
        assert_eq!(
            counter_value(
                &counters,
                "chasquimq_dlq_routed_total",
                &[("queue", "orders"), ("reason", reason), ("name", name)]
            ),
            Some(*want),
            "missing dlq counter for reason={reason} name={name:?}: {counters:?}"
        );
    }
}
