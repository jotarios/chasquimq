//! Bridge ChasquiMQ's [`MetricsSink`](chasquimq::MetricsSink) into the
//! [`metrics`](https://docs.rs/metrics) facade.
//!
//! Wire it up once at startup and every globally-installed `metrics`
//! recorder (Prometheus, StatsD, OTel, debug-print) automatically receives
//! ChasquiMQ's promoter telemetry.
//!
//! # Example
//!
//! ```no_run
//! use chasquimq::PromoterConfig;
//! use chasquimq_metrics::MetricsFacadeSink;
//! use std::sync::Arc;
//!
//! // After installing your metrics-rs recorder of choice, e.g. via
//! // `metrics_exporter_prometheus::PrometheusBuilder::new().install()`:
//! let cfg = PromoterConfig {
//!     metrics: Arc::new(MetricsFacadeSink::new()),
//!     ..Default::default()
//! };
//! ```
//!
//! # Emitted metrics
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `chasquimq_promoter_promoted_total` | counter | Jobs moved from delayed ZSET → stream this tick |
//! | `chasquimq_delayed_zset_depth` | gauge | `ZCARD` after promotion |
//! | `chasquimq_promoter_oldest_pending_lag_ms` | gauge | Lag of oldest still-pending entry |
//! | `chasquimq_promoter_lock_acquired_total` | counter | Transitions to leader |
//! | `chasquimq_promoter_lock_lost_total` | counter | Transitions away from leader |
//!
//! All metrics are emitted without labels by default. To attach labels (e.g.
//! per-queue), wrap your own [`MetricsSink`](chasquimq::MetricsSink) impl
//! that delegates to a [`MetricsFacadeSink`] after enriching the call —
//! `MetricsSink` is `Send + Sync + 'static`, so composing two sinks is
//! straightforward.

use chasquimq::{LockOutcome, MetricsSink, PromoterTick};
use metrics::{counter, gauge};

/// `MetricsSink` impl that emits via the global `metrics` facade. Construct
/// once and pass into `PromoterConfig::metrics` / `ConsumerConfig::metrics`.
///
/// # Why per-call macros (not cached handles)
///
/// Each `promoter_tick` call resolves the handle through `counter!()` /
/// `gauge!()` rather than caching a `Counter`/`Gauge` at construction.
/// Two reasons:
///
/// 1. **Late-recorder safety.** A user who constructs the sink before
///    installing their global recorder (very natural in `main()` setup
///    code) would otherwise freeze a no-op handle that keeps dropping
///    events even after the real recorder is installed.
/// 2. **Per-call cost is one HashMap lookup + Arc clone** in real
///    recorders (verified for `metrics-exporter-prometheus`'s
///    `FrozenRecorder` and `FreezableRecorder`). At promoter tick rates
///    (50ms+ default), the lookup is well under 1% of the per-tick budget.
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
        gauge!("chasquimq_promoter_oldest_pending_lag_ms")
            .set(tick.oldest_pending_lag_ms as f64);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use std::sync::Arc;

    fn install_debug_recorder() -> Snapshotter {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install recorder");
        snapshotter
    }

    /// Snapshot once and bucket the values; calling `snapshot()` repeatedly
    /// can yield different numbers because some recorder implementations
    /// reset counter handles between calls. One snapshot per assertion run.
    fn collect(
        snapshotter: &Snapshotter,
    ) -> (
        std::collections::HashMap<String, u64>,
        std::collections::HashMap<String, f64>,
    ) {
        let mut counters = std::collections::HashMap::new();
        let mut gauges = std::collections::HashMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let name = key.key().name().to_string();
            match value {
                DebugValue::Counter(n) => {
                    *counters.entry(name).or_insert(0) += n;
                }
                DebugValue::Gauge(n) => {
                    gauges.insert(name, n.into_inner());
                }
                _ => {}
            }
        }
        (counters, gauges)
    }

    #[test]
    fn forwards_tick_and_lock_events() {
        let snapshotter = install_debug_recorder();
        let sink: Arc<dyn MetricsSink> = Arc::new(MetricsFacadeSink::new());

        sink.promoter_tick(PromoterTick {
            promoted: 7,
            depth: 42,
            oldest_pending_lag_ms: 1500,
        });
        sink.promoter_tick(PromoterTick {
            promoted: 3,
            depth: 100,
            oldest_pending_lag_ms: 250,
        });
        sink.promoter_lock_outcome(LockOutcome::Acquired);
        sink.promoter_lock_outcome(LockOutcome::Held);
        sink.promoter_lock_outcome(LockOutcome::Held);

        let (counters, gauges) = collect(&snapshotter);

        // Counter: cumulative across both ticks.
        assert_eq!(
            counters.get("chasquimq_promoter_promoted_total").copied(),
            Some(10),
            "counters seen: {counters:?}"
        );
        // Gauges: take the most recent value.
        assert_eq!(
            gauges.get("chasquimq_delayed_zset_depth").copied(),
            Some(100.0)
        );
        assert_eq!(
            gauges
                .get("chasquimq_promoter_oldest_pending_lag_ms")
                .copied(),
            Some(250.0)
        );
        // Lock counters: per-event increments.
        assert_eq!(
            counters
                .get("chasquimq_promoter_lock_acquired_total")
                .copied(),
            Some(1)
        );
        assert_eq!(
            counters.get("chasquimq_promoter_lock_lost_total").copied(),
            Some(2)
        );

    }
}
