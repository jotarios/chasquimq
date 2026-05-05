//! Bridge ChasquiMQ's [`MetricsSink`](chasquimq::MetricsSink) into the
//! [`metrics`](https://docs.rs/metrics) facade.
//!
//! Wire it up once at startup and every globally-installed `metrics`
//! recorder (Prometheus, StatsD, OTel, debug-print) automatically receives
//! ChasquiMQ's telemetry.
//!
//! # Example
//!
//! ```no_run
//! use chasquimq::PromoterConfig;
//! use chasquimq_metrics::{MetricsFacadeSink, QueueLabeled};
//! use std::sync::Arc;
//!
//! // After installing your metrics-rs recorder of choice, e.g. via
//! // `metrics_exporter_prometheus::PrometheusBuilder::new().install()`:
//! let cfg = PromoterConfig {
//!     metrics: Arc::new(QueueLabeled::new(MetricsFacadeSink::new(), "default")),
//!     ..Default::default()
//! };
//! ```
//!
//! # Module layout
//!
//! - [`facade`] — [`MetricsFacadeSink`], the unlabeled base sink.
//! - [`labeled`] — [`QueueLabeled<S>`], wrapper that adds a `queue` label
//!   to any inner sink.
//!
//! # Emitted metrics
//!
//! Promoter:
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `chasquimq_promoter_promoted_total` | counter | Jobs moved from delayed ZSET → stream this tick |
//! | `chasquimq_delayed_zset_depth` | gauge | `ZCARD` after promotion |
//! | `chasquimq_promoter_oldest_pending_lag_ms` | gauge | Lag of oldest still-pending entry |
//! | `chasquimq_promoter_lock_acquired_total` | counter | Transitions to leader |
//! | `chasquimq_promoter_lock_lost_total` | counter | Transitions away from leader |
//!
//! Reader / worker / retry / DLQ:
//!
//! | Metric | Type | Meaning |
//! |---|---|---|
//! | `chasquimq_consumer_batch_size` | histogram | Entries returned by a non-empty `XREADGROUP` |
//! | `chasquimq_consumer_reclaimed_total` | counter | Entries reclaimed via `CLAIM` (delivery_count > 1) |
//! | `chasquimq_jobs_completed_total{name=...}` | counter | Handler invocations that returned `Ok` |
//! | `chasquimq_jobs_failed_total{kind=...,name=...}` | counter | Handler invocations that returned `Err` (`error`) or panicked (`panic`) |
//! | `chasquimq_handler_duration_seconds{name=...}` | histogram | Wall-clock per handler invocation (Prometheus convention) |
//! | `chasquimq_retries_scheduled_total{name=...}` | counter | Retries that were actually rescheduled (script gate fired) |
//! | `chasquimq_retry_backoff_seconds` | histogram | Backoff applied per retry (Prometheus convention) |
//! | `chasquimq_dlq_routed_total{reason=...,name=...}` | counter | DLQ relocations by reason (`retries_exhausted` / `decode_failed` / `malformed` / `oversize_payload`) |
//!
//! Per-job metrics carry a `name` label sourced from the source stream
//! entry's `n` field (slice 5 of name-on-the-wire). Empty name renders as
//! an empty label rather than dropping the metric — pin one expectation in
//! the recorder rather than two. Histogram bucket configuration (e.g. for
//! `chasquimq_handler_duration_seconds`) is recorder-side: tune via your
//! Prometheus / OTel exporter, not here.
//!
//! # Adding labels
//!
//! [`MetricsFacadeSink`] emits unlabeled metrics. To attach a `queue` label
//! (the common case), wrap it in [`QueueLabeled`]. The wrapper composes —
//! you can stack your own wrappers around it for tenant / region / etc.
//! without touching this crate.

pub mod facade;
pub mod labeled;

pub use facade::MetricsFacadeSink;
pub use labeled::QueueLabeled;
