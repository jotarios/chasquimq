//! Example: the canonical path through `chasquimq-metrics`.
//!
//! Wire `MetricsFacadeSink` (this crate) into a `metrics-rs` global
//! recorder (here, `metrics-exporter-prometheus`'s built-in HTTP listener)
//! and let the existing `metrics` ecosystem do the heavy lifting.
//!
//! Run with:
//!
//! ```bash
//! REDIS_URL=redis://127.0.0.1:6379 \
//!   cargo run --example facade_sink -p chasquimq-metrics
//! ```
//!
//! Then in another terminal:
//!
//! ```bash
//! curl http://127.0.0.1:9899/metrics
//! ```
//!
//! Compared with `examples/prometheus_sink.rs` (which writes a
//! `MetricsSink` impl directly against `prometheus::IntCounter`), this
//! example is shorter, has no `unsafe`-ish `Registry` plumbing, and
//! automatically picks up any other `counter!` / `gauge!` calls happening
//! in the same process — that's the value of going through the facade.

use chasquimq::{Promoter, PromoterConfig};
use chasquimq_metrics::{MetricsFacadeSink, QueueLabeled};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let queue_name = std::env::var("QUEUE_NAME").unwrap_or_else(|_| "default".into());
    let metrics_addr: SocketAddr = std::env::var("METRICS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9899".into())
        .parse()
        .expect("METRICS_ADDR must be a valid socket address");

    // Install the metrics-rs global recorder. `with_http_listener` spawns a
    // background task that serves /metrics — we don't write any HTTP code
    // ourselves. This is the install-recorder-first ordering that
    // `MetricsFacadeSink::new()`'s docs describe.
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()
        .expect("install prometheus recorder");

    println!("metrics endpoint: http://{metrics_addr}/metrics");

    // Wrap the base sink in `QueueLabeled` so every emitted metric carries a
    // `queue=<name>` label. Cheap, composable, and the canonical pattern for
    // multi-queue processes — stack additional wrappers (tenant, region) the
    // same way.
    let cfg = PromoterConfig {
        queue_name: queue_name.clone(),
        metrics: Arc::new(QueueLabeled::new(MetricsFacadeSink::new(), queue_name)),
        ..Default::default()
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    Promoter::new(redis_url, cfg).run(shutdown).await?;
    Ok(())
}
