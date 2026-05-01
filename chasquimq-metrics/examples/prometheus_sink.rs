//! Example: wire `MetricsSink` to the `prometheus` crate.
//!
//! Run with:
//!
//! ```bash
//! REDIS_URL=redis://127.0.0.1:6379 \
//!   cargo run --example prometheus_sink
//! ```
//!
//! Then in another terminal:
//!
//! ```bash
//! curl http://127.0.0.1:9898/metrics
//! ```
//!
//! Output looks like:
//!
//! ```text
//! # HELP chasquimq_promoter_promoted_total Jobs promoted from the delayed ZSET to the stream by this promoter.
//! # TYPE chasquimq_promoter_promoted_total counter
//! chasquimq_promoter_promoted_total 0
//! # HELP chasquimq_delayed_zset_depth ZCARD of the delayed sorted set after the most recent promote tick.
//! # TYPE chasquimq_delayed_zset_depth gauge
//! chasquimq_delayed_zset_depth 0
//! ...
//! ```
//!
//! The `tiny_http` server below is example-only — in production, wire the
//! `gather()` / `encode()` calls into your existing HTTP framework
//! (Axum / Actix / hyper / etc). The `MetricsSink` impl is the part you
//! actually copy.

use chasquimq::metrics::{LockOutcome, MetricsSink, PromoterTick};
use chasquimq::{Promoter, PromoterConfig};
use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Bridges ChasquiMQ promoter events into a Prometheus registry.
///
/// `IntCounter::inc_by` and `IntGauge::set` are O(1) atomic operations — the
/// per-event cost is negligible, which is what `MetricsSink`'s contract
/// requires.
struct PrometheusSink {
    promoted_total: IntCounter,
    delayed_zset_depth: IntGauge,
    oldest_pending_lag_ms: IntGauge,
    lock_acquired_total: IntCounter,
    lock_lost_total: IntCounter,
}

impl PrometheusSink {
    fn new(registry: &Registry) -> Self {
        let promoted_total = IntCounter::new(
            "chasquimq_promoter_promoted_total",
            "Jobs promoted from the delayed ZSET to the stream by this promoter.",
        )
        .expect("counter");
        let delayed_zset_depth = IntGauge::new(
            "chasquimq_delayed_zset_depth",
            "ZCARD of the delayed sorted set after the most recent promote tick.",
        )
        .expect("gauge");
        let oldest_pending_lag_ms = IntGauge::new(
            "chasquimq_promoter_oldest_pending_lag_ms",
            "Milliseconds the oldest still-pending entry is overdue. \
             Saturates at 0 when the queue is caught up.",
        )
        .expect("gauge");
        let lock_acquired_total = IntCounter::new(
            "chasquimq_promoter_lock_acquired_total",
            "Number of times this promoter transitioned to leader.",
        )
        .expect("counter");
        let lock_lost_total = IntCounter::new(
            "chasquimq_promoter_lock_lost_total",
            "Number of times this promoter transitioned away from leader.",
        )
        .expect("counter");

        registry.register(Box::new(promoted_total.clone())).unwrap();
        registry.register(Box::new(delayed_zset_depth.clone())).unwrap();
        registry
            .register(Box::new(oldest_pending_lag_ms.clone()))
            .unwrap();
        registry.register(Box::new(lock_acquired_total.clone())).unwrap();
        registry.register(Box::new(lock_lost_total.clone())).unwrap();

        Self {
            promoted_total,
            delayed_zset_depth,
            oldest_pending_lag_ms,
            lock_acquired_total,
            lock_lost_total,
        }
    }
}

impl MetricsSink for PrometheusSink {
    fn promoter_tick(&self, tick: PromoterTick) {
        self.promoted_total.inc_by(tick.promoted);
        self.delayed_zset_depth.set(tick.depth as i64);
        self.oldest_pending_lag_ms
            .set(tick.oldest_pending_lag_ms as i64);
    }

    fn promoter_lock_outcome(&self, outcome: LockOutcome) {
        match outcome {
            LockOutcome::Acquired => self.lock_acquired_total.inc(),
            LockOutcome::Held => self.lock_lost_total.inc(),
        }
    }
}

/// Minimal `tiny_http`-backed `/metrics` endpoint. Production code should
/// use the HTTP framework that already exists in the application.
fn serve_metrics(registry: Arc<Registry>, addr: &str, shutdown: CancellationToken) {
    let server = tiny_http::Server::http(addr).expect("bind metrics server");
    println!("metrics endpoint: http://{addr}/metrics");
    while !shutdown.is_cancelled() {
        // 250ms recv timeout so we notice shutdown promptly without busy-looping.
        let req = match server.recv_timeout(std::time::Duration::from_millis(250)) {
            Ok(Some(req)) => req,
            Ok(None) => continue,
            Err(_) => break,
        };
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        if encoder.encode(&registry.gather(), &mut buf).is_err() {
            let _ = req.respond(tiny_http::Response::empty(500));
            continue;
        }
        let response = tiny_http::Response::from_data(buf)
            .with_header("Content-Type: text/plain; version=0.0.4".parse::<tiny_http::Header>().unwrap());
        let _ = req.respond(response);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let queue_name = std::env::var("QUEUE_NAME").unwrap_or_else(|_| "default".into());
    let metrics_addr =
        std::env::var("METRICS_ADDR").unwrap_or_else(|_| "127.0.0.1:9898".into());

    let registry = Arc::new(Registry::new());
    let sink: Arc<dyn MetricsSink> = Arc::new(PrometheusSink::new(&registry));

    let cfg = PromoterConfig {
        queue_name,
        metrics: sink,
        ..Default::default()
    };

    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.cancel();
    });

    // Serve /metrics on a dedicated blocking thread so tiny_http's blocking
    // recv() doesn't starve the tokio runtime.
    let metrics_registry = Arc::clone(&registry);
    let metrics_shutdown = shutdown.clone();
    let metrics_thread = std::thread::spawn(move || {
        serve_metrics(metrics_registry, &metrics_addr, metrics_shutdown);
    });

    let promoter_outcome = Promoter::new(redis_url, cfg).run(shutdown).await;

    // Wait for the metrics server to notice `shutdown.cancelled()` and exit
    // cleanly so any in-flight /metrics request finishes responding before
    // the process tears down.
    let _ = metrics_thread.join();

    promoter_outcome?;
    Ok(())
}
