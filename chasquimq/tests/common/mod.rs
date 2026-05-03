//! Shared helpers for the observability integration tests.
//!
//! Cargo treats `tests/*.rs` as separate test binaries; sub-directory
//! modules (like this one) are *not* compiled as their own binary, only
//! when a sibling binary does `mod common;`. Helpers live here so each
//! domain-focused test file (`observability_promoter.rs`,
//! `observability_lifecycle.rs`, `observability_reader_dlq.rs`) can pull
//! them in without duplication.
//!
//! Some helpers in here are unused by some of the binaries that import
//! this module. Cargo's per-binary dead-code analysis would warn — we
//! `allow(dead_code)` for that reason rather than gating each helper
//! behind a feature flag (which would be heavier).

#![allow(dead_code)]

use chasquimq::config::{ProducerConfig, PromoterConfig};
use chasquimq::metrics::MetricsSink;
use chasquimq::{ConsumerConfig, RetryConfig};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Sample {
    pub n: u32,
}

pub fn redis_url() -> String {
    std::env::var("REDIS_URL").expect("REDIS_URL must be set to run integration tests")
}

pub async fn admin() -> Client {
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

pub async fn flush_all(admin: &Client, queue: &str) {
    for suffix in ["stream", "dlq", "delayed", "promoter:lock", "events"] {
        let key = format!("{{chasqui:{queue}}}:{suffix}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                vec![Value::from(key)],
            )
            .await
            .expect("DEL");
    }
}

pub async fn wait_until<F, Fut>(timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if check().await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("wait_until timed out after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
        ..Default::default()
    }
}

pub fn promoter_cfg_with_sink(
    queue: &str,
    holder_id: &str,
    sink: Arc<dyn MetricsSink>,
) -> PromoterConfig {
    PromoterConfig {
        queue_name: queue.to_string(),
        poll_interval_ms: 50,
        promote_batch: 256,
        max_stream_len: 100_000,
        lock_ttl_secs: 5,
        holder_id: holder_id.to_string(),
        metrics: sink,
        ..Default::default()
    }
}

pub async fn zcard(admin: &Client, key: &str) -> i64 {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("ZCARD", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("ZCARD")
    {
        Value::Integer(n) => n,
        Value::Null => 0,
        other => panic!("ZCARD unexpected: {other:?}"),
    }
}

/// Lifecycle / reader-DLQ tests use this. Tightly tuned: tiny backoff +
/// zero jitter so retry tests don't sit around for 100ms × 2^N, and
/// `delayed_enabled: false` by default so the consumer doesn't spawn an
/// inline promoter (tests that need the promoter override that field).
pub fn consumer_cfg(queue: &str, sink: Arc<dyn MetricsSink>, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: format!("c-{}", uuid::Uuid::new_v4()),
        block_ms: 50,
        retry: RetryConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 500,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        max_attempts,
        delayed_enabled: false,
        concurrency: 4,
        metrics: sink,
        ..Default::default()
    }
}

pub async fn raw_xadd(admin: &Client, stream_key: &str, field: &str, payload: Vec<u8>) {
    let args: Vec<Value> = vec![
        Value::from(stream_key),
        Value::from("*"),
        Value::from(field),
        Value::Bytes(payload.into()),
    ];
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            args,
        )
        .await
        .expect("raw XADD");
}
