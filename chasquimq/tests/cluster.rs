//! Redis Cluster end-to-end coverage.
//!
//! These tests are the proof that the engine's cluster story holds in
//! practice, not just on paper:
//!
//! - every key for one queue carries the `{chasqui:<queue>}` hash tag, so
//!   the queue's whole keyspace lands on a single slot, and
//! - every command / multi-key Lua script is dispatched with
//!   `ClusterHash::FirstKey`, so it routes to the slot that owns that tag.
//!
//! If either invariant ever regresses, a multi-key script (PROMOTE,
//! RELOCATE_DLQ, JOB_OK, ...) starts returning `CROSSSLOT` on a real
//! cluster and these tests fail loudly. They are `#[ignore]` and gated on
//! `REDIS_CLUSTER_URL` (a `redis-cluster://` seed URL) so a contributor
//! without a local cluster — and the single-node CI `test` job — skip
//! them; the dedicated `cluster` CI job provides the cluster and runs the
//! full file with `--include-ignored`.
//!
//! ```text
//!   producer ──XADD──▶ {chasqui:q}:stream ─┐
//!                                          │  same slot (hash tag)
//!   producer ──ZADD──▶ {chasqui:q}:delayed │  ⇒ PROMOTE / RELOCATE_DLQ /
//!   consumer ─XACKDEL▶ {chasqui:q}:dlq ─────┘     JOB_OK stay atomic
//! ```

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::Producer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn cluster_url() -> String {
    std::env::var("REDIS_CLUSTER_URL")
        .expect("REDIS_CLUSTER_URL (a redis-cluster:// seed URL) must be set")
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Sample {
    n: u32,
}

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
        ..Default::default()
    }
}

fn consumer_cfg(queue: &str, consumer_id: &str) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: consumer_id.to_string(),
        batch: 64,
        block_ms: 100,
        claim_min_idle_ms: 30_000,
        concurrency: 16,
        max_attempts: 2,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        // Retries reschedule onto the delayed ZSET, so the promoter must
        // be on (delayed_enabled) for a retryable failure to ever exhaust
        // its attempt budget and land in the DLQ. Fast backoff keeps the
        // DLQ test inside its 20s wait.
        delayed_enabled: true,
        delayed_poll_interval_ms: 50,
        retry: RetryConfig {
            initial_backoff_ms: 50,
            max_backoff_ms: 200,
            multiplier: 2.0,
            jitter_ms: 10,
        },
        ..Default::default()
    }
}

async fn wait_until<F, Fut>(timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if check().await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("wait_until timed out after {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

/// Bulk produce + concurrent consume + batched XACKDEL on a real cluster.
/// Exercises the producer XADD path and the consumer reader / ack hot
/// path — the two commands that matter for the headline throughput claim
/// — through fred's slot router.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_CLUSTER_URL"]
async fn cluster_produce_consume_ack() {
    init_tracing();
    let queue = "cl_happy";

    let producer: Producer<Sample> = Producer::connect(&cluster_url(), producer_cfg(queue))
        .await
        .expect("connect producer to cluster");
    let payloads: Vec<Sample> = (0..1_000).map(|n| Sample { n }).collect();
    producer.add_bulk(payloads).await.expect("add_bulk");

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_h = counter.clone();
    let consumer: Consumer<Sample> = Consumer::new(cluster_url(), consumer_cfg(queue, "cl1"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job| {
                    let counter = counter_h.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(20), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1_000 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
}

/// Delayed enqueue + promoter on a cluster. The PROMOTE script touches
/// `{chasqui:q}:delayed` and `{chasqui:q}:stream` in one EVAL; if those
/// ever landed on different slots it would `CROSSSLOT`. A green run is
/// proof the hash tag co-locates the ZSET and the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_CLUSTER_URL"]
async fn cluster_delayed_promote() {
    init_tracing();
    let queue = "cl_delayed";

    let producer: Producer<Sample> = Producer::connect(&cluster_url(), producer_cfg(queue))
        .await
        .expect("connect producer to cluster");
    for n in 0..50 {
        producer
            .add_in(Duration::from_millis(200), Sample { n })
            .await
            .expect("add_in");
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_h = counter.clone();
    let consumer: Consumer<Sample> = Consumer::new(cluster_url(), consumer_cfg(queue, "cld"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job| {
                    let counter = counter_h.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(20), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 50 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
}

/// Poison job → DLQ relocate on a cluster. The RELOCATE_DLQ script
/// `XACKDEL`s `{chasqui:q}:stream` and `XADD`s `{chasqui:q}:dlq` in one
/// invocation. Then peek the DLQ. A handler that always errors past
/// `max_attempts` must land the job in the DLQ without a `CROSSSLOT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_CLUSTER_URL"]
async fn cluster_dlq_relocate() {
    init_tracing();
    let queue = "cl_dlq";

    let producer: Producer<Sample> = Producer::connect(&cluster_url(), producer_cfg(queue))
        .await
        .expect("connect producer to cluster");
    producer.add(Sample { n: 7 }).await.expect("add");

    // delayed_enabled stays on: a retryable error reschedules onto the
    // delayed ZSET and only reaches the DLQ once the promoter has
    // replayed it past max_attempts.
    let consumer: Consumer<Sample> = Consumer::new(cluster_url(), consumer_cfg(queue, "cldlq"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job| async move {
                    Err::<chasquimq::Bytes, _>(chasquimq::HandlerError::new(
                        std::io::Error::other("always fails"),
                    ))
                },
                shutdown_clone,
            )
            .await
    });

    let peek_producer: Producer<Sample> = Producer::connect(&cluster_url(), producer_cfg(queue))
        .await
        .expect("connect peek producer");
    wait_until(Duration::from_secs(20), || {
        let p = &peek_producer;
        async move { p.peek_dlq(10).await.map(|v| !v.is_empty()).unwrap_or(false) }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
}

/// Result backend on a cluster. The JOB_OK script `XACKDEL`s
/// `{chasqui:q}:stream` and `SET`s `{chasqui:q}:result:<id>` in one EVAL;
/// the producer then `GET`s the result key. Same-slot co-location is what
/// keeps the gated result write atomic with the ack.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_CLUSTER_URL"]
async fn cluster_result_backend() {
    init_tracing();
    let queue = "cl_result";

    let producer: Producer<Sample> = Producer::connect(&cluster_url(), producer_cfg(queue))
        .await
        .expect("connect producer to cluster");
    let id = producer.add(Sample { n: 42 }).await.expect("add");

    let mut cfg = consumer_cfg(queue, "clres");
    cfg.delayed_enabled = false;
    cfg.store_results = true;
    cfg.result_ttl_secs = 60;
    let consumer: Consumer<Sample> = Consumer::new(cluster_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job| async move { Ok(chasquimq::Bytes::from_static(b"done")) },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(20), || {
        let p = &producer;
        let id = id.clone();
        async move {
            p.get_result(&id)
                .await
                .map(|r| r.as_deref() == Some(b"done".as_slice()))
                .unwrap_or(false)
        }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;
}
