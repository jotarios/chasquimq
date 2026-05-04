//! Integration tests for `HandlerError::unrecoverable`.
//!
//! A handler that returns `HandlerError::unrecoverable(err)` short-circuits
//! the retry budget: the consumer routes the job straight to the DLQ with
//! `DlqReason::Unrecoverable`, regardless of the queue-wide or per-job
//! `max_attempts` setting.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, dlq_key};
use chasquimq::{HandlerError, Job};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

fn redis_url() -> String {
    std::env::var("REDIS_URL").expect("REDIS_URL must be set to run integration tests")
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Sample {
    n: u32,
}

async fn admin() -> Client {
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

async fn flush_all(admin: &Client, queue: &str) {
    for suffix in ["stream", "dlq", "delayed", "promoter:lock"] {
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

async fn xlen(admin: &Client, key: &str) -> i64 {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("XLEN")
    {
        Value::Integer(n) => n,
        Value::Null => 0,
        other => panic!("XLEN unexpected: {other:?}"),
    }
}

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 10_000,
        ..Default::default()
    }
}

fn fast_consumer_cfg(queue: &str, consumer_id: &str, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: consumer_id.to_string(),
        max_attempts,
        block_ms: 50,
        delayed_poll_interval_ms: 25,
        retry: RetryConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 200,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        ..Default::default()
    }
}

async fn wait_until<F, Fut>(timeout: Duration, mut check: F)
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

/// A handler that always returns `HandlerError::unrecoverable` runs exactly
/// once and lands in the DLQ with reason `unrecoverable` — even though the
/// queue's `max_attempts` is generously set to 5.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn handler_unrecoverable_routes_directly_to_dlq() {
    let admin = admin().await;
    let queue = "unrecoverable_direct_dlq";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    // Queue-wide budget is 5: a normal `HandlerError::new` would retry 5 times
    // before DLQing. With `HandlerError::unrecoverable` the handler must run
    // exactly once.
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 5));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::unrecoverable(std::io::Error::other(
                            "terminal failure",
                        )))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dlq = dlq.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    // The crucial assertion: the handler ran exactly once. A flag-failure
    // here would mean the engine retried an unrecoverable error.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "unrecoverable error must produce exactly 1 handler invocation"
    );

    // The DLQ entry's reason field must record the new variant so operators
    // can distinguish terminal failures from exhausted retries.
    let entries = producer.peek_dlq(10).await.expect("peek_dlq");
    assert_eq!(entries.len(), 1, "exactly 1 DLQ entry expected");
    assert_eq!(
        entries[0].reason, "unrecoverable",
        "DLQ entry must carry the unrecoverable reason"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// `HandlerError::unrecoverable` overrides per-job retry overrides too:
/// even with a per-job `max_attempts: 10`, an unrecoverable error must
/// run the handler exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn unrecoverable_beats_per_job_retry_override() {
    use chasquimq::JobRetryOverride;
    use chasquimq::producer::AddOptions;

    let admin = admin().await;
    let queue = "unrecoverable_beats_per_job";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    // Per-job override of 10 retries — would normally produce 10 invocations.
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(10),
                backoff: None,
            }),
        )
        .await
        .expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::unrecoverable(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dlq = dlq.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "unrecoverable must beat per-job max_attempts=10"
    );

    let entries = producer.peek_dlq(10).await.expect("peek_dlq");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].reason, "unrecoverable");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Sanity check on the constructor / accessor — pure unit semantics, no
/// Redis. Locks the public API so renames don't slip past CI silently.
#[test]
fn constructor_and_accessor_round_trip() {
    let recoverable = HandlerError::new(std::io::Error::other("retry me"));
    assert!(!recoverable.is_unrecoverable());

    let terminal = HandlerError::unrecoverable(std::io::Error::other("game over"));
    assert!(terminal.is_unrecoverable());

    // Display still wraps the inner error with the `handler:` prefix.
    let s = format!("{terminal}");
    assert!(s.starts_with("handler:"), "Display output: {s}");
    assert!(s.contains("game over"), "Display output: {s}");
}
