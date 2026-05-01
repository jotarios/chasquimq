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

fn fast_retry_consumer_cfg(queue: &str, consumer_id: &str, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: consumer_id.to_string(),
        max_attempts,
        block_ms: 50,
        delayed_poll_interval_ms: 25,
        retry: RetryConfig {
            initial_backoff_ms: 50,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn handler_err_then_ok_after_backoff() {
    let admin = admin().await;
    let queue = "retry_recover";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), fast_retry_consumer_cfg(queue, "c1", 5));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let started = Instant::now();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        let n = calls.fetch_add(1, Ordering::SeqCst);
                        if n < 2 {
                            Err(HandlerError::new(std::io::Error::other("flaky")))
                        } else {
                            Ok(())
                        }
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(10), || {
        let calls = calls.clone();
        async move { calls.load(Ordering::SeqCst) == 3 }
    })
    .await;

    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(140),
        "elapsed too short for 50ms+100ms backoffs, got {elapsed:?}"
    );

    let dlq = dlq_key(queue);
    assert_eq!(xlen(&admin, &dlq).await, 0, "DLQ must remain empty");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn always_err_lands_in_dlq_fast() {
    let admin = admin().await;
    let queue = "retry_dlq";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), fast_retry_consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let started = Instant::now();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
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

    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "DLQ landing should be fast with 50/100/200ms backoffs, took {elapsed:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "exactly max_attempts handler calls"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn attempt_counter_survives_promote_round_trip() {
    let admin = admin().await;
    let queue = "retry_attempt_persist";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let observed_attempts: Arc<std::sync::Mutex<Vec<u32>>> =
        Arc::new(std::sync::Mutex::new(vec![]));
    let observed_h = observed_attempts.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), fast_retry_consumer_cfg(queue, "c1", 4));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let observed = observed_h.clone();
                    async move {
                        observed.lock().unwrap().push(job.attempt);
                        if job.attempt < 2 {
                            Err(HandlerError::new(std::io::Error::other("flaky")))
                        } else {
                            Ok(())
                        }
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let observed = observed_attempts.clone();
        async move { observed.lock().unwrap().len() == 3 }
    })
    .await;

    let attempts = observed_attempts.lock().unwrap().clone();
    assert_eq!(
        attempts,
        vec![0, 1, 2],
        "in-payload attempt counter should monotonically increase across retries: {attempts:?}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn panic_treated_as_retry() {
    let admin = admin().await;
    let queue = "retry_panic";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), fast_retry_consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        panic!("boom");
                        #[allow(unreachable_code)]
                        Ok(())
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
        3,
        "panic path retries the same number of times as Err"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}
