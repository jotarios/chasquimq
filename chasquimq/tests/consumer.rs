use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
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
    for suffix in ["stream", "dlq"] {
        let key = format!("chasqui:{queue}:{suffix}");
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

async fn xpending_count(admin: &Client, key: &str, group: &str) -> i64 {
    let res = admin
        .custom::<Value, Value>(
            CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
            vec![Value::from(key), Value::from(group)],
        )
        .await
        .expect("XPENDING");
    match res {
        Value::Array(items) => match items.first() {
            Some(Value::Integer(n)) => *n,
            _ => 0,
        },
        _ => 0,
    }
}

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
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
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
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
            panic!("wait_until timed out after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn two_consumers_split_work() {
    init_tracing();
    let admin = admin().await;
    let queue = "c_split";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let payloads: Vec<Sample> = (0..1_000).map(|n| Sample { n }).collect();
    producer.add_bulk(payloads).await.expect("add_bulk");

    let count_a = Arc::new(AtomicUsize::new(0));
    let count_b = Arc::new(AtomicUsize::new(0));

    let shutdown = CancellationToken::new();

    let consumer_a: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "ca"));
    let count_a_h = count_a.clone();
    let shutdown_a = shutdown.clone();
    let join_a = tokio::spawn(async move {
        consumer_a
            .run(
                move |_job| {
                    let counter = count_a_h.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_a,
            )
            .await
    });

    let consumer_b: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "cb"));
    let count_b_h = count_b.clone();
    let shutdown_b = shutdown.clone();
    let join_b = tokio::spawn(async move {
        consumer_b
            .run(
                move |_job| {
                    let counter = count_b_h.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_b,
            )
            .await
    });

    wait_until(Duration::from_secs(15), || {
        let count_a = count_a.clone();
        let count_b = count_b.clone();
        async move {
            count_a.load(Ordering::SeqCst) + count_b.load(Ordering::SeqCst) == 1_000
        }
    })
    .await;

    let a = count_a.load(Ordering::SeqCst);
    let b = count_b.load(Ordering::SeqCst);
    assert!(a >= 100 && b >= 100, "uneven split: a={a} b={b}");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        let _ = join_a.await;
        let _ = join_b.await;
    })
    .await;

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn consumer_happy_path_acks_all() {
    init_tracing();
    let admin = admin().await;
    let queue = "c_happy";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let payloads: Vec<Sample> = (0..1_000).map(|n| Sample { n }).collect();
    producer.add_bulk(payloads).await.expect("add_bulk");

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_handler = counter.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job| {
                    let counter = counter_handler.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(15), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1_000 }
    })
    .await;

    let key = stream_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let key = key.clone();
        async move {
            xlen(&admin, &key).await == 0
                && xpending_count(&admin, &key, "default").await == 0
        }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;

    let _: () = admin.quit().await.unwrap();
}
