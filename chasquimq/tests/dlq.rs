use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, dlq_key, stream_key};
use chasquimq::{HandlerError, Job};
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
        max_stream_len: 10_000,
    }
}

fn consumer_cfg(queue: &str, consumer_id: &str, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: consumer_id.to_string(),
        batch: 16,
        block_ms: 50,
        claim_min_idle_ms: 100,
        concurrency: 4,
        max_attempts,
        ack_batch: 16,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn err_repeated_lands_in_dlq() {
    let admin = admin().await;
    let queue = "dlq_err";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await >= 1
                && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    let total_calls = calls.load(Ordering::SeqCst);
    assert!(
        total_calls >= 3,
        "handler should have been retried at least max_attempts times, got {total_calls}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn err_then_ok_succeeds_normally() {
    let admin = admin().await;
    let queue = "dlq_recover";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1", 5));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
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

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        async move {
            xlen(&admin, &main_key).await == 0
                && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    assert_eq!(xlen(&admin, &dlq).await, 0, "DLQ should be empty");
    let total_calls = calls.load(Ordering::SeqCst);
    assert!((3..=6).contains(&total_calls), "calls = {total_calls}");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn panic_treated_as_err() {
    let admin = admin().await;
    let queue = "dlq_panic";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| async move {
                    panic!("boom");
                    #[allow(unreachable_code)]
                    Ok(())
                },
                shutdown_clone,
            )
            .await
    });

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await >= 1
                && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn oversize_payload_lands_in_dlq() {
    let admin = admin().await;
    let queue = "dlq_oversize";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let big = bytes::Bytes::from(vec![0u8; 4096]);
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(main_key.as_str()),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(big),
            ],
        )
        .await
        .expect("XADD raw");

    let mut cfg = consumer_cfg(queue, "c1", 3);
    cfg.max_payload_bytes = 1024;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(10), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await >= 1
                && xpending_count(&admin, &main_key, "default").await == 0
                && xlen(&admin, &main_key).await == 0
        }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "handler must never decode an oversize payload"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn malformed_entry_without_payload_field_lands_in_dlq() {
    let admin = admin().await;
    let queue = "dlq_malformed";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(main_key.as_str()),
                Value::from("*"),
                Value::from("not_d"),
                Value::from("nope"),
            ],
        )
        .await
        .expect("XADD with wrong field name");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(10), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await >= 1
                && xpending_count(&admin, &main_key, "default").await == 0
                && xlen(&admin, &main_key).await == 0
        }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "handler must never see a malformed entry"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn poison_message_lands_in_dlq() {
    let admin = admin().await;
    let queue = "dlq_poison";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(main_key.as_str()),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes::Bytes::from_static(b"\xff\xff\xff\xff")),
            ],
        )
        .await
        .expect("XADD raw junk");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, "c1", 3));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(10), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await >= 1
                && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "handler must never see poison bytes"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}
