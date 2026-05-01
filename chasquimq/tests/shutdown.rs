use chasquimq::Job;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn shutdown_drains_in_flight() {
    let admin = admin().await;
    let queue = "shutdown_drain";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(
        &redis_url(),
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size: 2,
            max_stream_len: 10_000,
            ..Default::default()
        },
    )
    .await
    .expect("producer");
    let payloads: Vec<Sample> = (0..100).map(|n| Sample { n }).collect();
    producer.add_bulk(payloads).await.expect("add_bulk");

    let cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: "c1".to_string(),
        batch: 32,
        block_ms: 50,
        claim_min_idle_ms: 30_000,
        concurrency: 10,
        max_attempts: 3,
        ack_batch: 32,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled: false,
        ..Default::default()
    };

    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let started_h = started.clone();
    let finished_h = finished.clone();

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let started = started_h.clone();
                    let finished = finished_h.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("consumer join did not finish in time")
        .expect("consumer task panicked");
    outcome.expect("consumer returned err");

    let s = started.load(Ordering::SeqCst);
    let f = finished.load(Ordering::SeqCst);
    assert!(s >= 10, "expected workers to have started, got {s}");
    assert_eq!(s, f, "all started handlers must finish under deadline");

    let key = stream_key(queue);
    let pending = xpending_count(&admin, &key, "default").await;
    assert!(
        pending <= (100 - f as i64),
        "in-flight should be acked or never-dispatched; pending={pending}, finished={f}"
    );

    let _: () = admin.quit().await.unwrap();
}
