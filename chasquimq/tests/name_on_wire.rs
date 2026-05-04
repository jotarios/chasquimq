//! End-to-end tests for slice 1 of the name-on-wire feature: producer adds
//! a job with a `name`, consumer reads it back via the engine's normal hot
//! path, and observes `Job::name` in the handler. Forward-compat is also
//! pinned: a consumer running against entries with no `n` field decodes
//! `Job::name == ""`, regardless of whether the producer was on the new
//! shape or hand-built the XADD without `n`.

use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::job::Job;
use chasquimq::producer::{AddOptions, Producer, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
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
        concurrency: 4,
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled: false,
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
            panic!("wait_until timed out after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Helper: spin up a consumer that records every `(name, payload)` it sees
/// onto a shared `Vec`, then return after `expected` jobs land.
async fn drive_consumer(queue: &str, consumer_id: &str, expected: usize) -> Vec<(String, u32)> {
    let observed: Arc<Mutex<Vec<(String, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_h = observed.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, consumer_id));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let observed = observed_h.clone();
                    async move {
                        observed
                            .lock()
                            .unwrap()
                            .push((job.name.clone(), job.payload.n));
                        Ok(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let observed_check = observed.clone();
    wait_until(Duration::from_secs(15), move || {
        let observed = observed_check.clone();
        async move { observed.lock().unwrap().len() >= expected }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;

    let mut out = observed.lock().unwrap().clone();
    out.sort_by_key(|(_, n)| *n);
    out
}

/// Slice 1 happy path: producer adds a named job via `add_with_options`,
/// consumer observes `Job::name == "<the-name>"` end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn named_job_round_trips_to_consumer() {
    let admin = admin().await;
    let queue = "name_round_trip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer
        .add_with_options(Sample { n: 7 }, AddOptions::new().with_name("send-email"))
        .await
        .expect("add_with_options");

    let observed = drive_consumer(queue, "name_c1", 1).await;
    assert_eq!(observed, vec![("send-email".to_string(), 7)]);

    let _: () = admin.quit().await.unwrap();
}

/// Forward-compat: the legacy single-arg `add(payload)` does NOT attach an
/// `n` field (producer omits it on the wire). A consumer reading that entry
/// observes `Job::name == ""`. This is what makes mixed-version deploys safe
/// and what the design doc means by "absent and empty are equivalent".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn unnamed_producer_path_yields_empty_name_at_consumer() {
    let admin = admin().await;
    let queue = "name_unnamed_path";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 11 }).await.expect("add");

    let observed = drive_consumer(queue, "name_c2", 1).await;
    assert_eq!(observed, vec![(String::new(), 11)]);

    let _: () = admin.quit().await.unwrap();
}

/// Forward-compat (hand-crafted): mimic a pre-slice-1 producer by issuing
/// an `XADD ... d <bytes>` directly with no `n` field at all. The consumer
/// must still decode `Job::name == ""`. This is the case the parser's
/// missing-field fallback exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn pre_slice1_xadd_without_n_field_decodes_with_empty_name() {
    let admin = admin().await;
    let queue = "name_legacy_xadd";
    let key = stream_key(queue);
    flush_all(&admin, queue).await;

    // Hand-build a `Job<Sample>` and write it under `d` only — exactly the
    // shape a pre-slice-1 producer would emit.
    let job: Job<Sample> = Job::new(Sample { n: 23 });
    let bytes = rmp_serde::to_vec(&job).expect("encode");

    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(key.clone()),
                Value::from("MAXLEN"),
                Value::from("~"),
                Value::from(100_000_i64),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes::Bytes::from(bytes)),
            ],
        )
        .await
        .expect("XADD legacy");

    let observed = drive_consumer(queue, "name_c3", 1).await;
    assert_eq!(observed, vec![(String::new(), 23)]);

    let _: () = admin.quit().await.unwrap();
}

/// Bulk variant of the round-trip: each `(name, payload)` pair routes to
/// the matching observed entry on the consumer side. This pins the
/// per-entry name semantic for `add_bulk_named` (vs. shared-name in
/// `add_bulk_with_options`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_named_round_trips_per_entry_names() {
    let admin = admin().await;
    let queue = "name_bulk_named";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let items = vec![
        ("send-email".to_string(), Sample { n: 1 }),
        ("resize-image".to_string(), Sample { n: 2 }),
        ("post-webhook".to_string(), Sample { n: 3 }),
    ];
    producer
        .add_bulk_named(items)
        .await
        .expect("add_bulk_named");

    let observed = drive_consumer(queue, "name_c4", 3).await;
    assert_eq!(
        observed,
        vec![
            ("send-email".to_string(), 1),
            ("resize-image".to_string(), 2),
            ("post-webhook".to_string(), 3),
        ]
    );

    let _: () = admin.quit().await.unwrap();
}
