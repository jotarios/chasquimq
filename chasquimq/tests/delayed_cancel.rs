//! Integration tests for `Producer::cancel_delayed` and
//! `cancel_delayed_bulk`.
//!
//! Cancel relies on a side-index key written by the slice 6 idempotent
//! schedule script: `{chasqui:<queue>}:didx:<job_id>` stores the exact
//! ZSET member so cancel can `ZREM` precisely without scanning. The
//! cancel-vs-promote race is serialized at Redis (both paths are Lua
//! under the same hash-tagged slot); the only outcomes are
//! "(removed=true, never delivered)" or "(removed=false, delivered)".

use chasquimq::Promoter;
use chasquimq::config::{ConsumerConfig, ProducerConfig, PromoterConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, dedup_marker_key, delayed_index_key, delayed_key};
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

async fn admin() -> Client {
    init_tracing();
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

/// Aggressive cleanup: walk every `{chasqui:<queue>}:*` key on the slot
/// (SCAN is hash-tag-pinned to one slot) and delete it. This catches both
/// the slice-6 dedup markers (`:dlid:*`) and the new side-index keys
/// (`:didx:*`) without listing every suffix.
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
    for prefix in ["dlid", "didx"] {
        let pattern = format!("{{chasqui:{queue}}}:{prefix}:*");
        let mut cursor: String = "0".to_string();
        loop {
            let res: Value = admin
                .custom(
                    CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
                    vec![
                        Value::from(cursor.as_str()),
                        Value::from("MATCH"),
                        Value::from(pattern.as_str()),
                        Value::from("COUNT"),
                        Value::from(100_i64),
                    ],
                )
                .await
                .expect("SCAN");
            let (next, keys) = parse_scan(res);
            for key in keys {
                let _: Value = admin
                    .custom(
                        CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                        vec![Value::from(key)],
                    )
                    .await
                    .expect("DEL key");
            }
            if next == "0" {
                break;
            }
            cursor = next;
        }
    }
}

fn parse_scan(v: Value) -> (String, Vec<String>) {
    match v {
        Value::Array(items) if items.len() == 2 => {
            let cursor = match &items[0] {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => std::str::from_utf8(b).unwrap_or("0").to_string(),
                Value::Integer(n) => n.to_string(),
                _ => "0".to_string(),
            };
            let keys: Vec<String> = match &items[1] {
                Value::Array(arr) => arr
                    .iter()
                    .filter_map(|v| match v {
                        Value::String(s) => Some(s.to_string()),
                        Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            (cursor, keys)
        }
        _ => ("0".to_string(), Vec::new()),
    }
}

async fn zcard(admin: &Client, key: &str) -> i64 {
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

async fn exists(admin: &Client, key: &str) -> bool {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("EXISTS")
    {
        Value::Integer(n) => n > 0,
        _ => false,
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

fn consumer_cfg(queue: &str, consumer_id: &str, delayed_enabled: bool) -> ConsumerConfig {
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
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled,
        delayed_poll_interval_ms: 50,
        delayed_promote_batch: 256,
        delayed_max_stream_len: 100_000,
        delayed_lock_ttl_secs: 5,
        ..Default::default()
    }
}

fn promoter_cfg(queue: &str, holder_id: &str) -> PromoterConfig {
    PromoterConfig {
        queue_name: queue.to_string(),
        poll_interval_ms: 50,
        promote_batch: 256,
        max_stream_len: 100_000,
        lock_ttl_secs: 5,
        holder_id: holder_id.to_string(),
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

fn spawn_consumer(
    queue: &str,
    consumer_id: &str,
    delayed_enabled: bool,
    counter: Arc<AtomicUsize>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<chasquimq::Result<()>> {
    let consumer: Consumer<Sample> = Consumer::new(
        redis_url(),
        consumer_cfg(queue, consumer_id, delayed_enabled),
    );
    tokio::spawn(async move {
        consumer
            .run(
                move |_job| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown,
            )
            .await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_delayed_removes_scheduled_entry() {
    let admin = admin().await;
    let queue = "delayed_cancel_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let id = "cancel-target-1".to_string();
    producer
        .add_in_with_id(id.clone(), Duration::from_secs(60), Sample { n: 42 })
        .await
        .expect("add_in_with_id");

    let dkey = delayed_key(queue);
    let idx = delayed_index_key(queue, &id);
    let marker = dedup_marker_key(queue, &id);
    assert_eq!(zcard(&admin, &dkey).await, 1, "scheduled before cancel");
    assert!(
        exists(&admin, &idx).await,
        "side-index exists before cancel"
    );
    assert!(exists(&admin, &marker).await, "marker exists before cancel");

    let cancelled = producer.cancel_delayed(&id).await.expect("cancel");
    assert!(cancelled, "cancel must report true on a scheduled job");

    assert_eq!(
        zcard(&admin, &dkey).await,
        0,
        "ZSET must be empty after cancel"
    );
    assert!(
        !exists(&admin, &idx).await,
        "side-index must be DEL'd by cancel"
    );
    assert!(
        !exists(&admin, &marker).await,
        "dedup marker must be DEL'd so the same id can be rescheduled"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_unknown_id_returns_false() {
    let admin = admin().await;
    let queue = "delayed_cancel_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let cancelled = producer
        .cancel_delayed(&"never-scheduled".to_string())
        .await
        .expect("cancel");
    assert!(
        !cancelled,
        "cancelling an id that was never scheduled returns false"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_after_promote_returns_false() {
    let admin = admin().await;
    let queue = "delayed_cancel_e3";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    let id = "promote-then-cancel".to_string();
    producer
        .add_in_with_id(id.clone(), Duration::from_millis(100), Sample { n: 1 })
        .await
        .expect("add_in_with_id");

    // Wait until the consumer actually saw and ran it.
    wait_until(Duration::from_secs(3), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    // Cancel arrives after promotion: ZSET entry is gone, side-index may
    // still be present (TTL outlives promote) but ZREM returns 0 → false.
    let cancelled = producer.cancel_delayed(&id).await.expect("cancel late");
    assert!(
        !cancelled,
        "cancel must return false if the promoter already moved the entry"
    );
    // The job ran exactly once; cancel did not trigger a duplicate.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn schedule_cancel_reschedule_same_id() {
    let admin = admin().await;
    let queue = "delayed_cancel_e4";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let id = "recyclable-id".to_string();
    producer
        .add_in_with_id(id.clone(), Duration::from_secs(60), Sample { n: 1 })
        .await
        .expect("schedule #1");
    let cancelled = producer.cancel_delayed(&id).await.expect("cancel");
    assert!(cancelled);

    // After cancel, the dedup marker is gone — a fresh schedule with the
    // same id must succeed and produce a new ZSET entry.
    producer
        .add_in_with_id(id.clone(), Duration::from_secs(60), Sample { n: 2 })
        .await
        .expect("schedule #2");

    let dkey = delayed_key(queue);
    assert_eq!(
        zcard(&admin, &dkey).await,
        1,
        "rescheduled with same id must land a single new entry"
    );
    assert!(exists(&admin, &delayed_index_key(queue, &id)).await);
    assert!(exists(&admin, &dedup_marker_key(queue, &id)).await);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_delayed_bulk_partial_then_drain() {
    let admin = admin().await;
    let queue = "delayed_cancel_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // Five scheduled jobs, all firing in ~250ms.
    let items: Vec<(String, Sample)> = (0..5)
        .map(|i| (format!("bulk-cancel-{i}"), Sample { n: i }))
        .collect();
    producer
        .add_in_bulk_with_ids(Duration::from_millis(250), items)
        .await
        .expect("bulk schedule");
    assert_eq!(zcard(&admin, &delayed_key(queue)).await, 5);

    // Cancel three of them in bulk. Mix in one unknown id to confirm the
    // returned vector lines up with the input order.
    let cancel_ids: Vec<String> = vec![
        "bulk-cancel-0".to_string(),
        "unknown-id".to_string(),
        "bulk-cancel-2".to_string(),
        "bulk-cancel-4".to_string(),
    ];
    let results = producer
        .cancel_delayed_bulk(&cancel_ids)
        .await
        .expect("cancel_delayed_bulk");
    assert_eq!(results, vec![true, false, true, true]);
    assert_eq!(
        zcard(&admin, &delayed_key(queue)).await,
        2,
        "ZCARD = 5 scheduled - 3 cancelled = 2 remaining"
    );

    // Stand up a consumer to drain the survivors and verify exactly two
    // deliveries (ids -1 and -3).
    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 2 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "exactly 2 jobs must run; the 3 cancelled ids must never reach the consumer"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_delayed_bulk_empty_input_is_noop() {
    let admin = admin().await;
    let queue = "delayed_cancel_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let results = producer
        .cancel_delayed_bulk(&[])
        .await
        .expect("empty bulk cancel");
    assert!(results.is_empty());

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_vs_promote_race_is_exclusive() {
    // The load-bearing correctness invariant: cancel and promote serialize
    // at Redis (both are Lua under the same hash-tagged slot). For any
    // single (job_id, schedule, cancel) triple, exactly one of these must
    // hold:
    //   (a) cancel returned true  AND consumer never received the job
    //   (b) cancel returned false AND consumer received the job
    // Never both, never neither.
    let admin = admin().await;
    let queue = "delayed_cancel_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(
        queue,
        "c1",
        false, // standalone Promoter handles promotion below
        counter.clone(),
        shutdown_consumer.clone(),
    );
    let shutdown_promoter = CancellationToken::new();
    let promoter = Promoter::new(redis_url(), promoter_cfg(queue, "p1"));
    let h_promoter = tokio::spawn(promoter.run(shutdown_promoter.clone()));

    let id = "race-id".to_string();
    producer
        .add_in_with_id(id.clone(), Duration::from_millis(50), Sample { n: 99 })
        .await
        .expect("schedule");

    // Race the cancel against the imminent promotion. We don't know which
    // wins on any given run; we assert the exclusivity invariant.
    let cancelled = producer.cancel_delayed(&id).await.expect("cancel");

    // Give the consumer enough wall-clock to either receive (if promote
    // won) or stay quiet (if cancel won).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let delivered = counter.load(Ordering::SeqCst);

    assert!(
        (cancelled && delivered == 0) || (!cancelled && delivered == 1),
        "race outcome must be exclusive (cancelled={cancelled}, delivered={delivered})"
    );

    shutdown_promoter.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_promoter).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

/// Regression: when slice 7 added the per-job side-index `:didx:<job_id>`
/// (so `cancel_delayed` can `ZREM` precisely without scanning), it forgot
/// to teach the promoter to clean those entries up after promoting a job
/// to the stream. The marker key (`:dlid:<id>`) MUST stay alive on its
/// remaining TTL — it's the post-promote idempotence guard for slice 6 —
/// but the side-index is dead weight after promotion and would otherwise
/// accumulate for `delay_secs + DEDUP_MARKER_GRACE_SECS` before TTL'ing.
///
/// This test schedules N short-delay jobs with explicit ids, drains them
/// through the consumer, then asserts SCAN of `:didx:*` on the slot is
/// empty while the marker keys are still present.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn promoter_cleans_up_side_index_after_promotion() {
    let admin = admin().await;
    // Distinct from `delayed_cancel_e6` (used by the empty-input test) so
    // parallel `cargo test` runs can't cross-contaminate keyspaces.
    let queue = "delayed_cancel_e8";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // Schedule 5 jobs with explicit ids, all firing in ~150ms.
    let n_jobs = 5_usize;
    let items: Vec<(String, Sample)> = (0..n_jobs)
        .map(|i| (format!("leak-check-{i}"), Sample { n: i as u32 }))
        .collect();
    let ids: Vec<String> = items.iter().map(|(id, _)| id.clone()).collect();
    producer
        .add_in_bulk_with_ids(Duration::from_millis(150), items)
        .await
        .expect("bulk schedule");

    // Side-indices and markers must all be present pre-promote.
    for id in &ids {
        assert!(
            exists(&admin, &delayed_index_key(queue, id)).await,
            "side-index for {id} should exist before promotion"
        );
        assert!(
            exists(&admin, &dedup_marker_key(queue, id)).await,
            "marker for {id} should exist before promotion"
        );
    }

    // Spin up the consumer (which embeds the promoter) and wait for all
    // jobs to be processed.
    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());
    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == n_jobs }
    })
    .await;
    // Give the promoter one extra tick to run the cleanup pipeline.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // All side-index keys MUST be gone (this is the regression check).
    for id in &ids {
        assert!(
            !exists(&admin, &delayed_index_key(queue, id)).await,
            "side-index for {id} must be DEL'd by promoter after promotion (regression fix)"
        );
    }
    // Marker keys MUST still be alive — their TTL is the post-promote
    // idempotence guard for slice 6.
    for id in &ids {
        assert!(
            exists(&admin, &dedup_marker_key(queue, id)).await,
            "marker for {id} must survive promotion (slice 6 idempotence)"
        );
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}
