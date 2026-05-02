//! Integration tests for the idempotent delayed-schedule API
//! (`Producer::add_in_with_id`, `add_at_with_id`, `add_in_bulk_with_ids`).
//!
//! The contract under test: a network-driven caller retry of the same
//! logical schedule (same `JobId`) must NOT duplicate the scheduled job.
//! Implementation gates the `ZADD` behind a `SET NX EX` dedup marker
//! evaluated atomically inside one Lua script.

use chasquimq::Promoter;
use chasquimq::config::{ConsumerConfig, ProducerConfig, PromoterConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, dedup_marker_key, delayed_key, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
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

/// Aggressive cleanup: the dedup-marker keys (`{chasqui:<queue>}:dlid:<id>`)
/// don't share a fixed suffix list with the rest, so we walk the queue's
/// hash slot and DEL everything matching `{chasqui:<queue>}:*` via SCAN.
/// SCAN is limited to one slot by the hash tag, so this stays cluster-safe.
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
    // Drop any leftover dedup markers from a prior run of the same queue.
    let pattern = format!("{{chasqui:{queue}}}:dlid:*");
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
                .expect("DEL marker");
        }
        if next == "0" {
            break;
        }
        cursor = next;
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

async fn ttl_secs(admin: &Client, key: &str) -> i64 {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("TTL", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("TTL")
    {
        Value::Integer(n) => n,
        other => panic!("TTL unexpected: {other:?}"),
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
async fn add_in_with_id_dedupes_repeat_call() {
    let admin = admin().await;
    let queue = "delayed_idmp_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let stable = "stable-id-1".to_string();
    let id1 = producer
        .add_in_with_id(stable.clone(), Duration::from_secs(60), Sample { n: 1 })
        .await
        .expect("add_in_with_id #1");
    let id2 = producer
        .add_in_with_id(stable.clone(), Duration::from_secs(60), Sample { n: 1 })
        .await
        .expect("add_in_with_id #2 (duplicate must be a silent no-op)");

    assert_eq!(id1, stable, "first call returns the supplied id");
    assert_eq!(id2, stable, "second call returns the same id, no error");

    let dkey = delayed_key(queue);
    assert_eq!(
        zcard(&admin, &dkey).await,
        1,
        "ZSET must hold exactly one entry, not two"
    );

    let marker = dedup_marker_key(queue, &stable);
    let ttl = ttl_secs(&admin, &marker).await;
    assert!(
        ttl > 60,
        "marker TTL must extend beyond the scheduled fire time \
         (got {ttl}s, expected > 60s = delay + grace)"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_id_delivers_exactly_once() {
    let admin = admin().await;
    let queue = "delayed_idmp_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    let stable = "stable-id-once".to_string();
    // Caller "retries" 3 times — only one delivery must reach the consumer.
    for _ in 0..3 {
        producer
            .add_in_with_id(stable.clone(), Duration::from_millis(200), Sample { n: 7 })
            .await
            .expect("add_in_with_id");
    }

    wait_until(Duration::from_secs(3), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    // Stay parked for a bit longer to catch any late duplicate delivery.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "exactly one delivery despite 3 add_in_with_id calls"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn concurrent_add_in_with_id_same_id_delivers_once() {
    let admin = admin().await;
    let queue = "delayed_idmp_e3";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    // Race two concurrent tasks on the same JobId. The Lua script's atomic
    // SET NX EX gate must serialize them so only one ZADD lands.
    let stable = "stable-id-race".to_string();
    let p1 = producer.clone();
    let p2 = producer.clone();
    let s1 = stable.clone();
    let s2 = stable.clone();
    let t1 = tokio::spawn(async move {
        p1.add_in_with_id(s1, Duration::from_millis(250), Sample { n: 1 })
            .await
            .expect("racer 1")
    });
    let t2 = tokio::spawn(async move {
        p2.add_in_with_id(s2, Duration::from_millis(250), Sample { n: 1 })
            .await
            .expect("racer 2")
    });
    let (id1, id2) = tokio::join!(t1, t2);
    let id1 = id1.expect("task 1");
    let id2 = id2.expect("task 2");
    assert_eq!(id1, stable);
    assert_eq!(id2, stable);

    let dkey = delayed_key(queue);
    // ZCARD reflects the gate before the promoter has fired (or after it
    // drained — in which case we just confirm the consumer count).
    let zc = zcard(&admin, &dkey).await;
    assert!(
        zc <= 1,
        "racing add_in_with_id must never produce more than one ZSET entry, got {zc}"
    );

    wait_until(Duration::from_secs(3), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "exactly one delivery from a same-id race"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn different_ids_schedule_independently() {
    let admin = admin().await;
    let queue = "delayed_idmp_e4";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    for i in 0..5 {
        let id = format!("distinct-{i}");
        producer
            .add_in_with_id(id, Duration::from_millis(200), Sample { n: i })
            .await
            .expect("add_in_with_id");
    }

    wait_until(Duration::from_secs(3), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 5 }
    })
    .await;

    // No further deliveries should arrive.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        5,
        "five distinct IDs deliver five jobs"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_at_with_id_dedupes_repeat_call() {
    let admin = admin().await;
    let queue = "delayed_idmp_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let when = SystemTime::now() + Duration::from_secs(45);
    let stable = "at-id-1".to_string();
    let id1 = producer
        .add_at_with_id(stable.clone(), when, Sample { n: 1 })
        .await
        .expect("add_at_with_id #1");
    let id2 = producer
        .add_at_with_id(stable.clone(), when, Sample { n: 1 })
        .await
        .expect("add_at_with_id #2");

    assert_eq!(id1, stable);
    assert_eq!(id2, stable);

    let dkey = delayed_key(queue);
    assert_eq!(
        zcard(&admin, &dkey).await,
        1,
        "add_at_with_id must dedupe just like add_in_with_id"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_id_zero_delay_uses_idmp_immediate_path() {
    let admin = admin().await;
    let queue = "delayed_idmp_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let stable = "fast-path-1".to_string();
    // Two zero-delay calls fall through to add_with_id (XADD IDMP), so the
    // dedup happens in Redis Streams, not in our marker. Either way the
    // observable outcome is: stream gets exactly one entry, ZSET stays empty.
    let _ = producer
        .add_in_with_id(stable.clone(), Duration::ZERO, Sample { n: 1 })
        .await
        .expect("add_in_with_id zero #1");
    let _ = producer
        .add_in_with_id(stable.clone(), Duration::ZERO, Sample { n: 1 })
        .await
        .expect("add_in_with_id zero #2");

    let dkey = delayed_key(queue);
    let skey = stream_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0, "ZSET must stay empty");
    assert_eq!(
        xlen(&admin, &skey).await,
        1,
        "XADD IDMP must dedupe the immediate path"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_bulk_with_ids_dedupes_per_entry() {
    let admin = admin().await;
    let queue = "delayed_idmp_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // First batch: 3 fresh ids.
    let batch1: Vec<(String, Sample)> = (0..3)
        .map(|i| (format!("bulk-{i}"), Sample { n: i }))
        .collect();
    let ids1 = producer
        .add_in_bulk_with_ids(Duration::from_secs(60), batch1)
        .await
        .expect("bulk with ids #1");
    assert_eq!(ids1.len(), 3);

    // Second batch: overlapping ids 1,2,3 — only id 3 is new.
    let batch2: Vec<(String, Sample)> = (1..4)
        .map(|i| (format!("bulk-{i}"), Sample { n: i }))
        .collect();
    let ids2 = producer
        .add_in_bulk_with_ids(Duration::from_secs(60), batch2)
        .await
        .expect("bulk with ids #2");
    assert_eq!(ids2.len(), 3);

    let dkey = delayed_key(queue);
    assert_eq!(
        zcard(&admin, &dkey).await,
        4,
        "ZSET must hold 4 distinct entries (3 from batch 1 + 1 new from batch 2)"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_bulk_with_ids_empty_is_noop() {
    let admin = admin().await;
    let queue = "delayed_idmp_e8";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let ids = producer
        .add_in_bulk_with_ids(Duration::from_secs(60), Vec::new())
        .await
        .expect("empty bulk");
    assert!(ids.is_empty());

    let dkey = delayed_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_id_works_with_standalone_promoter() {
    let admin = admin().await;
    let queue = "delayed_idmp_e9";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(
        queue,
        "c1",
        false, // consumer's inline promoter off — standalone Promoter handles promotion
        counter.clone(),
        shutdown_consumer.clone(),
    );

    let shutdown_promoter = CancellationToken::new();
    let promoter = Promoter::new(redis_url(), promoter_cfg(queue, "p1"));
    let h_promoter = tokio::spawn(promoter.run(shutdown_promoter.clone()));

    let stable = "promoted-id".to_string();
    producer
        .add_in_with_id(stable.clone(), Duration::from_millis(150), Sample { n: 1 })
        .await
        .expect("add_in_with_id");
    producer
        .add_in_with_id(stable.clone(), Duration::from_millis(150), Sample { n: 1 })
        .await
        .expect("add_in_with_id duplicate");

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    shutdown_promoter.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_promoter).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}
