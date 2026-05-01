use chasquimq::config::{ConsumerConfig, ProducerConfig, PromoterConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, delayed_key, promoter_lock_key, stream_key};
use chasquimq::{Error, Promoter};
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

async fn get_string(admin: &Client, key: &str) -> Option<String> {
    let res: Value = admin
        .custom(
            CustomCommand::new_static("GET", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("GET");
    match res {
        Value::String(s) => Some(s.to_string()),
        Value::Bytes(b) => std::str::from_utf8(&b).ok().map(|s| s.to_string()),
        Value::Null => None,
        _ => None,
    }
}

async fn script_flush(admin: &Client) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false),
            vec![Value::from("FLUSH")],
        )
        .await
        .expect("SCRIPT FLUSH");
}

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
        ..Default::default()
    }
}

fn producer_cfg_with_max_delay(queue: &str, max_delay_secs: u64) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
        max_delay_secs,
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
async fn add_in_delivers_within_window() {
    let admin = admin().await;
    let queue = "delayed_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    let scheduled_at = Instant::now();
    producer
        .add_in(Duration::from_millis(150), Sample { n: 1 })
        .await
        .expect("add_in");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "handler must not fire before delay elapses"
    );

    wait_until(Duration::from_millis(800), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    let elapsed = scheduled_at.elapsed();
    assert!(
        elapsed >= Duration::from_millis(140),
        "delivered too early: {elapsed:?}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_zero_fast_paths() {
    let admin = admin().await;
    let queue = "delayed_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    producer
        .add_in(Duration::ZERO, Sample { n: 1 })
        .await
        .expect("add_in zero");

    let dkey = delayed_key(queue);
    let skey = stream_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0, "ZSET must stay empty");
    assert_eq!(xlen(&admin, &skey).await, 1, "stream gets the entry");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_at_past_fast_paths() {
    let admin = admin().await;
    let queue = "delayed_e3";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let past = SystemTime::now() - Duration::from_secs(1);
    producer
        .add_at(past, Sample { n: 1 })
        .await
        .expect("add_at past");

    let dkey = delayed_key(queue);
    let skey = stream_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0);
    assert_eq!(xlen(&admin, &skey).await, 1);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_bulk_delivers_all() {
    let admin = admin().await;
    let queue = "delayed_e4";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_consumer(queue, "c1", true, counter.clone(), shutdown.clone());

    let payloads: Vec<Sample> = (0..1_000).map(|n| Sample { n }).collect();
    producer
        .add_in_bulk(Duration::from_millis(100), payloads)
        .await
        .expect("add_in_bulk");

    wait_until(Duration::from_secs(15), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1_000 }
    })
    .await;

    let dkey = delayed_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dkey = dkey.clone();
        async move { zcard(&admin, &dkey).await == 0 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn delayed_survives_consumer_restart() {
    let admin = admin().await;
    let queue = "delayed_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    producer
        .add_in(Duration::from_millis(500), Sample { n: 1 })
        .await
        .expect("add_in");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown1 = CancellationToken::new();
    let h1 = spawn_consumer(queue, "c1", true, counter.clone(), shutdown1.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown1.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h1).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "should not fire before delay"
    );

    let shutdown2 = CancellationToken::new();
    let h2 = spawn_consumer(queue, "c2", true, counter.clone(), shutdown2.clone());

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    shutdown2.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h2).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn standalone_promoter_works() {
    let admin = admin().await;
    let queue = "delayed_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(
        queue,
        "c1",
        false,
        counter.clone(),
        shutdown_consumer.clone(),
    );

    let shutdown_promoter = CancellationToken::new();
    let promoter = Promoter::new(redis_url(), promoter_cfg(queue, "p1"));
    let h_promoter = tokio::spawn(promoter.run(shutdown_promoter.clone()));

    producer
        .add_in(Duration::from_millis(100), Sample { n: 1 })
        .await
        .expect("add_in");

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    shutdown_promoter.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_promoter).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn noscript_recovery() {
    let admin = admin().await;
    let queue = "delayed_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(
        queue,
        "c1",
        false,
        counter.clone(),
        shutdown_consumer.clone(),
    );

    let shutdown_promoter = CancellationToken::new();
    let promoter = Promoter::new(redis_url(), promoter_cfg(queue, "p1"));
    let h_promoter = tokio::spawn(promoter.run(shutdown_promoter.clone()));

    tokio::time::sleep(Duration::from_millis(200)).await;
    script_flush(&admin).await;

    producer
        .add_in(Duration::from_millis(50), Sample { n: 1 })
        .await
        .expect("add_in");

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    shutdown_promoter.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_promoter).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn leader_election_no_double_promote() {
    let admin = admin().await;
    let queue = "delayed_e8";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let mut promoter_handles = Vec::new();
    let mut promoter_shutdowns = Vec::new();
    for i in 0..3 {
        let shutdown = CancellationToken::new();
        let promoter = Promoter::new(redis_url(), promoter_cfg(queue, &format!("p{i}")));
        promoter_handles.push(tokio::spawn(promoter.run(shutdown.clone())));
        promoter_shutdowns.push(shutdown);
    }

    producer
        .add_in(Duration::from_millis(100), Sample { n: 1 })
        .await
        .expect("add_in");

    let skey = stream_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let skey = skey.clone();
        async move { xlen(&admin, &skey).await >= 1 }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        xlen(&admin, &skey).await,
        1,
        "exactly one XADD even with 3 racing promoters"
    );

    for s in promoter_shutdowns {
        s.cancel();
    }
    for h in promoter_handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn leader_handover() {
    let admin = admin().await;
    let queue = "delayed_e9";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let shutdown_a = CancellationToken::new();
    let promoter_a = Promoter::new(redis_url(), promoter_cfg(queue, "pA"));
    let h_a = tokio::spawn(promoter_a.run(shutdown_a.clone()));

    let lock_key = promoter_lock_key(queue);
    wait_until(Duration::from_secs(2), || {
        let admin = admin.clone();
        let lock_key = lock_key.clone();
        async move { get_string(&admin, &lock_key).await == Some("pA".to_string()) }
    })
    .await;

    shutdown_a.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_a).await;

    let shutdown_b = CancellationToken::new();
    let promoter_b = Promoter::new(redis_url(), promoter_cfg(queue, "pB"));
    let h_b = tokio::spawn(promoter_b.run(shutdown_b.clone()));

    producer
        .add_in(Duration::from_millis(100), Sample { n: 1 })
        .await
        .expect("add_in");

    let skey = stream_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let skey = skey.clone();
        async move { xlen(&admin, &skey).await == 1 }
    })
    .await;

    shutdown_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_b).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn permanent_error_escalates() {
    let cfg = PromoterConfig {
        queue_name: "delayed_e10".to_string(),
        poll_interval_ms: 50,
        promote_batch: 64,
        max_stream_len: 1_000,
        lock_ttl_secs: 2,
        holder_id: "p1".to_string(),
    };
    let promoter = Promoter::new("redis://127.0.0.1:1", cfg);
    let shutdown = CancellationToken::new();

    let outcome = tokio::time::timeout(Duration::from_secs(5), promoter.run(shutdown))
        .await
        .expect("promoter should escalate before timeout");

    assert!(outcome.is_err(), "unreachable Redis must escalate to Err");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_bulk_empty_is_noop() {
    let admin = admin().await;
    let queue = "delayed_e11";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let ids = producer
        .add_in_bulk(Duration::from_millis(100), Vec::new())
        .await
        .expect("add_in_bulk empty");
    assert!(ids.is_empty());

    let dkey = delayed_key(queue);
    let skey = stream_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0);
    assert_eq!(xlen(&admin, &skey).await, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_bulk_zero_fast_paths() {
    let admin = admin().await;
    let queue = "delayed_e12";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let payloads: Vec<Sample> = (0..50).map(|n| Sample { n }).collect();
    producer
        .add_in_bulk(Duration::ZERO, payloads)
        .await
        .expect("add_in_bulk zero");

    let dkey = delayed_key(queue);
    let skey = stream_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0, "ZSET must stay empty");
    assert_eq!(
        xlen(&admin, &skey).await,
        50,
        "all 50 entries land in stream"
    );

    let _: () = admin.quit().await.unwrap();
}

struct AlwaysFailEncode;

impl Serialize for AlwaysFailEncode {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("encode failure for test"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_partial_encode_fails_atomically() {
    let admin = admin().await;
    let queue = "delayed_e13";
    flush_all(&admin, queue).await;

    let producer: Producer<AlwaysFailEncode> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let res = producer
        .add_in_bulk(
            Duration::from_millis(100),
            vec![AlwaysFailEncode, AlwaysFailEncode],
        )
        .await;
    assert!(matches!(res, Err(Error::Encode(_))), "got {res:?}");

    let dkey = delayed_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0, "no ZADD should reach Redis");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_rejects_beyond_max_delay() {
    let admin = admin().await;
    let queue = "delayed_e14";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> =
        Producer::connect(&redis_url(), producer_cfg_with_max_delay(queue, 60))
            .await
            .expect("connect producer");
    let res = producer
        .add_in(Duration::from_secs(120), Sample { n: 1 })
        .await;
    assert!(matches!(res, Err(Error::Config(_))), "got {res:?}");

    let dkey = delayed_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_at_rejects_beyond_max_delay() {
    let admin = admin().await;
    let queue = "delayed_e15";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> =
        Producer::connect(&redis_url(), producer_cfg_with_max_delay(queue, 60))
            .await
            .expect("connect producer");
    let when = SystemTime::now() + Duration::from_secs(120);
    let res = producer.add_at(when, Sample { n: 1 }).await;
    assert!(matches!(res, Err(Error::Config(_))), "got {res:?}");

    let dkey = delayed_key(queue);
    assert_eq!(zcard(&admin, &dkey).await, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn promoter_releases_lock_on_shutdown() {
    let admin = admin().await;
    let queue = "delayed_e16";
    flush_all(&admin, queue).await;

    let shutdown = CancellationToken::new();
    let promoter = Promoter::new(redis_url(), promoter_cfg(queue, "p1"));
    let handle = tokio::spawn(promoter.run(shutdown.clone()));

    let lock_key = promoter_lock_key(queue);
    wait_until(Duration::from_secs(2), || {
        let admin = admin.clone();
        let lock_key = lock_key.clone();
        async move { get_string(&admin, &lock_key).await == Some("p1".to_string()) }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    wait_until(Duration::from_millis(500), || {
        let admin = admin.clone();
        let lock_key = lock_key.clone();
        async move { get_string(&admin, &lock_key).await.is_none() }
    })
    .await;

    let _: () = admin.quit().await.unwrap();
}
