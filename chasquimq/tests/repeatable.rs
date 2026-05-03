//! Integration tests for slice 10: repeatable jobs (cron + fixed-interval).

use chasquimq::config::{ConsumerConfig, ProducerConfig, SchedulerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, repeat_key as repeat_key_fn, repeat_spec_key, stream_key};
use chasquimq::repeat::{MissedFiresPolicy, RepeatPattern, RepeatableSpec};
use chasquimq::scheduler::Scheduler;
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
    // Suffixes covering every key the engine writes for a queue, so each
    // test starts from a clean slate even if a previous run left specs
    // behind.
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "scheduler:lock",
    ] {
        let key = format!("{{chasqui:{queue}}}:{suffix}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                vec![Value::from(key)],
            )
            .await
            .expect("DEL");
    }
    // Repeat ZSET + spec hashes — wildcard-scan since the spec hash key
    // includes the spec key suffix.
    let rkey = repeat_key_fn(queue);
    let _: Value = admin
        .custom(
            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
            vec![Value::from(rkey.as_str())],
        )
        .await
        .expect("DEL repeat");
    // Best-effort spec hash cleanup: each test uses its own queue name so
    // collisions across runs are unlikely.
    let pattern = format!("{{chasqui:{queue}}}:repeat:spec:*");
    let scan_res: Value = admin
        .custom(
            CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
            vec![Value::from(pattern)],
        )
        .await
        .expect("KEYS");
    if let Value::Array(items) = scan_res {
        for item in items {
            let s = match item {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => match std::str::from_utf8(&b) {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                },
                _ => continue,
            };
            let _: Value = admin
                .custom(
                    CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                    vec![Value::from(s)],
                )
                .await
                .expect("DEL spec");
        }
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

async fn zscore(admin: &Client, key: &str, member: &str) -> Option<u64> {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("ZSCORE", ClusterHash::FirstKey, false),
            vec![Value::from(key), Value::from(member)],
        )
        .await
        .expect("ZSCORE")
    {
        Value::Double(d) => Some(d.max(0.0) as u64),
        Value::Integer(n) => Some(n.max(0) as u64),
        Value::String(s) => s.parse::<f64>().ok().map(|f| f.max(0.0) as u64),
        Value::Bytes(b) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f.max(0.0) as u64),
        Value::Null => None,
        _ => None,
    }
}

/// Backdate the spec's `next_fire_ms` to simulate scheduler downtime: the
/// spec was alive, then the scheduler was down long enough that
/// `next_fire_ms` is now far in the past. ZADD with same member updates the
/// score in place.
async fn backdate_spec_score(admin: &Client, queue: &str, spec_key: &str, score_ms: u64) {
    let rkey = repeat_key_fn(queue);
    let _: Value = admin
        .custom(
            CustomCommand::new_static("ZADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(rkey),
                Value::from(score_ms as i64),
                Value::from(spec_key),
            ],
        )
        .await
        .expect("ZADD backdate");
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
        block_ms: 50,
        // Tight tick so delivery doesn't stall on the inline promoter.
        delayed_poll_interval_ms: 25,
        delayed_promote_batch: 256,
        delayed_max_stream_len: 100_000,
        delayed_lock_ttl_secs: 5,
        delayed_enabled: true,
        concurrency: 8,
        ..Default::default()
    }
}

fn scheduler_cfg(queue: &str, holder_id: &str, tick_ms: u64) -> SchedulerConfig {
    SchedulerConfig {
        queue_name: queue.to_string(),
        tick_interval_ms: tick_ms,
        batch: 64,
        max_stream_len: 100_000,
        lock_ttl_secs: 5,
        holder_id: holder_id.to_string(),
        ..Default::default()
    }
}

fn spawn_consumer(
    queue: &str,
    consumer_id: &str,
    counter: Arc<AtomicUsize>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<chasquimq::Result<()>> {
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, consumer_id));
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

fn spawn_scheduler(
    queue: &str,
    holder_id: &str,
    tick_ms: u64,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<chasquimq::Result<()>> {
    let scheduler: Scheduler<Sample> =
        Scheduler::new(redis_url(), scheduler_cfg(queue, holder_id, tick_ms));
    tokio::spawn(scheduler.run(shutdown))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn every_pattern_fires_repeatedly() {
    let admin = admin().await;
    let queue = "repeat_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(queue, "c1", counter.clone(), shutdown_consumer.clone());

    let shutdown_sched = CancellationToken::new();
    // Tight 50ms tick so the test can observe ≥3 fires of a 100ms-period
    // spec inside a 700ms window without timing-induced flakes.
    let h_sched = spawn_scheduler(queue, "s1", 50, shutdown_sched.clone());

    producer
        .upsert_repeatable(RepeatableSpec {
            key: String::new(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 100 },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert");

    wait_until(Duration::from_millis(1500), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 3 }
    })
    .await;

    shutdown_sched.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_sched).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cron_pattern_fires_at_least_once() {
    let admin = admin().await;
    let queue = "repeat_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(queue, "c1", counter.clone(), shutdown_consumer.clone());

    let shutdown_sched = CancellationToken::new();
    let h_sched = spawn_scheduler(queue, "s1", 100, shutdown_sched.clone());

    // 6-field cron: every second.
    producer
        .upsert_repeatable(RepeatableSpec {
            key: String::new(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Cron {
                expression: "* * * * * *".into(),
                tz: None,
            },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert");

    // Tolerant assertion (≥1 fire in ~2.5s) — cron alignment can leave a
    // sub-second gap before the first match.
    wait_until(Duration::from_millis(3000), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 1 }
    })
    .await;

    shutdown_sched.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_sched).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn limit_caps_total_fires_and_removes_spec() {
    let admin = admin().await;
    let queue = "repeat_e3";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(queue, "c1", counter.clone(), shutdown_consumer.clone());

    let shutdown_sched = CancellationToken::new();
    let h_sched = spawn_scheduler(queue, "s1", 50, shutdown_sched.clone());

    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "limited".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 100 },
            payload: Sample { n: 0 },
            limit: Some(2),
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert");
    assert_eq!(key, "limited");

    // Wait for both fires to land.
    wait_until(Duration::from_millis(2000), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 2 }
    })
    .await;

    // Give the scheduler one more tick to confirm it doesn't fire a 3rd
    // time.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let observed = counter.load(Ordering::SeqCst);
    assert_eq!(observed, 2, "limit=2 must cap fires; saw {observed}");

    // Spec must be removed from the repeat ZSET and its hash deleted.
    let rkey = repeat_key_fn(queue);
    assert_eq!(zcard(&admin, &rkey).await, 0, "repeat ZSET must be drained");
    let hkey = repeat_spec_key(queue, "limited");
    assert!(!exists(&admin, &hkey).await, "spec hash must be deleted");

    shutdown_sched.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_sched).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn remove_repeatable_stops_future_fires() {
    let admin = admin().await;
    let queue = "repeat_e4";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(queue, "c1", counter.clone(), shutdown_consumer.clone());

    let shutdown_sched = CancellationToken::new();
    let h_sched = spawn_scheduler(queue, "s1", 50, shutdown_sched.clone());

    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "to-remove".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 100 },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert");

    // Let it fire at least once before removing.
    wait_until(Duration::from_millis(1500), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 1 }
    })
    .await;

    let removed = producer.remove_repeatable(&key).await.expect("remove");
    assert!(removed, "remove_repeatable must report success");

    let after_remove = counter.load(Ordering::SeqCst);
    // Wait long enough that any in-flight fire has resolved; new fires
    // should not appear.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let final_count = counter.load(Ordering::SeqCst);
    assert!(
        final_count <= after_remove + 1,
        "no new fires after remove: before={after_remove} after={final_count}"
    );

    let rkey = repeat_key_fn(queue);
    assert_eq!(zcard(&admin, &rkey).await, 0);

    shutdown_sched.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_sched).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn list_repeatable_returns_specs() {
    let admin = admin().await;
    let queue = "repeat_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let _k1 = producer
        .upsert_repeatable(RepeatableSpec {
            key: "a".into(),
            job_name: "alpha".into(),
            pattern: RepeatPattern::Every {
                interval_ms: 60_000,
            },
            payload: Sample { n: 1 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert a");
    let _k2 = producer
        .upsert_repeatable(RepeatableSpec {
            key: "b".into(),
            job_name: "beta".into(),
            pattern: RepeatPattern::Cron {
                expression: "0 * * * *".into(),
                tz: None,
            },
            payload: Sample { n: 2 },
            limit: Some(10),
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert b");

    let listed = producer.list_repeatable(100).await.expect("list");
    assert_eq!(listed.len(), 2, "expected 2 specs, got {:?}", listed);
    let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
    assert!(keys.contains(&"a"));
    assert!(keys.contains(&"b"));
    let beta = listed.iter().find(|m| m.key == "b").unwrap();
    assert_eq!(beta.job_name, "beta");
    assert_eq!(beta.limit, Some(10));
    matches!(beta.pattern, RepeatPattern::Cron { .. });

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn upsert_overwrites_existing_spec() {
    let admin = admin().await;
    let queue = "repeat_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    producer
        .upsert_repeatable(RepeatableSpec {
            key: "same-key".into(),
            job_name: "v1".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: Sample { n: 1 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert v1");
    producer
        .upsert_repeatable(RepeatableSpec {
            key: "same-key".into(),
            job_name: "v2".into(),
            pattern: RepeatPattern::Every { interval_ms: 5_000 },
            payload: Sample { n: 2 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert v2");

    let listed = producer.list_repeatable(10).await.expect("list");
    assert_eq!(listed.len(), 1, "overwrite must not duplicate: {listed:?}");
    assert_eq!(listed[0].job_name, "v2");
    assert!(matches!(
        listed[0].pattern,
        RepeatPattern::Every { interval_ms: 5_000 }
    ));

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn leader_election_no_double_fire() {
    let admin = admin().await;
    let queue = "repeat_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let counter = Arc::new(AtomicUsize::new(0));
    let shutdown_consumer = CancellationToken::new();
    let h_consumer = spawn_consumer(queue, "c1", counter.clone(), shutdown_consumer.clone());

    // Two schedulers competing for leader; only one should fire jobs.
    let shutdown_a = CancellationToken::new();
    let h_a = spawn_scheduler(queue, "sA", 50, shutdown_a.clone());
    let shutdown_b = CancellationToken::new();
    let h_b = spawn_scheduler(queue, "sB", 50, shutdown_b.clone());

    producer
        .upsert_repeatable(RepeatableSpec {
            key: "le".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 200 },
            payload: Sample { n: 0 },
            limit: Some(3),
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: Default::default(),
        })
        .await
        .expect("upsert");

    wait_until(Duration::from_millis(3500), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 3 }
    })
    .await;

    // Hold one extra tick window to confirm we don't see double-firing.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let observed = counter.load(Ordering::SeqCst);
    assert_eq!(
        observed, 3,
        "limit=3 with two competing schedulers must fire exactly 3 times; saw {observed}"
    );

    shutdown_a.cancel();
    shutdown_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), h_b).await;
    shutdown_consumer.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_consumer).await;
    let _: () = admin.quit().await.unwrap();
}

// ---------------------------------------------------------------------------
// Catch-up policy tests (slice 10 follow-up).
//
// Pattern: upsert a spec, then backdate its `next_fire_ms` in the repeat ZSET
// to simulate scheduler downtime. Spawn the scheduler for a brief window and
// observe what landed in the stream and what the new ZSET score is. We don't
// run a consumer in these tests — XLEN on the stream is the source of truth
// for "how many fires were dispatched". This decouples the test from any
// consumer-side timing noise.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_skip_advances_past_missed_fires() {
    let admin = admin().await;
    let queue = "repeat_catchup_skip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "skip-me".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::Skip,
        })
        .await
        .expect("upsert");

    // Backdate to 5 minutes ago — 300 missed fires of an `every:1s` spec.
    let now = now_ms();
    let backdated = now.saturating_sub(300_000);
    backdate_spec_score(&admin, queue, &key, backdated).await;

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    // One tick is enough; give the scheduler a small window to acquire the
    // lock + tick.
    tokio::time::sleep(Duration::from_millis(400)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    // Skip policy: zero jobs dispatched, zero in delayed ZSET. Spec still
    // alive with a future score.
    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    assert_eq!(
        n, 0,
        "Skip must drop missed fires; expected 0 stream entries, saw {n}"
    );

    let next_score = zscore(&admin, &repeat_key_fn(queue), &key).await;
    let next = next_score.expect("spec must still be in repeat ZSET");
    assert!(
        next > now,
        "Skip must advance next_fire_ms past now; now={now} next={next}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_fire_once_emits_one_job() {
    let admin = admin().await;
    let queue = "repeat_catchup_once";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "fire-once".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::FireOnce,
        })
        .await
        .expect("upsert");

    let now = now_ms();
    let backdated = now.saturating_sub(300_000);
    backdate_spec_score(&admin, queue, &key, backdated).await;

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    tokio::time::sleep(Duration::from_millis(400)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    assert_eq!(
        n, 1,
        "FireOnce must dispatch exactly 1 job for the missed window(s); saw {n}"
    );

    let next_score = zscore(&admin, &repeat_key_fn(queue), &key).await;
    let next = next_score.expect("spec must still be in repeat ZSET");
    assert!(
        next > now,
        "FireOnce must advance past now; now={now} next={next}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_fire_all_capped_at_max_catchup() {
    let admin = admin().await;
    let queue = "repeat_catchup_all_capped";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // every:60s, backdated 5 minutes → ~5 missed windows. Cap at 3.
    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "fire-all-capped".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every {
                interval_ms: 60_000,
            },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::FireAll { max_catchup: 3 },
        })
        .await
        .expect("upsert");

    let now = now_ms();
    let backdated = now.saturating_sub(300_000);
    backdate_spec_score(&admin, queue, &key, backdated).await;

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    tokio::time::sleep(Duration::from_millis(400)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    assert_eq!(
        n, 3,
        "FireAll {{ max_catchup: 3 }} must dispatch exactly 3 jobs; saw {n}"
    );

    let next_score = zscore(&admin, &repeat_key_fn(queue), &key).await;
    let next = next_score.expect("spec must still be in repeat ZSET");
    assert!(
        next > now,
        "FireAll cap-reached path must advance past now; now={now} next={next}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_fire_all_uncapped_under_limit() {
    let admin = admin().await;
    let queue = "repeat_catchup_all_uncapped";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // 5 missed fires, max_catchup well above that — must replay all 5.
    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "fire-all-uncapped".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every {
                interval_ms: 60_000,
            },
            payload: Sample { n: 0 },
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::FireAll { max_catchup: 100 },
        })
        .await
        .expect("upsert");

    let now = now_ms();
    let backdated = now.saturating_sub(300_000);
    backdate_spec_score(&admin, queue, &key, backdated).await;

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    tokio::time::sleep(Duration::from_millis(400)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    // Exactly 5 missed windows (offsets 0, +60s, +120s, +180s, +240s, all
    // <= now since backdated is now-300s); the 6th is at backdated+300s ==
    // now, which the loop also fires (at <= now). The 7th is at
    // backdated+360s, strictly > now. So we expect 6.
    //
    // (Allow a tolerance band of [5, 6] to account for the small wall-clock
    // drift between `now_ms()` capture and the scheduler's tick.)
    assert!(
        (5..=6).contains(&n),
        "FireAll uncapped must replay every missed window; expected 5-6, saw {n}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_respects_spec_limit() {
    let admin = admin().await;
    let queue = "repeat_catchup_limit";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // FireAll would otherwise replay ~5; spec limit caps at 2.
    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "limited".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every {
                interval_ms: 60_000,
            },
            payload: Sample { n: 0 },
            limit: Some(2),
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::FireAll { max_catchup: 100 },
        })
        .await
        .expect("upsert");

    let now = now_ms();
    let backdated = now.saturating_sub(300_000);
    backdate_spec_score(&admin, queue, &key, backdated).await;

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    tokio::time::sleep(Duration::from_millis(400)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    assert_eq!(n, 2, "spec limit=2 must cap catch-up replay at 2; saw {n}");

    // Spec should be removed (limit hit).
    assert_eq!(zcard(&admin, &repeat_key_fn(queue)).await, 0);
    let hkey = repeat_spec_key(queue, &key);
    assert!(!exists(&admin, &hkey).await);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn catchup_no_op_when_on_time() {
    // Sanity: when fire_at_ms is within one cadence of now (the normal
    // case, no catch-up), every policy behaves identically — one fire,
    // ZADD cadence-next.
    let admin = admin().await;
    let queue = "repeat_catchup_ontime";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // Use FireAll with a low cap — if the policy logic ever incorrectly
    // dispatches catch-up on the on-time path, this would visibly
    // multiply jobs.
    let key = producer
        .upsert_repeatable(RepeatableSpec {
            key: "on-time".into(),
            job_name: "tick".into(),
            pattern: RepeatPattern::Every { interval_ms: 200 },
            payload: Sample { n: 0 },
            limit: Some(1),
            start_after_ms: None,
            end_before_ms: None,
            missed_fires: MissedFiresPolicy::FireAll { max_catchup: 100 },
        })
        .await
        .expect("upsert");
    assert_eq!(key, "on-time");

    let shutdown = CancellationToken::new();
    let h = spawn_scheduler(queue, "s1", 50, shutdown.clone());
    // Wait long enough for the scheduler to fire on-time (interval=200ms).
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h).await;

    let stream = stream_key(queue);
    let n = xlen(&admin, &stream).await;
    assert_eq!(
        n, 1,
        "on-time path under FireAll must still fire exactly limit=1 job; saw {n}"
    );

    let _: () = admin.quit().await.unwrap();
}
