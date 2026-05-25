//! Integration tests for the job maintenance API (`remove`, `drain`,
//! `clean`, `obliterate`) against a live Redis 8.6+.
//!
//! Set `REDIS_URL` (e.g. `REDIS_URL=redis://127.0.0.1:6379`) and run with
//! `cargo test -p chasquimq --test maintenance -- --include-ignored`.

use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::Producer;
use chasquimq::{DrainOptions, HandlerError, Job, JobState};
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
    s: String,
}

async fn admin() -> Client {
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

fn parse_scan(v: &Value) -> (String, Vec<String>) {
    let items = match v {
        Value::Array(items) if items.len() >= 2 => items,
        _ => return ("0".to_string(), Vec::new()),
    };
    let cursor = match &items[0] {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Integer(n) => n.to_string(),
        _ => "0".to_string(),
    };
    let keys: Vec<String> = match &items[1] {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|k| match k {
                Value::String(s) => Some(s.to_string()),
                Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (cursor, keys)
}

/// Delete every `{chasqui:<queue>}:*` key so a test starts clean.
async fn flush_all(admin: &Client, queue: &str) {
    let pattern = format!("{{chasqui:{queue}}}:*");
    let mut cursor = "0".to_string();
    loop {
        let v: Value = admin
            .custom(
                CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
                vec![
                    Value::from(cursor.clone()),
                    Value::from("MATCH"),
                    Value::from(pattern.clone()),
                    Value::from("COUNT"),
                    Value::from(256_i64),
                ],
            )
            .await
            .expect("SCAN");
        let (next, keys) = parse_scan(&v);
        for key in keys {
            let _: Value = admin
                .custom(
                    CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                    vec![Value::from(key)],
                )
                .await
                .expect("DEL");
        }
        if next == "0" {
            break;
        }
        cursor = next;
    }
}

async fn count_keys(admin: &Client, queue: &str) -> usize {
    let pattern = format!("{{chasqui:{queue}}}:*");
    let mut cursor = "0".to_string();
    let mut total = 0;
    loop {
        let v: Value = admin
            .custom(
                CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
                vec![
                    Value::from(cursor.clone()),
                    Value::from("MATCH"),
                    Value::from(pattern.clone()),
                    Value::from("COUNT"),
                    Value::from(256_i64),
                ],
            )
            .await
            .expect("SCAN");
        let (next, keys) = parse_scan(&v);
        total += keys.len();
        if next == "0" {
            break;
        }
        cursor = next;
    }
    total
}

async fn xlen(admin: &Client, key: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .unwrap_or(Value::Integer(0));
    match v {
        Value::Integer(n) => n,
        _ => 0,
    }
}

async fn zcard(admin: &Client, key: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("ZCARD", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .unwrap_or(Value::Integer(0));
    match v {
        Value::Integer(n) => n,
        _ => 0,
    }
}

async fn exists(admin: &Client, key: &str) -> bool {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .unwrap_or(Value::Integer(0));
    matches!(v, Value::Integer(n) if n >= 1)
}

/// Count of entries pending in a stream's consumer group (Active jobs).
/// `XPENDING <key> <group>` returns `[count, min, max, consumers]`.
async fn pending_count(admin: &Client, stream_key: &str, group: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
            vec![Value::from(stream_key), Value::from(group)],
        )
        .await
        .unwrap_or(Value::Integer(0));
    match v {
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
        pool_size: 4,
        max_stream_len: 100_000,
        ..Default::default()
    }
}

fn consumer_cfg(queue: &str, store_results: bool) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: format!("c-{}", uuid::Uuid::new_v4()),
        block_ms: 50,
        concurrency: 4,
        max_attempts: 3,
        store_results,
        result_ttl_secs: 120,
        delayed_enabled: false,
        ..Default::default()
    }
}

async fn wait_until<F, Fut>(timeout: Duration, label: &str, mut check: F)
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
            panic!("wait_until({label}) timed out after {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ============================================================================
// remove
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_waiting_stream_entry() {
    let admin = admin().await;
    let queue = "mnt_remove_waiting";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let id = producer
        .add(Sample {
            n: 1,
            s: "wait".into(),
        })
        .await
        .expect("add");
    producer
        .add(Sample {
            n: 2,
            s: "keep".into(),
        })
        .await
        .expect("add keep");

    let stream = format!("{{chasqui:{queue}}}:stream");
    assert_eq!(xlen(&admin, &stream).await, 2);

    let report = producer.remove(&id, "default").await.expect("remove");
    assert!(report.stream, "waiting entry should be removed");
    assert!(report.removed_anything());
    assert_eq!(xlen(&admin, &stream).await, 1, "only the target removed");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_delayed_job() {
    let admin = admin().await;
    let queue = "mnt_remove_delayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let id = producer
        .add_in_with_id(
            "delayed-target".to_string(),
            Duration::from_secs(3600),
            Sample {
                n: 7,
                s: "d".into(),
            },
        )
        .await
        .expect("add_in");

    let delayed = format!("{{chasqui:{queue}}}:delayed");
    let didx = format!("{{chasqui:{queue}}}:didx:{id}");
    assert_eq!(zcard(&admin, &delayed).await, 1);
    assert!(exists(&admin, &didx).await, "side-index written");

    let report = producer.remove(&id, "default").await.expect("remove");
    assert!(report.delayed, "delayed job should be removed");
    assert_eq!(zcard(&admin, &delayed).await, 0, "ZSET emptied");
    assert!(!exists(&admin, &didx).await, "side-index reaped");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_completed_result_key() {
    let admin = admin().await;
    let queue = "mnt_remove_result";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    // Write a result key directly so the test is deterministic.
    let id = "result-target";
    let result_key = format!("{{chasqui:{queue}}}:result:{id}");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![Value::from(result_key.clone()), Value::from("payload")],
        )
        .await
        .expect("SET");
    assert!(exists(&admin, &result_key).await);

    let report = producer
        .remove(&id.to_string(), "default")
        .await
        .expect("remove");
    assert!(report.result, "result key should be removed");
    assert!(!exists(&admin, &result_key).await);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_nonexistent_is_idempotent() {
    let admin = admin().await;
    let queue = "mnt_remove_missing";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let report = producer
        .remove(&"never-existed".to_string(), "default")
        .await
        .expect("remove must not error on a missing id");
    assert!(!report.removed_anything(), "all-false report");
    assert!(!report.delayed && !report.stream && !report.dlq && !report.result);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_also_purges_progress_and_log_keys() {
    let admin = admin().await;
    let queue = "mnt_remove_progress_log";
    flush_all(&admin, queue).await;

    let id = "remove-target";
    let progress_key = format!("{{chasqui:{queue}}}:progress:{id}");
    let log_key = format!("{{chasqui:{queue}}}:log:{id}");
    let result_key = format!("{{chasqui:{queue}}}:result:{id}");

    // Plant a progress key, a log entry, and a result key — all
    // three are independent surfaces `remove` should sweep.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![Value::from(progress_key.clone()), Value::from("42")],
        )
        .await
        .expect("SET progress");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(log_key.clone()),
                Value::from("*"),
                Value::from("line"),
                Value::from("first log"),
            ],
        )
        .await
        .expect("XADD log");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![Value::from(result_key.clone()), Value::from("r")],
        )
        .await
        .expect("SET result");

    assert!(exists(&admin, &progress_key).await);
    assert!(exists(&admin, &log_key).await);
    assert!(exists(&admin, &result_key).await);

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    let report = producer
        .remove(&id.to_string(), "default")
        .await
        .expect("remove");
    assert!(report.result, "result key removed");

    assert!(!exists(&admin, &progress_key).await, "progress purged");
    assert!(!exists(&admin, &log_key).await, "log stream purged");
    assert!(!exists(&admin, &result_key).await, "result purged");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn obliterate_sweeps_progress_and_log_keys() {
    let admin = admin().await;
    let queue = "mnt_obliterate_progress_log";
    flush_all(&admin, queue).await;

    // SCAN MATCH `{chasqui:<q>}:*` already covers progress + log keys
    // because they share the queue hash tag — pin that here so a
    // future change to the key shape (or to obliterate's pattern)
    // can't silently regress.
    let progress_key = format!("{{chasqui:{queue}}}:progress:job-A");
    let log_key = format!("{{chasqui:{queue}}}:log:job-A");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![Value::from(progress_key.clone()), Value::from("17")],
        )
        .await
        .expect("SET progress");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(log_key.clone()),
                Value::from("*"),
                Value::from("line"),
                Value::from("only line"),
            ],
        )
        .await
        .expect("XADD log");

    assert!(exists(&admin, &progress_key).await);
    assert!(exists(&admin, &log_key).await);

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    let _removed = producer.obliterate("default").await.expect("obliterate");

    assert!(!exists(&admin, &progress_key).await, "progress swept");
    assert!(!exists(&admin, &log_key).await, "log swept");
    assert_eq!(count_keys(&admin, queue).await, 0, "entire keyspace nuked");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn remove_active_pending_job() {
    let admin = admin().await;
    let queue = "mnt_remove_active";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let id = producer
        .add(Sample {
            n: 5,
            s: "active".into(),
        })
        .await
        .expect("add");

    // Run a consumer with a handler that blocks until cancelled, so the
    // job sits delivered-but-unacked (in the PEL = Active).
    let cancel = CancellationToken::new();
    let in_handler = Arc::new(AtomicUsize::new(0));
    let in_handler_c = in_handler.clone();
    let cfg = consumer_cfg(queue, false);
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let in_handler = in_handler_c.clone();
                    async move {
                        in_handler.fetch_add(1, Ordering::SeqCst);
                        // Block long enough for the test to act.
                        tokio::time::sleep(Duration::from_secs(120)).await;
                        Ok::<bytes::Bytes, HandlerError>(bytes::Bytes::new())
                    }
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    wait_until(Duration::from_secs(30), "job picked up", || async {
        in_handler.load(Ordering::SeqCst) >= 1
    })
    .await;

    let stream = format!("{{chasqui:{queue}}}:stream");
    let report = producer.remove(&id, "default").await.expect("remove");
    assert!(report.stream, "active (pending) entry should be removed");
    assert_eq!(
        xlen(&admin, &stream).await,
        0,
        "active entry acked + deleted"
    );

    cancel.cancel();
    join.await.ok();
    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

// ============================================================================
// drain
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn drain_clears_waiting_and_delayed() {
    let admin = admin().await;
    let queue = "mnt_drain_basic";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..10_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }
    for n in 0..4_u32 {
        producer
            .add_in(
                Duration::from_secs(3600),
                Sample {
                    n: 100 + n,
                    s: "d".into(),
                },
            )
            .await
            .expect("add_in");
    }

    let stream = format!("{{chasqui:{queue}}}:stream");
    let delayed = format!("{{chasqui:{queue}}}:delayed");
    assert_eq!(xlen(&admin, &stream).await, 10);
    assert_eq!(zcard(&admin, &delayed).await, 4);

    let removed = producer
        .drain("default", DrainOptions::default())
        .await
        .expect("drain");
    assert_eq!(removed, 14, "10 waiting + 4 delayed");
    assert_eq!(xlen(&admin, &stream).await, 0);
    assert_eq!(zcard(&admin, &delayed).await, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn drain_keeps_delayed_when_flag_off() {
    let admin = admin().await;
    let queue = "mnt_drain_keepdelayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..3_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }
    producer
        .add_in(
            Duration::from_secs(3600),
            Sample {
                n: 99,
                s: "d".into(),
            },
        )
        .await
        .expect("add_in");

    let stream = format!("{{chasqui:{queue}}}:stream");
    let delayed = format!("{{chasqui:{queue}}}:delayed");

    let removed = producer
        .drain("default", DrainOptions { delayed: false })
        .await
        .expect("drain");
    assert_eq!(removed, 3, "waiting only");
    assert_eq!(xlen(&admin, &stream).await, 0);
    assert_eq!(zcard(&admin, &delayed).await, 1, "delayed survives");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn drain_leaves_active_jobs() {
    let admin = admin().await;
    let queue = "mnt_drain_active";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..6_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    // One consumer with concurrency 1 and a batch of 1, so it pulls
    // exactly one job into the PEL at a time. The other 5 stay waiting
    // (not pending) — that is what drain should clear.
    let cancel = CancellationToken::new();
    let in_handler = Arc::new(AtomicUsize::new(0));
    let in_handler_c = in_handler.clone();
    let cfg = ConsumerConfig {
        concurrency: 1,
        batch: 1,
        ..consumer_cfg(queue, false)
    };
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let in_handler = in_handler_c.clone();
                    async move {
                        in_handler.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(120)).await;
                        Ok::<bytes::Bytes, HandlerError>(bytes::Bytes::new())
                    }
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    wait_until(Duration::from_secs(30), "one job in-flight", || async {
        in_handler.load(Ordering::SeqCst) >= 1
    })
    .await;

    let stream = format!("{{chasqui:{queue}}}:stream");
    let removed = producer
        .drain("default", DrainOptions::default())
        .await
        .expect("drain");
    // 6 produced, 1 picked up as active (batch=1). Drain removes only the
    // 5 waiting ones; the 1 active entry stays in the stream.
    assert!((1..=5).contains(&removed), "removed {removed}");
    assert!(
        xlen(&admin, &stream).await >= 1,
        "active entry survives drain"
    );

    cancel.cancel();
    join.await.ok();
    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn drain_empty_queue_is_noop() {
    let admin = admin().await;
    let queue = "mnt_drain_empty";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let removed = producer
        .drain("default", DrainOptions::default())
        .await
        .expect("drain empty");
    assert_eq!(removed, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn drain_multi_pass_clears_all_waiting_past_scan_page() {
    // Regression: the drain loop must keep going until a pass deletes
    // ZERO, not until a pass deletes fewer than a full scan page. With
    // more waiting jobs than MAINTENANCE_SCAN_PAGE (1024) plus an Active
    // job interleaved near the front, a pass legitimately deletes fewer
    // than a full page while waiting jobs remain further back. A
    // stop-on-partial-page loop would leave thousands undrained.
    let admin = admin().await;
    let queue = "mnt_drain_multipass";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    // 2500 waiting jobs — well past the 1024 scan-page cap.
    let total = 2500_u32;
    let batch: Vec<Sample> = (0..total).map(|n| Sample { n, s: "w".into() }).collect();
    producer.add_bulk(batch).await.expect("add_bulk");

    let stream = format!("{{chasqui:{queue}}}:stream");
    assert_eq!(xlen(&admin, &stream).await, total as i64);

    // Hold exactly one job in-flight (batch=1 → one entry in the PEL).
    let cancel = CancellationToken::new();
    let in_handler = Arc::new(AtomicUsize::new(0));
    let in_handler_c = in_handler.clone();
    let cfg = ConsumerConfig {
        concurrency: 1,
        batch: 1,
        ..consumer_cfg(queue, false)
    };
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let in_handler = in_handler_c.clone();
                    async move {
                        in_handler.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(120)).await;
                        Ok::<bytes::Bytes, HandlerError>(bytes::Bytes::new())
                    }
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    wait_until(Duration::from_secs(30), "one job in-flight", || async {
        in_handler.load(Ordering::SeqCst) >= 1
    })
    .await;

    // Snapshot the live PEL size — a blocking handler keeps it stable.
    // It is >= 1 (the in-flight job) but the reader may have prefetched
    // a few; assert against the real count, not a hard-coded 1.
    let active = pending_count(&admin, &stream, "default").await;
    assert!(active >= 1, "expected at least one in-flight job");

    let removed = producer
        .drain("default", DrainOptions { delayed: false })
        .await
        .expect("drain");
    // Every waiting job is drained; the Active jobs survive. The point of
    // this test: `removed` is well past one scan page (>1024), proving
    // the multi-pass loop does not stop on a partial page.
    assert_eq!(
        removed,
        total as u64 - active as u64,
        "every waiting job drained, active jobs spared"
    );
    assert!(
        removed > 1024,
        "drain crossed multiple scan pages (removed {removed})"
    );
    assert_eq!(
        xlen(&admin, &stream).await,
        active,
        "exactly the active entries remain"
    );

    cancel.cancel();
    join.await.ok();
    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

// ============================================================================
// clean
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_waiting_removes_old_entries() {
    let admin = admin().await;
    let queue = "mnt_clean_waiting";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..5_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    let stream = format!("{{chasqui:{queue}}}:stream");
    // grace=0 → everything older than "now" qualifies (all just-added
    // entries have a stream-id ms <= now).
    let removed = producer
        .clean("default", 0, 100, JobState::Waiting)
        .await
        .expect("clean");
    assert_eq!(removed.len(), 5, "all 5 waiting cleaned");
    assert_eq!(xlen(&admin, &stream).await, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_purges_progress_and_log_keys() {
    let admin = admin().await;
    let queue = "mnt_clean_progress_log";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    // Enqueue three waiting jobs and plant a progress key + a log
    // stream entry against each one — the same per-job surfaces
    // `remove(id)` is responsible for sweeping. `clean()` must do the
    // same for every job it removes.
    let mut ids: Vec<String> = Vec::new();
    for n in 0..3_u32 {
        let id = producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
        let progress_key = format!("{{chasqui:{queue}}}:progress:{id}");
        let log_key = format!("{{chasqui:{queue}}}:log:{id}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
                vec![Value::from(progress_key.clone()), Value::from("17")],
            )
            .await
            .expect("SET progress");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
                vec![
                    Value::from(log_key.clone()),
                    Value::from("*"),
                    Value::from("line"),
                    Value::from("first"),
                ],
            )
            .await
            .expect("XADD log");
        assert!(exists(&admin, &progress_key).await);
        assert!(exists(&admin, &log_key).await);
        ids.push(id);
    }

    let removed = producer
        .clean("default", 0, 1000, JobState::Waiting)
        .await
        .expect("clean");
    assert_eq!(removed.len(), 3, "all 3 waiting cleaned");

    for id in &ids {
        let progress_key = format!("{{chasqui:{queue}}}:progress:{id}");
        let log_key = format!("{{chasqui:{queue}}}:log:{id}");
        assert!(
            !exists(&admin, &progress_key).await,
            "progress key purged by clean for {id}"
        );
        assert!(
            !exists(&admin, &log_key).await,
            "log stream purged by clean for {id}"
        );
    }

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_grace_window_excludes_recent() {
    let admin = admin().await;
    let queue = "mnt_clean_grace";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..4_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    let stream = format!("{{chasqui:{queue}}}:stream");
    // A huge grace window → nothing is older than now-1h, so nothing
    // qualifies.
    let removed = producer
        .clean("default", 3_600_000, 100, JobState::Waiting)
        .await
        .expect("clean");
    assert_eq!(removed.len(), 0, "recent jobs survive the grace window");
    assert_eq!(xlen(&admin, &stream).await, 4);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_limit_caps_removals() {
    let admin = admin().await;
    let queue = "mnt_clean_limit";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..10_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    let stream = format!("{{chasqui:{queue}}}:stream");
    let removed = producer
        .clean("default", 0, 3, JobState::Waiting)
        .await
        .expect("clean");
    assert_eq!(removed.len(), 3, "limit caps removals at 3");
    assert_eq!(xlen(&admin, &stream).await, 7, "7 survive");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_delayed_removes_due_jobs() {
    let admin = admin().await;
    let queue = "mnt_clean_delayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    // Delayed jobs scheduled an hour out — they sit in the delayed ZSET.
    // clean(Delayed) ages by `created_at_ms` (when scheduled, ~now), not
    // the run-at score, so grace=0 makes every freshly-created one match.
    for n in 0..3_u32 {
        producer
            .add_in_with_id(
                format!("due-{n}"),
                Duration::from_secs(3600),
                Sample { n, s: "due".into() },
            )
            .await
            .expect("add_in");
    }

    let delayed = format!("{{chasqui:{queue}}}:delayed");
    assert_eq!(zcard(&admin, &delayed).await, 3);

    // grace=0 → cutoff = now → every job created at-or-before now matches.
    let removed = producer
        .clean("default", 0, 100, JobState::Delayed)
        .await
        .expect("clean delayed");
    assert_eq!(removed.len(), 3);
    assert_eq!(zcard(&admin, &delayed).await, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_failed_removes_dlq_entries() {
    let admin = admin().await;
    let queue = "mnt_clean_failed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..3_u32 {
        producer
            .add(Sample {
                n,
                s: "boom".into(),
            })
            .await
            .expect("add");
    }

    let cancel = CancellationToken::new();
    let cfg = ConsumerConfig {
        max_attempts: 1,
        ..consumer_cfg(queue, false)
    };
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err::<bytes::Bytes, _>(HandlerError::new(std::io::Error::other("boom")))
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    let dlq = format!("{{chasqui:{queue}}}:dlq");
    wait_until(Duration::from_secs(30), "DLQ populated", || async {
        xlen(&admin, &dlq).await >= 3
    })
    .await;
    cancel.cancel();
    join.await.ok();

    let removed = producer
        .clean("default", 0, 100, JobState::Failed)
        .await
        .expect("clean failed");
    assert_eq!(removed.len(), 3, "all DLQ entries cleaned");
    assert_eq!(xlen(&admin, &dlq).await, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_completed_removes_result_keys() {
    let admin = admin().await;
    let queue = "mnt_clean_completed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    for n in 0..4_u32 {
        let key = format!("{{chasqui:{queue}}}:result:done-{n}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
                vec![Value::from(key), Value::from("r")],
            )
            .await
            .expect("SET");
    }

    let removed = producer
        .clean("default", 0, 100, JobState::Completed)
        .await
        .expect("clean completed");
    assert_eq!(removed.len(), 4, "all 4 result keys removed");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_active_is_noop() {
    let admin = admin().await;
    let queue = "mnt_clean_active";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let removed = producer
        .clean("default", 0, 100, JobState::Active)
        .await
        .expect("clean active");
    assert!(removed.is_empty(), "clean(Active) is intentionally a no-op");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn clean_waiting_leaves_active_jobs() {
    // Regression: clean(Waiting) must not delete in-flight (PEL) entries.
    // The main stream mixes waiting and active entries; a naive XRANGE
    // scan would sweep an old-enough active entry into the delete set.
    let admin = admin().await;
    let queue = "mnt_clean_waiting_active";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let total = 30_u32;
    for n in 0..total {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    // Hold a job in-flight (batch=1 keeps the PEL small).
    let cancel = CancellationToken::new();
    let in_handler = Arc::new(AtomicUsize::new(0));
    let in_handler_c = in_handler.clone();
    let cfg = ConsumerConfig {
        concurrency: 1,
        batch: 1,
        ..consumer_cfg(queue, false)
    };
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let in_handler = in_handler_c.clone();
                    async move {
                        in_handler.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(120)).await;
                        Ok::<bytes::Bytes, HandlerError>(bytes::Bytes::new())
                    }
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    wait_until(Duration::from_secs(30), "one job in-flight", || async {
        in_handler.load(Ordering::SeqCst) >= 1
    })
    .await;

    let stream = format!("{{chasqui:{queue}}}:stream");
    // Snapshot the live PEL size — a blocking handler keeps it stable.
    let active = pending_count(&admin, &stream, "default").await;
    assert!(active >= 1, "expected at least one in-flight job");

    // grace=0 → every job is old enough; only the waiting ones go, the
    // Active (PEL) entries are spared.
    let removed = producer
        .clean("default", 0, total as usize, JobState::Waiting)
        .await
        .expect("clean waiting");
    assert_eq!(
        removed.len() as i64,
        total as i64 - active,
        "waiting jobs cleaned, the active ones spared"
    );
    assert_eq!(
        xlen(&admin, &stream).await,
        active,
        "the active entries survive clean(Waiting)"
    );

    cancel.cancel();
    join.await.ok();
    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

// ============================================================================
// obliterate
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn obliterate_nukes_entire_keyspace() {
    let admin = admin().await;
    let queue = "mnt_obliterate";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    // Touch as many key families as possible.
    for n in 0..5_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }
    producer
        .add_in(
            Duration::from_secs(3600),
            Sample {
                n: 99,
                s: "d".into(),
            },
        )
        .await
        .expect("add_in");
    producer.pause().await.expect("pause");
    let result_key = format!("{{chasqui:{queue}}}:result:r1");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![Value::from(result_key), Value::from("r")],
        )
        .await
        .expect("SET");

    assert!(count_keys(&admin, queue).await >= 3, "keyspace populated");

    let removed = producer.obliterate("default").await.expect("obliterate");
    assert!(removed >= 3, "obliterate removed {removed} keys");
    assert_eq!(count_keys(&admin, queue).await, 0, "entire keyspace nuked");

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn obliterate_then_reuse_works() {
    let admin = admin().await;
    let queue = "mnt_obliterate_reuse";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    producer
        .add(Sample {
            n: 1,
            s: "first".into(),
        })
        .await
        .expect("add");
    producer.obliterate("default").await.expect("obliterate");

    // A fresh queue with the same name must work after obliterate.
    let id = producer
        .add(Sample {
            n: 2,
            s: "fresh".into(),
        })
        .await
        .expect("add after obliterate");
    let stream = format!("{{chasqui:{queue}}}:stream");
    assert_eq!(xlen(&admin, &stream).await, 1, "fresh queue usable");
    assert!(!id.is_empty());

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn obliterate_empty_queue_is_idempotent() {
    let admin = admin().await;
    let queue = "mnt_obliterate_empty";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");

    let removed = producer.obliterate("default").await.expect("obliterate");
    assert_eq!(removed, 0, "nothing to remove");
    // Second call is also clean.
    let removed2 = producer.obliterate("default").await.expect("obliterate 2");
    assert_eq!(removed2, 0);

    producer.shutdown().await.ok();
    admin.quit().await.ok();
}
