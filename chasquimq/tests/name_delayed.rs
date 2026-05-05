//! Slice 3: `Job::name` survives every delayed-path engine site.
//!
//! Pins these round trips end-to-end against a real Redis:
//! - `add_in_with_options` → ZSET → promoter → consumer.
//! - `add_at_with_options` → ZSET → promoter → consumer.
//! - `add_in_bulk` (via `add_bulk_named`-style per-entry intent — currently
//!   shared empty name on bulk; we exercise the shared-name `add_bulk_with_options`
//!   plus per-id idempotent delayed adds with shared name).
//! - Repeatable spec → scheduler-fire → consumer reads `Job::name == job_name`.
//! - Handler-error retry-via-delayed-ZSET preserves the original name onto
//!   the second attempt.
//! - `cancel_delayed` works against a delayed entry that carries a name
//!   (the side-index byte-equality round-trips through the prefixed shape).

use chasquimq::config::{ConsumerConfig, ProducerConfig, SchedulerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::error::HandlerError;
use chasquimq::job::Job;
use chasquimq::producer::{
    AddOptions, Producer, repeat_key as repeat_key_fn, scheduler_lock_key, stream_key,
};
use chasquimq::repeat::{RepeatPattern, RepeatableSpec};
use chasquimq::scheduler::Scheduler;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};
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
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "scheduler:lock",
        "events",
        "repeat",
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
    // Best-effort spec hash cleanup.
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

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 100_000,
        max_delay_secs: 0, // unlimited
    }
}

fn delayed_consumer_cfg(queue: &str, consumer_id: &str) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: consumer_id.to_string(),
        batch: 64,
        block_ms: 50,
        claim_min_idle_ms: 30_000,
        concurrency: 4,
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        // Embedded promoter for delayed → stream.
        delayed_enabled: true,
        delayed_poll_interval_ms: 25,
        delayed_promote_batch: 256,
        delayed_max_stream_len: 100_000,
        delayed_lock_ttl_secs: 5,
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

/// Drive a consumer that records `(name, payload.n)` of every job it handles
/// onto a shared vec, with an embedded promoter so the delayed → stream hop
/// is exercised end-to-end.
async fn drive_consumer_recording(
    queue: &str,
    consumer_id: &str,
    expected: usize,
) -> Vec<(String, u32)> {
    let observed: Arc<Mutex<Vec<(String, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_h = observed.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), delayed_consumer_cfg(queue, consumer_id));
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
    wait_until(Duration::from_secs(20), move || {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_options_with_name_round_trips() {
    let admin = admin().await;
    let queue = "name_delayed_in";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    producer
        .add_in_with_options(
            Duration::from_millis(50),
            Sample { n: 7 },
            AddOptions::new().with_name("send-email"),
        )
        .await
        .expect("add_in_with_options with name");

    let observed = drive_consumer_recording(queue, "delayed_in_c1", 1).await;
    assert_eq!(
        observed,
        vec![("send-email".to_string(), 7)],
        "delayed-with-name must arrive at consumer with `Job::name` preserved across the prefix encoder + promoter"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_at_with_options_with_name_round_trips() {
    let admin = admin().await;
    let queue = "name_delayed_at";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let when = SystemTime::now() + Duration::from_millis(50);
    producer
        .add_at_with_options(
            when,
            Sample { n: 11 },
            AddOptions::new().with_name("resize-image"),
        )
        .await
        .expect("add_at_with_options with name");

    let observed = drive_consumer_recording(queue, "delayed_at_c1", 1).await;
    assert_eq!(observed, vec![("resize-image".to_string(), 11)]);

    let _: () = admin.quit().await.unwrap();
}

/// Bulk delayed adds via `add_bulk_with_options` share one name across all
/// entries — same posture as the immediate-XADD bulk path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_options_bulk_shared_name_round_trips() {
    // The producer doesn't ship `add_in_bulk_with_options` (only `add_bulk_with_options`
    // for immediate XADD); we exercise the same encoder via three separate
    // `add_in_with_options` calls instead, which is the supported per-name
    // delayed path today.
    let admin = admin().await;
    let queue = "name_delayed_in_multi";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    for (n, name) in [
        (1u32, "send-email"),
        (2, "resize-image"),
        (3, "post-webhook"),
    ] {
        producer
            .add_in_with_options(
                Duration::from_millis(50),
                Sample { n },
                AddOptions::new().with_name(name),
            )
            .await
            .expect("add_in_with_options");
    }

    let observed = drive_consumer_recording(queue, "delayed_in_multi_c", 3).await;
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

/// Repeatable spec with a non-empty `job_name` fires onto the stream with
/// `n` set, and the consumer reads `Job::name == job_name` on every fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn repeatable_with_job_name_fires_with_name() {
    let admin = admin().await;
    let queue = "name_repeat_fire";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // Every 100ms, ~3 fires by the time we shut down.
    let spec = RepeatableSpec::new(
        "billing-cron",
        RepeatPattern::Every { interval_ms: 100 },
        Sample { n: 42 },
    );
    producer.upsert_repeatable(spec).await.expect("upsert");

    // Spawn the scheduler.
    let scheduler_cfg = SchedulerConfig {
        queue_name: queue.to_string(),
        tick_interval_ms: 25,
        batch: 64,
        max_stream_len: 100_000,
        lock_ttl_secs: 5,
        holder_id: "sched-1".to_string(),
        ..Default::default()
    };
    let scheduler: Scheduler<Sample> = Scheduler::new(redis_url(), scheduler_cfg);
    let sched_shutdown = CancellationToken::new();
    let sched_shutdown_clone = sched_shutdown.clone();
    let sched_handle = tokio::spawn(async move { scheduler.run(sched_shutdown_clone).await });

    let observed = drive_consumer_recording(queue, "repeat_fire_c", 3).await;
    sched_shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), sched_handle).await;

    // Every observed entry must carry `Job::name == "billing-cron"`. We don't
    // pin the exact n-count (more than 3 fires can land before shutdown), but
    // the first 3 are enough to verify the property.
    assert!(
        observed
            .iter()
            .all(|(name, n)| name == "billing-cron" && *n == 42),
        "every repeatable fire must arrive with Job::name = job_name; got {observed:?}"
    );
    assert!(
        observed.len() >= 3,
        "expected at least 3 fires, got {}",
        observed.len()
    );

    // Cleanup.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
            vec![Value::from(scheduler_lock_key(queue))],
        )
        .await
        .expect("DEL scheduler:lock");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
            vec![Value::from(repeat_key_fn(queue))],
        )
        .await
        .expect("DEL repeat");

    let _: () = admin.quit().await.unwrap();
}

/// Handler fails first attempt → retry-via-delayed-ZSET re-encodes with the
/// slice-3 prefix → promoter strips and re-XADDs with `n` → second attempt's
/// `Job::name` matches the first attempt. This is the load-bearing
/// retry-preservation case the slice-1 doc warned about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn retry_via_delayed_zset_preserves_name() {
    let admin = admin().await;
    let queue = "name_retry_via_delayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    producer
        .add_with_options(Sample { n: 99 }, AddOptions::new().with_name("send-email"))
        .await
        .expect("add named");

    // Consumer: handler fails the first attempt, succeeds the second. Records
    // `(attempt, name)` on every invocation.
    let invocations: Arc<Mutex<Vec<(u32, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let invocations_h = invocations.clone();
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_h = attempts.clone();
    let cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: "retry_c1".to_string(),
        batch: 64,
        block_ms: 50,
        claim_min_idle_ms: 30_000,
        concurrency: 1,
        max_attempts: 5,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled: true,
        delayed_poll_interval_ms: 25,
        delayed_promote_batch: 256,
        delayed_max_stream_len: 100_000,
        delayed_lock_ttl_secs: 5,
        retry: chasquimq::RetryConfig {
            initial_backoff_ms: 5,
            max_backoff_ms: 50,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        ..Default::default()
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let invocations = invocations_h.clone();
                    let attempts = attempts_h.clone();
                    async move {
                        let attempt = job.attempt + 1; // 1-indexed observed
                        invocations
                            .lock()
                            .unwrap()
                            .push((attempt, job.name.clone()));
                        let prior = attempts.fetch_add(1, Ordering::SeqCst);
                        if prior == 0 {
                            // First invocation: fail to trigger retry.
                            Err::<(), _>(HandlerError::new(std::io::Error::other(
                                "retry-name-trigger",
                            )))
                        } else {
                            Ok(())
                        }
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let invocations_check = invocations.clone();
    wait_until(Duration::from_secs(15), move || {
        let invocations = invocations_check.clone();
        async move { invocations.lock().unwrap().len() >= 2 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;

    let invs = invocations.lock().unwrap().clone();
    assert!(
        invs.len() >= 2,
        "expected ≥2 invocations (fail + retry); got {invs:?}"
    );
    // The retry attempt must carry the original name. Pre-slice-3 the second
    // attempt would arrive with `name = ""` because the retry-relocator
    // ZADDs raw msgpack and the promoter doesn't know to re-emit `n`.
    for (attempt, name) in &invs {
        assert_eq!(
            name, "send-email",
            "retry attempt {attempt} dropped Job::name; got name={name:?}"
        );
    }
    // Sanity: the first invocation was attempt 1 (the failure), the second
    // is attempt 2 (the retry).
    assert_eq!(invs[0].0, 1);
    assert!(
        invs[1].0 >= 2,
        "second invocation must be a retry (attempt >= 2)"
    );

    let _: () = admin.quit().await.unwrap();
}

/// Cancel a delayed job that was scheduled with a name. The side-index
/// stores the exact prefixed bytes; the cancel script ZREMs by byte equality
/// without decoding the prefix. After cancel, the stream stays empty and
/// no consumer side-effect ever fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn cancel_delayed_with_name_succeeds() {
    let admin = admin().await;
    let queue = "name_delayed_cancel";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    let id = "stable-id-cancel".to_string();
    producer
        .add_in_with_options(
            Duration::from_secs(60), // far enough that the promoter never picks it up
            Sample { n: 1 },
            AddOptions::new()
                .with_id(id.clone())
                .with_name("send-email"),
        )
        .await
        .expect("add_in_with_options with id+name");

    let removed = producer.cancel_delayed(&id).await.expect("cancel_delayed");
    assert!(
        removed,
        "cancel_delayed must ZREM the prefixed member by byte equality"
    );

    // Stream remains empty (job was never promoted, was cancelled before its
    // run-at).
    let skey = stream_key(queue);
    let xlen: Value = admin
        .custom(
            CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
            vec![Value::from(skey)],
        )
        .await
        .expect("XLEN");
    assert!(matches!(xlen, Value::Integer(0)), "stream must be empty");

    let _: () = admin.quit().await.unwrap();
}
