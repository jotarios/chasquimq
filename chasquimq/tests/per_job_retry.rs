//! Integration tests for slice 8: per-job retry overrides.
//!
//! Each job carries an optional `JobRetryOverride { max_attempts, backoff }`
//! in its encoded payload. The consumer's retry / DLQ gates must honor those
//! overrides instead of (or in addition to) the queue-wide
//! `ConsumerConfig::max_attempts` / `ConsumerConfig::retry`.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{AddOptions, Producer, delayed_key, dlq_key};
use chasquimq::{BackoffKind, BackoffSpec, Error, HandlerError, Job, JobRetryOverride};
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

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 10_000,
        ..Default::default()
    }
}

fn fast_consumer_cfg(queue: &str, consumer_id: &str, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: consumer_id.to_string(),
        max_attempts,
        block_ms: 50,
        delayed_poll_interval_ms: 25,
        retry: RetryConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 200,
            multiplier: 2.0,
            jitter_ms: 0,
        },
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

/// Per-job `max_attempts: 1` overrides a generous queue-wide max=10:
/// the job should fail once and immediately DLQ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_max_attempts_smaller_than_queue() {
    let admin = admin().await;
    let queue = "perjob_max_smaller";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(1),
                backoff: None,
            }),
        )
        .await
        .expect("add");

    // Queue-wide budget is 10. Per-job override of 1 must win.
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 10));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dlq = dlq.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "per-job max_attempts=1 should produce exactly 1 handler invocation"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Per-job `max_attempts: 5` overrides a tight queue-wide max=2:
/// the job should run 5 times before landing in the DLQ.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_max_attempts_larger_than_queue() {
    let admin = admin().await;
    let queue = "perjob_max_larger";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(5),
                backoff: None,
            }),
        )
        .await
        .expect("add");

    // Queue-wide budget is 2. Per-job override of 5 must win.
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 2));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(8), || {
        let admin = admin.clone();
        let dlq = dlq.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        5,
        "per-job max_attempts=5 should produce exactly 5 handler invocations"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Per-job `backoff: { kind: "fixed", delay_ms: 80 }` should produce
/// approximately equal-length intervals between retries, even though the
/// queue-wide config is exponential.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_fixed_backoff_intervals() {
    let admin = admin().await;
    let queue = "perjob_fixed_backoff";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(4), // 4 runs total → 3 retry intervals
                backoff: Some(BackoffSpec {
                    kind: BackoffKind::Fixed,
                    delay_ms: 80,
                    max_delay_ms: None,
                    multiplier: None,
                    jitter_ms: Some(0),
                }),
            }),
        )
        .await
        .expect("add");

    let timestamps: Arc<std::sync::Mutex<Vec<Instant>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let timestamps_h = timestamps.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 99));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let timestamps = timestamps_h.clone();
                    async move {
                        timestamps.lock().unwrap().push(Instant::now());
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(10), || {
        let timestamps = timestamps.clone();
        async move { timestamps.lock().unwrap().len() >= 4 }
    })
    .await;

    let stamps = timestamps.lock().unwrap().clone();
    assert!(stamps.len() >= 4, "need 4 timestamps, got {}", stamps.len());
    let intervals: Vec<u64> = stamps
        .windows(2)
        .map(|w| w[1].duration_since(w[0]).as_millis() as u64)
        .collect();
    // All three intervals should be ~80ms (fixed). Tolerate scheduler /
    // delayed-poll-tick slack: interval >= 60, and not exponentially blowing up.
    for (i, ms) in intervals.iter().enumerate() {
        assert!(
            *ms >= 60,
            "fixed-backoff interval #{i} too short: {ms}ms (expected ~80ms)"
        );
        assert!(
            *ms <= 400,
            "fixed-backoff interval #{i} too long: {ms}ms (expected ~80ms)"
        );
    }
    // Last interval should not be much larger than first (no exponential growth).
    let first = intervals.first().copied().unwrap_or(0);
    let last = intervals.last().copied().unwrap_or(0);
    assert!(
        last <= first.saturating_mul(2) + 100,
        "fixed should not grow exponentially: {intervals:?}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Per-job exponential backoff with `delay_ms: 100, multiplier: 3.0` should
/// produce intervals close to 100ms / 300ms / 900ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_exponential_backoff_intervals() {
    let admin = admin().await;
    let queue = "perjob_exp_backoff";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(4),
                backoff: Some(BackoffSpec {
                    kind: BackoffKind::Exponential,
                    delay_ms: 100,
                    max_delay_ms: Some(10_000),
                    multiplier: Some(3.0),
                    jitter_ms: Some(0),
                }),
            }),
        )
        .await
        .expect("add");

    let timestamps: Arc<std::sync::Mutex<Vec<Instant>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let timestamps_h = timestamps.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 99));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let timestamps = timestamps_h.clone();
                    async move {
                        timestamps.lock().unwrap().push(Instant::now());
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(15), || {
        let timestamps = timestamps.clone();
        async move { timestamps.lock().unwrap().len() >= 4 }
    })
    .await;

    let stamps = timestamps.lock().unwrap().clone();
    assert!(stamps.len() >= 4, "need 4 timestamps, got {}", stamps.len());
    let intervals: Vec<u64> = stamps
        .windows(2)
        .map(|w| w[1].duration_since(w[0]).as_millis() as u64)
        .collect();
    let expected = [100u64, 300, 900];
    for (i, (got, want)) in intervals.iter().zip(expected.iter()).enumerate() {
        // Lower bound: backoff must be at least the prescribed amount.
        // (Promoter / consumer add a few ms of slack on top.)
        assert!(
            *got >= want.saturating_sub(20),
            "exp-backoff interval #{i} too short: {got}ms (expected ~{want}ms)"
        );
        // Upper bound: stay close enough that we know the multiplier applied.
        assert!(
            *got <= want.saturating_mul(3),
            "exp-backoff interval #{i} too long: {got}ms (expected ~{want}ms)"
        );
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Per-job override survives the retry round-trip: the second retry must
/// see the same `JobRetryOverride` the producer set on the first attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_override_survives_retry_round_trip() {
    let admin = admin().await;
    let queue = "perjob_round_trip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let want = JobRetryOverride {
        max_attempts: Some(4),
        backoff: Some(BackoffSpec {
            kind: BackoffKind::Exponential,
            delay_ms: 30,
            max_delay_ms: Some(5_000),
            multiplier: Some(2.0),
            jitter_ms: Some(0),
        }),
    };
    producer
        .add_with_options(Sample { n: 1 }, AddOptions::new().with_retry(want))
        .await
        .expect("add");

    let observed: Arc<std::sync::Mutex<Vec<Option<JobRetryOverride>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_h = observed.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 99));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let observed = observed_h.clone();
                    async move {
                        observed.lock().unwrap().push(job.retry);
                        if job.attempt < 2 {
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

    wait_until(Duration::from_secs(8), || {
        let observed = observed.clone();
        async move { observed.lock().unwrap().len() >= 3 }
    })
    .await;

    let seen = observed.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "should see 3 attempts before success");
    for (i, retry) in seen.iter().enumerate() {
        assert_eq!(
            retry.as_ref(),
            Some(&want),
            "retry override mismatch on attempt {i}"
        );
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// `replay_dlq` should reset `attempt` to 0 but leave the per-job
/// `retry` override intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn replay_dlq_preserves_retry_override() {
    let admin = admin().await;
    let queue = "perjob_replay_preserve";
    flush_all(&admin, queue).await;

    // Manually XADD a DLQ entry with attempt=99 + a retry override.
    let dlq = dlq_key(queue);
    let want = JobRetryOverride {
        max_attempts: Some(7),
        backoff: Some(BackoffSpec {
            kind: BackoffKind::Fixed,
            delay_ms: 42,
            max_delay_ms: None,
            multiplier: None,
            jitter_ms: None,
        }),
    };
    let mut job = Job::with_id("preserved-job".to_string(), Sample { n: 1 });
    job.attempt = 99;
    job.retry = Some(want);
    let bytes = bytes::Bytes::from(rmp_serde::to_vec(&job).unwrap());
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(dlq.as_str()),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes),
                Value::from("source_id"),
                Value::from("preserved-job"),
                Value::from("reason"),
                Value::from("retries_exhausted"),
            ],
        )
        .await
        .expect("XADD dlq");

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let n = producer.replay_dlq(10).await.expect("replay_dlq");
    assert_eq!(n, 1);

    // Read the replayed entry off the main stream and confirm:
    //   * attempt was reset to 0
    //   * retry override survived verbatim
    let stream = format!("{{chasqui:{queue}}}:stream");
    let res: Value = admin
        .custom(
            CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
            vec![
                Value::from(stream.as_str()),
                Value::from("-"),
                Value::from("+"),
                Value::from("COUNT"),
                Value::from(10_i64),
            ],
        )
        .await
        .expect("XRANGE");
    let payload = extract_first_payload(&res).expect("expected one entry on main stream");
    let decoded: Job<Sample> = rmp_serde::from_slice(&payload).expect("decode replayed job");
    assert_eq!(decoded.attempt, 0, "replay should reset attempt");
    assert_eq!(
        decoded.retry,
        Some(want),
        "replay must preserve per-job retry override"
    );

    let _: () = admin.quit().await.unwrap();
}

/// Reader-side CLAIM-path regression: when a consumer reads a job, leaves it
/// pending without acking, and then shuts down, the entry remains in the
/// PEL. A second consumer started against the same group will reclaim it
/// via XREADGROUP's `CLAIM <ms>` directive (Redis 8.4 idle-pending read)
/// once the idle threshold passes. The test pins that the per-job
/// `JobRetryOverride { max_attempts: 7 }` carried in the encoded payload
/// is still honored on the reclaim — i.e. the dispatch-time DLQ gate uses
/// the per-job override and not just the queue-wide `max_attempts`.
///
/// Without this guarantee a worker crash + reclaim would silently fall
/// back to the queue-wide retry budget, defeating the per-job override
/// for any job that was unlucky enough to be reclaimed. The same dispatch
/// code path serves fresh reads and reclaimed reads (`dispatch_one` in
/// `consumer/reader.rs`), so this is more a pinning test than a behavior
/// test, but the audit on PR #14 explicitly asked for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn per_job_override_honored_after_claim_reclaim() {
    let admin = admin().await;
    let queue = "perjob_claim_reclaim";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");

    // Per-job budget of 7 attempts; queue-wide budget is 2 — if the
    // reclaim path silently fell back to the queue config, the job would
    // hit DLQ after attempt 2 and the test would observe < 7 calls.
    producer
        .add_with_options(
            Sample { n: 1 },
            AddOptions::new().with_retry(JobRetryOverride {
                max_attempts: Some(7),
                backoff: Some(BackoffSpec {
                    kind: BackoffKind::Fixed,
                    delay_ms: 30,
                    max_delay_ms: None,
                    multiplier: None,
                    jitter_ms: Some(0),
                }),
            }),
        )
        .await
        .expect("add");

    // Consumer A: reads the job, parks the handler so it never acks, then
    // we shut it down. After shutdown the entry remains pending against
    // consumer A's name in the PEL.
    let a_started = Arc::new(AtomicUsize::new(0));
    let a_started_h = a_started.clone();
    let mut cfg_a = fast_consumer_cfg(queue, "c_a", 2);
    cfg_a.claim_min_idle_ms = 250;
    let consumer_a: Consumer<Sample> = Consumer::new(redis_url(), cfg_a);
    let shutdown_a = CancellationToken::new();
    let shutdown_a_clone = shutdown_a.clone();
    let join_a = tokio::spawn(async move {
        consumer_a
            .run(
                move |_: Job<Sample>| {
                    let started = a_started_h.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        // Park the handler past A's lifetime so the entry
                        // never gets acked and stays pending.
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        Ok::<(), HandlerError>(())
                    }
                },
                shutdown_a_clone,
            )
            .await
    });

    // Wait until consumer A picks up the job (handler started).
    wait_until(Duration::from_secs(5), || {
        let a_started = a_started.clone();
        async move { a_started.load(Ordering::SeqCst) >= 1 }
    })
    .await;
    assert_eq!(
        a_started.load(Ordering::SeqCst),
        1,
        "consumer A should have read the job exactly once"
    );

    // Tear consumer A down without letting the handler finish — the
    // shutdown deadline is short so the parked handler is aborted.
    shutdown_a.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), join_a).await;

    // Wait long enough that the PEL entry exceeds `claim_min_idle_ms` so
    // consumer B's first XREADGROUP picks it up via the CLAIM directive
    // (the Redis 8.4 idle-pending read).
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Consumer B: same group, different consumer name. Counts handler
    // calls. Returns Err every time so we exercise the full retry budget
    // — the override of max_attempts=7 must win over the queue-wide 2.
    let b_calls = Arc::new(AtomicUsize::new(0));
    let b_calls_h = b_calls.clone();
    let mut cfg_b = fast_consumer_cfg(queue, "c_b", 2);
    cfg_b.claim_min_idle_ms = 250;
    let consumer_b: Consumer<Sample> = Consumer::new(redis_url(), cfg_b);
    let shutdown_b = CancellationToken::new();
    let shutdown_b_clone = shutdown_b.clone();
    let join_b = tokio::spawn(async move {
        consumer_b
            .run(
                move |_: Job<Sample>| {
                    let calls = b_calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(HandlerError::new(std::io::Error::other("nope")))
                    }
                },
                shutdown_b_clone,
            )
            .await
    });

    let dlq = dlq_key(queue);
    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let dlq = dlq.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    // The job was first started by consumer A (counted once in `a_started`)
    // but never delivered (parked + aborted). Consumer B reclaims and runs
    // it. The dispatch-time gate honors the per-job `max_attempts: 7` so
    // B's handler should fire 7 times before the entry hits the DLQ.
    //
    // The CLAIM-derived `delivery_count` is at least 2 by the time B sees
    // the entry (one read by A + one read by B), so the arrival-time
    // check (`prior_attempts = max(job.attempt, claim_seen)`) accepts the
    // first delivery without immediately routing to DLQ. From there the
    // per-handler retry path drives `job.attempt` to grow naturally.
    //
    // We assert >= 5 to stay well clear of the queue-wide budget of 2 —
    // any reclaim regression would observe ≤ 2 here.
    let calls = b_calls.load(Ordering::SeqCst);
    assert!(
        calls >= 5,
        "per-job max_attempts=7 must survive CLAIM reclaim; observed {calls} handler calls (queue-wide budget is 2)"
    );

    shutdown_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join_b).await;
    let _: () = admin.quit().await.unwrap();
}

/// `add_in_with_options` already has implicit coverage; mirror it for the
/// absolute-time `add_at_with_options` so both delayed entry points are
/// pinned with per-job retry overrides riding inside the encoded payload.
///
/// The job is scheduled ~150ms in the future; the consumer (with the
/// inline promoter enabled) drains the delayed ZSET, and the per-job
/// override is observed on the handler's `Job<T>::retry` field — proof
/// the override survived ZADD → ZRANGEBYSCORE → XADD → XREADGROUP →
/// rmp_serde::from_slice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_at_with_options_carries_retry_override() {
    let admin = admin().await;
    let queue = "perjob_add_at_opts";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");

    let want = JobRetryOverride {
        max_attempts: Some(4),
        backoff: Some(BackoffSpec {
            kind: BackoffKind::Fixed,
            delay_ms: 25,
            max_delay_ms: None,
            multiplier: None,
            jitter_ms: Some(0),
        }),
    };

    let when = SystemTime::now() + Duration::from_millis(150);
    producer
        .add_at_with_options(when, Sample { n: 7 }, AddOptions::new().with_retry(want))
        .await
        .expect("add_at_with_options");

    // Until the delay fires the entry sits in the delayed ZSET and the
    // main stream is empty. Sanity-check that path before spinning up
    // the consumer.
    let dkey = delayed_key(queue);
    assert_eq!(
        zcard_admin(&admin, &dkey).await,
        1,
        "scheduled entry should land in the delayed ZSET"
    );

    let observed: Arc<std::sync::Mutex<Vec<Option<JobRetryOverride>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed_h = observed.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), fast_consumer_cfg(queue, "c1", 99));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let observed = observed_h.clone();
                    async move {
                        observed.lock().unwrap().push(job.retry);
                        Ok::<(), HandlerError>(())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let observed = observed.clone();
        async move { !observed.lock().unwrap().is_empty() }
    })
    .await;

    let seen = observed.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        1,
        "exactly one delivery expected (handler returns Ok)"
    );
    assert_eq!(
        seen[0].as_ref(),
        Some(&want),
        "per-job override must survive the delayed-ZSET → stream round trip"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// `add_bulk_with_options` rejects `opts.id` when there's more than one
/// payload — silently using the id for the first job only would be a
/// footgun. With exactly one payload `opts.id` is allowed (it's the
/// single-job idempotent path). Empty payloads short-circuit without
/// touching Redis or the validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_with_options_rejects_id_with_multiple_payloads() {
    let admin = admin().await;
    let queue = "perjob_bulk_id_rejects";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");

    // > 1 payload + opts.id → Error::Config, no Redis touch.
    let res = producer
        .add_bulk_with_options(
            vec![Sample { n: 1 }, Sample { n: 2 }],
            AddOptions::new().with_id("shared".to_string()),
        )
        .await;
    assert!(
        matches!(res, Err(Error::Config(_))),
        "expected Error::Config for opts.id + multiple payloads; got {res:?}"
    );

    // Exactly one payload + opts.id → fine, equivalent to add_with_options.
    let ids = producer
        .add_bulk_with_options(
            vec![Sample { n: 1 }],
            AddOptions::new().with_id("solo".to_string()),
        )
        .await
        .expect("single payload + opts.id should work");
    assert_eq!(ids, vec!["solo".to_string()]);

    // Empty payloads short-circuits without hitting the validation.
    let ids = producer
        .add_bulk_with_options(
            Vec::<Sample>::new(),
            AddOptions::new().with_id("ignored".to_string()),
        )
        .await
        .expect("empty + opts.id is a noop");
    assert!(ids.is_empty());

    let _: () = admin.quit().await.unwrap();
}

async fn zcard_admin(admin: &Client, key: &str) -> i64 {
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

fn extract_first_payload(v: &Value) -> Option<bytes::Bytes> {
    // XRANGE response is Array<[id, Array<[field1, val1, ...]>]>.
    let entries = match v {
        Value::Array(a) => a,
        _ => return None,
    };
    let first = entries.first()?;
    let pair = match first {
        Value::Array(a) => a,
        _ => return None,
    };
    let fields = pair.get(1)?;
    let fields = match fields {
        Value::Array(a) => a,
        _ => return None,
    };
    // Find the "d" field's value.
    let mut iter = fields.iter();
    while let Some(k) = iter.next() {
        let v = iter.next()?;
        let key = match k {
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => std::str::from_utf8(b).ok()?.to_string(),
            _ => continue,
        };
        if key == "d" {
            return match v {
                Value::Bytes(b) => Some(b.clone()),
                Value::String(s) => Some(bytes::Bytes::copy_from_slice(s.as_bytes())),
                _ => None,
            };
        }
    }
    None
}
