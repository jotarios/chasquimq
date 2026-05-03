//! Integration tests for slice 8: per-job retry overrides.
//!
//! Each job carries an optional `JobRetryOverride { max_attempts, backoff }`
//! in its encoded payload. The consumer's retry / DLQ gates must honor those
//! overrides instead of (or in addition to) the queue-wide
//! `ConsumerConfig::max_attempts` / `ConsumerConfig::retry`.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{AddOptions, Producer, dlq_key};
use chasquimq::{BackoffKind, BackoffSpec, HandlerError, Job, JobRetryOverride};
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
