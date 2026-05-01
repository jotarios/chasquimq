use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, dlq_key, stream_key};
use chasquimq::{HandlerError, Job};
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

fn small_dlq_consumer_cfg(queue: &str, consumer_id: &str, dlq_max: u64) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: consumer_id.to_string(),
        max_attempts: 2,
        block_ms: 50,
        delayed_poll_interval_ms: 25,
        shutdown_deadline_secs: 2,
        dlq_max_stream_len: dlq_max,
        retry: chasquimq::RetryConfig {
            initial_backoff_ms: 5,
            max_backoff_ms: 20,
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

/// Manually populate the DLQ stream with a single entry by encoding a Job<Sample>
/// directly. Bypasses the consumer pipeline so tests don't get tripped by a
/// blocking XREADGROUP that survives client teardown server-side.
async fn manually_populate_dlq(admin: &Client, queue: &str, attempt: u32) {
    let dlq = dlq_key(queue);
    let mut job = Job::with_id("test-job".to_string(), Sample { n: 1 });
    job.attempt = attempt;
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
                Value::from("orig-id"),
                Value::from("reason"),
                Value::from("retries_exhausted"),
            ],
        )
        .await
        .expect("XADD dlq");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn peek_dlq_returns_metadata() {
    let admin = admin().await;
    let queue = "dlq_peek";
    flush_all(&admin, queue).await;

    manually_populate_dlq(&admin, queue, 2).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let entries = producer.peek_dlq(10).await.expect("peek_dlq");
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.reason, "retries_exhausted");
    assert!(
        !e.payload.is_empty(),
        "payload should carry the encoded job"
    );
    assert_eq!(e.source_id, "orig-id");

    // peek doesn't consume.
    let dlq = dlq_key(queue);
    assert_eq!(xlen(&admin, &dlq).await, 1, "peek must not delete");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn replay_moves_dlq_entries_back_to_stream() {
    let admin = admin().await;
    let queue = "dlq_replay";
    flush_all(&admin, queue).await;

    manually_populate_dlq(&admin, queue, 2).await;

    let dlq = dlq_key(queue);
    let main = stream_key(queue);
    assert_eq!(xlen(&admin, &dlq).await, 1);
    assert_eq!(xlen(&admin, &main).await, 0);

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let replayed = producer.replay_dlq(10).await.expect("replay_dlq");
    assert_eq!(replayed, 1);

    assert_eq!(xlen(&admin, &dlq).await, 0, "DLQ drained");
    assert_eq!(xlen(&admin, &main).await, 1, "back in main stream");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn replay_resets_attempt_so_retries_run_again() {
    let admin = admin().await;
    let queue = "dlq_replay_reset";
    flush_all(&admin, queue).await;

    // Stage a DLQ entry whose payload encodes attempt=2 (exhausted).
    manually_populate_dlq(&admin, queue, 2).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let replayed = producer.replay_dlq(10).await.expect("replay_dlq");
    assert_eq!(replayed, 1);

    let main = stream_key(queue);
    assert_eq!(
        xlen(&admin, &main).await,
        1,
        "main should have replayed entry"
    );
    let dlq = dlq_key(queue);
    assert_eq!(xlen(&admin, &dlq).await, 0, "dlq should be drained");

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), small_dlq_consumer_cfg(queue, "c2", 100_000));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // After replay the attempt counter restarts at 0; second
                        // attempt (job.attempt == 1) succeeds. If attempt wasn't
                        // reset the entry would have re-DLQ'd on first dispatch.
                        if job.attempt == 0 {
                            Err::<(), _>(HandlerError::new(std::io::Error::other("flaky")))
                        } else {
                            Ok(())
                        }
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let calls = calls.clone();
        async move { calls.load(Ordering::SeqCst) >= 2 }
    })
    .await;

    let dlq = dlq_key(queue);
    assert_eq!(
        xlen(&admin, &dlq).await,
        0,
        "after replay-reset and successful retry, DLQ stays empty"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn replay_empty_dlq_is_zero() {
    let admin = admin().await;
    let queue = "dlq_replay_empty";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let n = producer.replay_dlq(100).await.expect("replay_dlq empty");
    assert_eq!(n, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn dlq_maxlen_caps_growth() {
    let admin = admin().await;
    let queue = "dlq_maxlen";
    flush_all(&admin, queue).await;

    // Stage many entries into DLQ stream with MAXLEN ~ 10. Approximate trim
    // only fires when it can drop a full radix-tree node, so the test needs
    // enough entries to cross at least one node boundary.
    let dlq = dlq_key(queue);
    for n in 0..2000_u32 {
        let payload = bytes::Bytes::from(format!("payload-{n}").into_bytes());
        let _: Value = admin
            .custom(
                CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
                vec![
                    Value::from(dlq.as_str()),
                    Value::from("MAXLEN"),
                    Value::from("~"),
                    Value::from(10_i64),
                    Value::from("*"),
                    Value::from("d"),
                    Value::Bytes(payload),
                    Value::from("source_id"),
                    Value::from(format!("orig-{n}")),
                    Value::from("reason"),
                    Value::from("retries_exhausted"),
                ],
            )
            .await
            .expect("XADD dlq");
    }

    let dlq_len = xlen(&admin, &dlq).await;
    assert!(
        dlq_len < 2000,
        "with MAXLEN ~ 10 over 2000 XADDs must trim something; got {dlq_len}"
    );
    assert!(
        dlq_len < 200,
        "approximate trim allows overshoot but should be far below 2000; got {dlq_len}"
    );

    let _: () = admin.quit().await.unwrap();
}
