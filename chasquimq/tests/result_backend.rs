//! Integration tests for the slice 5a result backend.
//!
//! `ConsumerConfig::store_results = true` opts in to writing the handler's
//! returned bytes to a per-job result key (`{chasqui:<queue>}:result:<id>`)
//! with TTL `result_ttl_secs`. `Producer::get_result` reads the key.
//! `None` collapses the three indistinguishable cases (not yet completed /
//! expired / never written), so tests assert on a single cause at a time.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::Producer;
use chasquimq::{Bytes, HandlerError, Job, JobId};
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
    // Wipe the queue's keyspace including any leftover result keys from
    // a prior aborted run. SCAN is cheap on tiny test keyspaces.
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "events",
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
    // Wipe any stray result keys from previous test runs in this queue.
    let pattern = format!("{{chasqui:{queue}}}:result:*");
    let scan: Value = admin
        .custom(
            CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
            vec![Value::from(pattern)],
        )
        .await
        .expect("KEYS");
    if let Value::Array(items) = scan
        && !items.is_empty()
    {
        let mut args = vec![];
        for v in items {
            args.push(v);
        }
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                args,
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

fn consumer_cfg(queue: &str, store_results: bool, ttl_secs: u64) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: format!("c-{}", uuid::Uuid::new_v4()),
        block_ms: 50,
        // delayed_enabled drives the in-process promoter, which is what
        // moves a retry from the delayed ZSET back onto the stream. Tests
        // that rely on retry-then-DLQ must keep this on.
        delayed_enabled: true,
        delayed_poll_interval_ms: 25,
        run_scheduler: false,
        events_enabled: false,
        concurrency: 4,
        max_attempts: 3,
        retry: RetryConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 200,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        store_results,
        result_ttl_secs: ttl_secs,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_round_trip() {
    let admin = admin().await;
    let queue = "result_round_trip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 7 }).await.expect("add");

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(Bytes::from_static(b"hello")) },
                shutdown_clone,
            )
            .await
    });

    // Wait for the result to land. `get_result` returning Some means the
    // engine successfully ran XACKDEL + SET in the same Lua call.
    let id_h = id.clone();
    let producer_h = producer.clone();
    wait_until(Duration::from_secs(5), || {
        let id = id_h.clone();
        let producer = producer_h.clone();
        async move {
            matches!(producer.get_result(&id).await, Ok(Some(b)) if b.as_ref() == b"hello")
        }
    })
    .await;

    let res = producer.get_result(&id).await.expect("get_result");
    assert_eq!(res.as_deref(), Some(b"hello".as_ref()));

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_store_results_false_no_result() {
    let admin = admin().await;
    let queue = "result_disabled";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 1 }).await.expect("add");

    // store_results = false: the consumer must take the batched-ack fast path
    // and never invoke JOB_OK_SCRIPT, regardless of what the handler returns.
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, false, 60));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(Bytes::from_static(b"would-be-result"))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let calls = calls.clone();
        async move { calls.load(Ordering::SeqCst) >= 1 }
    })
    .await;
    // Give the ack flusher a tick to run; result should never appear.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let res = producer.get_result(&id).await.expect("get_result");
    assert!(
        res.is_none(),
        "store_results=false must skip the result write"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_empty_result_skipped() {
    let admin = admin().await;
    let queue = "result_empty";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 1 }).await.expect("add");

    // store_results=true but handler returns Bytes::new() — the engine's
    // worker-side guard short-circuits and routes through the batched
    // XACKDEL path instead. JOB_OK_SCRIPT itself also gates SET on a
    // non-empty payload (defense-in-depth across the boundary).
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(Bytes::new())
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let calls = calls.clone();
        async move { calls.load(Ordering::SeqCst) >= 1 }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let res = producer.get_result(&id).await.expect("get_result");
    assert!(res.is_none(), "empty result must skip the SET");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_ttl_expiry() {
    let admin = admin().await;
    let queue = "result_ttl";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 1 }).await.expect("add");

    // 1-second TTL. After ~2 seconds Redis must have evicted the key.
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 1));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(Bytes::from_static(b"transient")) },
                shutdown_clone,
            )
            .await
    });

    let id_h = id.clone();
    let producer_h = producer.clone();
    wait_until(Duration::from_secs(5), || {
        let id = id_h.clone();
        let producer = producer_h.clone();
        async move { matches!(producer.get_result(&id).await, Ok(Some(_))) }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let res = producer.get_result(&id).await.expect("get_result");
    assert!(res.is_none(), "result must have TTL'd out");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_failed_handler_no_result() {
    let admin = admin().await;
    let queue = "result_failed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 1 }).await.expect("add");

    // Handler always errors → retry-then-DLQ path. No `Ok(_)` arm runs, so
    // no result key is ever written. max_attempts=3 keeps the test brisk.
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err::<Bytes, _>(HandlerError::new(std::io::Error::other("never-ok")))
                },
                shutdown_clone,
            )
            .await
    });

    let dlq_key_str = chasquimq::producer::dlq_key(queue);
    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let dlq = dlq_key_str.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    let res = producer.get_result(&id).await.expect("get_result");
    assert!(res.is_none(), "failed handler must never write a result");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_dlq_no_result() {
    let admin = admin().await;
    let queue = "result_dlq";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let id = producer.add(Sample { n: 1 }).await.expect("add");

    // Unrecoverable: short-circuits straight to DLQ. No retry, no result.
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err::<Bytes, _>(HandlerError::unrecoverable(std::io::Error::other(
                        "terminal",
                    )))
                },
                shutdown_clone,
            )
            .await
    });

    let dlq_key_str = chasquimq::producer::dlq_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dlq = dlq_key_str.clone();
        async move { xlen(&admin, &dlq).await >= 1 }
    })
    .await;

    let res = producer.get_result(&id).await.expect("get_result");
    assert!(
        res.is_none(),
        "DLQ'd unrecoverable job must not have a result key"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_backend_get_result_bulk() {
    let admin = admin().await;
    let queue = "result_bulk";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let ids = producer
        .add_bulk(vec![Sample { n: 1 }, Sample { n: 2 }])
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), 2);

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    let payload: Sample = job.payload;
                    Ok(Bytes::from(format!("ok-{}", payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    let ids_h: Vec<JobId> = ids.clone();
    let producer_h = producer.clone();
    wait_until(Duration::from_secs(5), || {
        let ids = ids_h.clone();
        let producer = producer_h.clone();
        async move {
            let res = producer.get_result_bulk(&ids).await.unwrap_or_default();
            res.len() == ids.len() && res.iter().all(|r| r.is_some())
        }
    })
    .await;

    let res = producer
        .get_result_bulk(&ids)
        .await
        .expect("get_result_bulk");
    assert_eq!(res.len(), 2);
    // Aligned by index. Order preserved.
    assert_eq!(res[0].as_deref(), Some(b"ok-1".as_ref()));
    assert_eq!(res[1].as_deref(), Some(b"ok-2".as_ref()));

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}
