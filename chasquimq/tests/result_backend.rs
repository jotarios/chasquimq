//! Integration tests for the slice 5a result backend.
//!
//! `ConsumerConfig::store_results = true` opts in to writing the handler's
//! returned bytes to a per-job result key (`{chasqui:<queue>}:result:<id>`)
//! with TTL `result_ttl_secs`. `Producer::get_result` reads the key.
//! `None` collapses the three indistinguishable cases (not yet completed /
//! expired / never written), so tests assert on a single cause at a time.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::metrics::{MetricsSink, testing::InMemorySink};
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

// -----------------------------------------------------------------------------
// Pipelined result-writer (issue #92): batching, idle flush, NOSCRIPT recovery,
// partial-Lua-zero drop, metric cardinality, shutdown drain.
// -----------------------------------------------------------------------------

async fn count_result_keys(admin: &Client, queue: &str) -> usize {
    let pattern = format!("{{chasqui:{queue}}}:result:*");
    let scan: Value = admin
        .custom(
            CustomCommand::new_static("KEYS", ClusterHash::FirstKey, false),
            vec![Value::from(pattern)],
        )
        .await
        .expect("KEYS");
    match scan {
        Value::Array(items) => items.len(),
        _ => 0,
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

async fn xpending_ids(admin: &Client, key: &str, group: &str, max: u32) -> Vec<String> {
    // XPENDING <key> <group> - + <max> — returns array of [id, consumer, ms,
    // count] per entry. We just want the IDs. Returns an empty Vec if the
    // group hasn't been created yet (consumer still booting), so callers can
    // poll until it shows up.
    let res = admin
        .custom::<Value, _>(
            CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
            vec![
                Value::from(key),
                Value::from(group),
                Value::from("-"),
                Value::from("+"),
                Value::from(max as i64),
            ],
        )
        .await;
    let Ok(res) = res else {
        // NOGROUP / NOKEY before the consumer has called `ensure_group` —
        // treat as "no pending entries yet".
        return Vec::new();
    };
    let mut ids = Vec::new();
    if let Value::Array(rows) = res {
        for row in rows {
            if let Value::Array(parts) = row
                && let Some(first) = parts.into_iter().next()
            {
                match first {
                    Value::String(s) => ids.push(s.to_string()),
                    Value::Bytes(b) => {
                        if let Ok(s) = std::str::from_utf8(&b) {
                            ids.push(s.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    ids
}

async fn xackdel_one(admin: &Client, key: &str, group: &str, id: &str) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false),
            vec![
                Value::from(key),
                Value::from(group),
                Value::from("IDS"),
                Value::from(1_i64),
                Value::from(id),
            ],
        )
        .await
        .expect("XACKDEL");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_batches_on_size() {
    // 128 jobs through a writer configured with batch=32 and a long idle
    // window: every flush has to be triggered by size, not by the deadline.
    // We don't have an in-engine "flushes" counter so the correctness oracle
    // is "all 128 result keys land within a tight deadline" — under the
    // pre-pipeline one-EVALSHA-per-job path this same 128/concurrent path
    // sat at ~8k jobs/s (issue #92), so a 10-second budget proves batching
    // is doing real work.
    let admin = admin().await;
    let queue = "result_writer_batches_on_size";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let samples: Vec<Sample> = (0..128).map(|n| Sample { n: n as u32 }).collect();
    let ids = producer.add_bulk(samples).await.expect("add_bulk");
    assert_eq!(ids.len(), 128);

    let mut cfg = consumer_cfg(queue, true, 60);
    cfg.concurrency = 64;
    cfg.batch = 64;
    cfg.result_batch = 32;
    cfg.result_idle_ms = 500;
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    let admin_h = admin.clone();
    let queue_h = queue.to_string();
    wait_until(Duration::from_secs(10), || {
        let admin = admin_h.clone();
        let queue = queue_h.clone();
        async move { count_result_keys(&admin, &queue).await == 128 }
    })
    .await;

    assert_eq!(count_result_keys(&admin, queue).await, 128);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_flushes_on_idle() {
    // 3 jobs into a writer whose batch cap is far larger (128). The buffer
    // never fills; the only path that flushes is the idle deadline. Wait
    // just past `result_idle_ms` and assert all 3 result keys exist.
    let admin = admin().await;
    let queue = "result_writer_flushes_on_idle";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let ids = producer
        .add_bulk(vec![Sample { n: 1 }, Sample { n: 2 }, Sample { n: 3 }])
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), 3);

    let mut cfg = consumer_cfg(queue, true, 60);
    cfg.result_batch = 128;
    cfg.result_idle_ms = 25;
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    let admin_h = admin.clone();
    let queue_h = queue.to_string();
    wait_until(Duration::from_secs(2), || {
        let admin = admin_h.clone();
        let queue = queue_h.clone();
        async move { count_result_keys(&admin, &queue).await == 3 }
    })
    .await;

    assert_eq!(count_result_keys(&admin, queue).await, 3);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_handles_mid_pipeline_noscript() {
    // SCRIPT FLUSH between SHA load and first flush forces the first
    // pipelined EVALSHA to return NOSCRIPT. The locked recovery contract:
    // reload the SHA and rebuild the same pipeline as inline EVALs, single
    // retry. End state: every result key still lands and every entry is
    // acked off the stream.
    let admin = admin().await;
    let queue = "result_writer_mid_pipeline_noscript";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    let ids = producer
        .add_bulk((0..8).map(|n| Sample { n }).collect())
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), 8);

    // Flush the script cache up front to clear any leftover SHA from prior
    // tests; this is the "before SHA load" baseline.
    script_flush(&admin).await;

    let mut cfg = consumer_cfg(queue, true, 60);
    // Long idle so handler completions park in the writer's buffer; we
    // race the SCRIPT FLUSH against the first size-triggered flush.
    cfg.result_batch = 16;
    cfg.result_idle_ms = 300;
    cfg.concurrency = 8;
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    // Race a SCRIPT FLUSH against the worker pool's startup: the writer
    // SCRIPT-LOADs early, then we drop the cache out from under it before
    // any handler completes. The next pipelined flush hits NOSCRIPT and
    // takes the EVAL fallback. Correctness oracle: every entry lands
    // regardless of where on the timeline the FLUSH falls.
    tokio::time::sleep(Duration::from_millis(5)).await;
    script_flush(&admin).await;

    let admin_h = admin.clone();
    let queue_h = queue.to_string();
    wait_until(Duration::from_secs(10), || {
        let admin = admin_h.clone();
        let queue = queue_h.clone();
        async move { count_result_keys(&admin, &queue).await == 8 }
    })
    .await;

    assert_eq!(count_result_keys(&admin, queue).await, 8);

    // All entries acked off the stream.
    let stream = chasquimq::producer::stream_key(queue);
    let xlen_after = xlen(&admin, &stream).await;
    assert_eq!(
        xlen_after, 0,
        "all entries should be XACKDEL'd, none left on stream"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_partial_lua_zero_drops_silently() {
    // Setup: push N jobs, start consumer with a long result-writer idle so
    // every handler-Ok lands in the writer's buffer before any flush. While
    // the buffer is parked, manually XACKDEL one specific entry out of band.
    // When the pipelined flush fires, that entry's `JOB_OK_SCRIPT` sees
    // XACKDEL=-1 → returns 0 → debug-log only → no result key written for
    // that entry, while the others land normally.
    let admin = admin().await;
    let queue = "result_writer_partial_lua_zero";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    const N: u32 = 4;
    let ids = producer
        .add_bulk((0..N).map(|n| Sample { n }).collect())
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), N as usize);

    let mut cfg = consumer_cfg(queue, true, 60);
    cfg.concurrency = (N as usize).max(1);
    // Long idle: handler completions park for ~600ms after the worker
    // finishes, giving the test plenty of headroom to XACKDEL one entry.
    cfg.result_batch = 64;
    cfg.result_idle_ms = 600;
    let group = cfg.group.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    // Wait until all N entries are pending in the group (workers have read
    // them; handlers may have completed and the JobOk is parked in the
    // writer's buffer waiting for the idle flush).
    let stream = chasquimq::producer::stream_key(queue);
    let admin_h = admin.clone();
    let stream_h = stream.clone();
    let group_h = group.clone();
    wait_until(Duration::from_secs(5), || {
        let admin = admin_h.clone();
        let stream = stream_h.clone();
        let group = group_h.clone();
        async move { xpending_ids(&admin, &stream, &group, 32).await.len() == N as usize }
    })
    .await;

    // Pick one pending entry and XACKDEL it out of band. The next pipelined
    // flush will see XACKDEL=-1 for this id and skip the SET.
    let pending = xpending_ids(&admin, &stream, &group, 32).await;
    let victim = pending.first().expect("at least one pending").clone();
    xackdel_one(&admin, &stream, &group, &victim).await;

    // Wait for the rest to land.
    let admin_h = admin.clone();
    let queue_h = queue.to_string();
    wait_until(Duration::from_secs(10), || {
        let admin = admin_h.clone();
        let queue = queue_h.clone();
        async move { count_result_keys(&admin, &queue).await >= (N as usize - 1) }
    })
    .await;

    // Stabilization grace: let any in-flight flush settle.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let n_keys = count_result_keys(&admin, queue).await;
    assert_eq!(
        n_keys,
        N as usize - 1,
        "victim entry must not produce a result key (got {n_keys})"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_emits_one_completed_per_job() {
    // Cardinality regression net for the pipeline path. `JobOutcome::Ok`
    // fires per-handler-invocation in `consumer/worker.rs` BEFORE the
    // `JobOk` is handed off to the result writer. Pipelining the writer
    // must not change this count.
    let admin = admin().await;
    let queue = "result_writer_one_completed_per_job";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    const N: u32 = 24;
    let ids = producer
        .add_bulk((0..N).map(|n| Sample { n }).collect())
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), N as usize);

    let sink = Arc::new(InMemorySink::new());
    let metrics: Arc<dyn MetricsSink> = sink.clone();
    let mut cfg = consumer_cfg(queue, true, 60);
    cfg.metrics = metrics;
    cfg.result_batch = 8;
    cfg.result_idle_ms = 10;
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    let admin_h = admin.clone();
    let queue_h = queue.to_string();
    wait_until(Duration::from_secs(10), || {
        let admin = admin_h.clone();
        let queue = queue_h.clone();
        async move { count_result_keys(&admin, &queue).await == N as usize }
    })
    .await;

    // Stabilization grace so any straggler `job_outcome` event has time to
    // post before we sample.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let completed = sink.jobs_completed();
    assert_eq!(
        completed, N as u64,
        "exactly one JobOutcome::Ok per handler invocation (got {completed} for N={N})"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn result_writer_drains_on_shutdown() {
    // Consumer shutdown drops the writer's `ok_result_tx`. The writer
    // returning from `rx.recv()` with `None` while its buffer is non-empty
    // must flush the buffer before exiting, otherwise a handler outcome
    // could be silently dropped on a clean shutdown.
    let admin = admin().await;
    let queue = "result_writer_drains_on_shutdown";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");
    const N: u32 = 6;
    let ids = producer
        .add_bulk((0..N).map(|n| Sample { n }).collect())
        .await
        .expect("add_bulk");
    assert_eq!(ids.len(), N as usize);

    let mut cfg = consumer_cfg(queue, true, 60);
    // Idle window much longer than the test's shutdown horizon so handler
    // completions sit in the buffer at shutdown time. The shutdown drain
    // path (rx -> None on the inner timeout-recv) must flush them.
    cfg.result_batch = 64;
    cfg.result_idle_ms = 5_000;
    cfg.concurrency = N as usize;
    cfg.shutdown_deadline_secs = 5;
    let group = cfg.group.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let stream = chasquimq::producer::stream_key(queue);

    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    Ok(Bytes::from(format!("ok-{}", job.payload.n)))
                },
                shutdown_clone,
            )
            .await
    });

    // Wait until all N handler completions are queued in the writer (XPENDING
    // shows N entries — they're pending until the writer's XACKDEL flush).
    let admin_h = admin.clone();
    let stream_h = stream.clone();
    let group_h = group.clone();
    wait_until(Duration::from_secs(5), || {
        let admin = admin_h.clone();
        let stream = stream_h.clone();
        let group = group_h.clone();
        async move { xpending_ids(&admin, &stream, &group, 32).await.len() == N as usize }
    })
    .await;

    // Sanity: result keys have not all landed yet — idle is 5s and we shut
    // down well before. A flush window may have fired for some entries, but
    // not necessarily all.
    let pre = count_result_keys(&admin, queue).await;
    assert!(
        pre < N as usize,
        "result keys should not all have landed yet (got {pre}/{N}, idle=5s)"
    );

    // Trigger shutdown. The writer's tx (cloned into workers) is dropped
    // when workers wind down, which closes `rx` → writer flushes its buffer.
    shutdown.cancel();
    let join_res = tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("consumer.run join timed out");
    let _ = join_res.expect("consumer task panicked");

    let after = count_result_keys(&admin, queue).await;
    assert_eq!(
        after, N as usize,
        "shutdown must drain the writer's pending buffer (got {after}, expected {N})"
    );

    let _: () = admin.quit().await.unwrap();
}
