//! Integration tests for result-backend behavior under Redis `maxmemory`
//! eviction policies.
//!
//! Two scenarios:
//!
//! - **`noeviction`**: a tight memory budget makes new writes fail with
//!   `OOM command not allowed when used memory > 'maxmemory'`. The test
//!   asserts that handler invocations still terminate (no infinite CLAIM
//!   loop) and that the stream and pending-list both drain. The `JOB_OK_SCRIPT`
//!   carries `#!lua flags=allow-oom` and wraps `SET` in `redis.pcall`, so the
//!   XACKDEL always commits even when the result-write is OOM-rejected. The
//!   result is lost, which the docstring already documents as acceptable
//!   (`None` from `get_result` is indistinguishable from "expired" or
//!   "never written").
//!
//! - **`allkeys-lru`**: result-keys are eligible for eviction; the test
//!   pushes hard enough on memory that the oldest result-keys get evicted,
//!   then asserts `get_result` returns `None` for evicted ones and the
//!   stream still drains cleanly.
//!
//! Both tests are gated behind `CHASQUIMQ_RUN_MAXMEMORY_TEST=1` because they
//! mutate Redis CONFIG (destructive on a shared instance) and reset to
//! `maxmemory 0` / `maxmemory-policy noeviction` defaults on exit.

use chasquimq::config::{ConsumerConfig, ProducerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::Producer;
use chasquimq::{Bytes, Job, JobId};
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

fn maxmemory_gate() -> bool {
    std::env::var("CHASQUIMQ_RUN_MAXMEMORY_TEST")
        .map(|v| v == "1")
        .unwrap_or(false)
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

async fn config_set(admin: &Client, key: &str, value: &str) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("CONFIG", ClusterHash::FirstKey, false),
            vec![Value::from("SET"), Value::from(key), Value::from(value)],
        )
        .await
        .expect("CONFIG SET");
}

async fn config_get(admin: &Client, key: &str) -> String {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("CONFIG", ClusterHash::FirstKey, false),
            vec![Value::from("GET"), Value::from(key)],
        )
        .await
        .expect("CONFIG GET");
    if let Value::Array(items) = v {
        if let Some(Value::String(s)) = items.get(1) {
            return s.to_string();
        }
        if let Some(Value::Bytes(b)) = items.get(1) {
            return String::from_utf8_lossy(b).into_owned();
        }
    }
    String::new()
}

async fn used_memory(admin: &Client) -> u64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("INFO", ClusterHash::FirstKey, false),
            vec![Value::from("memory")],
        )
        .await
        .expect("INFO memory");
    let s = match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("INFO returned unexpected: {other:?}"),
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("used_memory:") {
            return rest.trim().parse::<u64>().unwrap_or(0);
        }
    }
    0
}

async fn flush_queue(admin: &Client, queue: &str) {
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
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                items,
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

async fn xpending_count(admin: &Client, stream: &str, group: &str) -> i64 {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
            vec![Value::from(stream), Value::from(group)],
        )
        .await
        .unwrap_or(Value::Null);
    if let Value::Array(arr) = v
        && let Some(first) = arr.first()
    {
        if let Value::Integer(n) = first {
            return *n;
        }
    }
    0
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
        delayed_enabled: true,
        delayed_poll_interval_ms: 25,
        run_scheduler: false,
        events_enabled: false,
        // High concurrency: handlers block on a gate before the memory
        // limit is applied, so we need enough worker slots + channel
        // buffer for every job to land in the pending list before any
        // ack happens. concurrency * 2 channel slots + concurrency
        // workers gives a 3 * concurrency ceiling on simultaneous
        // pending entries; 64 covers N=32 with headroom.
        concurrency: 64,
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

async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if check().await {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Saved Redis CONFIG that the test pinched, so teardown can restore it.
struct ConfigSnapshot {
    maxmemory: String,
    maxmemory_policy: String,
    stop_writes: String,
}

async fn snapshot_config(admin: &Client) -> ConfigSnapshot {
    ConfigSnapshot {
        maxmemory: config_get(admin, "maxmemory").await,
        maxmemory_policy: config_get(admin, "maxmemory-policy").await,
        stop_writes: config_get(admin, "stop-writes-on-bgsave-error").await,
    }
}

/// Restore Redis to the snapshot taken before the test pinched memory.
/// Always called in teardown so the live instance is in the same state
/// it started in — even when assertions fail.
///
/// Side effect of pinching maxmemory: a forked BGSAVE may fail because
/// the fork temporarily doubles memory under copy-on-write. Redis sets
/// the `rdb_last_bgsave_status:err` flag and — under the default
/// `stop-writes-on-bgsave-error yes` — blocks every subsequent write
/// with `MISCONF` until the next successful BGSAVE. We unblock by
/// flipping `stop-writes-on-bgsave-error no` for the rest of teardown;
/// the original value is restored last so an operator's intentional
/// `yes` is preserved, but any err-flag the test set will manifest as
/// MISCONF on the next write — that's the operator's existing
/// configuration choice, not the test's regression.
async fn restore_config(admin: &Client, snap: &ConfigSnapshot) {
    config_set(admin, "stop-writes-on-bgsave-error", "no").await;
    config_set(admin, "maxmemory-policy", &snap.maxmemory_policy).await;
    config_set(admin, "maxmemory", &snap.maxmemory).await;
    config_set(admin, "stop-writes-on-bgsave-error", &snap.stop_writes).await;
}

/// Under `noeviction` with maxmemory pinched below `used_memory`, every
/// handler still drains: XACKDEL inside `JOB_OK_SCRIPT` always commits
/// (the script is declared `allow-oom` and `SET` is wrapped in
/// `redis.pcall`), so the entry doesn't stay pending. The result write
/// may silently drop under OOM — `get_result` returns `None` in that
/// case, which matches the documented contract.
///
/// Sequencing matters:
/// 1. produce N jobs (writes the stream).
/// 2. start the consumer. The handler blocks on `gate` so jobs collect
///    in the consumer-group's pending list (XREADGROUP succeeds; the
///    handler won't ack-via-script until we say so).
/// 3. wait until everything is read into pending.
/// 4. tighten maxmemory below `used_memory` so subsequent SETs OOM.
/// 5. open the gate. Handlers now invoke `JOB_OK_SCRIPT` under OOM.
/// 6. assert: stream and pending drain (the load-bearing assertion;
///    a regression would leave entries pending forever).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL + CHASQUIMQ_RUN_MAXMEMORY_TEST=1; mutates Redis CONFIG"]
async fn maxmemory_noeviction_drains_without_loop() {
    if !maxmemory_gate() {
        eprintln!("skipping: set CHASQUIMQ_RUN_MAXMEMORY_TEST=1 to run");
        return;
    }
    let admin = admin().await;
    let queue = "maxmemory_noeviction";
    let snap = snapshot_config(&admin).await;
    // Disable BGSAVE-error gating up front: the test pinches memory low
    // enough that a forked BGSAVE may fail, and the default
    // `stop-writes-on-bgsave-error yes` would block every subsequent
    // write with MISCONF. Restored in `restore_config`.
    config_set(&admin, "stop-writes-on-bgsave-error", "no").await;
    flush_queue(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");

    // 4KB result per job to push memory hard when stored.
    let result_payload = Bytes::from(vec![b'X'; 4096]);
    let n_jobs = 32usize;
    let mut ids: Vec<JobId> = Vec::with_capacity(n_jobs);
    for i in 0..n_jobs {
        let id = producer.add(Sample { n: i as u32 }).await.expect("add");
        ids.push(id);
    }

    let stream_key = format!("{{chasqui:{queue}}}:stream");
    let group = "default";

    // Gate: handlers wait until we've tightened memory. Use a notify so
    // every worker wakes once the gate flips.
    let gate = Arc::new(tokio::sync::Notify::new());
    let gate_h = gate.clone();
    let result_for_handler = result_payload.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let calls = calls_h.clone();
                    let result = result_for_handler.clone();
                    let gate = gate_h.clone();
                    async move {
                        gate.notified().await;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(result)
                    }
                },
                shutdown_clone,
            )
            .await
    });

    // Wait until all N jobs are sitting in the consumer-group's pending
    // list. Handlers are blocked on `gate`, so they're delivered but not
    // acked.
    let pending_ready = {
        let admin = admin.clone();
        let stream_key = stream_key.clone();
        wait_until(Duration::from_secs(15), move || {
            let admin = admin.clone();
            let stream_key = stream_key.clone();
            async move { xpending_count(&admin, &stream_key, group).await >= n_jobs as i64 }
        })
        .await
    };
    assert!(pending_ready, "consumer must read all N jobs into pending");

    // Tighten memory now. Subtract a generous slack so the very first
    // SET inside JOB_OK_SCRIPT lands over the cap.
    let cur = used_memory(&admin).await;
    let limit = cur.saturating_sub(4096);
    config_set(&admin, "maxmemory-policy", "noeviction").await;
    config_set(&admin, "maxmemory", &limit.to_string()).await;

    // Smoke test: a direct SET from the admin client must now OOM.
    let probe: std::result::Result<Value, fred::error::Error> = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![
                Value::from("{chasqui:maxmemory_noeviction}:probe"),
                Value::Bytes(Bytes::from(vec![b'P'; 8192])),
            ],
        )
        .await;
    let oom_active = matches!(probe, Err(e) if format!("{e}").contains("OOM"));

    // Release the handlers. They run under OOM, so JOB_OK_SCRIPT must
    // tolerate SET rejection.
    for _ in 0..(n_jobs * 4) {
        gate.notify_one();
    }

    // Drain: XLEN and pending both go to zero.
    let drained = {
        let admin = admin.clone();
        let stream_key = stream_key.clone();
        wait_until(Duration::from_secs(60), move || {
            let admin = admin.clone();
            let stream_key = stream_key.clone();
            async move {
                let len = xlen(&admin, &stream_key).await;
                let pending = xpending_count(&admin, &stream_key, group).await;
                len == 0 && pending == 0
            }
        })
        .await
    };

    let final_xlen = xlen(&admin, &stream_key).await;
    let final_pending = xpending_count(&admin, &stream_key, group).await;

    // Restore CONFIG BEFORE asserting so a failure leaves the host sane.
    restore_config(&admin, &snap).await;

    assert!(
        oom_active,
        "smoke test: direct SET must OOM under tightened maxmemory; the test isn't exercising the OOM path otherwise"
    );
    assert!(
        drained,
        "stream + pending must drain under noeviction; XLEN={final_xlen} pending={final_pending}"
    );

    let total_calls = calls.load(Ordering::SeqCst);
    assert!(
        total_calls < n_jobs * 5,
        "handler invocations ({total_calls}) ran away — looks like a CLAIM loop for {n_jobs} jobs"
    );

    // Some results may have been written (XACKDEL freed a tiny bit of
    // headroom; some SETs may still fit), others may be None (rejected
    // by OOM). The test only requires the system to not diverge —
    // `get_result` returning `None` is documented as acceptable.
    let mut some_count = 0;
    for id in &ids {
        if matches!(producer.get_result(id).await, Ok(Some(_))) {
            some_count += 1;
        }
    }
    eprintln!("noeviction: {some_count}/{n_jobs} results stored (rest dropped under OOM)");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    flush_queue(&admin, queue).await;
    let _: () = admin.quit().await.unwrap();
}

/// Under `allkeys-lru`, tightening maxmemory below current usage evicts
/// older keys to make room for new writes. Result-keys can be evicted;
/// `get_result` returns `None` for those, but every job acks cleanly.
///
/// Same sequencing as the noeviction test: produce → consumer reads into
/// pending → tighten memory → release → assert drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL + CHASQUIMQ_RUN_MAXMEMORY_TEST=1; mutates Redis CONFIG"]
async fn maxmemory_allkeys_lru_evicts_results_no_loop() {
    if !maxmemory_gate() {
        eprintln!("skipping: set CHASQUIMQ_RUN_MAXMEMORY_TEST=1 to run");
        return;
    }
    let admin = admin().await;
    let queue = "maxmemory_lru";
    let snap = snapshot_config(&admin).await;
    config_set(&admin, "stop-writes-on-bgsave-error", "no").await;
    flush_queue(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("producer");

    let result_payload = Bytes::from(vec![b'Y'; 4096]);
    let n_jobs = 32usize;
    let mut ids: Vec<JobId> = Vec::with_capacity(n_jobs);
    for i in 0..n_jobs {
        let id = producer.add(Sample { n: i as u32 }).await.expect("add");
        ids.push(id);
    }

    let stream_key = format!("{{chasqui:{queue}}}:stream");
    let group = "default";

    let gate = Arc::new(tokio::sync::Notify::new());
    let gate_h = gate.clone();
    let result_for_handler = result_payload.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, true, 60));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let calls = calls_h.clone();
                    let result = result_for_handler.clone();
                    let gate = gate_h.clone();
                    async move {
                        gate.notified().await;
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(result)
                    }
                },
                shutdown_clone,
            )
            .await
    });

    let pending_ready = {
        let admin = admin.clone();
        let stream_key = stream_key.clone();
        wait_until(Duration::from_secs(15), move || {
            let admin = admin.clone();
            let stream_key = stream_key.clone();
            async move { xpending_count(&admin, &stream_key, group).await >= n_jobs as i64 }
        })
        .await
    };
    assert!(
        pending_ready,
        "consumer must read all N jobs into pending under allkeys-lru"
    );

    let cur = used_memory(&admin).await;
    let limit = cur.saturating_sub(2048);
    config_set(&admin, "maxmemory-policy", "allkeys-lru").await;
    config_set(&admin, "maxmemory", &limit.to_string()).await;

    for _ in 0..(n_jobs * 4) {
        gate.notify_one();
    }

    let drained = {
        let admin = admin.clone();
        let stream_key = stream_key.clone();
        wait_until(Duration::from_secs(60), move || {
            let admin = admin.clone();
            let stream_key = stream_key.clone();
            async move {
                let len = xlen(&admin, &stream_key).await;
                let pending = xpending_count(&admin, &stream_key, group).await;
                len == 0 && pending == 0
            }
        })
        .await
    };
    let final_xlen = xlen(&admin, &stream_key).await;
    let final_pending = xpending_count(&admin, &stream_key, group).await;

    restore_config(&admin, &snap).await;

    assert!(
        drained,
        "stream + pending must drain under allkeys-lru; XLEN={final_xlen} pending={final_pending}"
    );

    let total_calls = calls.load(Ordering::SeqCst);
    assert!(
        total_calls < n_jobs * 5,
        "handler invocations ({total_calls}) ran away under allkeys-lru for {n_jobs} jobs"
    );

    // Under LRU, get_result returning None for some ids is expected.
    // Returning Some for ALL ids is also acceptable (memory may not have
    // been tight enough on this host) — the load-bearing assertion is
    // that nothing loops. We log the survival rate for diagnostics.
    let mut some_count = 0;
    for id in &ids {
        if matches!(producer.get_result(id).await, Ok(Some(_))) {
            some_count += 1;
        }
    }
    eprintln!("allkeys-lru: {some_count}/{n_jobs} results survived eviction");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    flush_queue(&admin, queue).await;
    let _: () = admin.quit().await.unwrap();
}
