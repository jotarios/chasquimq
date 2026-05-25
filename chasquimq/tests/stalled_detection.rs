//! Integration tests for the stalled-job detector (slice 12).
//!
//! Each test stands up a `Consumer` against a live Redis with very short
//! `claim_min_idle_ms` (and therefore detector tick + idle threshold,
//! since the embedded path inherits both from `claim_min_idle_ms`) so
//! the detector can fire multiple times within seconds.
//!
//! Pinned behaviors:
//!
//! 1. `relocates_after_max_stalled_attempts` — a hung handler that holds
//!    the entry past N consecutive idle thresholds is atomically
//!    relocated to the DLQ as `DlqReason::Stalled` (distinct from
//!    `RetriesExhausted`).
//! 2. `no_false_positives_under_healthy_load` — handlers that complete
//!    within `idle_threshold_ms` never trip the counter.
//! 3. `counter_reset_on_successful_ack` — `JOB_OK_SCRIPT` DELs the
//!    counter; a successful ack erases any partial streak.
//! 4. `disabled_skips_spawn` — `stalled_detector_enabled = false` means
//!    the lock key never appears in Redis and no `stalled_tick` ever
//!    fires.
//! 5. `replay_dlq_clears_counter` — `REPLAY_DLQ_SCRIPT` DELs the
//!    counter on replay (regression for the slice's ARGV-shape change).
//! 6. `job_ok_script_acks_without_stall_counter` — the common path
//!    (no counter ever written) acks fine; DEL of a non-existent
//!    counter is not an error.

#![allow(clippy::needless_late_init)]

mod common;

use bytes::Bytes;
use chasquimq::config::StalledDetectorConfig;
use chasquimq::metrics::DlqReason;
use chasquimq::metrics::testing::InMemorySink;
use chasquimq::{Consumer, Producer};
use common::{Sample, admin, consumer_cfg, flush_all, producer_cfg, redis_url, wait_until};
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const STALLED_QUEUE_PREFIX: &str = "stalled_";

fn unique_queue(suffix: &str) -> String {
    format!(
        "{}{}_{}",
        STALLED_QUEUE_PREFIX,
        suffix,
        uuid::Uuid::new_v4().simple()
    )
}

/// Clean up every per-queue key the detector / consumer might touch,
/// including the stall counter and the detector lock.
async fn flush_stalled(admin: &fred::clients::Client, queue: &str) {
    flush_all(admin, queue).await;
    for suffix in ["scheduler:lock", "stalled:lock", "paused", "events"] {
        let key = format!("{{chasqui:{queue}}}:{suffix}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                vec![Value::from(key)],
            )
            .await
            .expect("DEL");
    }
    // Stall-counter keys are per-job; nuke any leftover via a SCAN +
    // DEL on the queue's namespace. Small queue → cheap.
    let pattern = format!("{{chasqui:{queue}}}:stalls:*", queue = queue);
    let res: Value = admin
        .custom(
            CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false),
            vec![
                Value::from("0"),
                Value::from("MATCH"),
                Value::from(pattern),
                Value::from("COUNT"),
                Value::from(256_i64),
            ],
        )
        .await
        .expect("SCAN");
    if let Value::Array(items) = res {
        if let Some(Value::Array(keys)) = items.get(1) {
            for k in keys {
                if let Value::String(s) = k {
                    let _: Value = admin
                        .custom(
                            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                            vec![Value::from(s.to_string())],
                        )
                        .await
                        .expect("DEL stall counter");
                }
            }
        }
    }
}

/// Build a ConsumerConfig with a fast detector cadence: `claim_min_idle_ms`
/// = `tick == idle`, inherited at spawn time. Useful for hung-handler
/// tests where the test runtime can't wait for 30s ticks.
fn fast_detector_cfg(
    queue: &str,
    sink: Arc<InMemorySink>,
    max_stalled_attempts: u32,
) -> chasquimq::ConsumerConfig {
    let mut cfg = consumer_cfg(queue, sink.clone(), 25);
    cfg.claim_min_idle_ms = 200;
    cfg.stalled_detector_enabled = true;
    cfg.stalled_detector = StalledDetectorConfig {
        // Embedded spawn overrides tick + idle from claim_min_idle_ms;
        // these defaults are echo-only.
        tick_interval_ms: 200,
        idle_threshold_ms: 200,
        max_stalled_attempts,
        scan_batch: 64,
        metrics: sink.clone(),
        ..StalledDetectorConfig::default()
    };
    cfg
}

/// Seed a single delivered PEL entry directly via raw XADD + XREADGROUP,
/// then return the (stream_key, group, consumer_id, entry_id, job_id).
/// Used by detector tests that exercise the standalone detector path
/// (no Consumer wrapping, so no reader-CLAIM race against the detector).
#[allow(dead_code)]
async fn seed_pending_entry(
    admin: &fred::clients::Client,
    queue: &str,
    group: &str,
    consumer_id: &str,
) -> (String, String) {
    use chasquimq::Job as JobS;
    let stream_key = format!("{{chasqui:{queue}}}:stream");
    // XGROUP CREATE MKSTREAM.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XGROUP", ClusterHash::FirstKey, false),
            vec![
                Value::from("CREATE"),
                Value::from(stream_key.as_str()),
                Value::from(group),
                Value::from("$"),
                Value::from("MKSTREAM"),
            ],
        )
        .await
        .unwrap_or(Value::Null);
    // XADD a real Job<Sample> payload so the script's job_id decode
    // works.
    let job = JobS::new(Sample { n: 1 });
    let job_id = job.id.clone();
    let bytes = rmp_serde::to_vec(&job).expect("encode");
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(stream_key.as_str()),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes.into()),
            ],
        )
        .await
        .expect("XADD");
    let entry_id = match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
        other => panic!("XADD unexpected: {other:?}"),
    };
    // XREADGROUP to land it in the PEL.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false),
            vec![
                Value::from("GROUP"),
                Value::from(group),
                Value::from(consumer_id),
                Value::from("COUNT"),
                Value::from(1_i64),
                Value::from("STREAMS"),
                Value::from(stream_key.as_str()),
                Value::from(">"),
            ],
        )
        .await
        .expect("XREADGROUP");
    (entry_id, job_id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn detector_relocates_after_max_stalled_attempts() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
    let queue = unique_queue("relocate");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    // `max_stalled_attempts = 1` matches the BullMQ default ("one
    // missed scan and you're out"). For the hung-handler simulation,
    // each tick the detector and the reader's CLAIM safety net race
    // for the entry: detector-wins → INCR=1 → threshold → relocate;
    // reader-wins → CLAIM-redeliver, idle resets, repeat. Probability
    // of N consecutive reader wins is 0.5^N; the 30s wait window
    // tolerates up to ~15 ticks (~30 ticks at 1s) without flaking.
    let mut cfg = fast_detector_cfg(&queue, sink.clone(), 1);
    // 1-second claim threshold + detector tick (embedded path forces
    // tick=idle=claim_min_idle). Concurrency=1 so only one worker
    // grabs hangs — fewer redeliveries to other workers in the race.
    cfg.claim_min_idle_ms = 1_000;
    cfg.stalled_detector.tick_interval_ms = 1_000;
    cfg.stalled_detector.idle_threshold_ms = 1_000;
    cfg.concurrency = 1;
    cfg.block_ms = 200;
    let consumer = Consumer::<Sample>::new(redis_url(), cfg);

    // Handler hangs forever — first delivery sits in PEL; the
    // detector's first tick past `idle_threshold_ms` sees it idle,
    // INCRs to 1 == max_stalled_attempts, and the script's XACKDEL
    // gate fires. Rust sends to dlq_tx with DlqReason::Stalled.
    let started = Arc::new(Notify::new());
    let started_clone = started.clone();
    let handler = move |_job: chasquimq::Job<Sample>| {
        let started = started_clone.clone();
        async move {
            started.notify_waiters();
            std::future::pending::<()>().await;
            Ok::<_, chasquimq::HandlerError>(Bytes::new())
        }
    };

    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    // Produce one job.
    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    // Wait for the handler to be dispatched at least once.
    let _ = tokio::time::timeout(Duration::from_secs(5), started.notified()).await;

    // Wait for the detector to relocate. See cfg comment for the
    // race-window math.
    let wait_result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if sink.dlq_count(DlqReason::Stalled) >= 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if wait_result.is_err() {
        // Diagnostic: dump Redis state to help see why we're stuck.
        let stream_len: Value = admin
            .custom(
                CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
                vec![Value::from(format!("{{chasqui:{queue}}}:stream"))],
            )
            .await
            .unwrap_or(Value::Null);
        let lock_exists: Value = admin
            .custom(
                CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
                vec![Value::from(format!("{{chasqui:{queue}}}:stalled:lock"))],
            )
            .await
            .unwrap_or(Value::Null);
        let pending: Value = admin
            .custom(
                CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
                vec![
                    Value::from(format!("{{chasqui:{queue}}}:stream")),
                    Value::from("default"),
                ],
            )
            .await
            .unwrap_or(Value::Null);
        let ticks = sink.stalled_ticks().len();
        let incremented = sink.stalled_incremented_total();
        let relocated = sink.stalled_relocated_total();
        panic!(
            "relocate timeout: stream_len={stream_len:?}, lock={lock_exists:?}, \
             xpending={pending:?}, ticks={ticks}, incremented={incremented}, \
             relocated={relocated}",
        );
    }

    let dlq_stalled = sink.dlq_count(DlqReason::Stalled);
    assert!(
        dlq_stalled >= 1,
        "expected >=1 DlqReason::Stalled, saw {dlq_stalled}"
    );
    // RetriesExhausted should be zero on this path — we're testing
    // the worker-crash-loop bucket, not handler-failure-loop.
    let dlq_retries = sink.dlq_count(DlqReason::RetriesExhausted);
    assert_eq!(
        dlq_retries, 0,
        "stalled detector must not over-route to retries_exhausted"
    );

    // Tick metrics: at least one tick fired with `relocated >= 1`.
    let total_relocated = sink.stalled_relocated_total();
    assert!(
        total_relocated >= 1,
        "stalled_tick must report >=1 relocated; saw {total_relocated}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn detector_no_false_positives_under_healthy_load() {
    let queue = unique_queue("healthy");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    let mut cfg = fast_detector_cfg(&queue, sink.clone(), 2);
    // Healthy handler completes well within idle threshold — bump
    // claim_min_idle_ms up to give handlers room.
    cfg.claim_min_idle_ms = 1_000;
    cfg.stalled_detector.tick_interval_ms = 1_000;
    cfg.stalled_detector.idle_threshold_ms = 1_000;

    let consumer = Consumer::<Sample>::new(redis_url(), cfg);
    let handler = move |_job: chasquimq::Job<Sample>| async move {
        // 5ms is well under the 1s idle threshold.
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok::<_, chasquimq::HandlerError>(Bytes::new())
    };

    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    for i in 0..10 {
        producer.add(Sample { n: i }).await.expect("add");
    }

    // Wait until all jobs complete.
    wait_until(Duration::from_secs(15), || async {
        sink.jobs_completed() >= 10
    })
    .await;

    // Give the detector at least one tick to confirm zero relocates.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let relocated = sink.stalled_relocated_total();
    let incremented = sink.stalled_incremented_total();
    assert_eq!(
        relocated, 0,
        "healthy load must not trip relocate; saw {relocated}"
    );
    assert_eq!(
        incremented, 0,
        "healthy load must not INCR any counter; saw {incremented}"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn detector_disabled_skips_spawn() {
    let queue = unique_queue("disabled");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    let mut cfg = consumer_cfg(&queue, sink.clone(), 25);
    cfg.claim_min_idle_ms = 200;
    cfg.stalled_detector_enabled = false;

    let consumer = Consumer::<Sample>::new(redis_url(), cfg);
    let handler = move |_job: chasquimq::Job<Sample>| async move {
        Ok::<_, chasquimq::HandlerError>(Bytes::new())
    };

    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    wait_until(Duration::from_secs(10), || async {
        sink.jobs_completed() >= 1
    })
    .await;

    // Detector lock key must never appear — that's the cheap signal
    // the detector task didn't spawn at all.
    let lock_key = format!("{{chasqui:{queue}}}:stalled:lock");
    let v: Value = admin
        .custom(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(lock_key)],
        )
        .await
        .expect("EXISTS");
    assert!(
        matches!(v, Value::Integer(0)),
        "detector lock key must not exist when disabled, saw {v:?}"
    );
    assert_eq!(
        sink.stalled_relocated_total(),
        0,
        "no stalled_tick should fire when disabled"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn job_ok_script_acks_without_stall_counter() {
    // REGRESSION-CRITICAL: two invariants on the JOB_OK_SCRIPT path.
    //
    // 1. The common case (job never stalled, no counter key) still
    //    acks. Slice-12 added an unconditional DEL of the stall
    //    counter; DEL on a missing key returns `0` and must not
    //    surface as a handler failure.
    // 2. When a live stall counter DOES exist for an in-flight job,
    //    the JOB_OK_SCRIPT ack clears it — the counter must be gone
    //    after the handler succeeds. This is the invariant a follow-up
    //    successful run after a near-miss stall relies on; without it
    //    the counter would carry over (sliding TTL) and a future minor
    //    blip would trip threshold prematurely.
    let queue = unique_queue("ok_no_counter");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    let mut cfg = consumer_cfg(&queue, sink.clone(), 25);
    cfg.store_results = true; // Forces JOB_OK_SCRIPT path.
    cfg.stalled_detector_enabled = false; // Isolate from detector races.

    let consumer = Consumer::<Sample>::new(redis_url(), cfg);
    // Capture the job id the handler observes so the test can probe
    // the corresponding stall counter key by name after the ack.
    let observed_id = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let observed_id_clone = observed_id.clone();
    let pre_seed_done = Arc::new(Notify::new());
    let pre_seed_done_clone = pre_seed_done.clone();
    let handler = move |job: chasquimq::Job<Sample>| {
        let observed_id_clone = observed_id_clone.clone();
        let pre_seed_done_clone = pre_seed_done_clone.clone();
        async move {
            // Capture the first job id we see; wait for the test
            // thread to seed a fake stall counter against it before
            // the handler resolves.
            let id = job.id.clone();
            let mut guard = observed_id_clone.lock().await;
            if guard.is_none() {
                *guard = Some(id);
                drop(guard);
                pre_seed_done_clone.notified().await;
            }
            Ok::<_, chasquimq::HandlerError>(Bytes::from_static(b"result"))
        }
    };

    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    for _ in 0..5 {
        producer.add(Sample { n: 1 }).await.expect("add");
    }

    // Wait for the handler to see the first job, then seed a fake
    // stall counter under its id. Release the handler — the JOB_OK
    // ack path must DEL the counter.
    wait_until(Duration::from_secs(10), || async {
        observed_id.lock().await.is_some()
    })
    .await;
    let job_id = observed_id.lock().await.clone().expect("job id");
    let stall_key = format!("{{chasqui:{queue}}}:stalls:{job_id}");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("INCR", ClusterHash::FirstKey, false),
            vec![Value::from(stall_key.as_str())],
        )
        .await
        .expect("INCR seed stall counter");
    pre_seed_done.notify_waiters();

    wait_until(Duration::from_secs(15), || async {
        sink.jobs_completed() >= 5
    })
    .await;

    // No failures must have surfaced — DEL of a non-existent counter
    // (the common path for jobs 2..=5) must not error out.
    let failed = sink.jobs_failed();
    assert_eq!(
        failed, 0,
        "JOB_OK_SCRIPT must not surface DEL errors as failed; saw {failed}"
    );

    // The seeded stall counter must be DEL'd by the successful ack.
    let exists: Value = admin
        .custom(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(stall_key.as_str())],
        )
        .await
        .expect("EXISTS stall counter");
    let n = match exists {
        Value::Integer(n) => n,
        other => panic!("EXISTS returned non-integer: {other:?}"),
    };
    assert_eq!(
        n, 0,
        "JOB_OK_SCRIPT must DEL the stall counter on successful ack; \
         key {stall_key} still exists ({n} hits)"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn replay_dlq_quad_argv_shape_works() {
    // REGRESSION-CRITICAL: pin two slice-12 invariants on the
    // REPLAY_DLQ_SCRIPT path.
    //
    // 1. ARGV layout moved from triples (id, payload, name) to quads
    //    (+ job_id). Existing replay paths must still work.
    // 2. The script DELs any leftover stall counter for the job being
    //    replayed. A replayed entry that was originally stalled (and
    //    therefore had a live counter under sliding TTL) must start
    //    its fresh attempt with a zeroed counter — otherwise the next
    //    detector tick would trip threshold against a stale value
    //    from the previous incarnation.
    let queue = unique_queue("replay_quad");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    // Drive a job into the DLQ via UnrecoverableError, then replay.
    let sink = Arc::new(InMemorySink::new());
    let mut cfg = consumer_cfg(&queue, sink.clone(), 25);
    cfg.stalled_detector_enabled = false;
    let consumer = Consumer::<Sample>::new(redis_url(), cfg);

    // Capture the job id the handler observes so we can seed the
    // corresponding stall counter before replay.
    let observed_id = Arc::new(tokio::sync::Mutex::new(None::<String>));
    let observed_id_clone = observed_id.clone();
    let handler = move |job: chasquimq::Job<Sample>| {
        let observed_id_clone = observed_id_clone.clone();
        async move {
            *observed_id_clone.lock().await = Some(job.id.clone());
            Err::<Bytes, _>(chasquimq::HandlerError::unrecoverable(
                std::io::Error::other("poison"),
            ))
        }
    };
    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    // Wait for it to land in DLQ.
    wait_until(Duration::from_secs(10), || async {
        sink.dlq_count(DlqReason::Unrecoverable) >= 1
    })
    .await;

    let job_id = observed_id
        .lock()
        .await
        .clone()
        .expect("handler should have observed job id");
    let stall_key = format!("{{chasqui:{queue}}}:stalls:{job_id}");

    // Seed a leftover stall counter under the dlq'd job's id (no live
    // detector to do this naturally because we ran with
    // stalled_detector_enabled=false). REPLAY_DLQ_SCRIPT must DEL it.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("INCR", ClusterHash::FirstKey, false),
            vec![Value::from(stall_key.as_str())],
        )
        .await
        .expect("INCR seed stall counter");
    // Sanity: the seed worked.
    let pre_exists: Value = admin
        .custom(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(stall_key.as_str())],
        )
        .await
        .expect("EXISTS pre-replay");
    let pre_n = match pre_exists {
        Value::Integer(n) => n,
        other => panic!("EXISTS returned non-integer: {other:?}"),
    };
    assert_eq!(pre_n, 1, "stall-counter seed sanity check");

    // Now replay — must succeed with the new quad ARGV shape.
    let replayed = producer
        .replay_dlq(10)
        .await
        .expect("replay_dlq with quad shape must work");
    assert_eq!(replayed, 1, "expected exactly 1 entry replayed");

    // The leftover stall counter must be gone after replay.
    let post_exists: Value = admin
        .custom(
            CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false),
            vec![Value::from(stall_key.as_str())],
        )
        .await
        .expect("EXISTS post-replay");
    let post_n = match post_exists {
        Value::Integer(n) => n,
        other => panic!("EXISTS returned non-integer: {other:?}"),
    };
    assert_eq!(
        post_n, 0,
        "REPLAY_DLQ_SCRIPT must DEL the stall counter on replay; \
         key {stall_key} still exists ({post_n} hits)"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn stalled_event_payload_shape() {
    // Pin the wire-format fields on `e=stalled`: id, attempt, prev=active.
    let queue = unique_queue("event_shape");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    let cfg = fast_detector_cfg(&queue, sink.clone(), 3);
    let consumer = Consumer::<Sample>::new(redis_url(), cfg);

    let handler = move |_job: chasquimq::Job<Sample>| async move {
        std::future::pending::<()>().await;
        Ok::<_, chasquimq::HandlerError>(Bytes::new())
    };
    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    let producer = Producer::connect(&redis_url(), producer_cfg(&queue))
        .await
        .expect("producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    // Wait for at least one `e=stalled` entry to land on the events
    // stream. With max_stalled_attempts=3, we expect 2 incremented
    // events before the 3rd hits threshold and emits e=dlq.
    let events_key = format!("{{chasqui:{queue}}}:events");
    wait_until(Duration::from_secs(15), || async {
        let v: Value = admin
            .custom(
                CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
                vec![
                    Value::from(events_key.as_str()),
                    Value::from("-"),
                    Value::from("+"),
                ],
            )
            .await
            .expect("XRANGE");
        match v {
            Value::Array(items) => items.iter().any(|entry| {
                let Value::Array(parts) = entry else {
                    return false;
                };
                let Some(Value::Array(fields)) = parts.get(1) else {
                    return false;
                };
                let mut iter = fields.iter();
                while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                    let key_match = matches!(k, Value::String(s) if s.as_bytes() == b"e")
                        || matches!(k, Value::Bytes(b) if b.as_ref() == b"e");
                    if key_match {
                        let val_match = matches!(v, Value::String(s) if s.as_bytes() == b"stalled")
                            || matches!(v, Value::Bytes(b) if b.as_ref() == b"stalled");
                        if val_match {
                            return true;
                        }
                    }
                }
                false
            }),
            _ => false,
        }
    })
    .await;

    // Now read back the matching entry and verify the field shape.
    let v: Value = admin
        .custom(
            CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
            vec![
                Value::from(events_key.as_str()),
                Value::from("-"),
                Value::from("+"),
            ],
        )
        .await
        .expect("XRANGE");
    let entries = match v {
        Value::Array(items) => items,
        _ => panic!("XRANGE unexpected shape"),
    };
    let mut found_stalled = false;
    for entry in entries {
        let Value::Array(parts) = entry else { continue };
        let Some(Value::Array(fields)) = parts.get(1) else {
            continue;
        };
        let mut kv: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut iter = fields.iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            let ks = match k {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };
            let vs = match v {
                Value::String(s) => s.to_string(),
                Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
                _ => continue,
            };
            kv.insert(ks, vs);
        }
        if kv.get("e").map(String::as_str) == Some("stalled") {
            found_stalled = true;
            assert_eq!(kv.get("prev").map(String::as_str), Some("active"));
            assert!(kv.contains_key("id"), "e=stalled must carry id");
            assert!(kv.contains_key("attempt"), "e=stalled must carry attempt");
            assert!(kv.contains_key("ts"), "e=stalled must carry ts");
            // attempt must parse as a positive integer.
            let attempt: u32 = kv.get("attempt").unwrap().parse().expect("attempt int");
            assert!(attempt >= 1, "attempt must be >= 1");
        }
    }
    assert!(found_stalled, "must see at least one e=stalled entry");

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn embedded_spawn_honors_user_supplied_tick_and_idle() {
    // Slice-12 fix-up (High 6): the embedded spawn used to UNCONDITIONALLY
    // override `stalled_detector.tick_interval_ms` and `idle_threshold_ms`
    // from `claim_min_idle_ms`. A user / FFI shim setting either value
    // explicitly was a dead letter — silently silenced. Post-fix-up, the
    // override only kicks in when the field is still at its default
    // sentinel; an explicit non-default value reaches the detector
    // verbatim.
    //
    // Wire: drive the user-supplied tick fast (200ms) while leaving
    // `claim_min_idle_ms` at a value (5000ms) that — under the old
    // "always override" behavior — would silence the detector for the
    // length of the test. If the explicit tick wins, we observe several
    // tick events inside a few seconds; if the override silently fired,
    // we'd observe zero.
    let queue = unique_queue("user_tick_honored");
    let admin = admin().await;
    flush_stalled(&admin, &queue).await;

    let sink = Arc::new(InMemorySink::new());
    let mut cfg = consumer_cfg(&queue, sink.clone(), 25);
    // Long CLAIM idle → if the embedded path still overrode tick/idle
    // from `claim_min_idle_ms`, ticks would fire every 5s and we'd
    // observe ~1 in the 3s observation window below.
    cfg.claim_min_idle_ms = 5_000;
    cfg.stalled_detector_enabled = true;
    cfg.stalled_detector = StalledDetectorConfig {
        // Explicit non-default tick + idle — these must be preserved.
        tick_interval_ms: 200,
        idle_threshold_ms: 200,
        max_stalled_attempts: 100, // very high → no DLQ during the test
        scan_batch: 64,
        metrics: sink.clone(),
        ..StalledDetectorConfig::default()
    };

    let consumer = Consumer::<Sample>::new(redis_url(), cfg);
    let handler = move |_job: chasquimq::Job<Sample>| async move {
        // Resolve immediately — we don't need stalled events, only
        // tick events from the detector loop.
        Ok::<_, chasquimq::HandlerError>(Bytes::new())
    };
    let shutdown = CancellationToken::new();
    let consumer_task = tokio::spawn(consumer.run(handler, shutdown.clone()));

    // Give the consumer + detector time to spawn + observe several
    // ticks at 200ms. Wait for at least 5 ticks (>= 1s of activity at
    // the requested cadence); without the fix, this would loop forever
    // (would only see ~1 tick at the 5s claim_min_idle).
    wait_until(Duration::from_secs(15), || async {
        sink.stalled_ticks().len() >= 5
    })
    .await;

    let ticks = sink.stalled_ticks().len();
    assert!(
        ticks >= 5,
        "embedded detector must honor user-supplied tick_interval_ms=200ms; \
         observed {ticks} ticks in 15s (would expect <=3 under the silent override)"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer_task).await;
    flush_stalled(&admin, &queue).await;
}
