//! End-to-end tests for slice 1 of the name-on-wire feature: producer adds
//! a job with a `name`, consumer reads it back via the engine's normal hot
//! path, and observes `Job::name` in the handler. Forward-compat is also
//! pinned: a consumer running against entries with no `n` field decodes
//! `Job::name == ""`, regardless of whether the producer was on the new
//! shape or hand-built the XADD without `n`.

use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::error::HandlerError;
use chasquimq::job::Job;
use chasquimq::producer::{AddOptions, Producer, dlq_key, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
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
    for suffix in ["stream", "dlq", "delayed", "promoter:lock", "events"] {
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
        batch: 64,
        block_ms: 100,
        claim_min_idle_ms: 30_000,
        concurrency: 4,
        max_attempts: 3,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled: false,
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

/// Helper: spin up a consumer that records every `(name, payload)` it sees
/// onto a shared `Vec`, then return after `expected` jobs land.
async fn drive_consumer(queue: &str, consumer_id: &str, expected: usize) -> Vec<(String, u32)> {
    let observed: Arc<Mutex<Vec<(String, u32)>>> = Arc::new(Mutex::new(Vec::new()));
    let observed_h = observed.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), consumer_cfg(queue, consumer_id));
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
    wait_until(Duration::from_secs(15), move || {
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

/// Slice 1 happy path: producer adds a named job via `add_with_options`,
/// consumer observes `Job::name == "<the-name>"` end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn named_job_round_trips_to_consumer() {
    let admin = admin().await;
    let queue = "name_round_trip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer
        .add_with_options(Sample { n: 7 }, AddOptions::new().with_name("send-email"))
        .await
        .expect("add_with_options");

    let observed = drive_consumer(queue, "name_c1", 1).await;
    assert_eq!(observed, vec![("send-email".to_string(), 7)]);

    let _: () = admin.quit().await.unwrap();
}

/// Forward-compat: the legacy single-arg `add(payload)` does NOT attach an
/// `n` field (producer omits it on the wire). A consumer reading that entry
/// observes `Job::name == ""`. This is what makes mixed-version deploys safe
/// and what the design doc means by "absent and empty are equivalent".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn unnamed_producer_path_yields_empty_name_at_consumer() {
    let admin = admin().await;
    let queue = "name_unnamed_path";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 11 }).await.expect("add");

    let observed = drive_consumer(queue, "name_c2", 1).await;
    assert_eq!(observed, vec![(String::new(), 11)]);

    let _: () = admin.quit().await.unwrap();
}

/// Forward-compat (hand-crafted): mimic a pre-slice-1 producer by issuing
/// an `XADD ... d <bytes>` directly with no `n` field at all. The consumer
/// must still decode `Job::name == ""`. This is the case the parser's
/// missing-field fallback exists for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn pre_slice1_xadd_without_n_field_decodes_with_empty_name() {
    let admin = admin().await;
    let queue = "name_legacy_xadd";
    let key = stream_key(queue);
    flush_all(&admin, queue).await;

    // Hand-build a `Job<Sample>` and write it under `d` only — exactly the
    // shape a pre-slice-1 producer would emit.
    let job: Job<Sample> = Job::new(Sample { n: 23 });
    let bytes = rmp_serde::to_vec(&job).expect("encode");

    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(key.clone()),
                Value::from("MAXLEN"),
                Value::from("~"),
                Value::from(100_000_i64),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes::Bytes::from(bytes)),
            ],
        )
        .await
        .expect("XADD legacy");

    let observed = drive_consumer(queue, "name_c3", 1).await;
    assert_eq!(observed, vec![(String::new(), 23)]);

    let _: () = admin.quit().await.unwrap();
}

/// Bulk variant of the round-trip: each `(name, payload)` pair routes to
/// the matching observed entry on the consumer side. This pins the
/// per-entry name semantic for `add_bulk_named` (vs. shared-name in
/// `add_bulk_with_options`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_named_round_trips_per_entry_names() {
    let admin = admin().await;
    let queue = "name_bulk_named";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let items = vec![
        ("send-email".to_string(), Sample { n: 1 }),
        ("resize-image".to_string(), Sample { n: 2 }),
        ("post-webhook".to_string(), Sample { n: 3 }),
    ];
    producer
        .add_bulk_named(items)
        .await
        .expect("add_bulk_named");

    let observed = drive_consumer(queue, "name_c4", 3).await;
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

// -- Slice 3: delayed paths preserve `name` end-to-end --

/// Positive control: empty `name` (or unset) on the delayed path still works,
/// matching the legacy `add_in` shape — verifies the prefix-encode path
/// handles the empty-name case (zero-length prefix + msgpack).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires REDIS_URL"]
async fn add_in_with_options_accepts_empty_name() {
    let admin = admin().await;
    let queue = "name_reject_in_ok";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    // Default-built `AddOptions` carries `name == ""` — must be accepted.
    producer
        .add_in_with_options(Duration::from_secs(0), Sample { n: 5 }, AddOptions::new())
        .await
        .expect("empty-name AddOptions on delayed path must be accepted");

    let _: () = admin.quit().await.unwrap();
}

// -- Fix 2 + Fix 3 (PR #56 review): peek_dlq surfaces name; replay preserves it --

fn dlq_round_trip_consumer_cfg(queue: &str, consumer_id: &str) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: consumer_id.to_string(),
        batch: 64,
        block_ms: 50,
        // max_attempts=1 → one handler invocation, then DLQ. Keeps the test
        // bounded; the retry-via-delayed-ZSET drop is also covered by the
        // doc warning so no need to time out a real backoff here.
        max_attempts: 1,
        ack_batch: 64,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        max_payload_bytes: 1_048_576,
        dlq_inflight: 32,
        delayed_enabled: false,
        retry: chasquimq::RetryConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 5,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        ..Default::default()
    }
}

/// Poll `XLEN <dlq>` until it reports at least `expected` entries.
///
/// This is the system-of-record check: the relocator is async wrt the
/// handler's return, and the DLQ entry only exists once the relocator's
/// XADD+XACKDEL pipeline has landed in Redis. Any test that gates on the
/// in-memory handler counter will race the relocator under host load.
async fn wait_dlq_depth(admin: &Client, queue: &str, expected: usize) {
    let dlq = dlq_key(queue);
    let admin_h = admin.clone();
    let expected_i64 = i64::try_from(expected).unwrap_or(i64::MAX);
    wait_until(Duration::from_secs(15), move || {
        let admin_h = admin_h.clone();
        let dlq = dlq.clone();
        async move {
            let res: Value = admin_h
                .custom(
                    CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false),
                    vec![Value::from(dlq.as_str())],
                )
                .await
                .expect("XLEN dlq");
            matches!(res, Value::Integer(n) if n >= expected_i64)
        }
    })
    .await;
}

/// Drive a consumer that fails every job with a recoverable handler error,
/// returning after `expected` failures have landed in the DLQ. With
/// `max_attempts=1`, each failure routes straight to the DLQ.
///
/// The wait condition is deterministic on Redis state — `XLEN <dlq> >=
/// expected` — not on the in-memory handler counter. The handler counter
/// trips as soon as the closure runs, but the DlqRelocate event still has
/// to traverse the worker→mpsc→relocator path and complete an XADD+XACKDEL
/// pipeline. On a contended host those steps take long enough that gating
/// shutdown on the handler counter races the relocator and the DLQ entry
/// can be lost on shutdown drain. Polling XLEN closes that race: by the
/// time we cancel, Redis has already accepted the entry.
async fn drive_failing_consumer(admin: &Client, queue: &str, consumer_id: &str, expected: usize) {
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), dlq_round_trip_consumer_cfg(queue, consumer_id));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err::<(), _>(HandlerError::new(std::io::Error::other("fail-on-purpose")))
                },
                shutdown_clone,
            )
            .await
    });

    wait_dlq_depth(admin, queue, expected).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), join).await;

    // Race-closer: the reader's last XREADGROUP at shutdown may still be
    // server-side-blocked on Redis with `BLOCK <block_ms> ... >`. Even
    // though the local future was dropped on `shutdown.cancel()`, Redis
    // holds the BLOCK until either timeout expiry or TCP close. If a
    // new entry XADDs onto the stream during that window — for example a
    // subsequent `replay_dlq` from this same test — Redis serves the
    // still-blocked call, assigning the entry to *this* consumer's
    // pending list and advancing the group's `last-delivered-id` past
    // the new entry. A second consumer in the same group then sees
    // `>` deliver nothing and the test deadlocks.
    //
    // Wait one full BLOCK window plus padding so any in-flight call
    // definitely times out before the next test step issues an XADD.
    // This sleep is the deterministic race-closer for the flake tracked
    // under task #48.
    let block_ms = dlq_round_trip_consumer_cfg("_", "_").block_ms;
    tokio::time::sleep(Duration::from_millis(block_ms.saturating_mul(2).max(150))).await;
}

/// Fix 2 + Fix 3 round trip: producer adds a named job → consumer fails it →
/// `peek_dlq` returns `name` populated → `replay_dlq` re-emits with `n` →
/// a second consumer drain sees the original `Job::name`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn dlq_peek_and_replay_preserve_name() {
    let admin = admin().await;
    let queue = "name_dlq_round_trip";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer
        .add_with_options(Sample { n: 7 }, AddOptions::new().with_name("send-email"))
        .await
        .expect("named add");

    // Drive one failing handler invocation → DLQ. `drive_failing_consumer`
    // gates its own shutdown on `XLEN <dlq> >= 1`, so by the time it
    // returns the DLQ entry has already been XADD'd + XACKDEL'd.
    drive_failing_consumer(&admin, queue, "name_dlq_c1", 1).await;

    // Fix 2: peek_dlq surfaces name.
    let entries = producer.peek_dlq(10).await.expect("peek_dlq");
    assert_eq!(entries.len(), 1, "expected exactly one DLQ entry");
    assert_eq!(
        entries[0].name, "send-email",
        "DlqEntry::name must carry the source entry's `n` field verbatim"
    );

    // Fix 3: replay_dlq preserves the name on re-emit. The replayed XADD must
    // include `n` so a second consumer drain reads `Job::name == "send-email"`.
    let replayed = producer.replay_dlq(10).await.expect("replay_dlq");
    assert_eq!(replayed, 1);

    // Drive a second consumer that succeeds, just observing the name.
    let observed = drive_consumer(queue, "name_dlq_c2", 1).await;
    assert_eq!(
        observed,
        vec![("send-email".to_string(), 7)],
        "replayed job must arrive on consumer side with Job::name preserved"
    );

    let _: () = admin.quit().await.unwrap();
}

/// Forward-compat: a DLQ entry that had no `n` field at the source (legacy
/// producer or reader-side malformed route) replays cleanly with no `n` on
/// the new XADD. The replay path's `if name ~= ''` branch is what makes
/// this work — without it, every replay would emit `'n' ''`, polluting the
/// downstream `Job::name`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn dlq_replay_omits_name_when_source_had_none() {
    let admin = admin().await;
    let queue = "name_dlq_no_n";
    flush_all(&admin, queue).await;

    // Hand-build a DLQ entry without an `n` field. Mimics a pre-slice-1
    // producer's job that landed in the DLQ.
    let dlq = dlq_key(queue);
    let job = Job::with_id("legacy-job".to_string(), Sample { n: 99 });
    let bytes = bytes::Bytes::from(rmp_serde::to_vec(&job).expect("encode"));
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(dlq.as_str()),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes),
                Value::from("source_id"),
                Value::from("legacy-source"),
                Value::from("reason"),
                Value::from("retries_exhausted"),
            ],
        )
        .await
        .expect("XADD legacy dlq");

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let entries = producer.peek_dlq(10).await.expect("peek_dlq");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "", "no `n` at source → empty name");

    let replayed = producer.replay_dlq(10).await.expect("replay_dlq");
    assert_eq!(replayed, 1);

    let observed = drive_consumer(queue, "name_dlq_legacy_c", 1).await;
    assert_eq!(observed, vec![(String::new(), 99)]);

    let _: () = admin.quit().await.unwrap();
}
