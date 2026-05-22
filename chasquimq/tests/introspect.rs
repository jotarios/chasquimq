//! Integration tests for the `Introspector` against a live Redis 8.6+.
//!
//! Set `REDIS_URL` (e.g. `REDIS_URL=redis://127.0.0.1:6379`) and run with
//! `cargo test -p chasquimq --test introspect -- --include-ignored`.

use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::{Producer, stream_key};
use chasquimq::{HandlerError, Introspector, Job, JobState};
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

async fn flush_all(admin: &Client, queue: &str) {
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "events",
        "paused",
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
    // SCAN-delete any per-id keys (result, didx, dlid) so a stale key
    // doesn't bleed across tests.
    for prefix in ["result:", "didx:", "dlid:"] {
        let pattern = format!("{{chasqui:{queue}}}:{prefix}*");
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

fn producer_cfg(queue: &str) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
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
        result_ttl_secs: 60,
        delayed_enabled: false,
        ..Default::default()
    }
}

async fn introspector(queue: &str) -> Introspector {
    Introspector::connect(
        &redis_url(),
        queue,
        &chasquimq::ConnectionTuning::default(),
        Some("default"),
    )
    .await
    .expect("connect introspector")
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
// JobCounts
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_counts_empty_queue() {
    let admin = admin().await;
    let queue = "iq_counts_empty";
    flush_all(&admin, queue).await;

    let insp = introspector(queue).await;
    let counts = insp.get_job_counts().await.expect("counts");
    assert_eq!(counts.waiting, 0);
    assert_eq!(counts.active, 0);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.completed, 0);
    assert_eq!(counts.paused, 0);
    assert!(!counts.completed_is_capped);

    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_counts_with_no_consumer_group_yet() {
    // A queue with stream entries but no consumer group has never been
    // read; XPENDING returns NOGROUP. The introspector must swallow that
    // and treat active=0.
    let admin = admin().await;
    let queue = "iq_counts_nogroup";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    for n in 0..5_u32 {
        producer
            .add(Sample { n, s: "x".into() })
            .await
            .expect("add");
    }

    let insp = introspector(queue).await;
    let counts = insp.get_job_counts().await.expect("counts");
    assert_eq!(counts.waiting, 5, "waiting = XLEN - active(=0)");
    assert_eq!(counts.active, 0);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.failed, 0);

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_counts_paused_flag_tracks_durable_pause() {
    let admin = admin().await;
    let queue = "iq_counts_paused";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let insp = introspector(queue).await;

    assert_eq!(insp.get_job_counts().await.unwrap().paused, 0);
    producer.pause().await.expect("pause");
    assert_eq!(insp.get_job_counts().await.unwrap().paused, 1);
    producer.resume().await.expect("resume");
    assert_eq!(insp.get_job_counts().await.unwrap().paused, 0);

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_counts_with_pending_active_delayed_failed() {
    let admin = admin().await;
    let queue = "iq_counts_mixed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // 5 waiting
    for n in 0..5_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add waiting");
    }

    // 3 delayed (1h out)
    for n in 0..3_u32 {
        producer
            .add_in(
                Duration::from_secs(3600),
                Sample {
                    n: 100 + n,
                    s: "d".into(),
                },
            )
            .await
            .expect("add delayed");
    }

    // 1 failed: produce a job, run a consumer that always fails until
    // it exhausts retries → DLQ.
    let fail_id = producer
        .add(Sample {
            n: 999,
            s: "fail".into(),
        })
        .await
        .expect("add fail");

    let cancel = CancellationToken::new();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls_c = handler_calls.clone();
    let cfg = ConsumerConfig {
        max_attempts: 1, // straight to DLQ
        ..consumer_cfg(queue, false)
    };
    let cancel_c = cancel.clone();
    let join = tokio::spawn(async move {
        let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
        consumer
            .run(
                move |job: Job<Sample>| {
                    let handler_calls = handler_calls_c.clone();
                    async move {
                        handler_calls.fetch_add(1, Ordering::SeqCst);
                        let _ = job;
                        Err::<bytes::Bytes, _>(HandlerError::new(std::io::Error::other("boom")))
                    }
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    // Wait for the DLQ entry to land.
    let insp = introspector(queue).await;
    wait_until(Duration::from_secs(10), "DLQ populated", || async {
        insp.get_job_counts()
            .await
            .map(|c| c.failed >= 1)
            .unwrap_or(false)
    })
    .await;

    let counts = insp.get_job_counts().await.expect("counts");
    // At this point: waiting = original 5 (jobs being held by the
    // blocking handler keep them "active"), but a handful might already
    // be in the PEL. Just check the invariants we know hold:
    assert!(
        counts.delayed >= 3,
        "expected 3 delayed, got {}",
        counts.delayed
    );
    assert!(counts.failed >= 1, "expected at least 1 failed");

    // Now write a result key for a fresh id and confirm `completed` ticks.
    let result_key = format!("{{chasqui:{queue}}}:result:res-test-id");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![
                Value::from(result_key),
                Value::Bytes(bytes::Bytes::from_static(b"\xa2ok")),
            ],
        )
        .await
        .expect("SET");
    let counts = insp.get_job_counts().await.expect("counts");
    assert_eq!(counts.completed, 1, "result key bumps completed by 1");

    let _ = fail_id;
    cancel.cancel();
    let _ = join.await;
    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

// ============================================================================
// JobState single-id lookups
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_state_unknown_for_missing_id() {
    let admin = admin().await;
    let queue = "iq_state_unknown";
    flush_all(&admin, queue).await;
    let insp = introspector(queue).await;

    let state = insp.get_job_state("nonexistent").await.expect("state");
    assert_eq!(state, JobState::Unknown);

    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_state_waiting_for_unread_entry() {
    let admin = admin().await;
    let queue = "iq_state_waiting";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    let id = producer
        .add(Sample {
            n: 1,
            s: "w".into(),
        })
        .await
        .expect("add");

    let insp = introspector(queue).await;
    assert_eq!(
        insp.get_job_state(&id).await.expect("state"),
        JobState::Waiting
    );

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_state_delayed_for_zset_entry() {
    let admin = admin().await;
    let queue = "iq_state_delayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    let id = "delayed-stable-id".to_string();
    use chasquimq::producer::AddOptions;
    producer
        .add_in_with_options(
            Duration::from_secs(3600),
            Sample {
                n: 2,
                s: "d".into(),
            },
            AddOptions::new().with_id(id.clone()),
        )
        .await
        .expect("add_in");

    let insp = introspector(queue).await;
    assert_eq!(
        insp.get_job_state(&id).await.expect("state"),
        JobState::Delayed
    );

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_state_completed_when_only_result_key_present() {
    let admin = admin().await;
    let queue = "iq_state_completed";
    flush_all(&admin, queue).await;

    let result_key = format!("{{chasqui:{queue}}}:result:completed-id");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![
                Value::from(result_key),
                Value::Bytes(bytes::Bytes::from_static(b"\xa2ok")),
            ],
        )
        .await
        .expect("SET");

    let insp = introspector(queue).await;
    assert_eq!(
        insp.get_job_state("completed-id").await.expect("state"),
        JobState::Completed
    );

    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn live_state_beats_terminal_state() {
    // Inject both a result key AND a waiting stream entry with the same
    // id. The inspector must return "waiting" (live state) ahead of
    // "completed" (terminal state) so callers see the work the next
    // worker tick is about to do.
    let admin = admin().await;
    let queue = "iq_live_state_priority";
    flush_all(&admin, queue).await;

    let stable_id = "race-id".to_string();
    let result_key = format!("{{chasqui:{queue}}}:result:{stable_id}");
    let _: Value = admin
        .custom(
            CustomCommand::new_static("SET", ClusterHash::FirstKey, false),
            vec![
                Value::from(result_key),
                Value::Bytes(bytes::Bytes::from_static(b"\xa2ok")),
            ],
        )
        .await
        .expect("SET");

    // Produce a waiting entry with the same stable id (using
    // `add_with_options` to pin the id).
    use chasquimq::producer::AddOptions;
    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    producer
        .add_with_options(
            Sample {
                n: 1,
                s: "w".into(),
            },
            AddOptions::new().with_id(stable_id.clone()),
        )
        .await
        .expect("add");

    let insp = introspector(queue).await;
    assert_eq!(
        insp.get_job_state(&stable_id).await.expect("state"),
        JobState::Waiting,
        "live waiting must beat terminal completed"
    );

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_with_undecodable_entry_skips_safely() {
    // XADD a poison `d` field (not msgpack), then a sibling proper job.
    // The inspector's `find_in_stream(known_id)` must still find the
    // sibling.
    let admin = admin().await;
    let queue = "iq_poison_skips";
    flush_all(&admin, queue).await;

    // Poison entry: just three garbage bytes for `d`.
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(stream_key(queue)),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(bytes::Bytes::from_static(&[0xff, 0xfe, 0xfd])),
            ],
        )
        .await
        .expect("XADD poison");

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    let sibling_id = producer
        .add(Sample {
            n: 1,
            s: "sibling".into(),
        })
        .await
        .expect("add sibling");

    let insp = introspector(queue).await;
    let job = insp.get_job(&sibling_id).await.expect("get_job");
    assert!(
        job.is_some(),
        "sibling must be locatable despite poison neighbor"
    );
    let job = job.unwrap();
    assert_eq!(job.id, sibling_id);
    assert_eq!(job.state, JobState::Waiting);

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_job_preserves_name_field() {
    let admin = admin().await;
    let queue = "iq_name_preserved";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    use chasquimq::producer::AddOptions;
    let id = producer
        .add_with_options(
            Sample {
                n: 1,
                s: "x".into(),
            },
            AddOptions::new().with_name("send-email"),
        )
        .await
        .expect("add");

    let insp = introspector(queue).await;
    let job = insp.get_job(&id).await.expect("get_job").expect("Some");
    assert_eq!(job.name, "send-email");
    assert_eq!(job.id, id);

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

// ============================================================================
// Pagination
// ============================================================================

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_jobs_paginates_waiting() {
    let admin = admin().await;
    let queue = "iq_page_waiting";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    for n in 0..23_u32 {
        producer
            .add(Sample { n, s: "w".into() })
            .await
            .expect("add");
    }

    let insp = introspector(queue).await;
    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = insp
            .get_jobs(JobState::Waiting, 0, 10, cursor.clone())
            .await
            .expect("page");
        total += page.jobs.len();
        pages += 1;
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
        assert!(pages < 10, "runaway pagination");
    }
    assert_eq!(total, 23, "all 23 jobs visited");

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_jobs_paginates_delayed() {
    let admin = admin().await;
    let queue = "iq_page_delayed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    for n in 0..12_u32 {
        producer
            .add_in(
                Duration::from_secs(3600 + n as u64), // distinct scores
                Sample { n, s: "d".into() },
            )
            .await
            .expect("add_in");
    }

    let insp = introspector(queue).await;
    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = insp
            .get_jobs(JobState::Delayed, 0, 5, cursor.clone())
            .await
            .expect("page");
        total += page.jobs.len();
        pages += 1;
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
        assert!(pages < 10, "runaway pagination");
    }
    assert_eq!(total, 12);

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_jobs_paginates_delayed_with_tied_scores() {
    // Regression: multiple delayed members sharing a single fire-ms
    // (cron specs firing on the minute, identical add_at scores) must
    // not be dropped at the page boundary. The cursor encodes
    // score:offset_into_score so a tied tail resumes cleanly.
    use std::time::{SystemTime, UNIX_EPOCH};

    let admin = admin().await;
    let queue = "iq_page_delayed_ties";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    // Five tied-score jobs scheduled for a fixed absolute time far in
    // the future, then five with distinct later scores.
    let tied_at = SystemTime::now() + Duration::from_secs(7200);
    let tied_ms = tied_at
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let tied_at = UNIX_EPOCH + Duration::from_millis(tied_ms as u64);
    for n in 0..5_u32 {
        producer
            .add_at(
                tied_at,
                Sample {
                    n,
                    s: "tied".into(),
                },
            )
            .await
            .expect("add_at tied");
    }
    for n in 0..5_u32 {
        producer
            .add_in(
                Duration::from_secs(7300 + n as u64),
                Sample {
                    n,
                    s: "distinct".into(),
                },
            )
            .await
            .expect("add_in distinct");
    }

    let insp = introspector(queue).await;
    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    // Page size 3 forces a boundary mid-tied-cluster: page 1 emits 3 of
    // the 5 tied members; page 2 must emit the other 2 (not skip
    // them).
    loop {
        let page = insp
            .get_jobs(JobState::Delayed, 0, 3, cursor.clone())
            .await
            .expect("page");
        total += page.jobs.len();
        pages += 1;
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
        assert!(pages < 20, "runaway pagination");
    }
    assert_eq!(total, 10, "all 10 delayed jobs must page out");

    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_jobs_paginates_failed() {
    let admin = admin().await;
    let queue = "iq_page_failed";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect");
    for n in 0..7_u32 {
        producer
            .add(Sample {
                n,
                s: "fail".into(),
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
                |_job: Job<Sample>| async {
                    Err::<bytes::Bytes, _>(HandlerError::new(std::io::Error::other("nope")))
                },
                cancel_c.clone(),
            )
            .await
            .ok();
    });

    let insp = introspector(queue).await;
    wait_until(Duration::from_secs(15), "7 DLQ", || async {
        insp.get_job_counts()
            .await
            .map(|c| c.failed >= 7)
            .unwrap_or(false)
    })
    .await;

    let mut total = 0;
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let page = insp
            .get_jobs(JobState::Failed, 0, 3, cursor.clone())
            .await
            .expect("page");
        total += page.jobs.len();
        pages += 1;
        if page.next_cursor.is_none() {
            break;
        }
        cursor = page.next_cursor;
        assert!(pages < 10, "runaway pagination");
    }
    assert!(total >= 7, "expected at least 7 failed (got {total})");

    cancel.cancel();
    let _ = join.await;
    producer.shutdown().await.ok();
    insp.shutdown().await.ok();
    admin.quit().await.ok();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn get_jobs_unknown_state_returns_empty() {
    let admin = admin().await;
    let queue = "iq_unknown_state_page";
    flush_all(&admin, queue).await;
    let insp = introspector(queue).await;

    let page = insp
        .get_jobs(JobState::Unknown, 0, 10, None)
        .await
        .expect("page");
    assert!(page.jobs.is_empty());
    assert!(page.next_cursor.is_none());

    insp.shutdown().await.ok();
    admin.quit().await.ok();
}
