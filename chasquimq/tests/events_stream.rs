//! Cross-process events-stream integration tests (slice 9).
//!
//! Each test runs against a real Redis (REDIS_URL) and asserts the
//! `{chasqui:<queue>}:events` stream is populated with the expected
//! event names + fields. The reader-side parsing of stream entries here
//! is intentionally hand-rolled (XRANGE → flat key/value pairs) so the
//! test asserts the wire format, not the engine's internal helpers.

mod common;

use chasquimq::producer::{Producer, events_key};
use chasquimq::{Consumer, ConsumerConfig, HandlerError, Job, RetryConfig};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use common::{Sample, admin, flush_all, producer_cfg, redis_url, wait_until};

/// One decoded events-stream entry. `id` is the stream entry id (Redis
/// timestamp-seq), `fields` is the flat key→value map (e/id/ts/...).
#[derive(Debug, Clone)]
struct EventEntry {
    #[allow(dead_code)]
    entry_id: String,
    fields: HashMap<String, String>,
}

impl EventEntry {
    fn name(&self) -> &str {
        self.fields.get("e").map(|s| s.as_str()).unwrap_or("")
    }
    fn job_id(&self) -> &str {
        self.fields.get("id").map(|s| s.as_str()).unwrap_or("")
    }
    fn attempt(&self) -> Option<u32> {
        self.fields.get("attempt")?.parse().ok()
    }
    #[allow(dead_code)]
    fn duration_us(&self) -> Option<u64> {
        self.fields.get("duration_us")?.parse().ok()
    }
    #[allow(dead_code)]
    fn reason(&self) -> Option<&str> {
        self.fields.get("reason").map(|s| s.as_str())
    }
}

async fn xlen(admin: &Client, key: &str) -> i64 {
    match admin
        .custom::<Value, _>(
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

/// Read every entry from the events stream via `XRANGE - +`. Returns them
/// in chronological order (Redis preserves insertion order in a single
/// stream). Each entry's flat field list is folded into a HashMap for
/// easy assertion lookups.
async fn read_events(admin: &Client, queue: &str) -> Vec<EventEntry> {
    let key = events_key(queue);
    let raw: Value = admin
        .custom(
            CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
            vec![
                Value::from(key.as_str()),
                Value::from("-"),
                Value::from("+"),
            ],
        )
        .await
        .expect("XRANGE");
    let entries = match raw {
        Value::Array(items) => items,
        Value::Null => return Vec::new(),
        other => panic!("XRANGE unexpected: {other:?}"),
    };
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let pair = match entry {
            Value::Array(p) if p.len() == 2 => p,
            other => panic!("entry not a 2-tuple: {other:?}"),
        };
        let entry_id = match &pair[0] {
            Value::String(s) => s.to_string(),
            Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
            other => panic!("entry id not string: {other:?}"),
        };
        let kv = match &pair[1] {
            Value::Array(items) => items.clone(),
            other => panic!("entry fields not array: {other:?}"),
        };
        let mut fields = HashMap::new();
        let mut iter = kv.into_iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            let k = value_to_string(k);
            let v = value_to_string(v);
            fields.insert(k, v);
        }
        out.push(EventEntry { entry_id, fields });
    }
    out
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
        Value::Integer(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

/// Helper: look up exactly one event with this name and (optional) job id.
/// Panics if zero or multiple match — tests should always know which one
/// they expect.
fn find_one<'a>(events: &'a [EventEntry], name: &str, job_id: Option<&str>) -> &'a EventEntry {
    let matches: Vec<&EventEntry> = events
        .iter()
        .filter(|e| e.name() == name && job_id.is_none_or(|id| e.job_id() == id))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one {name} event for {job_id:?}, got {} (events: {events:?})",
        matches.len()
    );
    matches[0]
}

fn count_by_name(events: &[EventEntry], name: &str) -> usize {
    events.iter().filter(|e| e.name() == name).count()
}

/// Standard consumer config: tiny backoff, no jitter, max_attempts as
/// supplied. `events_enabled` defaults to true. `delayed_enabled` toggled
/// per-test as needed.
fn consumer_cfg(queue: &str, max_attempts: u32) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: format!("c-{}", uuid::Uuid::new_v4()),
        block_ms: 50,
        retry: RetryConfig {
            initial_backoff_ms: 20,
            max_backoff_ms: 500,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        max_attempts,
        delayed_enabled: false,
        concurrency: 4,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn end_to_end_active_and_completed_emitted() {
    let admin = admin().await;
    let queue = "events_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let job_id = producer.add(Sample { n: 7 }).await.expect("add");

    let cfg = consumer_cfg(queue, 3);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(move |_job: Job<Sample>| async move { Ok(()) }, shutdown_h)
            .await
    });

    // Wait until both `active` and `completed` events for this id are present.
    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_secs(5), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "completed" && e.job_id() == job_id)
                    && evs
                        .iter()
                        .any(|e| e.name() == "active" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let evs = read_events(&admin, queue).await;
    let active = find_one(&evs, "active", Some(&job_id));
    assert_eq!(active.attempt(), Some(1), "first run is attempt 1");
    let completed = find_one(&evs, "completed", Some(&job_id));
    assert_eq!(completed.attempt(), Some(1));
    assert!(
        completed.duration_us().is_some(),
        "completed event must carry duration_us"
    );
    // No failed/dlq on the success path.
    assert_eq!(count_by_name(&evs, "failed"), 0);
    assert_eq!(count_by_name(&evs, "dlq"), 0);

    // ts is a unix-ms decimal string and must parse.
    for e in &evs {
        let ts = e
            .fields
            .get("ts")
            .expect("every event has ts")
            .parse::<u64>()
            .expect("ts is decimal u64");
        assert!(ts > 0);
    }

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn retry_then_success_emits_failed_retry_and_active2() {
    let admin = admin().await;
    let queue = "events_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let job_id = producer.add(Sample { n: 1 }).await.expect("add");

    // Fail attempt 1, succeed on attempt 2 → max_attempts=3, delayed_enabled
    // so retry actually re-enters the stream.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        ..consumer_cfg(queue, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    if job.attempt < 1 {
                        Err(HandlerError(Box::new(std::io::Error::other("retry me"))))
                    } else {
                        Ok(())
                    }
                },
                shutdown_h,
            )
            .await
    });

    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_secs(5), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "completed" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let evs = read_events(&admin, queue).await;
    // Two `active` events (attempts 1 and 2).
    let actives: Vec<&EventEntry> = evs
        .iter()
        .filter(|e| e.name() == "active" && e.job_id() == job_id)
        .collect();
    assert_eq!(actives.len(), 2, "active fired for attempt 1 and 2");
    let mut attempts: Vec<u32> = actives.iter().filter_map(|e| e.attempt()).collect();
    attempts.sort();
    assert_eq!(attempts, vec![1, 2]);

    // Exactly one failed (attempt 1).
    let failed = find_one(&evs, "failed", Some(&job_id));
    assert_eq!(failed.attempt(), Some(1));
    assert!(failed.reason().is_some());

    // Exactly one retry-scheduled (next attempt = 2).
    let retry = find_one(&evs, "retry-scheduled", Some(&job_id));
    assert_eq!(retry.attempt(), Some(2));
    assert!(retry.fields.contains_key("backoff_ms"));

    // Exactly one completed (attempt 2).
    let completed = find_one(&evs, "completed", Some(&job_id));
    assert_eq!(completed.attempt(), Some(2));

    // No dlq.
    assert_eq!(count_by_name(&evs, "dlq"), 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn always_failing_handler_emits_dlq() {
    let admin = admin().await;
    let queue = "events_e3";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let job_id = producer.add(Sample { n: 1 }).await.expect("add");

    // max_attempts=2: one retry between two failures, then DLQ.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        ..consumer_cfg(queue, 2)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err(HandlerError(Box::new(std::io::Error::other("nope"))))
                },
                shutdown_h,
            )
            .await
    });

    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_secs(5), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "dlq" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let evs = read_events(&admin, queue).await;
    let dlq = find_one(&evs, "dlq", Some(&job_id));
    // attempt = the 1-indexed run that just failed and exhausted retries.
    assert_eq!(dlq.attempt(), Some(2));
    assert_eq!(dlq.reason(), Some("retries_exhausted"));
    // At least one failed event before the DLQ.
    assert!(count_by_name(&evs, "failed") >= 1);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn promoter_emits_waiting_after_delayed_promote() {
    let admin = admin().await;
    let queue = "events_e4";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let job_id = producer
        .add_in(Duration::from_millis(50), Sample { n: 1 })
        .await
        .expect("add_in");

    // Run a consumer with the embedded promoter (delayed_enabled=true) so
    // we exercise the full schedule → promote → waiting event path. Use a
    // never-completing handler so the test asserts on `waiting` without
    // racing the `completed` event.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        // Don't actually run the handler; we only care about `waiting`.
        // A handler that blocks indefinitely would still let the promoter
        // run, but the active→completed events would race the wait_until.
        // Instead, just make the handler succeed; we filter events by name.
        ..consumer_cfg(queue, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(move |_job: Job<Sample>| async move { Ok(()) }, shutdown_h)
            .await
    });

    // Allow up to 1s for the promoter to fire (50ms scheduled delay + a
    // poll interval or two).
    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_millis(2_000), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "waiting" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let evs = read_events(&admin, queue).await;
    let waiting = find_one(&evs, "waiting", Some(&job_id));
    // `waiting` carries no extra fields beyond e/id/ts.
    assert!(waiting.fields.contains_key("ts"));

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn maxlen_caps_events_stream_growth() {
    // Cap the events stream at 64; produce + consume 200 jobs (one event
    // pair per job at minimum); assert XLEN stays within ~tolerance of
    // the cap. The `~` in `MAXLEN ~ <cap>` is approximate trim, so Redis
    // may sit a few hundred above the cap before trimming.
    let admin = admin().await;
    let queue = "events_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    for n in 0..200 {
        producer.add(Sample { n }).await.expect("add");
    }

    let cfg = ConsumerConfig {
        events_max_stream_len: 64,
        ..consumer_cfg(queue, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(move |_job: Job<Sample>| async move { Ok(()) }, shutdown_h)
            .await
    });

    // Wait until at least 200 active events have been emitted (one per
    // job), or the stream cap is observed to have hit (whichever comes
    // first). XLEN under the cap means trim hasn't kicked in yet.
    let key = events_key(queue);
    {
        let admin = admin.clone();
        let key = key.clone();
        wait_until(Duration::from_secs(10), move || {
            let admin = admin.clone();
            let key = key.clone();
            async move { xlen(&admin, &key).await > 64 }
        })
        .await;
    }
    // Give it another moment to keep producing events, so we can verify
    // the cap is actually enforced.
    tokio::time::sleep(Duration::from_millis(500)).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let len = xlen(&admin, &key).await;
    // `MAXLEN ~ 64` permits Redis to trim approximately. The actual
    // observed bound on Redis 8.x is the cap rounded up to the nearest
    // listpack node; in practice this stays well under cap*4 even under
    // load. We pick a generous tolerance (4x) so this test is not flaky.
    assert!(
        (64..64 * 4).contains(&len),
        "events stream XLEN should be bounded near cap=64; got {len}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn events_disabled_produces_no_xadd() {
    let admin = admin().await;
    let queue = "events_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    for n in 0..10 {
        producer.add(Sample { n }).await.expect("add");
    }

    // events_enabled=false should short-circuit every emit_*. Even on a
    // happy-path consumer with 10 jobs, XLEN(events_key) must stay 0.
    let cfg = ConsumerConfig {
        events_enabled: false,
        ..consumer_cfg(queue, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();

    use std::sync::atomic::{AtomicUsize, Ordering};
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_h = counter.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| {
                    let c = counter_h.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) >= 10 }
    })
    .await;

    // Give the disabled writer a generous chance to (incorrectly) flush.
    tokio::time::sleep(Duration::from_millis(200)).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let key = events_key(queue);
    let len = xlen(&admin, &key).await;
    assert_eq!(
        len, 0,
        "events stream must stay empty when events_enabled=false; got XLEN={len}"
    );

    let _: () = admin.quit().await.unwrap();
}

/// V1 design decision: the producer hot path does NOT emit a `delayed`
/// event for `Producer::add_in` / `add_at`. Reasoning: the produce path
/// is throughput-critical (bulk produce baseline ~61k/s) and adding a
/// second XADD would cut it ~half. Subscribers that need a `delayed`
/// signal can derive it from the producer-side acknowledgment of
/// `add_in` (the call returning a `JobId` is itself the schedule
/// confirmation), or wait for the promoter's `waiting` event. The
/// `EventsWriter::emit_delayed` method is wired through so a future
/// caller can opt in without re-plumbing.
///
/// This test pins the decision: if a future change adds a producer-side
/// `delayed` emit, this test will start failing and we'll have to make
/// an explicit call to either accept the throughput cost (with bench
/// numbers) or back the change out. Marking `#[ignore]` so it only runs
/// with `--include-ignored` (the workspace default in CI).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn delayed_event_skipped_on_producer_v1() {
    let admin = admin().await;
    let queue = "events_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    // Schedule far enough in the future that no promoter (none running
    // in this test) could promote it during the assertion window.
    let _ = producer
        .add_in(Duration::from_secs(60), Sample { n: 1 })
        .await
        .expect("add_in");

    // Producer alone, no consumer/promoter: no events should land.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let evs = read_events(&admin, queue).await;
    let delayed_count = count_by_name(&evs, "delayed");
    assert_eq!(
        delayed_count, 0,
        "v1 design: producer does not emit `delayed` events; got {delayed_count}. \
         If this fails, see the doc-comment on this test before changing the assertion."
    );

    let _: () = admin.quit().await.unwrap();
}

/// Slice 9a — confirm the standalone `Promoter::new` path still builds its
/// own events writer when no shared writer is supplied. Spawns a producer +
/// standalone Promoter (no Consumer in the loop) and asserts the
/// promoter's `waiting` event still lands on the per-queue events stream.
/// This guards against a regression where collapsing the writer onto the
/// embedded path silently disables events for users who run `Promoter`
/// outside `Consumer::run`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn standalone_promoter_still_emits_waiting_with_own_writer() {
    use chasquimq::{Promoter, PromoterConfig};

    let admin = admin().await;
    let queue = "events_e9";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    let job_id = producer
        .add_in(Duration::from_millis(50), Sample { n: 1 })
        .await
        .expect("add_in");

    // Standalone Promoter with events_enabled=true — must build its own
    // EventsWriter from its own Client (no shared writer plumbed). The
    // engine creates the consumer-group lazily on first XADD, but we don't
    // need a consumer here: the promoter's `waiting` emit happens before
    // any XREADGROUP.
    let cfg = PromoterConfig {
        queue_name: queue.to_string(),
        poll_interval_ms: 10,
        events_enabled: true,
        ..Default::default()
    };
    let promoter = Promoter::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move { promoter.run(shutdown_h).await });

    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_secs(2), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "waiting" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let evs = read_events(&admin, queue).await;
    let waiting = find_one(&evs, "waiting", Some(&job_id));
    assert!(waiting.fields.contains_key("ts"));

    let _: () = admin.quit().await.unwrap();
}

/// Count Redis client connections via `CLIENT LIST`. Returns total open
/// client connections seen by the server right now. Used by the conn-share
/// test to demonstrate the embedded promoter no longer opens a second
/// events-stream connection. Best-effort: the count includes every
/// connection on the server, so the test compares deltas, not absolutes.
async fn client_list_count(admin: &Client) -> usize {
    let v: Value = admin
        .custom(
            CustomCommand::new_static("CLIENT", ClusterHash::FirstKey, false),
            vec![Value::from("LIST")],
        )
        .await
        .expect("CLIENT LIST");
    let s = match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
        other => panic!("CLIENT LIST unexpected: {other:?}"),
    };
    // CLIENT LIST returns one connection per line; a trailing newline is
    // typical so filter empty lines out.
    s.lines().filter(|l| !l.is_empty()).count()
}

/// Slice 9a — verify the embedded promoter and the consumer share one
/// `EventsWriter` (and therefore one Redis connection) for events-stream
/// emits. Two assertions:
///
/// 1. **Behavioral**: with `events_enabled + delayed_enabled` both on,
///    BOTH the embedded promoter's `waiting` event AND the consumer's
///    hot-path `completed` event for the same delayed job land on the
///    stream. If the promoter were silently disabled (because the shared
///    writer wasn't plumbed) this test would miss `waiting`; if the
///    consumer were silently disabled (because the Arc clone broke the
///    enable bit) it would miss `completed`.
/// 2. **Connection delta**: the count of Redis connections after consumer
///    startup is one fewer than the previous-PR baseline. The previous
///    baseline (before slice 9a) was: reader + dlq_writer + ack_client +
///    retry_client + consumer-events + promoter-events + promoter-main
///    = 7 engine connections plus the admin probe + producer pool. After
///    slice 9a: promoter-events is collapsed into consumer-events, so 6
///    engine connections + admin + pool. We pin the *delta* via
///    before-and-after `CLIENT LIST` snapshots rather than an absolute
///    number — fred internals (e.g. a possible future second multiplexer
///    socket) make absolute pinning brittle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn embedded_promoter_shares_events_writer_with_consumer() {
    let admin = admin().await;
    let queue = "events_e8";
    flush_all(&admin, queue).await;

    // Drain any old per-test connections that might still be loitering.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    // Schedule a near-future delayed job. The embedded promoter will pick
    // it up and emit `waiting`; the consumer will then dispatch + complete
    // the handler, emitting `active` + `completed`. All three events must
    // land on the same events stream.
    let job_id = producer
        .add_in(Duration::from_millis(50), Sample { n: 17 })
        .await
        .expect("add_in");

    let baseline_clients = client_list_count(&admin).await;

    // Both `events_enabled` and `delayed_enabled` on. Use a fast promoter
    // tick so the test doesn't have to wait long.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        events_enabled: true,
        ..consumer_cfg(queue, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(move |_job: Job<Sample>| async move { Ok(()) }, shutdown_h)
            .await
    });

    // Let the engine register all its connections. 200ms is enough on a
    // healthy host; if it isn't, the assertion below has a tolerance.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let running_clients = client_list_count(&admin).await;
    let delta = running_clients.saturating_sub(baseline_clients);

    // Wait until the consumer has emitted `completed` for our job (which
    // implies the embedded promoter already emitted `waiting` upstream).
    {
        let admin = admin.clone();
        let queue = queue.to_string();
        let job_id = job_id.clone();
        wait_until(Duration::from_secs(5), move || {
            let admin = admin.clone();
            let queue = queue.clone();
            let job_id = job_id.clone();
            async move {
                let evs = read_events(&admin, &queue).await;
                evs.iter()
                    .any(|e| e.name() == "completed" && e.job_id() == job_id)
            }
        })
        .await;
    }

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    // Behavioral assertion: both ends emitted through the (shared) writer.
    let evs = read_events(&admin, queue).await;
    let waiting = find_one(&evs, "waiting", Some(&job_id));
    assert!(
        waiting.fields.contains_key("ts"),
        "embedded promoter must emit `waiting` through the shared writer"
    );
    let completed = find_one(&evs, "completed", Some(&job_id));
    assert_eq!(
        completed.attempt(),
        Some(1),
        "consumer hot path must emit `completed` through the same shared writer"
    );

    // Connection-count assertion: with events_enabled + delayed_enabled,
    // the engine should add at most 6 new connections (reader + dlq_writer
    // + ack_client + retry_client + shared-events + promoter-main).
    // Pre-slice-9a this was 7 (a separate promoter-events connection).
    // Allow up to 6 to pin the post-slice behavior; if a future change
    // adds a connection this fails loudly. Use `<=` so an environment
    // that pools tighter than expected is also accepted (less is more).
    assert!(
        delta <= 6,
        "engine should open at most 6 connections after slice 9a (was 7 before \
         the embedded promoter started sharing the events writer); observed \
         delta = {delta} (baseline={baseline_clients}, running={running_clients})"
    );

    let _: () = admin.quit().await.unwrap();
}
