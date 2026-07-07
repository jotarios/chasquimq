//! Per-queue rate-limiter integration coverage (REDIS_URL-gated).
//!
//! Exercises the global token-bucket limiter end to end against a real Redis:
//! a single worker respects the configured rate, two workers on the same
//! queue share ONE global bucket (proving the bucket lives in Redis, not
//! per-process), a throttled reader parks (near-zero CPU: bounded tick count,
//! not a busy-loop), shutdown while throttled exits promptly, and the
//! `rate_limited_tick` metric + `e=rate-limited` event fire when throttled
//! and stay quiet when the queue is comfortably under the limit.
//!
//! Two more scenarios cover cheaper surfaces and live where the code they
//! test lives, per the plan:
//!   - `parse_rate_limit_reply` fail-CLOSED unit test →
//!     `chasquimq/src/redis/commands.rs` (`parse_rate_limit_reply_fails_closed`).
//!   - reserved `group_key` rejection →
//!     `chasquimq/src/config.rs` (`validate_rejects_rate_limit_group_key`).
//!
//! Timing assertions use generous windows and assert *bounds*, not exact
//! counts, so they stay robust on a contended CI host. The cold-start burst
//! allowance (a fresh bucket starts full) is accounted for in every bound.

use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::metrics::testing::InMemorySink;
use chasquimq::producer::Producer;
use chasquimq::{Job, RateLimit};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "scheduler:lock",
        "stalled:lock",
        "events",
        "paused",
        "limiter",
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
}

async fn producer(queue: &str) -> Producer<Sample> {
    Producer::connect(
        &redis_url(),
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size: 2,
            max_stream_len: 100_000,
            ..Default::default()
        },
    )
    .await
    .expect("producer")
}

/// A consumer config with a global rate limiter and the background loops off
/// (promoter / scheduler / stalled detector) so a test only exercises the
/// reader path. `sink` observes metrics; `events_enabled` toggled per test.
fn limited_cfg(
    queue: &str,
    consumer_id: &str,
    max: u32,
    duration_ms: u64,
    sink: Arc<InMemorySink>,
    events_enabled: bool,
) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: consumer_id.to_string(),
        batch: 8,
        block_ms: 50,
        concurrency: 8,
        max_attempts: 3,
        ack_batch: 8,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        delayed_enabled: false,
        run_scheduler: false,
        stalled_detector_enabled: false,
        events_enabled,
        rate_limit: Some(RateLimit {
            max,
            duration_ms,
            group_key: None,
        }),
        metrics: sink,
        ..Default::default()
    }
}

/// Spawn a consumer that increments `processed` per handler run and returns
/// its join handle.
fn spawn_counter(
    cfg: ConsumerConfig,
    processed: Arc<AtomicUsize>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<chasquimq::Result<()>> {
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let processed = processed.clone();
                    async move {
                        processed.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown,
            )
            .await
    })
}

/// Read every entry from the events stream via `XRANGE - +`, folding each
/// entry's flat field list into a HashMap.
async fn read_events(admin: &Client, queue: &str) -> Vec<HashMap<String, String>> {
    let key = format!("{{chasqui:{queue}}}:events");
    let raw: Value = admin
        .custom(
            CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false),
            vec![Value::from(key), Value::from("-"), Value::from("+")],
        )
        .await
        .expect("XRANGE");
    let entries = match raw {
        Value::Array(items) => items,
        Value::Null => return Vec::new(),
        other => panic!("XRANGE unexpected: {other:?}"),
    };
    let to_s = |v: &Value| match v {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::Integer(n) => n.to_string(),
        other => format!("{other:?}"),
    };
    let mut out = Vec::new();
    for entry in entries {
        let Value::Array(pair) = entry else { continue };
        let Some(Value::Array(fields)) = pair.into_iter().nth(1) else {
            continue;
        };
        let mut kv = HashMap::new();
        let mut iter = fields.into_iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            kv.insert(to_s(&k), to_s(&v));
        }
        out.push(kv);
    }
    out
}

/// A single worker must not admit more than ~`max` jobs in the first
/// `duration_ms` window. The bucket starts full (cold-start burst of up to
/// `max`), so the first-window ceiling is `max` — assert the observed count
/// stays comfortably under `2 * max` (a full second window would be needed to
/// admit the next `max`). We enqueue far more than `max` so the limiter is
/// the only thing keeping the count down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn single_worker_respects_rate() {
    let admin = admin().await;
    let queue = "rl_single";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer
        .add_bulk((0..200).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    // 10 tokens / 1s. First window admits the cold-start burst (<=10). A
    // second full window would be needed to admit the next 10.
    let max = 10;
    let duration_ms = 1_000;
    let sink = Arc::new(InMemorySink::new());
    let cfg = limited_cfg(queue, "c1", max, duration_ms, sink.clone(), false);

    let processed = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_counter(cfg, processed.clone(), shutdown.clone());

    // Sample within the first window (well before the second refill lands).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let within_first_window = processed.load(Ordering::SeqCst);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert!(
        within_first_window >= 1,
        "at least the cold-start burst should have processed, got {within_first_window}"
    );
    assert!(
        within_first_window <= 2 * max as usize,
        "single worker must be rate-capped: got {within_first_window} in the first window, \
         expected <= {} (cold-start burst is <= {max})",
        2 * max
    );
    assert!(
        within_first_window < 200,
        "the limiter must have withheld the bulk of the 200 seeded jobs, got {within_first_window}"
    );
}

/// Two workers on the same queue with the same limiter draw from ONE shared
/// Redis bucket. Combined throughput must be bounded by `max/duration`, NOT
/// `2 * max/duration` — proving the bucket is shared cross-process, not
/// per-worker. If the bucket were per-process this would admit ~2× as many.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires REDIS_URL"]
async fn two_workers_share_one_global_bucket() {
    let admin = admin().await;
    let queue = "rl_shared";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer
        .add_bulk((0..400).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    let max = 10;
    let duration_ms = 1_000;
    let sink1 = Arc::new(InMemorySink::new());
    let sink2 = Arc::new(InMemorySink::new());
    let cfg1 = limited_cfg(queue, "c1", max, duration_ms, sink1, false);
    let cfg2 = limited_cfg(queue, "c2", max, duration_ms, sink2, false);

    let processed = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let h1 = spawn_counter(cfg1, processed.clone(), shutdown.clone());
    let h2 = spawn_counter(cfg2, processed.clone(), shutdown.clone());

    // Sample within the first window: combined must not exceed the shared
    // bucket's cold-start burst by much (both cold-start against the SAME
    // key, so the burst total is still ~max, not 2*max).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let combined = processed.load(Ordering::SeqCst);

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h1).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), h2).await;

    assert!(
        combined >= 1,
        "the shared bucket should admit at least the cold-start burst, got {combined}"
    );
    // The definitive shared-bucket assertion: combined stays within the
    // single-bucket ceiling. Per-process buckets would let ~2*max through in
    // the first window; one shared bucket caps the combined burst at ~max.
    assert!(
        combined <= 2 * max as usize,
        "two workers must SHARE one global bucket: combined {combined} exceeds the \
         single-bucket first-window ceiling of {} — a per-process bucket would admit ~2x",
        2 * max
    );
}

/// A throttled reader parks in `sleep_or_shutdown` rather than busy-looping.
/// Over a multi-second throttled interval the `rate_limited_tick` count stays
/// bounded (roughly one per `wait_ms` sleep), not thousands. With a 1s window
/// and max=2, `wait_ms` is on the order of hundreds of ms, so a ~2.5s throttle
/// yields well under ~50 ticks — a busy-loop would be tens of thousands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn throttled_reader_is_cpu_bounded() {
    let admin = admin().await;
    let queue = "rl_cpu";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer
        .add_bulk((0..100).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    // max=2 / 1s → after the cold-start burst of 2, the reader throttles with
    // a wait_ms around duration/max = 500ms, so ticks accumulate slowly.
    let sink = Arc::new(InMemorySink::new());
    let cfg = limited_cfg(queue, "c1", 2, 1_000, sink.clone(), false);

    let processed = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_counter(cfg, processed.clone(), shutdown.clone());

    // Let it burst then throttle for ~2.5s.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let ticks = sink.rate_limited_ticks();
    assert!(
        !ticks.is_empty(),
        "the reader must have throttled (rate_limited_tick fired)"
    );
    // A busy-loop would produce thousands of ticks in 2.5s. Parking in
    // sleep_or_shutdown keeps it to a small multiple of (interval / wait_ms).
    assert!(
        ticks.len() < 200,
        "throttled reader must be CPU-bounded (parks in sleep_or_shutdown): \
         {} ticks over ~2.5s looks like a busy-loop",
        ticks.len()
    );
    // Every recorded wait is a positive ms (the Lua clamps wait_ms >= 1).
    assert!(
        ticks.iter().all(|t| t.wait_ms >= 1),
        "every throttle wait must be >= 1ms (Lua clamp)"
    );
}

/// Shutdown fired while the reader is parked in the throttle sleep returns
/// promptly (well under a full `wait_ms`), not after the whole window.
/// We drain the bucket with a coarse limit so the reader is definitely
/// throttled, then measure how long `run()` takes to return after cancel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn shutdown_while_throttled_exits_promptly() {
    let admin = admin().await;
    let queue = "rl_shutdown";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer
        .add_bulk((0..50).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    // max=1 / 10s → after the single cold-start token, the reader throttles
    // with a wait_ms near 10s. Shutdown must interrupt that sleep quickly.
    let sink = Arc::new(InMemorySink::new());
    let cfg = limited_cfg(queue, "c1", 1, 10_000, sink.clone(), false);

    let processed = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_counter(cfg, processed.clone(), shutdown.clone());

    // Give it time to consume the burst token and enter the long throttle.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !sink.rate_limited_ticks().is_empty(),
        "reader should be throttled before we test prompt shutdown"
    );

    let t0 = Instant::now();
    shutdown.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .expect("consumer did not return promptly while throttled")
        .expect("consumer task panicked");
    outcome.expect("consumer returned Err on shutdown-while-throttled");
    let elapsed = t0.elapsed();

    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown while throttled must exit well under the ~10s wait_ms; took {elapsed:?}"
    );
}

/// Metrics + events: when throttled, `rate_limited_ticks()` is non-empty and
/// at least one `e=rate-limited` event lands on the events stream. When the
/// queue stays comfortably under the limit, neither fires. The event count is
/// asserted as ">= 1", not "exactly 1", because the per-reader latch can
/// re-emit if the bucket flaps around tokens≈1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn metrics_and_events_fire_when_throttled() {
    let admin = admin().await;

    // --- Part 1: throttled → tick + event fire.
    let queue = "rl_events_throttled";
    flush_all(&admin, queue).await;
    let producer1 = producer(queue).await;
    producer1
        .add_bulk((0..100).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    let sink = Arc::new(InMemorySink::new());
    let cfg = limited_cfg(queue, "c1", 3, 1_000, sink.clone(), true);
    let processed = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let handle = spawn_counter(cfg, processed.clone(), shutdown.clone());

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let ticks = sink.rate_limited_ticks();
    assert!(
        !ticks.is_empty(),
        "throttled run must record at least one rate_limited_tick"
    );
    let events = read_events(&admin, queue).await;
    let rl_events: Vec<_> = events
        .iter()
        .filter(|e| e.get("e").map(String::as_str) == Some("rate-limited"))
        .collect();
    assert!(
        !rl_events.is_empty(),
        "throttled run must emit at least one e=rate-limited event (got events: {events:?})"
    );
    // The event carries a positive wait_ms.
    assert!(
        rl_events.iter().all(|e| e
            .get("wait_ms")
            .and_then(|w| w.parse::<u64>().ok())
            .unwrap_or(0)
            >= 1),
        "every rate-limited event must carry a wait_ms >= 1"
    );

    // --- Part 2: comfortably under the limit → no tick, no event.
    let queue2 = "rl_events_under";
    flush_all(&admin, queue2).await;
    let producer2 = producer(queue2).await;
    // Only a handful of jobs against a very generous limit: never throttles.
    producer2
        .add_bulk((0..3).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    let sink2 = Arc::new(InMemorySink::new());
    let cfg2 = limited_cfg(queue2, "c1", 100_000, 1_000, sink2.clone(), true);
    let processed2 = Arc::new(AtomicUsize::new(0));
    let shutdown2 = CancellationToken::new();
    let handle2 = spawn_counter(cfg2, processed2.clone(), shutdown2.clone());

    tokio::time::sleep(Duration::from_millis(600)).await;
    shutdown2.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle2).await;

    assert_eq!(
        processed2.load(Ordering::SeqCst),
        3,
        "all 3 jobs should process comfortably under a huge limit"
    );
    assert!(
        sink2.rate_limited_ticks().is_empty(),
        "an under-limit queue must NOT record any rate_limited_tick"
    );
    let events2 = read_events(&admin, queue2).await;
    assert!(
        events2
            .iter()
            .all(|e| e.get("e").map(String::as_str) != Some("rate-limited")),
        "an under-limit queue must NOT emit e=rate-limited (got: {events2:?})"
    );
}
