//! Pause / resume integration coverage (REDIS_URL-gated).
//!
//! Exercises the engine primitive end to end against a real Redis:
//! in-process pause stops dispatch while producers keep enqueueing,
//! in-flight jobs drain on pause, resume drains the backlog, shutdown
//! while paused still drains cleanly, and the cross-process Redis key is
//! honoured both by a running consumer and durably across a restart.

use chasquimq::Job;
use chasquimq::config::{ConsumerConfig, ProducerConfig};
use chasquimq::consumer::Consumer;
use chasquimq::producer::Producer;
use fred::clients::Client;
use fred::interfaces::{ClientLike, KeysInterface};
use fred::prelude::Config;
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
    for suffix in [
        "stream",
        "dlq",
        "delayed",
        "promoter:lock",
        "scheduler:lock",
        "events",
        "paused",
    ] {
        let key = format!("{{chasqui:{queue}}}:{suffix}");
        let _: i64 = admin.del(key).await.expect("DEL");
    }
}

fn cfg(queue: &str) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: "c1".to_string(),
        batch: 16,
        block_ms: 50,
        claim_min_idle_ms: 30_000,
        concurrency: 8,
        max_attempts: 3,
        ack_batch: 16,
        ack_idle_ms: 5,
        shutdown_deadline_secs: 5,
        delayed_enabled: false,
        run_scheduler: false,
        events_enabled: false,
        // Tight poll so the cross-process key is observed quickly in tests.
        pause_poll_ms: 50,
        ..Default::default()
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

/// In-process pause halts dispatch; producers keep enqueueing freely; the
/// backlog grows; resume drains everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn in_proc_pause_stops_dispatch_then_resume_drains() {
    let admin = admin().await;
    let queue = "pause_inproc";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer
        .add_bulk((0..20).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    let processed = Arc::new(AtomicUsize::new(0));
    let processed_h = processed.clone();

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg(queue));
    let pause = consumer.pause_control();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();

    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let processed = processed_h.clone();
                    async move {
                        processed.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown_c,
            )
            .await
    });

    // Let the seed drain, then pause.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let before_pause = processed.load(Ordering::SeqCst);
    assert!(before_pause >= 20, "seed should drain, got {before_pause}");

    pause.pause();
    assert!(pause.is_paused());
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Producer keeps enqueueing while paused — must succeed.
    producer
        .add_bulk((100..130).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("produce while paused must succeed");
    tokio::time::sleep(Duration::from_millis(400)).await;

    let while_paused = processed.load(Ordering::SeqCst);
    assert_eq!(
        while_paused, before_pause,
        "no jobs may be dispatched while paused (before={before_pause} while={while_paused})"
    );

    // Resume → the 30 backlog jobs drain.
    pause.resume();
    assert!(!pause.is_paused());
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after_resume = processed.load(Ordering::SeqCst);
    assert!(
        after_resume >= before_pause + 30,
        "resume must drain the backlog enqueued while paused (after={after_resume}, expected >= {})",
        before_pause + 30
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

/// A job already handed to a worker when pause fires runs to completion —
/// pause stops *future* reads, it does not truncate in-flight work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn in_flight_job_completes_on_pause() {
    let admin = admin().await;
    let queue = "pause_inflight";
    flush_all(&admin, queue).await;

    let producer = producer(queue).await;
    producer.add(Sample { n: 1 }).await.expect("seed one");

    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let started_h = started.clone();
    let finished_h = finished.clone();

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg(queue));
    let pause = consumer.pause_control();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();

    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let started = started_h.clone();
                    let finished = finished_h.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        finished.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown_c,
            )
            .await
    });

    // Wait for the handler to start, then pause mid-flight.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(started.load(Ordering::SeqCst), 1, "handler should be running");
    assert_eq!(finished.load(Ordering::SeqCst), 0, "handler not done yet");
    pause.pause();

    // The in-flight handler must still finish despite the pause.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "in-flight job must complete; pause does not truncate it"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

/// Shutdown signalled while the reader is parked in the pause gate still
/// returns cleanly (Ok) and drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn shutdown_while_paused_drains_cleanly() {
    let admin = admin().await;
    let queue = "pause_shutdown";
    flush_all(&admin, queue).await;

    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg(queue));
    let pause = consumer.pause_control();
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();

    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| async move { Ok(chasquimq::Bytes::new()) },
                shutdown_c,
            )
            .await
    });

    pause.pause();
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("consumer join timed out while paused")
        .expect("consumer task panicked");
    outcome.expect("consumer returned Err on shutdown-while-paused");
}

/// The cross-process `{chasqui:<queue>}:paused` key pauses a running
/// consumer and, set before startup, parks a fresh consumer before its
/// first dispatch (durable across restart).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn redis_key_pause_is_observed_and_durable() {
    let admin = admin().await;
    let queue = "pause_rediskey";
    flush_all(&admin, queue).await;
    let paused_key = format!("{{chasqui:{queue}}}:paused");

    let producer = producer(queue).await;

    // --- Part 1: key set before the consumer starts → parks before any dispatch.
    let _: () = admin.set(&paused_key, "1", None, None, false).await.expect("SET");
    producer
        .add_bulk((0..10).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("seed");

    let processed = Arc::new(AtomicUsize::new(0));
    let processed_h = processed.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg(queue));
    let shutdown = CancellationToken::new();
    let shutdown_c = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let processed = processed_h.clone();
                    async move {
                        processed.fetch_add(1, Ordering::SeqCst);
                        Ok(chasquimq::Bytes::new())
                    }
                },
                shutdown_c,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        processed.load(Ordering::SeqCst),
        0,
        "consumer started against a pre-paused key must not dispatch"
    );

    // --- Part 2: delete the key → consumer resumes within pause_poll_ms.
    let _: i64 = admin.del(&paused_key).await.expect("DEL");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        processed.load(Ordering::SeqCst) >= 10,
        "deleting the pause key must resume dispatch (got {})",
        processed.load(Ordering::SeqCst)
    );

    // --- Part 3: set the key again on the running consumer → parks again.
    let _: () = admin.set(&paused_key, "1", None, None, false).await.expect("SET");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let baseline = processed.load(Ordering::SeqCst);
    producer
        .add_bulk((200..220).map(|n| Sample { n }).collect::<Vec<_>>())
        .await
        .expect("produce while key-paused");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        processed.load(Ordering::SeqCst),
        baseline,
        "running consumer must re-park when the pause key reappears"
    );

    let _: i64 = admin.del(&paused_key).await.expect("DEL");
    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}
