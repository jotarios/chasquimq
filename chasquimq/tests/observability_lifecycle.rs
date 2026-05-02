//! Worker lifecycle event tests: success, retry-then-success,
//! retries-exhausted-into-DLQ, handler panic. Covers `JobOutcome`,
//! `RetryScheduled`, and worker-side `DlqRouted` emission.

mod common;

use chasquimq::metrics::{DlqReason, JobOutcomeKind, MetricsSink, testing::InMemorySink};
use chasquimq::producer::Producer;
use chasquimq::{Consumer, ConsumerConfig, HandlerError, Job};
use fred::interfaces::ClientLike;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use common::{Sample, admin, consumer_cfg, flush_all, producer_cfg, redis_url, wait_until};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn job_lifecycle_success_path() {
    let admin = admin().await;
    let queue = "obs_e6";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    for n in 0..5 {
        producer.add(Sample { n }).await.expect("add");
    }

    let sink = Arc::new(InMemorySink::new());
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    // Microsecond resolution means we don't strictly need a
                    // sleep here, but a tiny one keeps the duration values
                    // recognisable in the assertion below (>= 1ms in micros).
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    Ok(())
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { sink.jobs_completed() >= 5 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert_eq!(sink.jobs_completed(), 5);
    assert_eq!(sink.jobs_failed(), 0);
    assert_eq!(
        sink.dlq_count(DlqReason::RetriesExhausted),
        0,
        "no DLQ on the success path"
    );

    // Every JobOutcome is attempt 1 on the success path.
    let outcomes = sink.job_outcomes();
    assert_eq!(outcomes.len(), 5);
    for o in &outcomes {
        assert_eq!(o.kind, JobOutcomeKind::Ok);
        assert_eq!(o.attempt, 1, "first run is attempt 1");
    }

    // ReaderBatch fired at least once. We don't pin to "exactly 1" because
    // depending on timing the reader can dispatch in 1 or 2 batches.
    let batches = sink.reader_batches();
    assert!(!batches.is_empty(), "at least one ReaderBatch expected");
    assert_eq!(
        batches.iter().map(|b| b.size).sum::<u64>(),
        5,
        "summed batch sizes should equal jobs produced"
    );
    assert!(
        batches.iter().all(|b| b.reclaimed == 0),
        "no CLAIM-recovery on a happy-path test"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn job_lifecycle_retry_then_success() {
    let admin = admin().await;
    let queue = "obs_e7";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 99 }).await.expect("add");

    let sink = Arc::new(InMemorySink::new());
    // max_attempts=3 + initial_backoff_ms=20: failure on attempts 1 and 2,
    // success on attempt 3.
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3);
    // Need delayed_enabled so the retry actually gets promoted from the
    // ZSET back into the stream.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        ..cfg
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |job: Job<Sample>| async move {
                    // Job::attempt is 0-indexed pre-run: attempts 0 and 1
                    // fail, attempt 2 (i.e. the third try) succeeds.
                    if job.attempt < 2 {
                        Err(HandlerError(Box::new(std::io::Error::other(
                            "scheduled failure",
                        ))))
                    } else {
                        Ok(())
                    }
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { sink.jobs_completed() >= 1 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert_eq!(sink.jobs_completed(), 1, "exactly one Ok at attempt 3");
    assert_eq!(sink.jobs_failed(), 2, "two failures before success");
    assert_eq!(
        sink.total_retries(),
        2,
        "two RetryScheduled events, one per failure"
    );
    assert_eq!(
        sink.dlq_count(DlqReason::RetriesExhausted),
        0,
        "did not exhaust retries"
    );

    // Backoff grew (initial 20 → 40, no jitter).
    let retries = sink.retry_events();
    assert_eq!(retries.len(), 2);
    assert!(
        retries[1].backoff_ms >= retries[0].backoff_ms,
        "backoff is non-decreasing across retries: {retries:?}"
    );
    // Retry events are 1-indexed (matching JobOutcome.attempt):
    // first retry will run as attempt 2, second as attempt 3.
    assert_eq!(retries[0].attempt, 2);
    assert_eq!(retries[1].attempt, 3);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn job_lifecycle_dlq_after_retries_exhausted() {
    let admin = admin().await;
    let queue = "obs_e8";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let sink = Arc::new(InMemorySink::new());
    // max_attempts=2 with the worker's `next_attempt >= max_attempts` gate:
    // handler runs at attempt 1, fails → next_attempt=1, schedules retry.
    // Handler runs at attempt 2, fails → next_attempt=2, gate fires, DLQ.
    // So the test observes 2 failures, 1 retry, 1 DLQ.
    let cfg = ConsumerConfig {
        delayed_enabled: true,
        delayed_poll_interval_ms: 10,
        ..consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 2)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    Err(HandlerError(Box::new(std::io::Error::other("always fail"))))
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { sink.dlq_count(DlqReason::RetriesExhausted) >= 1 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert_eq!(sink.jobs_completed(), 0);
    assert_eq!(sink.jobs_failed(), 2, "two failures: attempts 1 and 2");
    assert_eq!(sink.total_retries(), 1, "one retry scheduled between them");
    assert_eq!(sink.dlq_count(DlqReason::RetriesExhausted), 1);
    let dlq_events = sink.dlq_events();
    assert_eq!(dlq_events.len(), 1);
    // DlqRouted.attempt = the 1-indexed run that just failed and exhausted
    // retries. Two failures total → second one was attempt 2 → DLQ at 2.
    assert_eq!(dlq_events[0].attempt, 2);
    // The single retry between the two failures targets attempt 2.
    let retries = sink.retry_events();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].attempt, 2);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn handler_panic_emits_panic_outcome() {
    let admin = admin().await;
    let queue = "obs_e13";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 1 }).await.expect("add");

    let sink = Arc::new(InMemorySink::new());
    // max_attempts=2 so the panicking job ends up in DLQ quickly without
    // bouncing through the delayed ZSET.
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 2);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move {
                    panic!("intentional handler panic");
                    #[allow(unreachable_code)]
                    Ok(())
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { sink.panics() >= 1 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert_eq!(
        sink.panics(),
        1,
        "handler panic surfaced as JobOutcome::Panic"
    );
    assert_eq!(sink.jobs_completed(), 0);
    let outcomes = sink.job_outcomes();
    assert!(outcomes.iter().any(|o| o.kind == JobOutcomeKind::Panic));

    let _: () = admin.quit().await.unwrap();
}
