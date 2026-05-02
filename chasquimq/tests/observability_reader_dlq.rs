//! Reader-side DLQ event tests: malformed entries, oversize payloads,
//! decode failures, and retries-exhausted-on-arrival. These paths route
//! to DLQ before the handler runs, so they emit `DlqRouted` (with
//! `attempt: 0` for the first three) and **no** `JobOutcome`.

mod common;

use chasquimq::metrics::{MetricsSink, testing::InMemorySink};
use chasquimq::producer::Producer;
use chasquimq::{ConsumerConfig, Consumer, Job};
use fred::interfaces::ClientLike;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use common::{
    Sample, admin, consumer_cfg, flush_all, producer_cfg, raw_xadd, redis_url, wait_until,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn reader_dlq_malformed() {
    let admin = admin().await;
    let queue = "obs_e9";
    flush_all(&admin, queue).await;

    let stream_key = format!("{{chasqui:{queue}}}:stream");
    // Bypass the producer's encoding and write an entry with the wrong
    // field name. The reader's parser flags this as malformed and routes
    // straight to DLQ — handler never sees it.
    raw_xadd(&admin, &stream_key, "wrong", b"whatever".to_vec()).await;

    let sink = Arc::new(InMemorySink::new());
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(()) },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { !sink.dlq_events().is_empty() }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let dlq = sink.dlq_events();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].reason.as_str(), "malformed");
    assert_eq!(dlq[0].attempt, 0, "reader-side DLQ uses attempt=0");
    assert_eq!(sink.jobs_completed(), 0, "handler never ran");
    assert_eq!(sink.jobs_failed(), 0, "handler never ran");
    // ReaderBatch fires on every non-empty XREADGROUP, even when every entry
    // gets DLQ-routed. Validates the documented invariant.
    let batches = sink.reader_batches();
    assert!(
        batches.iter().any(|b| b.size >= 1),
        "ReaderBatch should fire even when all entries get DLQ-routed: {batches:?}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn reader_dlq_oversize_payload() {
    let admin = admin().await;
    let queue = "obs_e10";
    flush_all(&admin, queue).await;

    // Build a payload that's well-formed msgpack but exceeds the (lowered)
    // max_payload_bytes the consumer is configured with.
    let producer: Producer<Sample> =
        Producer::connect(&redis_url(), producer_cfg(queue)).await.unwrap();
    producer.add(Sample { n: 1 }).await.expect("add");

    let sink = Arc::new(InMemorySink::new());
    let cfg = ConsumerConfig {
        // Aggressive cap: any encoded Sample{n} is more than 1 byte.
        max_payload_bytes: 1,
        ..consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3)
    };
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(()) },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { !sink.dlq_events().is_empty() }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let dlq = sink.dlq_events();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].reason.as_str(), "oversize_payload");
    assert_eq!(dlq[0].attempt, 0);
    assert_eq!(sink.jobs_completed(), 0);
    assert!(
        sink.reader_batches().iter().any(|b| b.size >= 1),
        "ReaderBatch should fire for the oversize entry"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn reader_dlq_decode_failed() {
    let admin = admin().await;
    let queue = "obs_e11";
    flush_all(&admin, queue).await;

    // Correct field name (`d`), but the bytes aren't valid msgpack for a
    // `Job<Sample>`. The reader passes the structural check, fails decode.
    let stream_key = format!("{{chasqui:{queue}}}:stream");
    raw_xadd(&admin, &stream_key, "d", b"this is not msgpack".to_vec()).await;

    let sink = Arc::new(InMemorySink::new());
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(()) },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { !sink.dlq_events().is_empty() }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let dlq = sink.dlq_events();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].reason.as_str(), "decode_failed");
    assert_eq!(dlq[0].attempt, 0);
    assert!(
        sink.reader_batches().iter().any(|b| b.size >= 1),
        "ReaderBatch should fire for the undecodable entry"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn reader_dlq_retries_exhausted_on_arrival() {
    let admin = admin().await;
    let queue = "obs_e12";
    flush_all(&admin, queue).await;

    // Build a Job whose `attempt` is already at max_attempts — simulates
    // the CLAIM-recovery path arriving at a new consumer with a
    // delivery_count that exceeds the configured budget.
    let mut job = Job::new(Sample { n: 7 });
    job.attempt = 5;
    let bytes = rmp_serde::to_vec(&job).expect("encode");
    let stream_key = format!("{{chasqui:{queue}}}:stream");
    raw_xadd(&admin, &stream_key, "d", bytes).await;

    let sink = Arc::new(InMemorySink::new());
    // max_attempts=3, so an arrival at attempt 5 is past budget.
    let cfg = consumer_cfg(queue, sink.clone() as Arc<dyn MetricsSink>, 3);
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);

    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job: Job<Sample>| async move { Ok(()) },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let sink = sink.clone();
        async move { !sink.dlq_events().is_empty() }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    let dlq = sink.dlq_events();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].reason.as_str(), "retries_exhausted");
    assert_eq!(
        dlq[0].attempt, 5,
        "carries the prior attempt count, not 0"
    );
    assert_eq!(sink.jobs_completed(), 0, "handler never ran");
    assert!(
        sink.reader_batches().iter().any(|b| b.size >= 1),
        "ReaderBatch should fire for the over-budget arrival"
    );

    let _: () = admin.quit().await.unwrap();
}
