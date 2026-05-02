//! Promoter event emission tests: `PromoterTick` and `LockOutcome`,
//! plus the boundary check that the consumer's embedded promoter forwards
//! `ConsumerConfig::metrics` correctly.
//!
//! Lifecycle / reader-DLQ tests live in sibling test binaries.

mod common;

use chasquimq::config::PromoterConfig;
use chasquimq::metrics::{MetricsSink, testing::InMemorySink};
use chasquimq::producer::{Producer, delayed_key};
use chasquimq::{ConsumerConfig, Consumer, Promoter};
use fred::interfaces::ClientLike;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use common::{
    Sample, admin, flush_all, producer_cfg, promoter_cfg_with_sink, redis_url, wait_until, zcard,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn promoter_emits_promoted_depth_and_lag() {
    let admin = admin().await;
    let queue = "obs_e1";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    // Schedule 10 jobs for 100ms in the future. The promoter is started
    // *before* anything is due, so we expect to observe at least one tick
    // with `depth >= 1` and `oldest_pending_lag_ms == 0` (still in the future), followed
    // by a tick with `promoted == 10`.
    for n in 0..10 {
        producer
            .add_in(Duration::from_millis(100), Sample { n })
            .await
            .expect("add_in");
    }

    let sink = Arc::new(InMemorySink::new());
    let shutdown = CancellationToken::new();
    let promoter = Promoter::new(
        redis_url(),
        promoter_cfg_with_sink(queue, "p1", sink.clone()),
    );
    let handle = tokio::spawn(promoter.run(shutdown.clone()));

    let dkey = delayed_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dkey = dkey.clone();
        async move { zcard(&admin, &dkey).await == 0 }
    })
    .await;

    // Give the promoter one more poll interval to emit the post-drain tick.
    tokio::time::sleep(Duration::from_millis(150)).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let total = sink.promoted_total();
    assert_eq!(
        total, 10,
        "exactly 10 jobs should have been promoted; sink saw {total}"
    );

    assert!(
        sink.acquired_count() >= 1,
        "promoter should have acquired the lock at least once"
    );

    // We scheduled 100ms out, so at the moment of promotion lag must be small
    // but non-negative. The post-drain tick reports oldest_pending_lag_ms == 0 (depth == 0).
    // Ensure no tick reports a wildly negative or absurd lag (saturated to 0
    // by the script when oldest is in the future).
    let ticks = sink.ticks();
    assert!(
        !ticks.is_empty(),
        "at least one tick should have fired; got 0"
    );
    for t in &ticks {
        // oldest_pending_lag_ms is u64 so non-negative by construction; sanity-check upper bound.
        assert!(
            t.oldest_pending_lag_ms < 60_000,
            "lag should be small for fast promotion; got {} on tick {:?}",
            t.oldest_pending_lag_ms,
            t
        );
    }

    // Final tick should observe depth == 0 (drain complete).
    let last_depth = sink.last_depth().expect("at least one tick");
    assert_eq!(
        last_depth, 0,
        "final tick should observe drained depth; got {last_depth}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn promoter_lag_observed_when_backlog_remains() {
    // The script reports `oldest_pending_lag_ms` for the oldest entry STILL
    // PENDING after the tick promotes its batch. To observe non-zero lag,
    // we need a backlog larger than `promote_batch`: enqueue 3 past-due
    // jobs, set promote_batch=1, and the promoter will need three ticks
    // to drain — on ticks 1 and 2, exactly 1 was promoted and 2 (then 1)
    // remain past-due, so the metric must be > 0.
    let admin = admin().await;
    let queue = "obs_e2";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");

    for n in 0..3 {
        producer
            .add_in(Duration::from_millis(20), Sample { n })
            .await
            .expect("add_in");
    }
    // Wait long enough that all three are well past-due.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sink = Arc::new(InMemorySink::new());
    let shutdown = CancellationToken::new();
    let cfg = PromoterConfig {
        queue_name: queue.to_string(),
        poll_interval_ms: 50,
        promote_batch: 1, // force one-at-a-time so a backlog persists
        max_stream_len: 100_000,
        lock_ttl_secs: 5,
        holder_id: "p1".to_string(),
        metrics: sink.clone() as Arc<dyn MetricsSink>,
    };
    let promoter = Promoter::new(redis_url(), cfg);
    let handle = tokio::spawn(promoter.run(shutdown.clone()));

    let dkey = delayed_key(queue);
    wait_until(Duration::from_secs(5), || {
        let admin = admin.clone();
        let dkey = dkey.clone();
        async move { zcard(&admin, &dkey).await == 0 }
    })
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    assert_eq!(sink.promoted_total(), 3);

    // At least one tick must have observed pending past-due lag > 0.
    // Specifically the first tick: promotes 1, leaves 2 past-due → lag is
    // the age of the oldest remaining. With 200ms of pre-promoter wait
    // this should be at least ~150ms.
    let max_lag = sink.max_oldest_pending_lag_ms();
    assert!(
        max_lag >= 100,
        "expected at least one tick to observe oldest_pending_lag_ms >= 100ms; got max {max_lag}"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn empty_zset_tick_emits_zeros() {
    // Promoter started against an empty queue: the tick must still fire
    // (acquire-and-promote loop runs once), and report all zeros.
    let admin = admin().await;
    let queue = "obs_e3";
    flush_all(&admin, queue).await;

    let sink = Arc::new(InMemorySink::new());
    let shutdown = CancellationToken::new();
    let promoter = Promoter::new(
        redis_url(),
        promoter_cfg_with_sink(queue, "p1", sink.clone()),
    );
    let handle = tokio::spawn(promoter.run(shutdown.clone()));

    // Allow ~3 poll intervals worth of empty ticks.
    tokio::time::sleep(Duration::from_millis(200)).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;

    let ticks = sink.ticks();
    assert!(
        !ticks.is_empty(),
        "promoter should fire ticks even when ZSET is empty"
    );
    for t in &ticks {
        assert_eq!(t.promoted, 0);
        assert_eq!(t.depth, 0);
        assert_eq!(t.oldest_pending_lag_ms, 0);
    }

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn lock_outcome_emits_only_on_transition() {
    // Two promoters racing for the same queue. Only the leader emits
    // `Acquired`; the follower emits exactly one `Held` (on its first
    // failed acquisition) and not one Held per poll-interval.
    let admin = admin().await;
    let queue = "obs_e4";
    flush_all(&admin, queue).await;

    let sink_a = Arc::new(InMemorySink::new());
    let sink_b = Arc::new(InMemorySink::new());

    // Start A first; let it grab the lock.
    let shutdown_a = CancellationToken::new();
    let promoter_a = Promoter::new(
        redis_url(),
        promoter_cfg_with_sink(queue, "pA", sink_a.clone()),
    );
    let h_a = tokio::spawn(promoter_a.run(shutdown_a.clone()));

    // Wait until A has the lock.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Start B; it should observe Held repeatedly but emit only once.
    let shutdown_b = CancellationToken::new();
    let promoter_b = Promoter::new(
        redis_url(),
        promoter_cfg_with_sink(queue, "pB", sink_b.clone()),
    );
    let h_b = tokio::spawn(promoter_b.run(shutdown_b.clone()));

    // Let B poll for ~10 intervals (50ms each = 500ms).
    tokio::time::sleep(Duration::from_millis(500)).await;

    shutdown_a.cancel();
    shutdown_b.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), h_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), h_b).await;

    // A: exactly one Acquired (transition None -> Acquired on first tick).
    assert_eq!(
        sink_a.acquired_count(),
        1,
        "leader should emit Acquired exactly once, not per-tick; got {}",
        sink_a.acquired_count()
    );
    assert_eq!(sink_a.held_count(), 0, "leader never observes Held");

    // B: exactly one Held. If the per-tick emission was still in place we'd
    // see ~10 here.
    assert_eq!(
        sink_b.acquired_count(),
        0,
        "follower never acquires while leader holds"
    );
    assert_eq!(
        sink_b.held_count(),
        1,
        "follower should emit Held exactly once (transition only); got {} \
         (>1 means per-tick emission regressed)",
        sink_b.held_count()
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn consumer_embedded_promoter_forwards_metrics() {
    // The consumer's internal spawn_promoter must forward `cfg.metrics` so
    // users who never construct a standalone Promoter still get observability.
    use std::sync::atomic::{AtomicUsize, Ordering};

    let admin = admin().await;
    let queue = "obs_e5";
    flush_all(&admin, queue).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), producer_cfg(queue))
        .await
        .expect("connect producer");
    producer
        .add_in(Duration::from_millis(50), Sample { n: 1 })
        .await
        .expect("add_in");

    let sink = Arc::new(InMemorySink::new());
    let cfg = ConsumerConfig {
        queue_name: queue.to_string(),
        group: "default".to_string(),
        consumer_id: "c1".to_string(),
        block_ms: 100,
        delayed_enabled: true,
        delayed_poll_interval_ms: 50,
        metrics: sink.clone() as Arc<dyn MetricsSink>,
        ..Default::default()
    };

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_h = counter.clone();
    let consumer: Consumer<Sample> = Consumer::new(redis_url(), cfg);
    let shutdown = CancellationToken::new();
    let shutdown_h = shutdown.clone();
    let handle = tokio::spawn(async move {
        consumer
            .run(
                move |_job| {
                    let counter = counter_h.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
                shutdown_h,
            )
            .await
    });

    wait_until(Duration::from_secs(5), || {
        let counter = counter.clone();
        async move { counter.load(Ordering::SeqCst) == 1 }
    })
    .await;

    // Give the embedded promoter time to emit its tick post-promotion.
    tokio::time::sleep(Duration::from_millis(150)).await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;

    assert_eq!(
        sink.promoted_total(),
        1,
        "embedded promoter must forward metrics from ConsumerConfig::metrics; got {}",
        sink.promoted_total()
    );
    assert!(
        sink.acquired_count() >= 1,
        "embedded promoter should have emitted at least one Acquired transition"
    );

    let _: () = admin.quit().await.unwrap();
}
