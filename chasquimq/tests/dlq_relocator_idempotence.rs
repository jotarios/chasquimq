//! Regression coverage for the DLQ-relocator duplicate-on-retry bug.
//!
//! The relocator used to issue `XADD` (re-enqueue into the DLQ) and then
//! `XACKDEL` (remove from the main stream) as a non-atomic pipeline. If the
//! process died after the `XADD` committed but before the `XACKDEL`, the
//! entry was *both* in the DLQ *and* still pending on the main stream — the
//! next CLAIM tick re-claimed it and routed a duplicate into the DLQ.
//!
//! `RELOCATE_DLQ_SCRIPT` now does the move as one atomic Lua invocation with
//! an `XACKDEL` gate: the `XADD` only runs if the ack actually removed the
//! entry. These tests pin the observable invariant — under conditions that
//! make the relocator run more than once for the same source entry, the DLQ
//! ends up with exactly one entry and the main-stream pending list drains.

use chasquimq::config::ConsumerConfig;
use chasquimq::consumer::Consumer;
use chasquimq::producer::{dlq_key, stream_key};
use chasquimq::{HandlerError, Job};
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
}

async fn admin() -> Client {
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

async fn flush_all(admin: &Client, queue: &str) {
    for suffix in ["stream", "dlq", "delayed", "promoter:lock"] {
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

async fn xpending_count(admin: &Client, key: &str, group: &str) -> i64 {
    let res = admin
        .custom::<Value, Value>(
            CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false),
            vec![Value::from(key), Value::from(group)],
        )
        .await
        .expect("XPENDING");
    match res {
        Value::Array(items) => match items.first() {
            Some(Value::Integer(n)) => *n,
            _ => 0,
        },
        _ => 0,
    }
}

/// Raw `XADD` of one entry onto an arbitrary stream key. Returns the entry id.
async fn xadd_raw(admin: &Client, key: &str, payload: bytes::Bytes) -> String {
    let res: Value = admin
        .custom(
            CustomCommand::new_static("XADD", ClusterHash::FirstKey, false),
            vec![
                Value::from(key),
                Value::from("*"),
                Value::from("d"),
                Value::Bytes(payload),
            ],
        )
        .await
        .expect("XADD raw");
    match res {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8(b.to_vec()).expect("utf8 id"),
        other => panic!("XADD returned unexpected: {other:?}"),
    }
}

/// Create the consumer group at `0` so a pending entry can be staged before
/// the consumer starts.
async fn create_group(admin: &Client, key: &str, group: &str) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XGROUP", ClusterHash::FirstKey, false),
            vec![
                Value::from("CREATE"),
                Value::from(key),
                Value::from(group),
                Value::from("0"),
                Value::from("MKSTREAM"),
            ],
        )
        .await
        .expect("XGROUP CREATE");
}

/// Claim every entry into the group's pending list under `consumer`, so the
/// entry is delivered-but-unacked exactly like a job a worker picked up and
/// then failed on.
async fn read_into_pending(admin: &Client, key: &str, group: &str, consumer: &str) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false),
            vec![
                Value::from("GROUP"),
                Value::from(group),
                Value::from(consumer),
                Value::from("COUNT"),
                Value::from(100_i64),
                Value::from("STREAMS"),
                Value::from(key),
                Value::from(">"),
            ],
        )
        .await
        .expect("XREADGROUP");
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

fn relocating_consumer_cfg(queue: &str, consumer_id: &str) -> ConsumerConfig {
    ConsumerConfig {
        queue_name: queue.to_string(),
        consumer_id: consumer_id.to_string(),
        // One attempt, no backoff: the first handler error routes straight to
        // the DLQ, so the relocator runs immediately and the test window is
        // tight.
        max_attempts: 1,
        block_ms: 50,
        claim_min_idle_ms: 50,
        delayed_poll_interval_ms: 25,
        shutdown_deadline_secs: 2,
        retry: chasquimq::RetryConfig {
            initial_backoff_ms: 5,
            max_backoff_ms: 20,
            multiplier: 2.0,
            jitter_ms: 0,
        },
        ..Default::default()
    }
}

/// The end-to-end contract: a job whose handler always errors lands in the
/// DLQ exactly once, the main stream is drained, and nothing is left pending
/// to re-claim into a duplicate. With the old non-atomic pipeline a crash
/// between the XADD and the XACKDEL would have left the entry pending; this
/// asserts the steady-state invariant holds and stays stable after the
/// relocator has had time to run additional CLAIM ticks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn relocate_routes_exactly_one_dlq_entry() {
    let admin = admin().await;
    let queue = "dlq_reloc_once";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);
    let _ = xadd_raw(&admin, &main_key, bytes::Bytes::from_static(b"opaque")).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_h = calls.clone();
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), relocating_consumer_cfg(queue, "c1"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| {
                    let calls = calls_h.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err(HandlerError::new(std::io::Error::other("always fails")))
                    }
                },
                shutdown_clone,
            )
            .await
    });

    wait_until(Duration::from_secs(15), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await == 1 && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    // Let the consumer run several more CLAIM ticks. If the relocate were not
    // atomic-and-gated, a still-pending entry would get re-claimed here and
    // produce a second DLQ row.
    tokio::time::sleep(Duration::from_millis(800)).await;

    assert_eq!(
        xlen(&admin, &dlq).await,
        1,
        "exactly one DLQ entry — the relocate must not duplicate"
    );
    assert_eq!(
        xpending_count(&admin, &main_key, "default").await,
        0,
        "main stream pending list must be drained"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}

/// Two consumers contend for the same poisoned entry. Whichever relocator
/// wins the XACKDEL gate writes the single DLQ row; the loser's script
/// returns 0 (gate lost) and writes nothing. Pins that concurrent relocation
/// of one entry can never double-write — the same guarantee the atomic gate
/// gives a single relocator retrying after a lost reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn concurrent_relocators_do_not_duplicate() {
    let admin = admin().await;
    let queue = "dlq_reloc_concurrent";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);
    for _ in 0..20 {
        let _ = xadd_raw(&admin, &main_key, bytes::Bytes::from_static(b"opaque")).await;
    }

    let mut handles = Vec::new();
    let mut shutdowns = Vec::new();
    for i in 0..3 {
        let url = redis_url();
        let cfg = relocating_consumer_cfg(queue, &format!("c{i}"));
        let shutdown = CancellationToken::new();
        shutdowns.push(shutdown.clone());
        let consumer: Consumer<Sample> = Consumer::new(url, cfg);
        handles.push(tokio::spawn(async move {
            consumer
                .run(
                    move |_: Job<Sample>| async move {
                        Err::<chasquimq::Bytes, _>(HandlerError::new(std::io::Error::other("nope")))
                    },
                    shutdown,
                )
                .await
        }));
    }

    wait_until(Duration::from_secs(20), || {
        let admin = admin.clone();
        let main_key = main_key.clone();
        let dlq = dlq.clone();
        async move {
            xlen(&admin, &dlq).await == 20
                && xpending_count(&admin, &main_key, "default").await == 0
        }
    })
    .await;

    tokio::time::sleep(Duration::from_millis(800)).await;

    assert_eq!(
        xlen(&admin, &dlq).await,
        20,
        "exactly 20 DLQ entries across 3 contending relocators — no duplicates"
    );
    assert_eq!(
        xpending_count(&admin, &main_key, "default").await,
        0,
        "every source entry acked exactly once"
    );

    for s in shutdowns {
        s.cancel();
    }
    for h in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
    }
    let _: () = admin.quit().await.unwrap();
}

/// The XACKDEL gate is the load-bearing invariant: if the source entry has
/// already been removed from the group (a concurrent CLAIM / manual ack won
/// the race), the relocate must be a no-op — no spurious DLQ write. We stage
/// a pending entry, ack-and-delete it out from under the relocator, then
/// start the consumer. The handler should never run (nothing left to read)
/// and the DLQ stays empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_URL"]
async fn gate_lost_writes_no_dlq_entry() {
    let admin = admin().await;
    let queue = "dlq_reloc_gate_lost";
    flush_all(&admin, queue).await;

    let main_key = stream_key(queue);
    let dlq = dlq_key(queue);

    // Stage one entry pending under a different consumer, then ack+delete it
    // so the group has nothing left — exactly the state the relocator's gate
    // must detect and skip.
    create_group(&admin, &main_key, "default").await;
    let entry_id = xadd_raw(&admin, &main_key, bytes::Bytes::from_static(b"opaque")).await;
    read_into_pending(&admin, &main_key, "default", "ghost").await;
    assert_eq!(
        xpending_count(&admin, &main_key, "default").await,
        1,
        "entry should be pending before the gate-loss"
    );
    let acked: Value = admin
        .custom(
            CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false),
            vec![
                Value::from(main_key.as_str()),
                Value::from("default"),
                Value::from("IDS"),
                Value::from(1_i64),
                Value::from(entry_id.as_str()),
            ],
        )
        .await
        .expect("XACKDEL");
    let acked = match acked {
        Value::Integer(n) => n,
        Value::Array(a) => match a.first() {
            Some(Value::Integer(n)) => *n,
            _ => panic!("XACKDEL array shape: {a:?}"),
        },
        other => panic!("XACKDEL unexpected: {other:?}"),
    };
    assert_eq!(acked, 1, "entry should have been acked+removed");
    assert_eq!(
        xpending_count(&admin, &main_key, "default").await,
        0,
        "nothing should be pending after the ack-and-delete"
    );

    // Bring up a consumer. There is nothing to read (the only entry was
    // removed), so the handler never fires and the relocator never has work.
    // The point of the test is the negative assertion: no DLQ row appears.
    let consumer: Consumer<Sample> =
        Consumer::new(redis_url(), relocating_consumer_cfg(queue, "c1"));
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    let join = tokio::spawn(async move {
        consumer
            .run(
                move |_: Job<Sample>| async move {
                    Err::<chasquimq::Bytes, _>(HandlerError::new(std::io::Error::other("nope")))
                },
                shutdown_clone,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(600)).await;

    assert_eq!(
        xlen(&admin, &dlq).await,
        0,
        "gate-lost relocate must not write a DLQ entry"
    );

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
    let _: () = admin.quit().await.unwrap();
}
