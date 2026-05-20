//! Producer-side `max_payload_bytes` ingress cap.
//!
//! The consumer side already rejects oversize payloads on read (routing
//! them to the DLQ at `reader.rs`); these tests pin the symmetric
//! producer-side guard: an oversize encoded payload is rejected with
//! `Error::Config` *before* anything reaches Redis, on both the immediate
//! `add*` path and the repeatable-spec upsert path. A `<= cap` payload
//! still goes through, and the default cap is 1 MiB (matching the
//! consumer default).
//!
//! Redis-touching cases are `#[ignore]`'d (run with `--include-ignored`
//! and `REDIS_URL` set); the default-value assertion is a pure unit test
//! that always runs.

use chasquimq::config::ProducerConfig;
use chasquimq::producer::{Producer, stream_key};
use chasquimq::repeat::{RepeatPattern, RepeatableSpec};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};

fn redis_url() -> String {
    std::env::var("REDIS_URL").expect("REDIS_URL must be set to run integration tests")
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Blob {
    data: Vec<u8>,
}

async fn admin() -> Client {
    let cfg = Config::from_url(&redis_url()).expect("REDIS_URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

async fn flush_key(admin: &Client, key: &str) {
    let _: Value = admin
        .custom(
            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("DEL");
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

async fn zcard(admin: &Client, key: &str) -> i64 {
    match admin
        .custom::<Value, Value>(
            CustomCommand::new_static("ZCARD", ClusterHash::FirstKey, false),
            vec![Value::from(key)],
        )
        .await
        .expect("ZCARD")
    {
        Value::Integer(n) => n,
        Value::Null => 0,
        other => panic!("ZCARD unexpected: {other:?}"),
    }
}

fn cfg(queue: &str, max_payload_bytes: usize) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: 1_000,
        max_payload_bytes,
        ..Default::default()
    }
}

/// Pure unit test (no Redis): the producer default must equal the
/// consumer default — symmetric 1 MiB cap out of the box.
#[test]
fn default_max_payload_bytes_is_one_mib() {
    let p = ProducerConfig::default();
    let c = chasquimq::ConsumerConfig::default();
    assert_eq!(p.max_payload_bytes, 1_048_576);
    assert_eq!(
        p.max_payload_bytes, c.max_payload_bytes,
        "producer ingress cap must match consumer egress cap default"
    );
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_rejects_oversize_payload_with_config_error() {
    let admin = admin().await;
    let queue = "pcap_add_oversize";
    let key = stream_key(queue);
    flush_key(&admin, &key).await;

    // Cap at 1 KiB; ship 8 KiB of data so the encoded blob is well over.
    let producer: Producer<Blob> = Producer::connect(&redis_url(), cfg(queue, 1_024))
        .await
        .expect("connect producer");

    let res = producer
        .add(Blob {
            data: vec![0xAB; 8 * 1024],
        })
        .await;

    assert!(
        matches!(res, Err(chasquimq::Error::Config(_))),
        "oversize add() must be Err(Config), got {res:?}"
    );
    assert_eq!(
        xlen(&admin, &key).await,
        0,
        "rejected payload must never reach Redis"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_accepts_payload_at_or_under_cap() {
    let admin = admin().await;
    let queue = "pcap_add_under";
    let key = stream_key(queue);
    flush_key(&admin, &key).await;

    // Default 1 MiB cap; a ~4 KiB payload is comfortably under.
    let producer: Producer<Blob> = Producer::connect(&redis_url(), cfg(queue, 1_048_576))
        .await
        .expect("connect producer");

    producer
        .add(Blob {
            data: vec![0x01; 4 * 1024],
        })
        .await
        .expect("under-cap add must succeed");

    assert_eq!(xlen(&admin, &key).await, 1, "under-cap payload persists");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn upsert_repeatable_rejects_oversize_spec_payload() {
    let admin = admin().await;
    let queue = "pcap_repeat_oversize";
    let repeat_zset = format!("{{chasqui:{queue}}}:repeat");
    flush_key(&admin, &repeat_zset).await;

    let producer: Producer<Blob> = Producer::connect(&redis_url(), cfg(queue, 1_024))
        .await
        .expect("connect producer");

    let spec = RepeatableSpec::new(
        "oversize-job",
        RepeatPattern::Every {
            interval_ms: 60_000,
        },
        Blob {
            data: vec![0xCD; 8 * 1024],
        },
    );

    let res = producer.upsert_repeatable(spec).await;

    assert!(
        matches!(res, Err(chasquimq::Error::Config(_))),
        "oversize repeatable spec must be Err(Config), got {res:?}"
    );
    assert_eq!(
        zcard(&admin, &repeat_zset).await,
        0,
        "rejected spec must never reach the repeat index"
    );

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn upsert_repeatable_accepts_under_cap_spec() {
    let admin = admin().await;
    let queue = "pcap_repeat_under";
    let repeat_zset = format!("{{chasqui:{queue}}}:repeat");
    flush_key(&admin, &repeat_zset).await;

    let producer: Producer<Blob> = Producer::connect(&redis_url(), cfg(queue, 1_048_576))
        .await
        .expect("connect producer");

    let spec = RepeatableSpec::new(
        "small-job",
        RepeatPattern::Every {
            interval_ms: 60_000,
        },
        Blob {
            data: vec![0x02; 4 * 1024],
        },
    );

    producer
        .upsert_repeatable(spec)
        .await
        .expect("under-cap repeatable spec must succeed");

    assert_eq!(
        zcard(&admin, &repeat_zset).await,
        1,
        "under-cap spec persists in the repeat index"
    );

    let _: () = admin.quit().await.unwrap();
}
