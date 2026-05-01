use chasquimq::config::ProducerConfig;
use chasquimq::producer::{Producer, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};

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

async fn flush_stream(admin: &Client, key: &str) {
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
        other => panic!("XLEN unexpected: {other:?}"),
    }
}

fn cfg(queue: &str, max_len: u64) -> ProducerConfig {
    ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 2,
        max_stream_len: max_len,
    }
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_then_xlen_is_one() {
    let admin = admin().await;
    let queue = "p_add_one";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), cfg(queue, 1_000))
        .await
        .expect("connect producer");
    producer.add(Sample { n: 1, s: "x".into() }).await.expect("add");

    assert_eq!(xlen(&admin, &key).await, 1);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_xlen_matches_n() {
    let admin = admin().await;
    let queue = "p_add_bulk";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), cfg(queue, 100_000))
        .await
        .expect("connect producer");
    let payloads: Vec<Sample> = (0..1_000).map(|i| Sample { n: i, s: format!("x{i}") }).collect();
    let ids = producer.add_bulk(payloads).await.expect("add_bulk");
    assert_eq!(ids.len(), 1_000);

    assert_eq!(xlen(&admin, &key).await, 1_000);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_empty_is_noop() {
    let admin = admin().await;
    let queue = "p_add_empty";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), cfg(queue, 1_000))
        .await
        .expect("connect producer");
    let ids = producer.add_bulk(Vec::new()).await.expect("add_bulk empty");
    assert!(ids.is_empty());

    assert_eq!(xlen(&admin, &key).await, 0);

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn idempotent_retry_dedupes() {
    let admin = admin().await;
    let queue = "p_idmp";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), cfg(queue, 1_000))
        .await
        .expect("connect producer");
    let id = "stable-id-1".to_string();
    let id1 = producer
        .add_with_id(id.clone(), Sample { n: 1, s: "x".into() })
        .await
        .expect("add_with_id 1");
    let id2 = producer
        .add_with_id(id.clone(), Sample { n: 1, s: "x".into() })
        .await
        .expect("add_with_id 2");
    assert_eq!(id1, id2);
    assert_eq!(xlen(&admin, &key).await, 1, "IDMP must dedupe identical iid");

    let _: () = admin.quit().await.unwrap();
}

struct AlwaysFailEncode;

impl Serialize for AlwaysFailEncode {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("encode failure for test"))
    }
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn add_bulk_partial_encode_fails_atomically() {
    let admin = admin().await;
    let queue = "p_encode_fail";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<AlwaysFailEncode> =
        Producer::connect(&redis_url(), cfg(queue, 1_000))
            .await
            .expect("connect producer");

    let res = producer
        .add_bulk(vec![AlwaysFailEncode, AlwaysFailEncode])
        .await;
    assert!(matches!(res, Err(chasquimq::Error::Encode(_))), "got {res:?}");
    assert_eq!(xlen(&admin, &key).await, 0, "no XADD should reach Redis");

    let _: () = admin.quit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn maxlen_trim_caps_stream_growth() {
    let admin = admin().await;
    let queue = "p_maxlen";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(&redis_url(), cfg(queue, 100))
        .await
        .expect("connect producer");
    let payloads: Vec<Sample> = (0..500).map(|i| Sample { n: i, s: "x".into() }).collect();
    let _ = producer.add_bulk(payloads).await.expect("add_bulk 500");

    let len = xlen(&admin, &key).await;
    assert!(
        len <= 200,
        "approximate trim allows overshoot but should be near cap, got {len}"
    );
    assert!(
        len >= 100,
        "should retain at least the cap, got {len}"
    );

    let _: () = admin.quit().await.unwrap();
}
