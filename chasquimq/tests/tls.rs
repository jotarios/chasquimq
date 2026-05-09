use chasquimq::config::ProducerConfig;
use chasquimq::producer::{Producer, stream_key};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::{Deserialize, Serialize};

fn tls_url() -> String {
    std::env::var("REDIS_TLS_URL")
        .expect("REDIS_TLS_URL must be set (e.g. rediss://127.0.0.1:6390) for TLS tests")
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Sample {
    n: u32,
    s: String,
}

async fn admin_tls() -> Client {
    let cfg = Config::from_url(&tls_url()).expect("REDIS_TLS_URL parses with rediss:// scheme");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect TLS admin");
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

#[tokio::test]
#[ignore = "requires REDIS_TLS_URL pointing to a TLS-fronted Redis (see scripts/test-tls.sh)"]
async fn rediss_url_negotiates_tls_and_round_trips_xadd() {
    let admin = admin_tls().await;
    let queue = "tls_round_trip";
    let key = stream_key(queue);
    flush_stream(&admin, &key).await;

    let producer: Producer<Sample> = Producer::connect(
        &tls_url(),
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size: 2,
            max_stream_len: 1_000,
            ..Default::default()
        },
    )
    .await
    .expect("Producer must connect over rediss://");

    let id = producer
        .add(Sample {
            n: 42,
            s: "tls".into(),
        })
        .await
        .expect("XADD over TLS");
    assert!(!id.is_empty(), "Producer::add returns a JobId on success");

    assert_eq!(
        xlen(&admin, &key).await,
        1,
        "XADD reached TLS Redis end-to-end"
    );

    let _: () = admin.quit().await.unwrap();
}
