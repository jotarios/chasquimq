use crate::scenarios::{self, ScenarioReport};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};

pub async fn connect_admin(url: &str) -> Client {
    let cfg = Config::from_url(url).expect("REDIS URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

pub async fn flush_queue(admin: &Client, queue: &str) {
    for suffix in ["stream", "dlq"] {
        let key = format!("chasqui:{queue}:{suffix}");
        let _: Value = admin
            .custom(
                CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
                vec![Value::from(key)],
            )
            .await
            .expect("DEL");
    }
}

pub async fn run_scenario(
    name: &str,
    redis_url: &str,
    queue: &str,
    scale: u32,
) -> ScenarioReport {
    match name {
        "queue-add" => scenarios::queue_add::run(redis_url, queue, scale).await,
        "queue-add-bulk" => scenarios::queue_add_bulk::run(redis_url, queue, scale).await,
        "worker-generic" => scenarios::worker_generic::run(redis_url, queue, scale).await,
        "worker-concurrent" => scenarios::worker_concurrent::run(redis_url, queue, scale).await,
        other => panic!("unknown scenario: {other}"),
    }
}
