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

pub struct RunOptions {
    pub progress_events_enabled: bool,
}

pub async fn run_scenario(
    name: &str,
    redis_url: &str,
    queue: &str,
    scale: u32,
    opts: &RunOptions,
) -> ScenarioReport {
    match name {
        "queue-add" => scenarios::queue_add::run(redis_url, queue, scale).await,
        "queue-add-bulk" => scenarios::queue_add_bulk::run(redis_url, queue, scale).await,
        "queue-add-delayed" => scenarios::queue_add_delayed::run(redis_url, queue, scale).await,
        "worker-generic" => scenarios::worker_generic::run(redis_url, queue, scale).await,
        "worker-concurrent" => scenarios::worker_concurrent::run(redis_url, queue, scale).await,
        "worker-latency" => scenarios::worker_latency::run(redis_url, queue, scale).await,
        "worker-concurrent-store-results" => {
            scenarios::worker_concurrent_store_results::run(redis_url, queue, scale).await
        }
        "worker-delayed-end-to-end" => {
            scenarios::worker_delayed_end_to_end::run(redis_url, queue, scale).await
        }
        "worker-retry-throughput" => {
            scenarios::worker_retry_throughput::run(redis_url, queue, scale).await
        }
        "progress-throughput-1" => {
            scenarios::progress_throughput::run(
                redis_url,
                queue,
                scale,
                1,
                opts.progress_events_enabled,
            )
            .await
        }
        "progress-throughput-10" => {
            scenarios::progress_throughput::run(
                redis_url,
                queue,
                scale,
                10,
                opts.progress_events_enabled,
            )
            .await
        }
        "progress-throughput-100" => {
            scenarios::progress_throughput::run(
                redis_url,
                queue,
                scale,
                100,
                opts.progress_events_enabled,
            )
            .await
        }
        other => panic!("unknown scenario: {other}"),
    }
}
