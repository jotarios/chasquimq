use super::{ScenarioReport, Stopwatch};
use crate::sample::{Payload, generate_sample};
use chasquimq::Producer;
use chasquimq::config::ProducerConfig;

pub async fn run(redis_url: &str, queue: &str) -> ScenarioReport {
    let warmup: u64 = 1_000;
    let bench: u64 = 1_000;
    let payload: Payload = generate_sample(10, 10);

    let cfg = ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 1,
        max_stream_len: 1_000_000,
    };
    let producer: Producer<Payload> = Producer::connect(redis_url, cfg)
        .await
        .expect("connect producer");

    let mut sw = Stopwatch::new(warmup, bench);
    let total = warmup + bench;
    let mut outcome = None;
    for _ in 0..total {
        producer.add(payload.clone()).await.expect("add");
        if let Some(o) = sw.tick() {
            outcome = Some(o);
        }
    }
    outcome.expect("stopwatch must fire").into_report("queue-add")
}
