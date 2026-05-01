use super::{ScenarioReport, Stopwatch};
use crate::sample::{Payload, generate_sample};
use chasquimq::Producer;
use chasquimq::config::ProducerConfig;

pub async fn run(redis_url: &str, queue: &str) -> ScenarioReport {
    let warmup: u64 = 1_000;
    let bench: u64 = 10_000;
    let bulk_size: usize = 50;
    let payload: Payload = generate_sample(1, 1);

    let cfg = ProducerConfig {
        queue_name: queue.to_string(),
        pool_size: 8,
        max_stream_len: 1_000_000,
    };
    let producer: Producer<Payload> = Producer::connect(redis_url, cfg)
        .await
        .expect("connect producer");

    let mut sw = Stopwatch::new(warmup, bench);
    let total = warmup + bench;
    let mut emitted: u64 = 0;
    let mut outcome = None;
    while emitted < total && outcome.is_none() {
        let remaining = (total - emitted) as usize;
        let n = remaining.min(bulk_size);
        let payloads: Vec<Payload> = (0..n).map(|_| payload.clone()).collect();
        producer.add_bulk(payloads).await.expect("add_bulk");
        for _ in 0..n {
            if let Some(o) = sw.tick() {
                outcome = Some(o);
            }
        }
        emitted += n as u64;
    }
    outcome
        .expect("stopwatch must fire")
        .into_report("queue-add-bulk")
}
