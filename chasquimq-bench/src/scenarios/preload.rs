use crate::sample::Payload;
use chasquimq::Producer;
use chasquimq::config::ProducerConfig;

pub(crate) async fn preload_jobs(
    redis_url: &str,
    queue: &str,
    pool_size: usize,
    payload: &Payload,
    total: u64,
) {
    let producer: Producer<Payload> = Producer::connect(
        redis_url,
        ProducerConfig {
            queue_name: queue.to_string(),
            pool_size,
            max_stream_len: 1_000_000,
        },
    )
    .await
    .expect("connect producer");

    let mut emitted: u64 = 0;
    while emitted < total {
        let remaining = (total - emitted) as usize;
        let n = remaining.min(100);
        let payloads: Vec<Payload> = (0..n).map(|_| payload.clone()).collect();
        producer.add_bulk(payloads).await.expect("preload");
        emitted += n as u64;
    }
}
