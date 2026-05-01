// Throwaway perf spike: pipelined `XADD ... IDMP` against a real Redis 8.6
// to validate that fred + tokio can clear ≥3× the BullMQ `queue-add-bulk`
// baseline (~61k jobs/s on this M3) before we scaffold the real engine.
//
// Sweeps producer-pool sizes 1/2/4/8/16; for each size, that many
// concurrent tasks fire pipelines of 50 XADDs each. Payload is encoded
// once outside the timed loop. ULID per command serves as both the
// in-payload identifier and the `IDMP` idempotent-id.

use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;
use std::time::Instant;

const STREAM_KEY: &str = "chasqui:spike:stream";
const MAX_LEN: u64 = 1_000_000;
const BULK_SIZE: usize = 50;
const WARMUP_JOBS: usize = 1_000;
const BENCH_JOBS: usize = 10_000;
const RUNS_PER_POOL_SIZE: usize = 3;

fn payload_bytes() -> Bytes {
    // 1×1 payload (matches BullMQ queue-add-bulk shape: tiny).
    // Pre-encoded once — we are testing Redis throughput, not serde.
    Bytes::from_static(b"\x81\xa1k\xa1v") // msgpack {"k":"v"}
}

async fn fire_pipeline(
    client: &Client,
    producer_id: &str,
    bulk: usize,
    payload: &Bytes,
) -> Result<(), fred::error::Error> {
    let pipeline = client.pipeline();
    let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
    for _ in 0..bulk {
        let iid = ulid::Ulid::new().to_string();
        // XADD <key> IDMP <pid> <iid> MAXLEN ~ <n> * d <bytes>
        let args: Vec<Value> = vec![
            STREAM_KEY.into(),
            "IDMP".into(),
            producer_id.into(),
            iid.into(),
            "MAXLEN".into(),
            "~".into(),
            (MAX_LEN as i64).into(),
            "*".into(),
            "d".into(),
            Value::Bytes(payload.clone()),
        ];
        let _: () = pipeline.custom(cmd.clone(), args).await?;
    }
    let _: Vec<Value> = pipeline.all().await?;
    Ok(())
}

async fn run_one(pool_size: usize, total_jobs: usize) -> Result<f64, fred::error::Error> {
    let cfg = Config::from_url("redis://127.0.0.1:6379")?;
    let mut clients = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let c = Client::new(cfg.clone(), None, None, None);
        c.init().await?;
        clients.push(c);
    }

    let producer_id = uuid::Uuid::new_v4().to_string();
    let payload = payload_bytes();

    // Flush stream
    let _: Value = clients[0]
        .custom(
            CustomCommand::new_static("DEL", ClusterHash::FirstKey, false),
            vec![Value::from(STREAM_KEY)],
        )
        .await?;

    // Each task owns one client. Each task fires `pipelines_per_task` pipelines
    // of BULK_SIZE commands. Warmup + bench split across tasks evenly.
    let total_pipelines = total_jobs / BULK_SIZE;
    let pipelines_per_task = total_pipelines / pool_size;
    let warmup_pipelines = WARMUP_JOBS / BULK_SIZE / pool_size; // per-task warmup
    let bench_pipelines = pipelines_per_task - warmup_pipelines;

    let payload = Arc::new(payload);
    let producer_id = Arc::new(producer_id);

    // warmup
    let mut handles = Vec::new();
    for c in &clients {
        let c = c.clone();
        let pid = producer_id.clone();
        let pl = payload.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..warmup_pipelines {
                fire_pipeline(&c, &pid, BULK_SIZE, &pl).await?;
            }
            Ok::<(), fred::error::Error>(())
        }));
    }
    for h in handles {
        h.await.unwrap()?;
    }

    // bench
    let started = Instant::now();
    let mut handles = Vec::new();
    for c in &clients {
        let c = c.clone();
        let pid = producer_id.clone();
        let pl = payload.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..bench_pipelines {
                fire_pipeline(&c, &pid, BULK_SIZE, &pl).await?;
            }
            Ok::<(), fred::error::Error>(())
        }));
    }
    for h in handles {
        h.await.unwrap()?;
    }
    let elapsed = started.elapsed();

    for c in &clients {
        let _ = c.quit().await;
    }

    let total_bench_jobs = bench_pipelines * pool_size * BULK_SIZE;
    let jobs_per_sec = total_bench_jobs as f64 / elapsed.as_secs_f64();
    Ok(jobs_per_sec)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), fred::error::Error> {
    println!("ChasquiMQ perf spike: pipelined XADD ... IDMP");
    println!(
        "Bench window: {} jobs ({} pipelines × {} cmds), warmup: {} jobs",
        BENCH_JOBS,
        BENCH_JOBS / BULK_SIZE,
        BULK_SIZE,
        WARMUP_JOBS
    );
    println!("BullMQ unpinned baseline (queue-add-bulk): 60,828 jobs/s. 3× target: 182,484.\n");

    let pool_sizes = [1usize, 2, 4, 8, 16];
    let mut best = (0usize, 0.0f64);
    for &n in &pool_sizes {
        let mut samples = Vec::with_capacity(RUNS_PER_POOL_SIZE);
        for run in 0..RUNS_PER_POOL_SIZE {
            let total_jobs = WARMUP_JOBS + BENCH_JOBS;
            let jps = run_one(n, total_jobs).await?;
            samples.push(jps);
            println!("  pool={} run={} → {:>10.0} jobs/s", n, run + 1, jps);
        }
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = samples.iter().cloned().fold(0.0f64, f64::max);
        println!(
            "pool={:>2}: mean = {:>10.0} jobs/s  (range {:.0} – {:.0})",
            n, mean, min, max
        );
        if mean > best.1 {
            best = (n, mean);
        }
        println!();
    }

    println!("SPIKE RESULT: {:.0} jobs/s @ pool={}", best.1, best.0);
    let ratio = best.1 / 60_828.0;
    println!(
        "vs BullMQ baseline (60,828 jobs/s): {:.2}× ({})",
        ratio,
        if ratio >= 3.0 {
            "≥ 3× — proceed to scaffold"
        } else if ratio >= 1.5 {
            "1.5×–3× — try redis-rs spike"
        } else {
            "< 1.5× — STOP, escalate"
        }
    );
    Ok(())
}
