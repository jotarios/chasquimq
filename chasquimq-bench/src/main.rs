mod cpu;
mod sample;
mod scenarios;

use clap::Parser;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::prelude::Config;
use fred::types::{ClusterHash, CustomCommand, Value};
use scenarios::ScenarioReport;
use std::collections::BTreeMap;

const NOISY_SCENARIOS: &[&str] = &["worker-generic"];

#[derive(Parser, Debug)]
#[command(name = "chasquimq-bench", about = "Benchmark harness for ChasquiMQ")]
struct Args {
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    #[arg(long, value_delimiter = ',', default_values_t = vec![
        "queue-add".to_string(),
        "queue-add-bulk".to_string(),
        "worker-generic".to_string(),
        "worker-concurrent".to_string(),
    ])]
    scenario: Vec<String>,

    #[arg(long, default_value_t = 3)]
    repeats: u32,

    /// Multiply warmup+bench job counts by this factor.
    /// Default 1 keeps parity with bullmq-bench.
    /// Use --scale=10 for tighter numbers (bench window grows ~10×).
    #[arg(long, default_value_t = 1)]
    scale: u32,

    /// Drop the N slowest repeats per scenario from the mean (cold-start outliers).
    /// Effective only when repeats > N + 1.
    #[arg(long, default_value_t = 1)]
    discard_slowest: u32,

    #[arg(long, value_enum, default_value_t = Format::Markdown)]
    format: Format,

    /// Log level inside the bench process. Defaults to ERROR so retry warnings
    /// don't pollute timing.
    #[arg(long, default_value = "error")]
    log_level: String,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Format {
    Markdown,
    Jsonl,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let level: tracing::Level = args
        .log_level
        .parse()
        .unwrap_or(tracing::Level::ERROR);
    tracing_subscriber::fmt().with_max_level(level).init();

    let admin = connect_admin(&args.redis_url).await;

    let mut all_reports: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
    for scenario in &args.scenario {
        for repeat in 0..args.repeats {
            let queue = format!("bench-{scenario}-{repeat}");
            flush_queue(&admin, &queue).await;
            let report = match scenario.as_str() {
                "queue-add" => scenarios::queue_add::run(&args.redis_url, &queue, args.scale).await,
                "queue-add-bulk" => {
                    scenarios::queue_add_bulk::run(&args.redis_url, &queue, args.scale).await
                }
                "worker-generic" => {
                    scenarios::worker_generic::run(&args.redis_url, &queue, args.scale).await
                }
                "worker-concurrent" => {
                    scenarios::worker_concurrent::run(&args.redis_url, &queue, args.scale).await
                }
                other => panic!("unknown scenario: {other}"),
            };
            if matches!(args.format, Format::Jsonl) {
                println!(
                    "{}",
                    serde_json::to_string(&report).expect("serialize report")
                );
            }
            all_reports
                .entry(scenario.clone())
                .or_default()
                .push(report);
            flush_queue(&admin, &queue).await;
        }
    }

    if matches!(args.format, Format::Markdown) {
        print_markdown_table(&all_reports, args.discard_slowest);
    }

    let _: () = admin.quit().await.unwrap();
}

async fn connect_admin(url: &str) -> Client {
    let cfg = Config::from_url(url).expect("REDIS URL");
    let client = Client::new(cfg, None, None, None);
    client.init().await.expect("connect admin");
    client
}

async fn flush_queue(admin: &Client, queue: &str) {
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

struct Stats {
    n: usize,
    discarded: usize,
    mean: f64,
    stddev: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    min: f64,
    max: f64,
}

fn compute_stats(samples: &[f64], discard_slowest: u32) -> Stats {
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let total = sorted.len();
    let discard = (discard_slowest as usize).min(total.saturating_sub(1));
    let kept: &[f64] = &sorted[discard..];
    let n = kept.len();
    let mean = kept.iter().copied().sum::<f64>() / n as f64;
    let var = kept.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
    let stddev = var.sqrt();
    Stats {
        n,
        discarded: discard,
        mean,
        stddev,
        p50: percentile(kept, 50.0),
        p95: percentile(kept, 95.0),
        p99: percentile(kept, 99.0),
        min: *kept.first().unwrap_or(&0.0),
        max: *kept.last().unwrap_or(&0.0),
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn print_markdown_table(all: &BTreeMap<String, Vec<ScenarioReport>>, discard_slowest: u32) {
    let cores = num_logical_cores();
    println!();
    println!(
        "Each row drops the {} slowest repeat(s) before computing stats. \
         CPU load is across all worker threads (host has {} logical core(s)).",
        discard_slowest, cores
    );
    println!();
    println!("| Scenario | Mean (jobs/s) | p50 | p95 | p99 | stddev | CPU load (× core) | jobs/CPU-sec |");
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");
    for (name, runs) in all {
        if runs.is_empty() {
            continue;
        }
        let jps_samples: Vec<f64> = runs.iter().map(|r| r.jobs_per_sec).collect();
        let cpu_samples: Vec<f64> = runs.iter().map(|r| r.cpu_total_pct / 100.0).collect();
        let jpcs_samples: Vec<f64> = runs.iter().map(|r| r.jobs_per_cpu_sec).collect();
        let stats = compute_stats(&jps_samples, discard_slowest);
        let cpu = compute_stats(&cpu_samples, discard_slowest);
        let jpcs = compute_stats(&jpcs_samples, discard_slowest);
        let noisy = NOISY_SCENARIOS.contains(&name.as_str());
        let label = if noisy {
            format!("`{name}` ⚠ noisy")
        } else {
            format!("`{name}`")
        };
        println!(
            "| {label} | **{:.0}** | {:.0} | {:.0} | {:.0} | {:.0} | {:.2}× | {:.0} |",
            stats.mean, stats.p50, stats.p95, stats.p99, stats.stddev, cpu.mean, jpcs.mean
        );
        let _ = (stats.n, stats.discarded, stats.min, stats.max);
    }
    if all.keys().any(|k| NOISY_SCENARIOS.contains(&k.as_str())) {
        println!();
        println!(
            "⚠ noisy: bench window is too small (~ms) for stable measurement; \
             treat as direction-only, not a defensible headline."
        );
    }
}

fn num_logical_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
