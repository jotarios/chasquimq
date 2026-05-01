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

    #[arg(long, value_enum, default_value_t = Format::Markdown)]
    format: Format,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Format {
    Markdown,
    Jsonl,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let args = Args::parse();
    let admin = connect_admin(&args.redis_url).await;

    let mut all_reports: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
    for scenario in &args.scenario {
        for repeat in 0..args.repeats {
            let queue = format!("bench-{scenario}-{repeat}");
            flush_queue(&admin, &queue).await;
            let report = match scenario.as_str() {
                "queue-add" => scenarios::queue_add::run(&args.redis_url, &queue).await,
                "queue-add-bulk" => scenarios::queue_add_bulk::run(&args.redis_url, &queue).await,
                "worker-generic" => scenarios::worker_generic::run(&args.redis_url, &queue).await,
                "worker-concurrent" => {
                    scenarios::worker_concurrent::run(&args.redis_url, &queue).await
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
        print_markdown_table(&all_reports);
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

fn print_markdown_table(all: &BTreeMap<String, Vec<ScenarioReport>>) {
    println!();
    println!("| Scenario | Mean (jobs/s) | Range | Mean CPU% | jobs/CPU-sec |");
    println!("|---|---:|---|---:|---:|");
    for (name, runs) in all {
        if runs.is_empty() {
            continue;
        }
        let n = runs.len() as f64;
        let mean_jps: f64 = runs.iter().map(|r| r.jobs_per_sec).sum::<f64>() / n;
        let min_jps = runs.iter().map(|r| r.jobs_per_sec).fold(f64::INFINITY, f64::min);
        let max_jps = runs.iter().map(|r| r.jobs_per_sec).fold(0.0f64, f64::max);
        let mean_cpu: f64 = runs.iter().map(|r| r.cpu_total_pct).sum::<f64>() / n;
        let mean_jpcs: f64 = runs.iter().map(|r| r.jobs_per_cpu_sec).sum::<f64>() / n;
        println!(
            "| `{name}` | **{:.0}** | {:.0} – {:.0} | {:.0}% | {:.0} |",
            mean_jps, min_jps, max_jps, mean_cpu, mean_jpcs
        );
    }
}
