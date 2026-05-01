mod cli;
mod cpu;
mod report;
mod runner;
mod sample;
mod scenarios;
mod stats;

use clap::Parser;
use cli::{Args, Format};
use fred::interfaces::ClientLike;
use scenarios::ScenarioReport;
use std::collections::BTreeMap;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let level: tracing::Level = args.log_level.parse().unwrap_or(tracing::Level::ERROR);
    tracing_subscriber::fmt().with_max_level(level).init();

    let admin = runner::connect_admin(&args.redis_url).await;

    let mut all_reports: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
    for scenario in &args.scenario {
        for repeat in 0..args.repeats {
            let queue = format!("bench-{scenario}-{repeat}");
            runner::flush_queue(&admin, &queue).await;
            let report = runner::run_scenario(scenario, &args.redis_url, &queue, args.scale).await;
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
            runner::flush_queue(&admin, &queue).await;
        }
    }

    if matches!(args.format, Format::Markdown) {
        report::print_markdown_table(&all_reports, args.discard_slowest);
    }

    let _: () = admin.quit().await.unwrap();
}
