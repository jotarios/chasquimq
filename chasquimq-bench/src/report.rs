use crate::scenarios::ScenarioReport;
use crate::stats::compute_stats;
use std::collections::BTreeMap;

const NOISY_SCENARIOS: &[&str] = &["worker-generic"];

pub fn print_markdown_table(all: &BTreeMap<String, Vec<ScenarioReport>>, discard_slowest: u32) {
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
        let label = if NOISY_SCENARIOS.contains(&name.as_str()) {
            format!("`{name}` ⚠ noisy")
        } else {
            format!("`{name}`")
        };
        println!(
            "| {label} | **{:.0}** | {:.0} | {:.0} | {:.0} | {:.0} | {:.2}× | {:.0} |",
            stats.mean, stats.p50, stats.p95, stats.p99, stats.stddev, cpu.mean, jpcs.mean
        );
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
