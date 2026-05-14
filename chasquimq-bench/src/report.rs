use crate::scenarios::{LatencyReport, ScenarioReport};
use crate::stats::compute_stats;
use std::collections::BTreeMap;
use std::io::{self, Write};

const NOISY_SCENARIOS: &[&str] = &["worker-generic"];

pub fn print_markdown_table<W: Write>(
    all: &BTreeMap<String, Vec<ScenarioReport>>,
    discard_slowest: u32,
    out: &mut W,
) -> io::Result<()> {
    let cores = num_logical_cores();
    writeln!(out)?;
    writeln!(
        out,
        "Each row drops the {} slowest repeat(s) before computing stats. \
         CPU load is across all worker threads (host has {} logical core(s)).",
        discard_slowest, cores
    )?;
    writeln!(out)?;
    writeln!(
        out,
        "| Scenario | Mean (jobs/s) | p50 | p95 | p99 | stddev | CPU load (× core) | jobs/CPU-sec | p50 lat (us) | p99 lat (us) | p99.9 lat (us) |"
    )?;
    writeln!(
        out,
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )?;
    let mut latency_footnotes: Vec<(String, LatencyReport)> = Vec::new();
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
        let merged = merge_latency(runs);
        let (p50_lat, p99_lat, p999_lat) = match merged.as_ref() {
            Some(lat) => (
                format!("{}", lat.end_to_end_us.p50_us),
                format!("{}", lat.end_to_end_us.p99_us),
                format!("{}", lat.end_to_end_us.p999_us),
            ),
            None => ("—".into(), "—".into(), "—".into()),
        };
        writeln!(
            out,
            "| {label} | **{:.0}** | {:.0} | {:.0} | {:.0} | {:.0} | {:.2}× | {:.0} | {} | {} | {} |",
            stats.mean,
            stats.p50,
            stats.p95,
            stats.p99,
            stats.stddev,
            cpu.mean,
            jpcs.mean,
            p50_lat,
            p99_lat,
            p999_lat
        )?;
        if let Some(lat) = merged {
            latency_footnotes.push((name.clone(), lat));
        }
    }
    if all.keys().any(|k| NOISY_SCENARIOS.contains(&k.as_str())) {
        writeln!(out)?;
        writeln!(
            out,
            "⚠ noisy: bench window is too small (~ms) for stable measurement; \
             treat as direction-only, not a defensible headline."
        )?;
    }
    for (name, lat) in &latency_footnotes {
        writeln!(out)?;
        writeln!(
            out,
            "{name} headline columns report end_to_end_us. Handler-only (engine-measured handler future duration):"
        )?;
        writeln!(
            out,
            "  p50 {}us / p99 {}us / p99.9 {}us",
            lat.handler_us.p50_us, lat.handler_us.p99_us, lat.handler_us.p999_us
        )?;
        let overhead_p50 = lat
            .end_to_end_us
            .p50_us
            .saturating_sub(lat.handler_us.p50_us);
        let overhead_p99 = lat
            .end_to_end_us
            .p99_us
            .saturating_sub(lat.handler_us.p99_us);
        let overhead_p999 = lat
            .end_to_end_us
            .p999_us
            .saturating_sub(lat.handler_us.p999_us);
        writeln!(
            out,
            "  engine overhead (end_to_end - handler): p50 {}us / p99 {}us / p99.9 {}us",
            overhead_p50, overhead_p99, overhead_p999
        )?;
        if lat.overflow_count > 0 {
            writeln!(
                out,
                "  WARNING: {} value(s) clamped to 600s ceiling — measurement suspect, raise bound or fix the stall",
                lat.overflow_count
            )?;
        }
    }
    Ok(())
}

fn merge_latency(runs: &[ScenarioReport]) -> Option<LatencyReport> {
    let mut iter = runs.iter().filter_map(|r| r.latency.as_ref());
    let first = iter.next()?.clone();
    let mut handler_p50: Vec<u64> = vec![first.handler_us.p50_us];
    let mut handler_p90: Vec<u64> = vec![first.handler_us.p90_us];
    let mut handler_p99: Vec<u64> = vec![first.handler_us.p99_us];
    let mut handler_p999: Vec<u64> = vec![first.handler_us.p999_us];
    let mut handler_max: u64 = first.handler_us.max_us;
    let mut handler_samples: u64 = first.handler_us.samples;
    let mut e2e_p50: Vec<u64> = vec![first.end_to_end_us.p50_us];
    let mut e2e_p90: Vec<u64> = vec![first.end_to_end_us.p90_us];
    let mut e2e_p99: Vec<u64> = vec![first.end_to_end_us.p99_us];
    let mut e2e_p999: Vec<u64> = vec![first.end_to_end_us.p999_us];
    let mut e2e_max: u64 = first.end_to_end_us.max_us;
    let mut e2e_samples: u64 = first.end_to_end_us.samples;
    let mut overflow: u64 = first.overflow_count;
    for lat in iter {
        handler_p50.push(lat.handler_us.p50_us);
        handler_p90.push(lat.handler_us.p90_us);
        handler_p99.push(lat.handler_us.p99_us);
        handler_p999.push(lat.handler_us.p999_us);
        handler_max = handler_max.max(lat.handler_us.max_us);
        handler_samples = handler_samples.saturating_add(lat.handler_us.samples);
        e2e_p50.push(lat.end_to_end_us.p50_us);
        e2e_p90.push(lat.end_to_end_us.p90_us);
        e2e_p99.push(lat.end_to_end_us.p99_us);
        e2e_p999.push(lat.end_to_end_us.p999_us);
        e2e_max = e2e_max.max(lat.end_to_end_us.max_us);
        e2e_samples = e2e_samples.saturating_add(lat.end_to_end_us.samples);
        overflow = overflow.saturating_add(lat.overflow_count);
    }
    Some(LatencyReport {
        handler_us: crate::scenarios::LatencyDistribution {
            p50_us: median(&mut handler_p50),
            p90_us: median(&mut handler_p90),
            p99_us: median(&mut handler_p99),
            p999_us: median(&mut handler_p999),
            max_us: handler_max,
            samples: handler_samples,
        },
        end_to_end_us: crate::scenarios::LatencyDistribution {
            p50_us: median(&mut e2e_p50),
            p90_us: median(&mut e2e_p90),
            p99_us: median(&mut e2e_p99),
            p999_us: median(&mut e2e_p999),
            max_us: e2e_max,
            samples: e2e_samples,
        },
        overflow_count: overflow,
    })
}

fn median(values: &mut [u64]) -> u64 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    values[values.len() / 2]
}

fn num_logical_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenarios::{LatencyDistribution, LatencyReport, ScenarioReport};

    fn make_throughput_report(name: &str, jps: f64) -> ScenarioReport {
        ScenarioReport {
            name: name.to_string(),
            jobs_total: 1000,
            time_ms: 100,
            jobs_per_sec: jps,
            cpu_user_pct: 50.0,
            cpu_sys_pct: 20.0,
            cpu_total_pct: 70.0,
            jobs_per_cpu_sec: jps / 0.7,
            latency: None,
        }
    }

    fn make_latency_report(name: &str, jps: f64, lat: LatencyReport) -> ScenarioReport {
        let mut r = make_throughput_report(name, jps);
        r.latency = Some(lat);
        r
    }

    fn sample_latency() -> LatencyReport {
        LatencyReport {
            handler_us: LatencyDistribution {
                p50_us: 12,
                p90_us: 30,
                p99_us: 45,
                p999_us: 120,
                max_us: 250,
                samples: 10_000,
            },
            end_to_end_us: LatencyDistribution {
                p50_us: 240,
                p90_us: 600,
                p99_us: 980,
                p999_us: 2400,
                max_us: 5000,
                samples: 10_000,
            },
            overflow_count: 0,
        }
    }

    #[test]
    fn renders_em_dash_for_non_latency_rows() {
        let mut all: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
        all.insert(
            "queue-add".to_string(),
            vec![make_throughput_report("queue-add", 14_000.0)],
        );
        let mut buf = Vec::new();
        print_markdown_table(&all, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("| `queue-add` |"));
        assert!(s.contains("| — | — | — |"));
    }

    #[test]
    fn renders_integer_microseconds_for_latency_rows() {
        let mut all: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
        all.insert(
            "worker-latency".to_string(),
            vec![make_latency_report(
                "worker-latency",
                1003.0,
                sample_latency(),
            )],
        );
        let mut buf = Vec::new();
        print_markdown_table(&all, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("| 240 | 980 | 2400 |"));
        assert!(s.contains("p50 12us / p99 45us / p99.9 120us"));
        assert!(s.contains("engine overhead"));
    }

    #[test]
    fn mixed_rows_render_independently() {
        let mut all: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
        all.insert(
            "queue-add".to_string(),
            vec![make_throughput_report("queue-add", 14_000.0)],
        );
        all.insert(
            "worker-latency".to_string(),
            vec![make_latency_report(
                "worker-latency",
                1003.0,
                sample_latency(),
            )],
        );
        let mut buf = Vec::new();
        print_markdown_table(&all, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let queue_line = s
            .lines()
            .find(|l| l.contains("`queue-add`"))
            .expect("queue-add row");
        assert!(queue_line.contains("| — | — | — |"));
        let latency_line = s
            .lines()
            .find(|l| l.contains("`worker-latency`"))
            .expect("worker-latency row");
        assert!(latency_line.contains("| 240 | 980 | 2400 |"));
    }

    #[test]
    fn footnote_warns_on_overflow() {
        let mut lat = sample_latency();
        lat.overflow_count = 3;
        let mut all: BTreeMap<String, Vec<ScenarioReport>> = BTreeMap::new();
        all.insert(
            "worker-latency".to_string(),
            vec![make_latency_report("worker-latency", 1003.0, lat)],
        );
        let mut buf = Vec::new();
        print_markdown_table(&all, 0, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("3 value(s) clamped"));
    }
}
