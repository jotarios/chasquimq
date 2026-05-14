pub(crate) mod preload;
pub mod queue_add;
pub mod queue_add_bulk;
pub mod queue_add_delayed;
pub mod worker_concurrent;
pub mod worker_concurrent_store_results;
pub mod worker_delayed_end_to_end;
pub mod worker_generic;
pub mod worker_latency;
pub mod worker_retry_throughput;

use crate::cpu::Rusage;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub name: String,
    pub jobs_total: u64,
    pub time_ms: u64,
    pub jobs_per_sec: f64,
    pub cpu_user_pct: f64,
    pub cpu_sys_pct: f64,
    pub cpu_total_pct: f64,
    pub jobs_per_cpu_sec: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencyReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyDistribution {
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub samples: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyReport {
    pub handler_us: LatencyDistribution,
    pub end_to_end_us: LatencyDistribution,
    pub overflow_count: u64,
}

pub struct ScenarioParams {
    pub warmup: u64,
    pub bench: u64,
}

pub struct Stopwatch {
    started: Option<Instant>,
    rusage_at_start: Option<Rusage>,
    bench_count: u64,
    warmup: u64,
    bench: u64,
    done: bool,
}

impl Stopwatch {
    pub fn new(warmup: u64, bench: u64) -> Self {
        Self {
            started: None,
            rusage_at_start: None,
            bench_count: 0,
            warmup,
            bench,
            done: false,
        }
    }

    /// `true` once the bench window has begun (warmup jobs have all been ticked).
    /// Used to gate per-job recording (latency histograms, etc.) so warmup
    /// jobs don't pollute the distribution.
    pub fn is_warm(&self) -> bool {
        self.started.is_some()
    }

    pub fn tick(&mut self) -> Option<ScenarioOutcome> {
        if self.done {
            return None;
        }
        self.bench_count += 1;
        if self.bench_count == self.warmup {
            self.started = Some(Instant::now());
            self.rusage_at_start = Some(Rusage::now());
        } else if self.bench_count >= self.warmup + self.bench {
            self.done = true;
            let elapsed = self.started.unwrap().elapsed();
            let rusage_diff = Rusage::now().diff(self.rusage_at_start.as_ref().unwrap());
            return Some(ScenarioOutcome {
                jobs_total: self.warmup + self.bench,
                elapsed,
                cpu_user: rusage_diff.user,
                cpu_sys: rusage_diff.sys,
            });
        }
        None
    }
}

pub struct ScenarioOutcome {
    pub jobs_total: u64,
    pub elapsed: Duration,
    pub cpu_user: Duration,
    pub cpu_sys: Duration,
}

impl ScenarioOutcome {
    pub fn into_report(self, name: &str) -> ScenarioReport {
        let time_ms = self.elapsed.as_millis() as u64;
        let secs = self.elapsed.as_secs_f64();
        let jobs_per_sec = self.jobs_total as f64 / secs;
        let user_secs = self.cpu_user.as_secs_f64();
        let sys_secs = self.cpu_sys.as_secs_f64();
        let total_cpu = user_secs + sys_secs;
        ScenarioReport {
            name: name.to_string(),
            jobs_total: self.jobs_total,
            time_ms,
            jobs_per_sec,
            cpu_user_pct: 100.0 * user_secs / secs,
            cpu_sys_pct: 100.0 * sys_secs / secs,
            cpu_total_pct: 100.0 * total_cpu / secs,
            jobs_per_cpu_sec: if total_cpu > 0.0 {
                self.jobs_total as f64 / total_cpu
            } else {
                0.0
            },
            latency: None,
        }
    }
}

pub fn scaled_params(base_warmup: u64, base_bench: u64, scale: u32) -> ScenarioParams {
    let s = scale.max(1) as u64;
    ScenarioParams {
        warmup: base_warmup * s,
        bench: base_bench * s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_report() -> ScenarioReport {
        ScenarioReport {
            name: "x".into(),
            jobs_total: 100,
            time_ms: 50,
            jobs_per_sec: 2000.0,
            cpu_user_pct: 10.0,
            cpu_sys_pct: 5.0,
            cpu_total_pct: 15.0,
            jobs_per_cpu_sec: 13_333.33,
            latency: None,
        }
    }

    #[test]
    fn serde_round_trip_without_latency() {
        let r = base_report();
        let s = serde_json::to_string(&r).expect("serialize");
        assert!(!s.contains("latency"));
        let back: ScenarioReport = serde_json::from_str(&s).expect("deserialize");
        assert!(back.latency.is_none());
        assert_eq!(back.name, "x");
    }

    #[test]
    fn stopwatch_is_warm_flips_at_warmup_boundary() {
        let mut sw = Stopwatch::new(2, 3);
        assert!(!sw.is_warm());
        // warmup ticks 1 and 2 — second tick sets `started`.
        sw.tick();
        assert!(!sw.is_warm());
        sw.tick();
        assert!(sw.is_warm());
    }

    #[test]
    fn stopwatch_tick_fires_once_on_overshoot() {
        let mut sw = Stopwatch::new(2, 3);
        // warmup + bench = 5 ticks until completion.
        for _ in 0..4 {
            assert!(sw.tick().is_none());
        }
        // 5th tick is the boundary — fires.
        let first = sw.tick();
        assert!(first.is_some());
        // Any subsequent ticks (overshoot from retries) must be no-ops.
        for _ in 0..5 {
            assert!(sw.tick().is_none());
        }
    }

    #[test]
    fn serde_round_trip_with_latency() {
        let mut r = base_report();
        r.latency = Some(LatencyReport {
            handler_us: LatencyDistribution {
                p50_us: 12,
                p90_us: 30,
                p99_us: 45,
                p999_us: 120,
                max_us: 200,
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
        });
        let s = serde_json::to_string(&r).expect("serialize");
        assert!(s.contains("\"latency\""));
        let back: ScenarioReport = serde_json::from_str(&s).expect("deserialize");
        let lat = back.latency.expect("latency present");
        assert_eq!(lat.handler_us.p50_us, 12);
        assert_eq!(lat.end_to_end_us.p999_us, 2400);
        assert_eq!(lat.overflow_count, 0);
    }
}
