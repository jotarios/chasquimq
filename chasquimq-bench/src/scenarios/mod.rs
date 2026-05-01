pub mod queue_add;
pub mod queue_add_bulk;
pub mod worker_concurrent;
pub mod worker_generic;

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
}

pub struct Stopwatch {
    started: Option<Instant>,
    rusage_at_start: Option<Rusage>,
    bench_count: u64,
    warmup: u64,
    bench: u64,
}

impl Stopwatch {
    pub fn new(warmup: u64, bench: u64) -> Self {
        Self {
            started: None,
            rusage_at_start: None,
            bench_count: 0,
            warmup,
            bench,
        }
    }

    pub fn tick(&mut self) -> Option<ScenarioOutcome> {
        self.bench_count += 1;
        if self.bench_count == self.warmup {
            self.started = Some(Instant::now());
            self.rusage_at_start = Some(Rusage::now());
        } else if self.bench_count == self.warmup + self.bench {
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
        }
    }
}
