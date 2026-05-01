//! ChasquiMQ benchmark harness.
//!
//! Mirrors `bullmq-bench` exactly so jobs/sec numbers are directly comparable.
//! See `chasquimq-bench/src/scenarios/` for the four scenarios.

mod cpu;
mod scenarios;

fn main() {
    eprintln!("chasquimq-bench: scaffolded; scenarios land in Phase 11.");
}
