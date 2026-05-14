use pyo3::prelude::*;

mod consumer;
mod credential_provider;
mod job;
mod payload;
mod producer;
mod scheduler;

const ENGINE_VERSION: &str = env!("CHASQUIMQ_ENGINE_VERSION");

#[pyfunction]
fn version() -> &'static str {
    ENGINE_VERSION
}

/// Opt-in `tracing-subscriber` initialization for debugging the engine
/// from Python. Honors `RUST_LOG` (and `CHASQUIMQ_LOG` as an alias).
/// Idempotent — safe to call more than once. Returns `True` on the first
/// successful install, `False` if a subscriber was already set.
#[pyfunction]
fn _init_tracing() -> bool {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_from_env("CHASQUIMQ_LOG"))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
        .is_ok()
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(_init_tracing, m)?)?;
    m.add_class::<producer::Producer>()?;
    m.add_class::<consumer::Consumer>()?;
    m.add_class::<scheduler::Scheduler>()?;
    m.add_class::<job::Job>()?;
    Ok(())
}
