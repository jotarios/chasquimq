use pyo3::prelude::*;

mod consumer;
mod job;
mod payload;
mod producer;
mod scheduler;

const ENGINE_VERSION: &str = env!("CHASQUIMQ_ENGINE_VERSION");

#[pyfunction]
fn version() -> &'static str {
    ENGINE_VERSION
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_class::<producer::Producer>()?;
    m.add_class::<consumer::Consumer>()?;
    m.add_class::<scheduler::Scheduler>()?;
    m.add_class::<job::Job>()?;
    Ok(())
}
