use pyo3::prelude::*;

mod consumer;
mod job;
mod payload;
mod producer;

const ENGINE_VERSION: &str = env!("CHASQUIMQ_ENGINE_VERSION");

#[pyfunction]
fn version() -> &'static str {
    ENGINE_VERSION
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_class::<producer::NativeProducer>()?;
    m.add_class::<consumer::NativeConsumer>()?;
    m.add_class::<job::NativeJob>()?;
    Ok(())
}
