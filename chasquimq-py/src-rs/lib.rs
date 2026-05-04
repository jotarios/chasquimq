use pyo3::prelude::*;

const ENGINE_VERSION: &str = env!("CHASQUIMQ_ENGINE_VERSION");

#[pyfunction]
fn version() -> &'static str {
    ENGINE_VERSION
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
