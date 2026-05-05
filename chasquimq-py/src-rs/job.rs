//! `NativeJob` — Python class handed to the user-supplied async handler.
//!
//! Built once per delivery on the tokio task that pulled the entry off the
//! Redis stream; the GIL is acquired briefly to construct it, then dropped
//! before the handler coroutine is awaited.

use crate::payload::RawBytes;
use chasquimq::Job;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass(module = "chasquimq._native", name = "NativeJob", frozen)]
pub struct NativeJob {
    id: String,
    name: String,
    payload: Vec<u8>,
    created_at_ms: u64,
    attempt: u32,
}

impl NativeJob {
    pub fn from_engine(job: Job<RawBytes>) -> Self {
        Self {
            id: job.id,
            name: job.name,
            payload: job.payload.0.to_vec(),
            created_at_ms: job.created_at_ms,
            attempt: job.attempt,
        }
    }
}

#[pymethods]
impl NativeJob {
    #[getter]
    fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    #[getter]
    fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }

    #[getter]
    fn attempt(&self) -> u32 {
        self.attempt
    }

    fn __repr__(&self) -> String {
        format!(
            "NativeJob(id={:?}, name={:?}, attempt={}, created_at_ms={}, payload_len={})",
            self.id,
            self.name,
            self.attempt,
            self.created_at_ms,
            self.payload.len()
        )
    }
}
