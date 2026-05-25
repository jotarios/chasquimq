//! `Job` — Python class handed to the user-supplied async handler.
//!
//! Built once per delivery on the tokio task that pulled the entry off the
//! Redis stream; the GIL is acquired briefly to construct it, then dropped
//! before the handler coroutine is awaited.
//!
//! The pyclass carries an `Option<Arc<JobHandle>>`: `Some` for jobs the
//! engine's worker dispatched to a handler (the consumer's hot path
//! `JobHandle::new(...)` lands here), `None` would only occur on
//! synthesized `_Job` instances — none exist on the PyO3 surface today,
//! but the read-only branch is mirrored in the high-level Python `Job`
//! dataclass so users get a clear "this Job came from get_job() and is
//! read-only" error.

use crate::payload::RawBytes;
use crate::producer::map_engine_err;
use chasquimq::{Job as EngineJob, JobHandle as EngineJobHandle};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;

#[pyclass(module = "chasquimq._native", name = "_Job")]
pub struct Job {
    id: String,
    name: String,
    payload: Vec<u8>,
    created_at_ms: u64,
    attempt: u32,
    handle: Option<Arc<EngineJobHandle>>,
}

impl Job {
    pub fn from_engine(job: EngineJob<RawBytes>) -> Self {
        Self {
            id: job.id,
            name: job.name,
            payload: job.payload.0.to_vec(),
            created_at_ms: job.created_at_ms,
            attempt: job.attempt,
            handle: job.handle,
        }
    }
}

#[pymethods]
impl Job {
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

    /// Persist a `0..=100` progress value for this job under the engine's
    /// per-job progress key. Values outside `0..=100` are clamped at the
    /// engine boundary; the call resolves once the SET round trip
    /// completes. A `JobHandle` is attached only when the engine's
    /// worker dispatched this job to a handler; this raises a Python
    /// `RuntimeError` when the job has no backref (the high-level shim
    /// then re-raises as the read-only-Job guard).
    fn update_progress<'py>(&self, py: Python<'py>, n: u32) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone().ok_or_else(|| {
            PyRuntimeError::new_err(
                "Job.update_progress() requires the Job be passed to your worker handler; \
                 Jobs returned by Queue.get_job() are read-only",
            )
        })?;
        let clamped = n.min(u8::MAX as u32) as u8;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            handle
                .update_progress(clamped)
                .await
                .map_err(map_engine_err)
        })
    }

    /// Append `line` to the per-job log stream and return the new XLEN.
    /// Oversize lines are truncated on a UTF-8 char boundary with a
    /// `[…truncated]` marker (engine-side; see
    /// `ConsumerConfig::log_max_line_bytes`). Same read-only-Job guard
    /// as `update_progress`.
    fn log<'py>(&self, py: Python<'py>, line: String) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.handle.clone().ok_or_else(|| {
            PyRuntimeError::new_err(
                "Job.log() requires the Job be passed to your worker handler; \
                 Jobs returned by Queue.get_job() are read-only",
            )
        })?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let new_len = handle.log(&line).await.map_err(map_engine_err)?;
            Ok::<u64, PyErr>(new_len)
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Job(id={:?}, name={:?}, attempt={}, created_at_ms={}, payload_len={})",
            self.id,
            self.name,
            self.attempt,
            self.created_at_ms,
            self.payload.len()
        )
    }
}
