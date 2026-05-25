//! `Introspector` — async PyO3 wrapper over `chasquimq::Introspector`.
//!
//! Mirrors `chasquimq-node/src/introspector.rs` 1:1. The shape exposed
//! to Python is dict-based — JobCounts → dict, JobInfo → dict, page →
//! `(list[dict], next_cursor)` — so the high-level Python `Queue` shim
//! doesn't have to import any private pyclasses.

use crate::credential_provider::PyCredentialProvider;
use crate::producer::map_engine_err;
use chasquimq::{
    ConnectionTuning, Introspector as EngineIntrospector, JobInfo as EngineJobInfo,
    JobState as EngineJobState,
};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::Arc;

#[pyclass(name = "Introspector", module = "chasquimq._native")]
pub struct Introspector {
    inner: Arc<EngineIntrospector>,
}

#[pymethods]
impl Introspector {
    /// Construct + connect. Identical eager / blocking-on-runtime pattern
    /// as `Producer::new` when no credential provider is involved; the
    /// inspector connects on the first method call when one is. We keep
    /// it simple by always being eager here — introspection is bursty
    /// and a deferred connect would surprise the first caller with the
    /// `connect` latency.
    #[new]
    #[pyo3(signature = (
        redis_url,
        queue_name,
        *,
        consumer_group = None,
        reconnect_max_attempts = None,
        credential_provider = None,
    ))]
    fn new(
        py: Python<'_>,
        redis_url: String,
        queue_name: String,
        consumer_group: Option<String>,
        reconnect_max_attempts: Option<u32>,
        credential_provider: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut tuning = ConnectionTuning::default();
        if let Some(n) = reconnect_max_attempts {
            tuning.reconnect_max_attempts = n;
        }
        if let Some(cb) = credential_provider {
            let provider = PyCredentialProvider::new(py, cb)?;
            tuning.credential_provider = Some(Arc::new(provider));
        }
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let group = consumer_group.clone();
        let inner = py
            .detach(|| {
                runtime.block_on(async move {
                    EngineIntrospector::connect(&redis_url, &queue_name, &tuning, group.as_deref())
                        .await
                })
            })
            .map_err(map_engine_err)?;
        Ok(Introspector {
            inner: Arc::new(inner),
        })
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.shutdown().await.map_err(map_engine_err)
        })
    }

    fn get_job_counts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let counts = inner.get_job_counts().await.map_err(map_engine_err)?;
            Python::attach(|py| {
                let d = PyDict::new(py);
                d.set_item("waiting", counts.waiting)?;
                d.set_item("active", counts.active)?;
                d.set_item("delayed", counts.delayed)?;
                d.set_item("completed", counts.completed)?;
                d.set_item("failed", counts.failed)?;
                d.set_item("paused", counts.paused)?;
                d.set_item("completed_is_capped", counts.completed_is_capped)?;
                Ok::<_, PyErr>(d.unbind())
            })
        })
    }

    fn get_job_state<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = inner.get_job_state(&job_id).await.map_err(map_engine_err)?;
            Ok::<String, PyErr>(state.as_str().to_string())
        })
    }

    fn get_job<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let opt = inner.get_job(&job_id).await.map_err(map_engine_err)?;
            Python::attach(|py| match opt {
                Some(info) => {
                    let d = job_info_to_dict(py, &info)?;
                    Ok::<Py<PyAny>, PyErr>(d.into_py_any(py)?)
                }
                None => Ok(py.None()),
            })
        })
    }

    /// XRANGE / XREVRANGE the per-job log stream for `id`. Returns
    /// `(list[str], int)`: the captured lines plus the current XLEN of
    /// the log stream. `start = 0` / `end = -1` are the BullMQ defaults
    /// ("everything in order"). Negative `start` is "this many from the
    /// end" (translated via XLEN). Mirrors `chasquimq-node/src/
    /// introspector.rs::Introspector::getJobLogs`.
    #[pyo3(signature = (id, start = 0, end = -1, asc = true))]
    fn get_job_logs<'py>(
        &self,
        py: Python<'py>,
        id: String,
        start: i64,
        end: i64,
        asc: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (lines, total) = inner
                .get_job_logs(&id, start, end, asc)
                .await
                .map_err(map_engine_err)?;
            Python::attach(|py| {
                let list = PyList::empty(py);
                for line in lines {
                    list.append(line)?;
                }
                let tup = (list.unbind(), total).into_py_any(py)?;
                Ok::<Py<PyAny>, PyErr>(tup)
            })
        })
    }

    #[pyo3(signature = (state, offset = 0, limit = 100, cursor = None))]
    fn get_jobs<'py>(
        &self,
        py: Python<'py>,
        state: String,
        offset: u64,
        limit: u64,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let parsed = EngineJobState::parse(&state).ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown state {:?}; expected one of waiting | active | delayed | completed | failed",
                state
            ))
        })?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let page = inner
                .get_jobs(parsed, offset, limit, cursor)
                .await
                .map_err(map_engine_err)?;
            Python::attach(|py| {
                let list = PyList::empty(py);
                for info in page.jobs {
                    let d = job_info_to_dict(py, &info)?;
                    list.append(d)?;
                }
                let next: Py<PyAny> = match page.next_cursor {
                    Some(c) => c.into_py_any(py)?,
                    None => py.None(),
                };
                let tup = (list.unbind(), next).into_py_any(py)?;
                Ok::<Py<PyAny>, PyErr>(tup)
            })
        })
    }
}

fn job_info_to_dict<'py>(py: Python<'py>, info: &EngineJobInfo) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("id", &info.id)?;
    d.set_item("name", &info.name)?;
    d.set_item("payload", PyBytes::new(py, info.payload.as_ref()))?;
    d.set_item("attempt", info.attempt)?;
    d.set_item("state", info.state.as_str())?;
    d.set_item("created_at_ms", info.created_at_ms)?;
    d.set_item("processed_on_ms", info.processed_on_ms)?;
    d.set_item("finished_on_ms", info.finished_on_ms)?;
    d.set_item("failure_reason", info.failure_reason.clone())?;
    d.set_item("failure_detail", info.failure_detail.clone())?;
    d.set_item("decode_failed", info.decode_failed)?;
    d.set_item("progress", info.progress.map(|n| n as u32))?;
    d.set_item("stalled_count", info.stalled_count)?;
    Ok(d)
}
