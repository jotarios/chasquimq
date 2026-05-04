//! `NativeScheduler` — PyO3 wrapper over `chasquimq::Scheduler<RawBytes>`.
//!
//! Mirrors `chasquimq-node/src/scheduler.rs`. Standalone repeatable-job
//! scheduler. Pinned to `RawBytes` so the binding stays schema-agnostic —
//! the high-level Python shim msgpack-encodes user payloads at
//! `Queue.upsert_repeatable_job(...)` time, and the scheduler hands those
//! exact bytes through to the stream / delayed ZSET on every fire (the
//! engine's `Job<RawBytes>` envelope wraps them with a fresh ULID +
//! `attempt = 0` each tick).
//!
//! The high-level `Worker` shim auto-spawns one alongside the consumer so
//! user code that calls `Queue.add(name, data, repeat=...)` and runs a
//! `Worker` gets cron / interval fires without a separate scheduler
//! process. Multiple workers cooperate via Redis `SET NX EX` leader
//! election on `{chasqui:<queue>}:scheduler:lock`.

use crate::payload::RawBytes;
use chasquimq::Scheduler;
use chasquimq::config::SchedulerConfig;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[pyclass(module = "chasquimq._native", name = "NativeScheduler")]
pub struct NativeScheduler {
    redis_url: String,
    cfg: SchedulerConfig,
    shutdown: Arc<CancellationToken>,
}

#[pymethods]
impl NativeScheduler {
    #[new]
    #[pyo3(signature = (
        redis_url,
        queue_name,
        *,
        tick_interval_ms = None,
        batch = None,
        max_stream_len = None,
        lock_ttl_secs = None,
        holder_id = None,
    ))]
    fn new(
        redis_url: String,
        queue_name: String,
        tick_interval_ms: Option<i64>,
        batch: Option<u32>,
        max_stream_len: Option<i64>,
        lock_ttl_secs: Option<i64>,
        holder_id: Option<String>,
    ) -> PyResult<Self> {
        let mut cfg = SchedulerConfig {
            queue_name,
            ..SchedulerConfig::default()
        };
        if let Some(v) = tick_interval_ms {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "tick_interval_ms must be non-negative; got {v}"
                )));
            }
            cfg.tick_interval_ms = v as u64;
        }
        if let Some(v) = batch {
            cfg.batch = (v as usize).max(1);
        }
        if let Some(v) = max_stream_len {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "max_stream_len must be non-negative; got {v}"
                )));
            }
            cfg.max_stream_len = v as u64;
        }
        if let Some(v) = lock_ttl_secs {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "lock_ttl_secs must be non-negative; got {v}"
                )));
            }
            cfg.lock_ttl_secs = v as u64;
        }
        if let Some(v) = holder_id {
            cfg.holder_id = v;
        }

        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
        })
    }

    /// Run the scheduler loop until `shutdown()` is called. Resolves once
    /// the engine drains.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let redis_url = self.redis_url.clone();
        let cfg = self.cfg.clone();
        let shutdown = (*self.shutdown).clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let scheduler = Scheduler::<RawBytes>::new(redis_url, cfg);
            scheduler
                .run(shutdown)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{e}")))
        })
    }

    /// Signal graceful shutdown. Idempotent; safe to call from any thread
    /// or asyncio task.
    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}
