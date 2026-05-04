//! `NativeConsumer` — PyO3 wrapper over `chasquimq::Consumer<RawBytes>`.
//!
//! The hard part is the Python handler bridge: each engine worker, when it
//! pulls a `Job<RawBytes>` off the stream, hands it across the FFI boundary
//! to a user-supplied `async def handler(job: NativeJob) -> None`, awaits
//! the returned coroutine, and translates resolution / exception back into
//! the engine's `Result<(), HandlerError>` shape. The Python analog of the
//! Node TSFN is `pyo3_async_runtimes::tokio::into_future`, which converts
//! a Python awaitable into a `Future` the tokio task can `.await`.
//!
//! Shutdown is signal-based: `NativeConsumer::shutdown` cancels a
//! `CancellationToken` shared with the engine. `run` resolves once the
//! engine's drain (workers, ack flusher, DLQ relocator, retry relocator,
//! optional in-process promoter) all settle.

use crate::job::NativeJob;
use crate::payload::RawBytes;
use chasquimq::config::ConsumerConfig;
use chasquimq::consumer::Consumer;
use chasquimq::{HandlerError, Job};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pyo3_async_runtimes::TaskLocals;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[pyclass(module = "chasquimq._native", name = "NativeConsumer")]
pub struct NativeConsumer {
    redis_url: String,
    cfg: ConsumerConfig,
    shutdown: Arc<CancellationToken>,
}

#[pymethods]
impl NativeConsumer {
    #[new]
    #[pyo3(signature = (
        redis_url,
        queue_name,
        *,
        concurrency = 1,
        max_attempts = 25,
        group = "default".to_string(),
        consumer_id = None,
        read_block_ms = None,
        read_count = None,
        claim_min_idle_ms = None,
        max_payload_bytes = None,
        dlq_max_stream_len = None,
        events_enabled = true,
        delayed_enabled = true,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        redis_url: String,
        queue_name: String,
        concurrency: u32,
        max_attempts: u32,
        group: String,
        consumer_id: Option<String>,
        read_block_ms: Option<i64>,
        read_count: Option<u32>,
        claim_min_idle_ms: Option<i64>,
        max_payload_bytes: Option<u32>,
        dlq_max_stream_len: Option<i64>,
        events_enabled: bool,
        delayed_enabled: bool,
    ) -> PyResult<Self> {
        let mut cfg = ConsumerConfig {
            queue_name,
            group,
            concurrency: (concurrency as usize).max(1),
            max_attempts,
            events_enabled,
            delayed_enabled,
            ..ConsumerConfig::default()
        };
        if let Some(v) = consumer_id {
            cfg.consumer_id = v;
        }
        if let Some(v) = read_block_ms {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "read_block_ms must be non-negative; got {v}"
                )));
            }
            cfg.block_ms = v as u64;
        }
        if let Some(v) = read_count {
            cfg.batch = (v as usize).max(1);
        }
        if let Some(v) = claim_min_idle_ms {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "claim_min_idle_ms must be non-negative; got {v}"
                )));
            }
            cfg.claim_min_idle_ms = v as u64;
        }
        if let Some(v) = max_payload_bytes {
            cfg.max_payload_bytes = v as usize;
        }
        if let Some(v) = dlq_max_stream_len {
            if v < 0 {
                return Err(PyRuntimeError::new_err(format!(
                    "dlq_max_stream_len must be non-negative; got {v}"
                )));
            }
            cfg.dlq_max_stream_len = v as u64;
        }

        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
        })
    }

    /// Run the consumer loop. Resolves once the engine drains.
    ///
    /// `handler` must be an `async def handler(job: NativeJob) -> None`.
    /// A coroutine that returns normally → `XACK`. A coroutine that raises
    /// → `HandlerError` (engine retries with backoff up to `max_attempts`,
    /// then DLQ).
    ///
    /// **Unrecoverable errors:** if the raised exception's class name is
    /// `'UnrecoverableError'`, the binding maps it to
    /// `HandlerError::unrecoverable(...)` instead of `HandlerError::new(...)`,
    /// short-circuiting the retry budget and routing the job straight to
    /// the DLQ with `DlqReason::Unrecoverable`. Mirrors the Node binding.
    fn run<'py>(&self, py: Python<'py>, handler: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let redis_url = self.redis_url.clone();
        let cfg = self.cfg.clone();
        let shutdown = (*self.shutdown).clone();
        let handler = Arc::new(handler);
        // Capture the user's running asyncio loop + contextvars at `run()`
        // entry. The engine-side handler closure runs on tokio worker
        // threads which have no associated asyncio loop, so we must hand
        // each `into_future_with_locals` call back to *this* loop —
        // otherwise `get_running_loop()` fails and the user's coroutine
        // is dropped without ever being awaited.
        let task_locals = Arc::new(TaskLocals::with_running_loop(py)?.copy_context(py)?);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let consumer = Consumer::<RawBytes>::new(redis_url, cfg);
            let engine_handler = move |job: Job<RawBytes>| {
                let h = handler.clone();
                let locals = task_locals.clone();
                async move {
                    let coro_result = Python::attach(|py| -> PyResult<_> {
                        let native_job = NativeJob::from_engine(job);
                        let coro = h.call1(py, (native_job,))?;
                        pyo3_async_runtimes::into_future_with_locals(&locals, coro.into_bound(py))
                    });
                    let coro_fut = match coro_result {
                        Ok(fut) => fut,
                        Err(e) => return Err(map_py_err(&e)),
                    };
                    match coro_fut.await {
                        Ok(_) => Ok(()),
                        Err(e) => Err(map_py_err(&e)),
                    }
                }
            };

            consumer
                .run(engine_handler, shutdown)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{e}")))?;
            Ok(())
        })
    }

    /// Signal graceful shutdown. Idempotent; safe to call from any thread
    /// or asyncio task. The matching `run()` future resolves once the
    /// engine drains.
    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct PyHandlerError(String);

/// Translate a `PyErr` raised by the user handler (either at call time —
/// e.g. wrong arity — or from the awaited coroutine) into the engine's
/// `HandlerError`. When the exception's class name is
/// `"UnrecoverableError"`, return `HandlerError::unrecoverable(...)` so
/// the consumer routes the job straight to the DLQ. Every other
/// exception follows the standard retry-then-DLQ path via
/// `HandlerError::new(...)`. Mirrors the Node binding's name-based map.
fn map_py_err(e: &PyErr) -> HandlerError {
    let (class_name, repr) = Python::attach(|py| {
        let cls_name = e
            .get_type(py)
            .name()
            .map(|s| s.to_string())
            .unwrap_or_default();
        let value = e.value(py);
        let detail = match value.repr() {
            Ok(r) => r.to_string(),
            Err(_) => format!("{e}"),
        };
        (cls_name, detail)
    });

    let payload = PyHandlerError(format!("Python handler raised: {repr}"));
    if class_name == "UnrecoverableError" {
        HandlerError::unrecoverable(payload)
    } else {
        HandlerError::new(payload)
    }
}
