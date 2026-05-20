//! `Producer` — async PyO3 wrapper over `chasquimq::Producer<RawBytes>`.
//!
//! Mirrors `chasquimq-node/src/producer.rs` 1:1. JS dicts become PyDicts;
//! validation patterns (unknown `kind`, non-finite floats, negative ms)
//! are the same. Every async method returns a Python awaitable via
//! `pyo3_async_runtimes::tokio::future_into_py`.

use crate::credential_provider::PyCredentialProvider;
use crate::payload::RawBytes;
use bytes::Bytes;
use chasquimq::config::ProducerConfig;
use chasquimq::producer::{AddOptions, Producer as EngineProducer};
use chasquimq::repeat::{MissedFiresPolicy, RepeatPattern, RepeatableSpec};
use chasquimq::{BackoffKind, BackoffSpec, JobRetryOverride, RepeatableMeta};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

/// Producer state — either already-connected (eager path: legacy
/// behaviour for callers without a `credential_provider`) or deferred,
/// holding the config plus URL until the first async method connects
/// on the tokio runtime. The deferred path matters when a
/// `credential_provider` is set: connecting requires fred's router task
/// to dispatch the Python callable back to the asyncio loop, which is
/// only running when *this* call frame is awaiting (not blocking inside
/// `__new__`). See `credential_provider.rs` for the full rationale.
struct DeferredState {
    cell: OnceCell<Arc<EngineProducer<RawBytes>>>,
    redis_url: String,
    cfg: ProducerConfig,
    // Derived once at construction so `stream_key`/`delayed_key`/
    // `dlq_key`/`producer_id` accessors stay sync — they used to read
    // from the live `EngineProducer`, but on the deferred path those
    // names are deterministic from the queue name, so we can compute
    // them up front without round-tripping through Redis.
    stream_key: String,
    delayed_key: String,
    dlq_key: String,
    producer_id: String,
}

enum ProducerState {
    Eager(Arc<EngineProducer<RawBytes>>),
    // Boxed to keep the enum compact; the deferred variant carries a
    // full `ProducerConfig` (~300 bytes) and `OnceCell`, so without
    // boxing every `Eager` clone would also drag the deferred payload
    // size through the Arc.
    Deferred(Box<DeferredState>),
}

#[pyclass(name = "Producer", module = "chasquimq._native")]
pub struct Producer {
    state: Arc<ProducerState>,
}

impl Producer {
    /// Get or initialize the underlying engine producer. Returns the
    /// shared `Arc` clone — never re-connects after the first success.
    async fn ensure_connected(
        state: Arc<ProducerState>,
    ) -> PyResult<Arc<EngineProducer<RawBytes>>> {
        match &*state {
            ProducerState::Eager(inner) => Ok(inner.clone()),
            ProducerState::Deferred(d) => {
                let inner = d
                    .cell
                    .get_or_try_init(|| async {
                        EngineProducer::<RawBytes>::connect(&d.redis_url, d.cfg.clone())
                            .await
                            .map(Arc::new)
                            .map_err(map_engine_err)
                    })
                    .await?;
                Ok(inner.clone())
            }
        }
    }
}

#[pymethods]
impl Producer {
    #[new]
    #[pyo3(signature = (
        redis_url,
        queue_name,
        *,
        pool_size = None,
        max_stream_len = None,
        max_delay_secs = None,
        max_payload_bytes = None,
        reconnect_max_attempts = None,
        credential_provider = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        redis_url: String,
        queue_name: String,
        pool_size: Option<u64>,
        max_stream_len: Option<u64>,
        max_delay_secs: Option<u64>,
        max_payload_bytes: Option<u64>,
        reconnect_max_attempts: Option<u32>,
        credential_provider: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut cfg = ProducerConfig {
            queue_name,
            ..Default::default()
        };
        if let Some(p) = pool_size {
            cfg.pool_size = p as usize;
        }
        if let Some(m) = max_stream_len {
            cfg.max_stream_len = m;
        }
        if let Some(d) = max_delay_secs {
            cfg.max_delay_secs = d;
        }
        if let Some(b) = max_payload_bytes {
            cfg.max_payload_bytes = b as usize;
        }
        // Set before the deferred-vs-eager branch so it lands on both
        // construction paths. `0` (engine default) = retry forever.
        if let Some(n) = reconnect_max_attempts {
            cfg.connection.reconnect_max_attempts = n;
        }
        if let Some(cb) = credential_provider {
            // Deferred construction. Capture TaskLocals NOW (we are
            // still on the calling thread with a live asyncio loop) and
            // forward into ConnectionTuning. Skip the
            // `block_on(connect)` step entirely — connecting eagerly
            // here would deadlock: fred's router task would call back
            // into Python via the provider's `fetch`, but the asyncio
            // loop is parked behind us in `block_on`. Instead the first
            // awaited method (`add`, `shutdown`, ...) calls
            // `ensure_connected`, which runs `EngineProducer::connect`
            // on the tokio runtime *while* the asyncio loop is free to
            // schedule the provider coroutine.
            let provider = PyCredentialProvider::new(py, cb)?;
            cfg.connection.credential_provider = Some(Arc::new(provider));
            // Key formats are part of the engine's stable wire layout
            // (see `chasquimq::redis::keys`). Pre-compute synchronously
            // so the sync accessors don't have to wait for connect.
            let stream_key = format!("{{chasqui:{}}}:stream", cfg.queue_name);
            let delayed_key = format!("{{chasqui:{}}}:delayed", cfg.queue_name);
            let dlq_key = format!("{{chasqui:{}}}:dlq", cfg.queue_name);
            // The engine assigns a fresh UUID per `EngineProducer::connect`
            // call (see `chasquimq::producer::Producer::connect`), which on
            // the deferred path hasn't happened yet. Report a self-describing
            // sentinel rather than a random v4 UUID: a fake UUID would look
            // like a real engine id while never matching one, so anyone
            // logging it pre-connect and correlating against Redis consumer-
            // group introspection post-connect would chase a phantom. The
            // sentinel still satisfies `producer_id().len() > 0` and makes
            // the not-yet-connected state legible at a glance.
            let producer_id = format!("deferred-{}-pending", cfg.queue_name);
            return Ok(Producer {
                state: Arc::new(ProducerState::Deferred(Box::new(DeferredState {
                    cell: OnceCell::new(),
                    redis_url,
                    cfg,
                    stream_key,
                    delayed_key,
                    dlq_key,
                    producer_id,
                }))),
            });
        }
        // Legacy eager path: no callback → connect synchronously, same
        // observable behaviour as before this slice.
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let inner = py
            .detach(|| {
                runtime.block_on(async move {
                    EngineProducer::<RawBytes>::connect(&redis_url, cfg).await
                })
            })
            .map_err(map_engine_err)?;
        Ok(Producer {
            state: Arc::new(ProducerState::Eager(Arc::new(inner))),
        })
    }

    fn stream_key(&self) -> String {
        match &*self.state {
            ProducerState::Eager(inner) => inner.stream_key().to_string(),
            ProducerState::Deferred(d) => d.stream_key.clone(),
        }
    }

    fn delayed_key(&self) -> String {
        match &*self.state {
            ProducerState::Eager(inner) => inner.delayed_key().to_string(),
            ProducerState::Deferred(d) => d.delayed_key.clone(),
        }
    }

    fn dlq_key(&self) -> String {
        match &*self.state {
            ProducerState::Eager(inner) => inner.dlq_key().to_string(),
            ProducerState::Deferred(d) => d.dlq_key.clone(),
        }
    }

    fn producer_id(&self) -> String {
        match &*self.state {
            ProducerState::Eager(inner) => inner.producer_id().to_string(),
            ProducerState::Deferred(d) => d.producer_id.clone(),
        }
    }

    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Deferred + never-connected: nothing to drain. Skip the
            // implicit connect so a Queue that was constructed-then-
            // closed without enqueuing anything doesn't pay a round trip.
            if let ProducerState::Deferred(d) = &*state
                && d.cell.get().is_none()
            {
                return Ok(());
            }
            let inner = Producer::ensure_connected(state).await?;
            inner.shutdown().await.map_err(map_engine_err)
        })
    }

    fn add<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = pybytes_to_bytes(payload);
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.add(RawBytes(bytes)).await.map_err(map_engine_err)
        })
    }

    fn add_with_options<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyBytes>,
        opts: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = pybytes_to_bytes(payload);
        let engine_opts = dict_to_add_options(opts)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_with_options(RawBytes(bytes), engine_opts)
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_in<'py>(
        &self,
        py: Python<'py>,
        delay_ms: i64,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dur = ms_to_duration(delay_ms)?;
        let bytes = pybytes_to_bytes(payload);
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_in(dur, RawBytes(bytes))
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_in_with_options<'py>(
        &self,
        py: Python<'py>,
        delay_ms: i64,
        payload: &Bound<'py, PyBytes>,
        opts: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dur = ms_to_duration(delay_ms)?;
        let bytes = pybytes_to_bytes(payload);
        let engine_opts = dict_to_add_options(opts)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_in_with_options(dur, RawBytes(bytes), engine_opts)
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_at<'py>(
        &self,
        py: Python<'py>,
        when_ms: i64,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let when = ms_to_systemtime(when_ms)?;
        let bytes = pybytes_to_bytes(payload);
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_at(when, RawBytes(bytes))
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_at_with_options<'py>(
        &self,
        py: Python<'py>,
        when_ms: i64,
        payload: &Bound<'py, PyBytes>,
        opts: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let when = ms_to_systemtime(when_ms)?;
        let bytes = pybytes_to_bytes(payload);
        let engine_opts = dict_to_add_options(opts)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_at_with_options(when, RawBytes(bytes), engine_opts)
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_bulk<'py>(
        &self,
        py: Python<'py>,
        payloads: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let raw = pylist_of_bytes_to_raw(payloads)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.add_bulk(raw).await.map_err(map_engine_err)
        })
    }

    fn add_bulk_with_options<'py>(
        &self,
        py: Python<'py>,
        payloads: &Bound<'py, PyList>,
        opts: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let raw = pylist_of_bytes_to_raw(payloads)?;
        let engine_opts = dict_to_add_options(opts)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .add_bulk_with_options(raw, engine_opts)
                .await
                .map_err(map_engine_err)
        })
    }

    fn add_bulk_named<'py>(
        &self,
        py: Python<'py>,
        items: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pairs = pylist_of_named_payloads(items)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.add_bulk_named(pairs).await.map_err(map_engine_err)
        })
    }

    fn add_in_bulk<'py>(
        &self,
        py: Python<'py>,
        delay_ms: i64,
        payloads: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dur = ms_to_duration(delay_ms)?;
        let raw = pylist_of_bytes_to_raw(payloads)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.add_in_bulk(dur, raw).await.map_err(map_engine_err)
        })
    }

    fn cancel_delayed<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.cancel_delayed(&job_id).await.map_err(map_engine_err)
        })
    }

    fn cancel_delayed_bulk<'py>(
        &self,
        py: Python<'py>,
        job_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .cancel_delayed_bulk(&job_ids)
                .await
                .map_err(map_engine_err)
        })
    }

    /// Durably pause every consumer of this queue (cross-process). Sets
    /// the `{chasqui:<queue>}:paused` key; survives consumer restarts
    /// until `resume()`. Idempotent.
    fn pause<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.pause().await.map_err(map_engine_err)
        })
    }

    /// Lift a durable pause set by `pause()`. Idempotent.
    fn resume<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.resume().await.map_err(map_engine_err)
        })
    }

    /// Whether this queue is durably paused via the cross-process key.
    fn is_paused<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.is_paused().await.map_err(map_engine_err)
        })
    }

    fn peek_dlq<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            let entries = inner.peek_dlq(lim).await.map_err(map_engine_err)?;
            Python::attach(|py| {
                let out = PyList::empty(py);
                for e in entries {
                    let d = PyDict::new(py);
                    d.set_item("dlq_id", e.dlq_id)?;
                    d.set_item("source_id", e.source_id)?;
                    d.set_item("reason", e.reason)?;
                    d.set_item("detail", e.detail)?;
                    d.set_item("payload", PyBytes::new(py, e.payload.as_ref()))?;
                    d.set_item("name", e.name)?;
                    out.append(d)?;
                }
                Ok::<_, PyErr>(out.unbind())
            })
        })
    }

    fn replay_dlq<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            let n = inner.replay_dlq(lim).await.map_err(map_engine_err)?;
            Ok(n as u64)
        })
    }

    fn upsert_repeatable<'py>(
        &self,
        py: Python<'py>,
        spec: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine_spec = dict_to_repeatable_spec(spec)?;
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner
                .upsert_repeatable(engine_spec)
                .await
                .map_err(map_engine_err)
        })
    }

    fn list_repeatable<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            let metas = inner.list_repeatable(lim).await.map_err(map_engine_err)?;
            Python::attach(|py| {
                let out = PyList::empty(py);
                for m in metas {
                    out.append(repeatable_meta_to_dict(py, m)?)?;
                }
                Ok::<_, PyErr>(out.unbind())
            })
        })
    }

    fn remove_repeatable_by_key<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            inner.remove_repeatable(&key).await.map_err(map_engine_err)
        })
    }

    fn get_result<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            let opt = inner.get_result(&id).await.map_err(map_engine_err)?;
            Python::attach(|py| match opt {
                Some(b) => Ok::<_, PyErr>(Some(PyBytes::new(py, b.as_ref()).unbind())),
                None => Ok(None),
            })
        })
    }

    fn get_result_bulk<'py>(
        &self,
        py: Python<'py>,
        ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Producer::ensure_connected(state).await?;
            let results = inner.get_result_bulk(&ids).await.map_err(map_engine_err)?;
            Python::attach(|py| {
                let out = PyList::empty(py);
                for opt in results {
                    match opt {
                        Some(b) => out.append(PyBytes::new(py, b.as_ref()))?,
                        None => out.append(py.None())?,
                    }
                }
                Ok::<_, PyErr>(out.unbind())
            })
        })
    }
}

fn pybytes_to_bytes(p: &Bound<'_, PyBytes>) -> Bytes {
    Bytes::copy_from_slice(p.as_bytes())
}

fn pylist_of_bytes_to_raw(items: &Bound<'_, PyList>) -> PyResult<Vec<RawBytes>> {
    let mut out: Vec<RawBytes> = Vec::with_capacity(items.len());
    for item in items.iter() {
        let pb: Bound<'_, PyBytes> = item
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("payload list entries must be `bytes` objects"))?;
        out.push(RawBytes(pybytes_to_bytes(&pb)));
    }
    Ok(out)
}

fn pylist_of_named_payloads(items: &Bound<'_, PyList>) -> PyResult<Vec<(String, RawBytes)>> {
    let mut out: Vec<(String, RawBytes)> = Vec::with_capacity(items.len());
    for item in items.iter() {
        let tup: (String, Bound<'_, PyBytes>) = item.extract().map_err(|_| {
            PyValueError::new_err("named payload entries must be (str, bytes) tuples")
        })?;
        out.push((tup.0, RawBytes(pybytes_to_bytes(&tup.1))));
    }
    Ok(out)
}

fn ms_to_duration(ms: i64) -> PyResult<Duration> {
    if ms < 0 {
        return Err(PyValueError::new_err(format!(
            "delay_ms must be non-negative; got {ms}"
        )));
    }
    Ok(Duration::from_millis(ms as u64))
}

fn ms_to_systemtime(ms: i64) -> PyResult<SystemTime> {
    if ms < 0 {
        return Err(PyValueError::new_err(format!(
            "when_ms must be non-negative; got {ms}"
        )));
    }
    Ok(UNIX_EPOCH + Duration::from_millis(ms as u64))
}

pub(crate) fn map_engine_err(e: chasquimq::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{e}"))
}

fn dict_to_add_options(d: &Bound<'_, PyDict>) -> PyResult<AddOptions> {
    let mut ao = AddOptions::new();
    if let Some(id) = d.get_item("id")? {
        if !id.is_none() {
            let s: String = id
                .extract()
                .map_err(|_| PyValueError::new_err("opts.id must be a string"))?;
            ao = ao.with_id(s);
        }
    }
    if let Some(retry) = d.get_item("retry")? {
        if !retry.is_none() {
            let retry_dict: Bound<'_, PyDict> = retry
                .cast_into::<PyDict>()
                .map_err(|_| PyValueError::new_err("opts.retry must be a dict or None"))?;
            let over = dict_to_retry_override(&retry_dict)?;
            ao = ao.with_retry(over);
        }
    }
    if let Some(name) = d.get_item("name")? {
        if !name.is_none() {
            let s: String = name
                .extract()
                .map_err(|_| PyValueError::new_err("opts.name must be a string"))?;
            ao = ao.with_name(s);
        }
    }
    Ok(ao)
}

fn dict_to_retry_override(d: &Bound<'_, PyDict>) -> PyResult<JobRetryOverride> {
    let mut over = JobRetryOverride {
        max_attempts: None,
        backoff: None,
    };
    if let Some(ma) = d.get_item("max_attempts")? {
        if !ma.is_none() {
            let v: u32 = ma.extract().map_err(|_| {
                PyValueError::new_err("retry.max_attempts must be a non-negative int")
            })?;
            over.max_attempts = Some(v);
        }
    }
    if let Some(b) = d.get_item("backoff")? {
        if !b.is_none() {
            let bd: Bound<'_, PyDict> = b
                .cast_into::<PyDict>()
                .map_err(|_| PyValueError::new_err("retry.backoff must be a dict or None"))?;
            over.backoff = Some(dict_to_backoff_spec(&bd)?);
        }
    }
    Ok(over)
}

fn dict_to_backoff_spec(d: &Bound<'_, PyDict>) -> PyResult<BackoffSpec> {
    let kind_v = d
        .get_item("kind")?
        .ok_or_else(|| PyValueError::new_err("backoff.kind is required"))?;
    let kind_s: String = kind_v
        .extract()
        .map_err(|_| PyValueError::new_err("backoff.kind must be a string"))?;
    let kind = match kind_s.as_str() {
        "fixed" => BackoffKind::Fixed,
        "exponential" => BackoffKind::Exponential,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown backoff kind {other:?}; expected 'fixed' or 'exponential'"
            )));
        }
    };
    let delay_ms = extract_required_u64(d, "delay_ms", "backoff.delay_ms")?;
    let max_delay_ms = extract_optional_u64(d, "max_delay_ms", "backoff.max_delay_ms")?;
    let multiplier = match d.get_item("multiplier")? {
        Some(v) if !v.is_none() => Some(
            v.extract::<f64>()
                .map_err(|_| PyValueError::new_err("backoff.multiplier must be a float"))?,
        ),
        _ => None,
    };
    let jitter_ms = extract_optional_u64(d, "jitter_ms", "backoff.jitter_ms")?;
    Ok(BackoffSpec {
        kind,
        delay_ms,
        max_delay_ms,
        multiplier,
        jitter_ms,
    })
}

fn extract_required_u64(d: &Bound<'_, PyDict>, key: &str, label: &str) -> PyResult<u64> {
    let v = d
        .get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("{label} is required")))?;
    if v.is_none() {
        return Err(PyValueError::new_err(format!("{label} is required")));
    }
    v.extract::<u64>()
        .map_err(|_| PyValueError::new_err(format!("{label} must be a non-negative int")))
}

fn extract_optional_u64(d: &Bound<'_, PyDict>, key: &str, label: &str) -> PyResult<Option<u64>> {
    match d.get_item(key)? {
        Some(v) if !v.is_none() => {
            Ok(Some(v.extract::<u64>().map_err(|_| {
                PyValueError::new_err(format!("{label} must be a non-negative int"))
            })?))
        }
        _ => Ok(None),
    }
}

fn dict_to_repeatable_spec(d: &Bound<'_, PyDict>) -> PyResult<RepeatableSpec<RawBytes>> {
    let key = match d.get_item("key")? {
        Some(v) if !v.is_none() => v
            .extract::<String>()
            .map_err(|_| PyValueError::new_err("spec.key must be a string"))?,
        _ => String::new(),
    };
    let job_name: String = d
        .get_item("job_name")?
        .ok_or_else(|| PyValueError::new_err("spec.job_name is required"))?
        .extract()
        .map_err(|_| PyValueError::new_err("spec.job_name must be a string"))?;
    let pattern_dict: Bound<'_, PyDict> = d
        .get_item("pattern")?
        .ok_or_else(|| PyValueError::new_err("spec.pattern is required"))?
        .cast_into::<PyDict>()
        .map_err(|_| PyValueError::new_err("spec.pattern must be a dict"))?;
    let pattern = dict_to_pattern(&pattern_dict)?;
    let payload_obj = d
        .get_item("payload")?
        .ok_or_else(|| PyValueError::new_err("spec.payload is required"))?;
    let payload_bytes: Bound<'_, PyBytes> = payload_obj
        .cast_into::<PyBytes>()
        .map_err(|_| PyValueError::new_err("spec.payload must be `bytes`"))?;
    let payload = RawBytes(pybytes_to_bytes(&payload_bytes));
    let limit = extract_optional_u64(d, "limit", "spec.limit")?;
    let start_after_ms = extract_optional_u64(d, "start_after_ms", "spec.start_after_ms")?;
    let end_before_ms = extract_optional_u64(d, "end_before_ms", "spec.end_before_ms")?;
    let missed_fires = match d.get_item("missed_fires")? {
        Some(v) if !v.is_none() => {
            let mf: Bound<'_, PyDict> = v
                .cast_into::<PyDict>()
                .map_err(|_| PyValueError::new_err("spec.missed_fires must be a dict or None"))?;
            dict_to_missed_fires(&mf)?
        }
        _ => MissedFiresPolicy::default(),
    };
    Ok(RepeatableSpec {
        key,
        job_name,
        pattern,
        payload,
        limit,
        start_after_ms,
        end_before_ms,
        missed_fires,
    })
}

fn dict_to_pattern(d: &Bound<'_, PyDict>) -> PyResult<RepeatPattern> {
    let kind: String = d
        .get_item("kind")?
        .ok_or_else(|| PyValueError::new_err("pattern.kind is required"))?
        .extract()
        .map_err(|_| PyValueError::new_err("pattern.kind must be a string"))?;
    match kind.as_str() {
        "cron" => {
            let expression: String = d
                .get_item("expression")?
                .ok_or_else(|| {
                    PyValueError::new_err("cron pattern requires `expression` (e.g. \"0 2 * * *\")")
                })?
                .extract()
                .map_err(|_| PyValueError::new_err("pattern.expression must be a string"))?;
            let tz = match d.get_item("tz")? {
                Some(v) if !v.is_none() => Some(
                    v.extract::<String>()
                        .map_err(|_| PyValueError::new_err("pattern.tz must be a string"))?,
                ),
                _ => None,
            };
            Ok(RepeatPattern::Cron { expression, tz })
        }
        "every" => {
            let interval_ms = extract_required_u64(d, "interval_ms", "pattern.interval_ms")?;
            if interval_ms == 0 {
                return Err(PyValueError::new_err("pattern.interval_ms must be > 0"));
            }
            Ok(RepeatPattern::Every { interval_ms })
        }
        other => Err(PyValueError::new_err(format!(
            "unknown pattern kind {other:?}; expected 'cron' or 'every'"
        ))),
    }
}

fn dict_to_missed_fires(d: &Bound<'_, PyDict>) -> PyResult<MissedFiresPolicy> {
    let kind: String = d
        .get_item("kind")?
        .ok_or_else(|| PyValueError::new_err("missed_fires.kind is required"))?
        .extract()
        .map_err(|_| PyValueError::new_err("missed_fires.kind must be a string"))?;
    match kind.as_str() {
        "skip" => Ok(MissedFiresPolicy::Skip),
        "fire-once" => Ok(MissedFiresPolicy::FireOnce),
        "fire-all" => {
            let max_catchup: u32 = match d.get_item("max_catchup")? {
                Some(v) if !v.is_none() => v.extract().map_err(|_| {
                    PyValueError::new_err("missed_fires.max_catchup must be a non-negative int")
                })?,
                _ => {
                    return Err(PyValueError::new_err(
                        "missed_fires.max_catchup is required when kind is 'fire-all'",
                    ));
                }
            };
            if max_catchup < 1 {
                return Err(PyValueError::new_err(format!(
                    "missed_fires.max_catchup must be a positive integer (>= 1), got {max_catchup}"
                )));
            }
            Ok(MissedFiresPolicy::FireAll { max_catchup })
        }
        other => Err(PyValueError::new_err(format!(
            "unknown missed-fires kind {other:?}; expected 'skip' / 'fire-once' / 'fire-all'"
        ))),
    }
}

fn repeatable_meta_to_dict(py: Python<'_>, m: RepeatableMeta) -> PyResult<Bound<'_, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("key", m.key)?;
    d.set_item("job_name", m.job_name)?;
    d.set_item("pattern", pattern_to_dict(py, &m.pattern)?)?;
    d.set_item("next_fire_ms", m.next_fire_ms)?;
    d.set_item("limit", m.limit)?;
    d.set_item("start_after_ms", m.start_after_ms)?;
    d.set_item("end_before_ms", m.end_before_ms)?;
    d.set_item("missed_fires", missed_fires_to_dict(py, &m.missed_fires)?)?;
    Ok(d)
}

fn missed_fires_to_dict<'py>(
    py: Python<'py>,
    p: &MissedFiresPolicy,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    match p {
        // `Skip` is the engine default and is omitted from the stored
        // spec by `skip_serializing_if`. Surface it as `None` on the
        // Python side so callers can use `if meta["missed_fires"] is
        // None` as the "default policy" idiom.
        MissedFiresPolicy::Skip => Ok(None),
        MissedFiresPolicy::FireOnce => {
            let d = PyDict::new(py);
            d.set_item("kind", "fire-once")?;
            Ok(Some(d))
        }
        MissedFiresPolicy::FireAll { max_catchup } => {
            let d = PyDict::new(py);
            d.set_item("kind", "fire-all")?;
            d.set_item("max_catchup", max_catchup)?;
            Ok(Some(d))
        }
    }
}

fn pattern_to_dict<'py>(py: Python<'py>, p: &RepeatPattern) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    match p {
        RepeatPattern::Cron { expression, tz } => {
            d.set_item("kind", "cron")?;
            d.set_item("expression", expression)?;
            d.set_item("tz", tz.clone())?;
        }
        RepeatPattern::Every { interval_ms } => {
            d.set_item("kind", "every")?;
            d.set_item("interval_ms", *interval_ms)?;
        }
    }
    Ok(d)
}
