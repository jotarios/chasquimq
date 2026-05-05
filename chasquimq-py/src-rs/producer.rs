//! `NativeProducer` — async PyO3 wrapper over `chasquimq::Producer<RawBytes>`.
//!
//! Mirrors `chasquimq-node/src/producer.rs` 1:1. JS dicts become PyDicts;
//! validation patterns (unknown `kind`, non-finite floats, negative ms)
//! are the same. Every async method returns a Python awaitable via
//! `pyo3_async_runtimes::tokio::future_into_py`.

use crate::payload::RawBytes;
use bytes::Bytes;
use chasquimq::config::ProducerConfig;
use chasquimq::producer::{AddOptions, Producer};
use chasquimq::repeat::{MissedFiresPolicy, RepeatPattern, RepeatableSpec};
use chasquimq::{BackoffKind, BackoffSpec, JobRetryOverride, RepeatableMeta};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[pyclass(name = "NativeProducer", module = "chasquimq._native")]
pub struct NativeProducer {
    inner: Arc<Producer<RawBytes>>,
}

#[pymethods]
impl NativeProducer {
    #[new]
    #[pyo3(signature = (
        redis_url,
        queue_name,
        *,
        pool_size = None,
        max_stream_len = None,
        max_delay_secs = None,
    ))]
    fn new(
        py: Python<'_>,
        redis_url: String,
        queue_name: String,
        pool_size: Option<u64>,
        max_stream_len: Option<u64>,
        max_delay_secs: Option<u64>,
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
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let inner = py
            .detach(|| {
                runtime
                    .block_on(async move { Producer::<RawBytes>::connect(&redis_url, cfg).await })
            })
            .map_err(map_engine_err)?;
        Ok(NativeProducer {
            inner: Arc::new(inner),
        })
    }

    fn stream_key(&self) -> String {
        self.inner.stream_key().to_string()
    }

    fn delayed_key(&self) -> String {
        self.inner.delayed_key().to_string()
    }

    fn dlq_key(&self) -> String {
        self.inner.dlq_key().to_string()
    }

    fn producer_id(&self) -> String {
        self.inner.producer_id().to_string()
    }

    fn add<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bytes = pybytes_to_bytes(payload);
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.add_in_bulk(dur, raw).await.map_err(map_engine_err)
        })
    }

    fn cancel_delayed<'py>(&self, py: Python<'py>, job_id: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.cancel_delayed(&job_id).await.map_err(map_engine_err)
        })
    }

    fn cancel_delayed_bulk<'py>(
        &self,
        py: Python<'py>,
        job_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .cancel_delayed_bulk(&job_ids)
                .await
                .map_err(map_engine_err)
        })
    }

    fn peek_dlq<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner
                .upsert_repeatable(engine_spec)
                .await
                .map_err(map_engine_err)
        })
    }

    fn list_repeatable<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let lim = limit as usize;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.remove_repeatable(&key).await.map_err(map_engine_err)
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

fn map_engine_err(e: chasquimq::Error) -> PyErr {
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
                _ => 100,
            };
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
    Ok(d)
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
