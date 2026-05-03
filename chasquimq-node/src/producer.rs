//! `NativeProducer` — thin N-API wrapper over `chasquimq::Producer<RawBytes>`.
//!
//! Every method shape mirrors the engine 1:1; the only translation work is
//! `Buffer` <-> `Bytes` and the millisecond / `Duration` plumbing. We use
//! plain JS `number` (Rust `i64`) for ms parameters: 2^53-1 milliseconds is
//! year 287396 AD, which is past the engine's `max_delay_secs` cap by
//! several orders of magnitude. No `BigInt` ergonomic tax for the common
//! "schedule N seconds from now" call.

use crate::payload::RawBytes;
use crate::repeat::{
    NativeMissedFiresPolicy, NativeRepeatPattern, NativeRepeatableMeta, NativeRepeatableSpec,
};
use bytes::Bytes;
use chasquimq::config::ProducerConfig;
use chasquimq::producer::Producer;
use chasquimq::repeat::{MissedFiresPolicy, RepeatPattern, RepeatableSpec};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[napi(object)]
pub struct NativeProducerOpts {
    pub queue_name: Option<String>,
    pub pool_size: Option<u32>,
    /// Cap on `XADD MAXLEN ~`. Default matches the engine: 1_000_000.
    pub max_stream_len: Option<i64>,
    /// Reject `addIn` / `addAt` calls whose delay exceeds this many seconds.
    /// Default matches the engine: 30 days.
    pub max_delay_secs: Option<i64>,
}

#[napi(object)]
pub struct NativeDlqEntry {
    pub dlq_id: String,
    pub source_id: String,
    pub reason: String,
    pub detail: Option<String>,
    pub payload: Buffer,
}

#[napi]
pub struct NativeProducer {
    inner: Arc<Producer<RawBytes>>,
}

#[napi]
impl NativeProducer {
    /// Connect a producer pool against `redisUrl`. Async because the
    /// underlying `fred::Pool::connect` is async.
    #[napi(factory)]
    pub async fn connect(
        redis_url: String,
        opts: Option<NativeProducerOpts>,
    ) -> napi::Result<NativeProducer> {
        let cfg = build_producer_config(opts);
        let inner = Producer::<RawBytes>::connect(&redis_url, cfg)
            .await
            .map_err(map_engine_err)?;
        Ok(NativeProducer {
            inner: Arc::new(inner),
        })
    }

    #[napi]
    pub async fn add(&self, payload: Buffer) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        self.inner
            .add(RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_with_id(&self, id: String, payload: Buffer) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        self.inner
            .add_with_id(id, RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_bulk(&self, payloads: Vec<Buffer>) -> napi::Result<Vec<String>> {
        let raw: Vec<RawBytes> = payloads
            .iter()
            .map(|b| RawBytes(buffer_to_bytes(b)))
            .collect();
        self.inner.add_bulk(raw).await.map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_in(&self, delay_ms: i64, payload: Buffer) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let dur = ms_to_duration(delay_ms)?;
        self.inner
            .add_in(dur, RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_at(&self, run_at_ms: i64, payload: Buffer) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let when = ms_to_systemtime(run_at_ms)?;
        self.inner
            .add_at(when, RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_in_bulk(
        &self,
        delay_ms: i64,
        payloads: Vec<Buffer>,
    ) -> napi::Result<Vec<String>> {
        let dur = ms_to_duration(delay_ms)?;
        let raw: Vec<RawBytes> = payloads
            .iter()
            .map(|b| RawBytes(buffer_to_bytes(b)))
            .collect();
        self.inner
            .add_in_bulk(dur, raw)
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_in_with_id(
        &self,
        id: String,
        delay_ms: i64,
        payload: Buffer,
    ) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let dur = ms_to_duration(delay_ms)?;
        self.inner
            .add_in_with_id(id, dur, RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn add_at_with_id(
        &self,
        id: String,
        run_at_ms: i64,
        payload: Buffer,
    ) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let when = ms_to_systemtime(run_at_ms)?;
        self.inner
            .add_at_with_id(id, when, RawBytes(bytes))
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn cancel_delayed(&self, id: String) -> napi::Result<bool> {
        self.inner.cancel_delayed(&id).await.map_err(map_engine_err)
    }

    #[napi]
    pub async fn cancel_delayed_bulk(&self, ids: Vec<String>) -> napi::Result<Vec<bool>> {
        self.inner
            .cancel_delayed_bulk(&ids)
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub async fn peek_dlq(&self, limit: u32) -> napi::Result<Vec<NativeDlqEntry>> {
        let entries = self
            .inner
            .peek_dlq(limit as usize)
            .await
            .map_err(map_engine_err)?;
        Ok(entries
            .into_iter()
            .map(|e| NativeDlqEntry {
                dlq_id: e.dlq_id,
                source_id: e.source_id,
                reason: e.reason,
                detail: e.detail,
                // The engine hands us `Bytes` from XRANGE; one copy at the
                // FFI boundary into a Node-managed Buffer is unavoidable.
                payload: Buffer::from(e.payload.to_vec()),
            })
            .collect())
    }

    #[napi]
    pub async fn replay_dlq(&self, limit: u32) -> napi::Result<u32> {
        let n = self
            .inner
            .replay_dlq(limit as usize)
            .await
            .map_err(map_engine_err)?;
        Ok(n as u32)
    }

    /// Upsert a repeatable spec. Returns the resolved spec key (auto-derived
    /// from `<jobName>::<patternSignature>` when `spec.key` is empty / None).
    /// Re-upserting with the same key overwrites the spec and re-anchors the
    /// next fire time.
    #[napi]
    pub async fn upsert_repeatable(&self, spec: NativeRepeatableSpec) -> napi::Result<String> {
        let engine_spec = native_spec_into_engine(spec)?;
        self.inner
            .upsert_repeatable(engine_spec)
            .await
            .map_err(map_engine_err)
    }

    /// List repeatable specs ordered by next fire time, ascending. Returns
    /// up to `limit` entries. Pass a generous `limit` for full inventory —
    /// payloads are intentionally **not** included (see [`NativeRepeatableMeta`]).
    #[napi]
    pub async fn list_repeatable(&self, limit: u32) -> napi::Result<Vec<NativeRepeatableMeta>> {
        let metas = self
            .inner
            .list_repeatable(limit as usize)
            .await
            .map_err(map_engine_err)?;
        Ok(metas.into_iter().map(engine_meta_into_native).collect())
    }

    /// Remove a repeatable spec by key. Returns `true` if a spec was removed,
    /// `false` if no spec with that key existed.
    #[napi]
    pub async fn remove_repeatable(&self, key: String) -> napi::Result<bool> {
        self.inner
            .remove_repeatable(&key)
            .await
            .map_err(map_engine_err)
    }

    #[napi]
    pub fn stream_key(&self) -> String {
        self.inner.stream_key().to_string()
    }

    #[napi]
    pub fn delayed_key(&self) -> String {
        self.inner.delayed_key().to_string()
    }

    #[napi]
    pub fn dlq_key(&self) -> String {
        self.inner.dlq_key().to_string()
    }

    #[napi]
    pub fn producer_id(&self) -> String {
        self.inner.producer_id().to_string()
    }
}

fn build_producer_config(opts: Option<NativeProducerOpts>) -> ProducerConfig {
    let mut cfg = ProducerConfig::default();
    if let Some(o) = opts {
        if let Some(q) = o.queue_name {
            cfg.queue_name = q;
        }
        if let Some(p) = o.pool_size {
            cfg.pool_size = p as usize;
        }
        if let Some(m) = o.max_stream_len {
            if m >= 0 {
                cfg.max_stream_len = m as u64;
            }
        }
        if let Some(d) = o.max_delay_secs {
            if d >= 0 {
                cfg.max_delay_secs = d as u64;
            }
        }
    }
    cfg
}

fn buffer_to_bytes(buf: &Buffer) -> Bytes {
    // One copy: the Node-side Buffer is GC'd, so we cannot hold its memory
    // across the await. The engine wants `Bytes`, which we satisfy by
    // copying the slice into a fresh allocation. This is the unavoidable
    // FFI cost on the produce path; bench it under slice (e).
    Bytes::copy_from_slice(buf.as_ref())
}

fn ms_to_duration(ms: i64) -> napi::Result<Duration> {
    if ms < 0 {
        return Err(napi::Error::from_reason(format!(
            "delay_ms must be non-negative; got {ms}"
        )));
    }
    Ok(Duration::from_millis(ms as u64))
}

fn ms_to_systemtime(ms: i64) -> napi::Result<SystemTime> {
    if ms < 0 {
        return Err(napi::Error::from_reason(format!(
            "run_at_ms must be non-negative; got {ms}"
        )));
    }
    Ok(UNIX_EPOCH + Duration::from_millis(ms as u64))
}

pub(crate) fn map_engine_err(e: chasquimq::Error) -> napi::Error {
    napi::Error::from_reason(format!("{e}"))
}

fn native_pattern_into_engine(p: NativeRepeatPattern) -> napi::Result<RepeatPattern> {
    match p.kind.as_str() {
        "cron" => {
            let expression = p.expression.ok_or_else(|| {
                napi::Error::from_reason("cron pattern requires `expression` (e.g. \"0 2 * * *\")")
            })?;
            Ok(RepeatPattern::Cron {
                expression,
                tz: p.tz,
            })
        }
        "every" => {
            let interval_ms = p
                .interval_ms
                .ok_or_else(|| napi::Error::from_reason("every pattern requires `intervalMs`"))?;
            if !interval_ms.is_finite() || interval_ms < 0.0 {
                return Err(napi::Error::from_reason(format!(
                    "intervalMs must be a non-negative finite number; got {interval_ms}"
                )));
            }
            Ok(RepeatPattern::Every {
                interval_ms: interval_ms as u64,
            })
        }
        other => Err(napi::Error::from_reason(format!(
            "unknown pattern kind {other:?}; expected 'cron' or 'every'"
        ))),
    }
}

fn engine_pattern_into_native(p: RepeatPattern) -> NativeRepeatPattern {
    match p {
        RepeatPattern::Cron { expression, tz } => NativeRepeatPattern {
            kind: "cron".to_string(),
            expression: Some(expression),
            tz,
            interval_ms: None,
        },
        RepeatPattern::Every { interval_ms } => NativeRepeatPattern {
            kind: "every".to_string(),
            expression: None,
            tz: None,
            interval_ms: Some(interval_ms as f64),
        },
    }
}

fn native_missed_fires_into_engine(p: NativeMissedFiresPolicy) -> napi::Result<MissedFiresPolicy> {
    match p.kind.as_str() {
        "skip" => Ok(MissedFiresPolicy::Skip),
        "fire-once" => Ok(MissedFiresPolicy::FireOnce),
        "fire-all" => Ok(MissedFiresPolicy::FireAll {
            // Default cap matches the engine's typical-cron-catch-up budget;
            // callers wanting unbounded replay must pass `maxCatchup` explicitly.
            max_catchup: p.max_catchup.unwrap_or(100),
        }),
        other => Err(napi::Error::from_reason(format!(
            "unknown missed-fires kind {other:?}; expected 'skip' / 'fire-once' / 'fire-all'"
        ))),
    }
}

fn native_spec_into_engine(spec: NativeRepeatableSpec) -> napi::Result<RepeatableSpec<RawBytes>> {
    let pattern = native_pattern_into_engine(spec.pattern)?;
    let missed_fires = match spec.missed_fires {
        None => MissedFiresPolicy::default(),
        Some(p) => native_missed_fires_into_engine(p)?,
    };
    Ok(RepeatableSpec {
        key: spec.key.unwrap_or_default(),
        job_name: spec.job_name,
        pattern,
        payload: RawBytes(buffer_to_bytes(&spec.payload)),
        limit: f64_opt_to_u64_opt(spec.limit, "limit")?,
        start_after_ms: f64_opt_to_u64_opt(spec.start_after_ms, "startAfterMs")?,
        end_before_ms: f64_opt_to_u64_opt(spec.end_before_ms, "endBeforeMs")?,
        missed_fires,
    })
}

fn engine_meta_into_native(m: chasquimq::RepeatableMeta) -> NativeRepeatableMeta {
    NativeRepeatableMeta {
        key: m.key,
        job_name: m.job_name,
        pattern: engine_pattern_into_native(m.pattern),
        next_fire_ms: m.next_fire_ms as f64,
        limit: m.limit.map(|v| v as f64),
        start_after_ms: m.start_after_ms.map(|v| v as f64),
        end_before_ms: m.end_before_ms.map(|v| v as f64),
    }
}

fn f64_opt_to_u64_opt(v: Option<f64>, name: &str) -> napi::Result<Option<u64>> {
    match v {
        None => Ok(None),
        Some(n) if !n.is_finite() || n < 0.0 => Err(napi::Error::from_reason(format!(
            "{name} must be a non-negative finite number; got {n}"
        ))),
        Some(n) => Ok(Some(n as u64)),
    }
}
