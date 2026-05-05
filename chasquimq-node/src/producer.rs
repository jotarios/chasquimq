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
use chasquimq::producer::{AddOptions, Producer};
use chasquimq::repeat::{MissedFiresPolicy, RepeatPattern, RepeatableSpec};
use chasquimq::{BackoffKind, BackoffSpec, JobRetryOverride};
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
    /// Dispatch name preserved from the source stream entry's `n` field at
    /// DLQ-route time. Empty when the entry had no `n` (legacy producers,
    /// or reader-side malformed routes). Replayed verbatim by `replayDlq`.
    pub name: String,
}

/// Per-job backoff override carried in [`NativeJobRetryOverride::backoff`].
///
/// Mirrors `chasquimq::BackoffSpec` 1:1. `kind` is the backoff strategy
/// — `"fixed"` or `"exponential"` — anything else is rejected with an
/// `Error` at the FFI boundary so a typo in JS doesn't silently route
/// through the engine's `BackoffKind::Unknown` forward-compat sink.
#[napi(object)]
pub struct NativeBackoffSpec {
    /// `"fixed"` | `"exponential"`. Mirrors `BackoffKind` lowercase tags.
    pub kind: String,
    /// Base delay in milliseconds (`f64` so JS can pass plain `number`s
    /// without BigInt; non-negative integer values up to 2^53-1 round-trip
    /// safely).
    pub delay_ms: f64,
    pub max_delay_ms: Option<f64>,
    pub multiplier: Option<f64>,
    pub jitter_ms: Option<f64>,
}

/// Per-job retry override carried in [`NativeAddOptions::retry`].
///
/// Mirrors `chasquimq::JobRetryOverride`. When `max_attempts` and / or
/// `backoff` are set, they override the queue-wide `ConsumerConfig`
/// values for this specific job.
#[napi(object)]
pub struct NativeJobRetryOverride {
    pub max_attempts: Option<u32>,
    pub backoff: Option<NativeBackoffSpec>,
}

/// Options for the `*_with_options` family of producer methods.
///
/// Mirrors `chasquimq::producer::AddOptions`. `id` is the stable
/// [`chasquimq::JobId`] for at-most-once / idempotent scheduling;
/// `retry` carries per-job retry overrides; `name` is the dispatch
/// name surfaced to the worker via the stream entry's `n` field
/// (capped at 256 bytes by the engine).
#[napi(object)]
pub struct NativeAddOptions {
    pub id: Option<String>,
    pub retry: Option<NativeJobRetryOverride>,
    pub name: Option<String>,
}

/// Per-entry pair for [`NativeProducer::add_bulk_named`].
#[napi(object)]
pub struct NativeNamedPayload {
    pub name: String,
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

    /// Like `add`, but accepts a [`NativeAddOptions`] carrying an optional
    /// stable id and / or per-job retry override. Maps to
    /// `Producer::add_with_options` on the engine.
    #[napi]
    pub async fn add_with_options(
        &self,
        payload: Buffer,
        opts: NativeAddOptions,
    ) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let engine_opts = to_engine_add_options(opts)?;
        self.inner
            .add_with_options(RawBytes(bytes), engine_opts)
            .await
            .map_err(map_engine_err)
    }

    /// Like `addIn`, but accepts a [`NativeAddOptions`]. When `opts.id`
    /// is set, routes through the idempotent delayed-schedule path
    /// (same dedup-marker semantics as `addInWithId`). The retry
    /// override always rides inside the encoded `Job<T>`.
    #[napi]
    pub async fn add_in_with_options(
        &self,
        delay_ms: i64,
        payload: Buffer,
        opts: NativeAddOptions,
    ) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let dur = ms_to_duration(delay_ms)?;
        let engine_opts = to_engine_add_options(opts)?;
        self.inner
            .add_in_with_options(dur, RawBytes(bytes), engine_opts)
            .await
            .map_err(map_engine_err)
    }

    /// Like `addAt`, but accepts a [`NativeAddOptions`]. Same dedup /
    /// retry-carry model as [`Self::add_in_with_options`].
    #[napi]
    pub async fn add_at_with_options(
        &self,
        run_at_ms: i64,
        payload: Buffer,
        opts: NativeAddOptions,
    ) -> napi::Result<String> {
        let bytes = buffer_to_bytes(&payload);
        let when = ms_to_systemtime(run_at_ms)?;
        let engine_opts = to_engine_add_options(opts)?;
        self.inner
            .add_at_with_options(when, RawBytes(bytes), engine_opts)
            .await
            .map_err(map_engine_err)
    }

    /// Bulk variant of [`Self::add_with_options`]. The same retry override
    /// is applied to every job in the batch. Setting `opts.id` when
    /// `payloads.len() > 1` is rejected by the engine — propagate the
    /// error verbatim through `map_engine_err`.
    #[napi]
    pub async fn add_bulk_with_options(
        &self,
        payloads: Vec<Buffer>,
        opts: NativeAddOptions,
    ) -> napi::Result<Vec<String>> {
        let raw: Vec<RawBytes> = payloads
            .iter()
            .map(|b| RawBytes(buffer_to_bytes(b)))
            .collect();
        let engine_opts = to_engine_add_options(opts)?;
        self.inner
            .add_bulk_with_options(raw, engine_opts)
            .await
            .map_err(map_engine_err)
    }

    /// Bulk variant that accepts a per-entry `(name, payload)` pair so each
    /// job carries its own dispatch name through the stream entry's `n`
    /// field. Each name is validated against the engine's 256-byte cap before
    /// any XADD is issued; an oversize name fails the whole call atomically.
    #[napi]
    pub async fn add_bulk_named(
        &self,
        items: Vec<NativeNamedPayload>,
    ) -> napi::Result<Vec<String>> {
        let pairs: Vec<(String, RawBytes)> = items
            .into_iter()
            .map(|it| (it.name, RawBytes(buffer_to_bytes(&it.payload))))
            .collect();
        self.inner
            .add_bulk_named(pairs)
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
                name: e.name,
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
            // `intervalMs <= 0` would hot-loop the scheduler (every tick the
            // computed next-fire is already in the past, so the spec
            // re-schedules immediately). Reject up-front. NaN / non-finite
            // is rejected by the same guard.
            if !interval_ms.is_finite() || interval_ms <= 0.0 {
                return Err(napi::Error::from_reason(format!(
                    "intervalMs must be > 0; got {interval_ms}"
                )));
            }
            Ok(RepeatPattern::Every {
                interval_ms: f64_to_u64(interval_ms, "intervalMs")?,
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
            // `u32` field — already bounded by the NAPI value type.
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
        Some(n) => Ok(Some(f64_to_u64(n, name)?)),
    }
}

/// Convert an `f64` (incoming JS `number`) into a `u64` while rejecting
/// non-finite, negative, or out-of-range values. Without this guard, a
/// silent `as u64` cast would saturate on `+Infinity`, alias huge floats
/// (since `f64` has only 53 bits of mantissa), and produce surprising
/// `0` on negatives via `as` truncation — all bug-shaped behavior at the
/// FFI boundary. Cap chosen as `u64::MAX as f64` (exact representation,
/// covers the entire engine-side `u64` range); the caller code paths
/// using this helper (`intervalMs`, `limit`, `startAfterMs`,
/// `endBeforeMs`, `maxCatchup` would-be inputs) all sit comfortably
/// below `2^53` in practice, so the bound never bites real callers.
fn f64_to_u64(n: f64, field: &str) -> napi::Result<u64> {
    if !n.is_finite() || n < 0.0 || n > (u64::MAX as f64) {
        return Err(napi::Error::from_reason(format!(
            "{field} out of range: {n}"
        )));
    }
    Ok(n as u64)
}

/// Translate a `NativeBackoffSpec` (JS-side, `f64` numbers, string `kind`)
/// into the engine's `BackoffSpec`. Unknown `kind` values are rejected
/// at the FFI boundary so a typo in JS surfaces as an Error rather than
/// silently routing through `BackoffKind::Unknown`'s exponential
/// fallback. Numeric fields go through `f64_to_u64` (errors on
/// non-finite / negative / out-of-range values) so a `-100ms` slip is
/// surfaced rather than silently clamped.
fn to_engine_backoff(spec: NativeBackoffSpec) -> napi::Result<BackoffSpec> {
    let kind = match spec.kind.as_str() {
        "fixed" => BackoffKind::Fixed,
        "exponential" => BackoffKind::Exponential,
        other => {
            return Err(napi::Error::from_reason(format!(
                "unknown backoff kind {other:?}; expected 'fixed' or 'exponential'"
            )));
        }
    };
    Ok(BackoffSpec {
        kind,
        delay_ms: f64_to_u64(spec.delay_ms, "backoff.delayMs")?,
        max_delay_ms: spec
            .max_delay_ms
            .map(|v| f64_to_u64(v, "backoff.maxDelayMs"))
            .transpose()?,
        multiplier: spec.multiplier,
        jitter_ms: spec
            .jitter_ms
            .map(|v| f64_to_u64(v, "backoff.jitterMs"))
            .transpose()?,
    })
}

fn to_engine_add_options(opts: NativeAddOptions) -> napi::Result<AddOptions> {
    let mut ao = AddOptions::new();
    if let Some(id) = opts.id {
        ao = ao.with_id(id);
    }
    if let Some(retry) = opts.retry {
        let mut over = JobRetryOverride {
            max_attempts: retry.max_attempts,
            backoff: None,
        };
        if let Some(b) = retry.backoff {
            over.backoff = Some(to_engine_backoff(b)?);
        }
        ao = ao.with_retry(over);
    }
    if let Some(name) = opts.name {
        ao = ao.with_name(name);
    }
    Ok(ao)
}
