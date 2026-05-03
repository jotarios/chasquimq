//! `NativeProducer` — thin N-API wrapper over `chasquimq::Producer<RawBytes>`.
//!
//! Every method shape mirrors the engine 1:1; the only translation work is
//! `Buffer` <-> `Bytes` and the millisecond / `Duration` plumbing. We use
//! plain JS `number` (Rust `i64`) for ms parameters: 2^53-1 milliseconds is
//! year 287396 AD, which is past the engine's `max_delay_secs` cap by
//! several orders of magnitude. No `BigInt` ergonomic tax for the common
//! "schedule N seconds from now" call.

use crate::payload::RawBytes;
use bytes::Bytes;
use chasquimq::config::ProducerConfig;
use chasquimq::producer::Producer;
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
