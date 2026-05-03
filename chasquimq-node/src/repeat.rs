//! NAPI value types for repeatable / cron jobs.
//!
//! These mirror [`chasquimq::repeat`] but use the JS-friendly shapes the
//! `#[napi(object)]` machinery accepts: tagged enums become `kind` strings
//! (`"cron"` / `"every"`, `"skip"` / `"fire-once"` / `"fire-all"`), `u64` ms
//! values become `f64` (JS `number`), and the `payload` is the same opaque
//! `Buffer` the rest of the producer surface uses.
//!
//! Translation between these types and the engine's typed enums lives in
//! `producer.rs::upsert_repeatable` / `list_repeatable`. Failure to translate
//! (unknown `kind`, missing required field) becomes a `napi::Error`.

use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

#[napi(object)]
pub struct NativeRepeatPattern {
    /// `"cron"` or `"every"`.
    pub kind: String,
    /// Cron expression. Required when `kind == "cron"`. Ignored otherwise.
    pub expression: Option<String>,
    /// Cron timezone — `"UTC"`, fixed offset (`"+05:30"`), or any IANA name
    /// (`"America/New_York"`). Ignored when `kind == "every"`.
    pub tz: Option<String>,
    /// Fixed millisecond interval. Required when `kind == "every"`.
    /// Ignored when `kind == "cron"`. `f64` so JS `number` round-trips —
    /// 2^53 ms is well past any realistic cadence.
    pub interval_ms: Option<f64>,
}

#[napi(object)]
pub struct NativeMissedFiresPolicy {
    /// `"skip"` (default), `"fire-once"`, or `"fire-all"`.
    pub kind: String,
    /// Required when `kind == "fire-all"`. Ignored otherwise. `u32` is
    /// plenty — the per-tick build is capped at 10_000 by the scheduler.
    pub max_catchup: Option<u32>,
}

#[napi(object)]
pub struct NativeRepeatableSpec {
    /// Stable spec key. Empty / missing → engine derives one as
    /// `<jobName>::<patternSignature>`.
    pub key: Option<String>,
    pub job_name: String,
    pub pattern: NativeRepeatPattern,
    /// Opaque (msgpack-encoded by the high-level shim, but the binding
    /// doesn't care) payload bytes. Same convention as `add()`.
    pub payload: Buffer,
    pub limit: Option<f64>,
    pub start_after_ms: Option<f64>,
    pub end_before_ms: Option<f64>,
    pub missed_fires: Option<NativeMissedFiresPolicy>,
}

#[napi(object)]
pub struct NativeRepeatableMeta {
    pub key: String,
    pub job_name: String,
    pub pattern: NativeRepeatPattern,
    pub next_fire_ms: f64,
    pub limit: Option<f64>,
    pub start_after_ms: Option<f64>,
    pub end_before_ms: Option<f64>,
}
