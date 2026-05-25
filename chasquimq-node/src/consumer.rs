//! `Consumer` — N-API wrapper over `chasquimq::Consumer<RawBytes>`.
//!
//! The hard part is the JS handler bridge: each engine worker, when it
//! pulls a `Job<RawBytes>` off the stream, hands it across the libuv
//! boundary via a `ThreadsafeFunction` (TSFN), awaits the JS-returned
//! `Promise<void>`, and translates resolution/rejection back into the
//! engine's `Result<(), HandlerError>` shape.
//!
//! Shutdown is signal-based: `Consumer::shutdown` cancels a
//! `CancellationToken` shared with the engine. `run` resolves once the
//! engine's drain (workers, ack flusher, DLQ relocator, retry relocator,
//! optional in-process promoter) all settle.

use crate::credential_provider::{CredentialProviderTsfn, build_js_credential_provider};
use crate::payload::RawBytes;
use crate::producer::map_engine_err;
use bytes::Bytes;
use chasquimq::config::{ConsumerConfig, RetryConfig};
use chasquimq::consumer::Consumer as EngineConsumer;
use chasquimq::{HandlerError, Job as EngineJob, JobHandle as EngineJobHandle, PauseControl};
use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi_derive::napi;
use std::ptr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[napi(object)]
pub struct RetryOpts {
    pub initial_backoff_ms: Option<i64>,
    pub max_backoff_ms: Option<i64>,
    pub multiplier: Option<f64>,
    pub jitter_ms: Option<i64>,
}

#[napi(object)]
pub struct ConsumerOpts {
    pub queue_name: Option<String>,
    pub group: Option<String>,
    pub consumer_id: Option<String>,
    pub batch: Option<u32>,
    pub block_ms: Option<i64>,
    pub claim_min_idle_ms: Option<i64>,
    pub concurrency: Option<u32>,
    pub max_attempts: Option<u32>,
    pub ack_batch: Option<u32>,
    pub ack_idle_ms: Option<i64>,
    pub shutdown_deadline_secs: Option<i64>,
    pub max_payload_bytes: Option<u32>,
    pub retry: Option<RetryOpts>,
    pub delayed_enabled: Option<bool>,
    pub run_scheduler: Option<bool>,
    pub scheduler_tick_ms: Option<i64>,
    /// When `true`, the engine writes the handler's returned `Buffer`
    /// to `{chasqui:<queue>}:result:<job_id>` with TTL `resultTtlMs`,
    /// readable via `Producer.getResult(jobId)`. Default `false`.
    pub store_results: Option<bool>,
    /// Result-key TTL in milliseconds when `storeResults = true`.
    /// Default 3,600,000 (1h). Internally rounded to whole seconds.
    pub result_ttl_ms: Option<i64>,
    /// Cap on fred's exponential reconnect attempts. `0` (the engine
    /// default) = retry forever. Set a positive value so a permanently
    /// rejecting `credentialProvider` gives up instead of looping
    /// forever on reconnect. Maps to `ConnectionTuning::reconnect_max_attempts`.
    pub reconnect_max_attempts: Option<u32>,
    /// `MAXLEN ~` cap on the per-job log stream
    /// (`{chasqui:<queue>}:log:<id>`). Default `1000`. Must be `>= 16`;
    /// below that, Redis's `MAXLEN ~` rounding can leave the stream
    /// effectively empty between writes. Maps to
    /// `ConsumerConfig::log_max_stream_len`.
    pub log_max_len: Option<u32>,
    /// Per-line byte cap for `Job.log`. Oversize lines are truncated on
    /// a UTF-8 char boundary with a `[…truncated]` marker appended.
    /// Default `4096`. Maps to `ConsumerConfig::log_max_line_bytes`.
    pub log_max_line_bytes: Option<u32>,
    /// Gate on the engine's `e=progress` events-stream entry. The
    /// persisted progress key is always written; setting this to `false`
    /// only mutes the events fan-out (useful when a hot-loop handler
    /// would otherwise flood the events stream). Default `true`. Maps
    /// to `ConsumerConfig::events_progress_enabled`.
    pub events_progress_enabled: Option<bool>,
    /// Maximum stall attempts before the stalled-job detector relocates
    /// the entry to the DLQ with `DlqReason::Stalled`. Maps to engine
    /// `stalled_detector.max_stalled_attempts`. Default `1`.
    pub max_stalled_attempts: Option<u32>,
    /// Toggle the embedded stalled-job detector. Maps to
    /// `ConsumerConfig::stalled_detector_enabled`. Default `true`.
    pub stalled_detector_enabled: Option<bool>,
    /// Override the detector's tick interval (ms). Maps to
    /// `stalled_detector.tick_interval_ms`. Note: the embedded spawn
    /// overrides this from `claim_min_idle_ms` to preserve the
    /// per-crash counting invariant; setting this only matters when
    /// the operator is running the detector standalone. Default
    /// inherits from `claim_min_idle_ms`.
    pub stalled_detector_tick_ms: Option<i64>,
    /// Override the detector's `XPENDING ... IDLE` threshold (ms).
    /// Maps to `stalled_detector.idle_threshold_ms`. Same embedded-
    /// spawn-override note as `stalled_detector_tick_ms`.
    pub stalled_detector_idle_threshold_ms: Option<i64>,
    /// Override the detector's per-tick scan cap. Maps to
    /// `stalled_detector.scan_batch`. Default `256`.
    pub stalled_detector_scan_batch: Option<u32>,
}

/// `Job` is a `#[napi]` class (not a plain object) so it can carry the
/// engine's `Arc<JobHandle>` opaquely across the FFI boundary and expose
/// `updateProgress` / `log` as async instance methods. The data fields
/// are surfaced via `#[napi(getter)]` to preserve the same JS-side
/// `(job.id, job.name, ...)` access shape the previous plain-object
/// binding had — high-level shim consumers do not need to change.
///
/// `payload` is stored as `Vec<u8>` (not `Buffer`) so the class is
/// `Send + Sync` and the async `updateProgress` / `log` methods can
/// await across thread boundaries; a fresh `Buffer` is materialized in
/// the getter (one copy per JS access, same per-job-dispatch cost as
/// before — the previous binding also copied once at the FFI edge).
#[napi]
pub struct Job {
    id: String,
    name: String,
    payload: Vec<u8>,
    created_at_ms: i64,
    attempt: u32,
    /// `Some` for jobs the engine's worker dispatched to a handler
    /// (the only path the consumer runs through). `None` would only
    /// occur on synthesized `Job` instances — none exist on the NAPI
    /// surface today, but the read-only branch is mirrored in the
    /// TypeScript shim's `Job` so users get a clear "this Job came from
    /// getJob() and is read-only" error.
    handle: Option<Arc<EngineJobHandle>>,
}

#[napi]
impl Job {
    #[napi(getter)]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Dispatch name from the source stream entry's `n` field. Empty when
    /// the entry had no `n` (legacy producers, delayed-path re-encodes,
    /// repeatable scheduler fires).
    #[napi(getter)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[napi(getter)]
    pub fn payload(&self) -> Buffer {
        Buffer::from(self.payload.clone())
    }

    /// `i64` so JS can read it as a regular `number` (safe up to 2^53-1
    /// ms ≈ year 287396; far past any realistic Job timestamp). Using
    /// BigInt here would force every JS handler to do `Number(ts)`
    /// arithmetic, which we'd rather not impose on the hot path.
    #[napi(getter, js_name = "createdAtMs")]
    pub fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[napi(getter)]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Update the persisted progress key for this job. Values outside
    /// `0..=100` are clamped at the engine boundary; the call resolves
    /// once the SET round trip completes. A `JobHandle` is attached only
    /// when the engine's worker dispatched this job to a handler; this
    /// rejects with an error when the job has no backref (the high-level
    /// shim then re-throws as the read-only-Job guard).
    #[napi(js_name = "updateProgress")]
    pub async fn update_progress(&self, n: u32) -> napi::Result<()> {
        let handle = self.handle.as_ref().ok_or_else(|| {
            napi::Error::from_reason(
                "Job.updateProgress() requires a live JobHandle (this Job has none)",
            )
        })?;
        let clamped = n.min(u8::MAX as u32) as u8;
        handle
            .update_progress(clamped)
            .await
            .map_err(map_engine_err)
    }

    /// Append `line` to the per-job log stream and return the new XLEN
    /// (one XADD + XLEN, pipelined). Oversize lines are truncated on a
    /// UTF-8 char boundary with a `[…truncated]` marker; the engine
    /// reads the cap from `ConsumerConfig::log_max_line_bytes`.
    #[napi]
    pub async fn log(&self, line: String) -> napi::Result<u32> {
        let handle = self.handle.as_ref().ok_or_else(|| {
            napi::Error::from_reason("Job.log() requires a live JobHandle (this Job has none)")
        })?;
        let len = handle.log(&line).await.map_err(map_engine_err)?;
        Ok(len.min(u32::MAX as u64) as u32)
    }
}

#[napi]
pub struct Consumer {
    redis_url: String,
    cfg: ConsumerConfig,
    shutdown: Arc<CancellationToken>,
    // Constructed here (not in `run`) so `pause()` / `resume()` are
    // callable on the JS handle before the run future is awaited and from
    // any thread, mirroring the `shutdown` token's sharing model.
    pause: Arc<PauseControl>,
}

#[napi]
impl Consumer {
    /// `credentialProvider` is an opt-in JS callback fred invokes on every
    /// reconnect / `AUTH` cycle. Passed through to
    /// `ConnectionTuning::credential_provider` on the engine's
    /// `ConsumerConfig`; mirrors the producer's `connect` signature so the
    /// high-level `Worker` shim can plumb the same `connection.credentialProvider`
    /// into both sides of the pipeline. `None` (the common path) leaves the
    /// engine on its default auth-from-URL behaviour.
    #[napi(
        constructor,
        ts_args_type = "redisUrl: string, opts?: ConsumerOpts | undefined | null, credentialProvider?: ((host: string | null) => Promise<CredentialResponseJs>) | undefined | null"
    )]
    pub fn new(
        redis_url: String,
        opts: Option<ConsumerOpts>,
        credential_provider: Option<CredentialProviderTsfn>,
    ) -> napi::Result<Self> {
        let mut cfg = build_consumer_config(opts)?;
        if let Some(tsfn) = credential_provider {
            cfg.connection.credential_provider = Some(build_js_credential_provider(tsfn));
        }
        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
            pause: Arc::new(PauseControl::new()),
        })
    }

    /// Run the consumer loop. Resolves once the engine drains.
    ///
    /// `handler` is a JS `(job: Job) => Promise<void>`. A resolved
    /// promise → `XACK`. A rejected promise → `HandlerError` (engine
    /// retries with backoff up to `maxAttempts`, then DLQ).
    ///
    /// **Unrecoverable errors:** if the JS rejection's `error.name`
    /// is `'UnrecoverableError'`, the binding maps it to
    /// `HandlerError::unrecoverable(...)` instead of `HandlerError::new(...)`.
    /// The engine then short-circuits the retry budget and routes the
    /// job straight to the DLQ with `DlqReason::Unrecoverable`. Detection
    /// works off the rejection's stringified prefix — `JsUnknown` gets
    /// `coerce_to_string`'d, which for a JS `Error` produces
    /// `"<name>: <message>"`. We match the prefix `"UnrecoverableError"`
    /// followed by either `:` (the standard `Error.toString()` form) or
    /// end-of-string (an `UnrecoverableError` thrown with no message).
    ///
    /// **Return value:** the JS handler's resolved `Promise<Buffer | undefined | null>`
    /// is plumbed through to the engine's result backend. `null` /
    /// `undefined` → ack-only path. A `Buffer` is forwarded as the
    /// result bytes (gated by `storeResults`). A non-Buffer / non-nullish
    /// resolution silently collapses to the ack-only path with a
    /// `tracing::warn!` so it surfaces in operator logs without failing
    /// the job — matches the Python shim's behavior.
    #[napi(ts_args_type = "handler: (job: Job) => Promise<Buffer | undefined | null>")]
    pub async fn run(
        &self,
        // `ErrorStrategy::Fatal` — the JS handler is invoked with **only**
        // the job arg, *not* a Node-style `(err, job) => ...`. Conversion
        // failures from Rust to JS values would `panic!`, but our
        // `Job` is a plain struct of strings / Buffer / numbers, so
        // there is no realistic conversion-failure path. The default
        // (`CalleeHandled`) would prepend a `null` error arg and break
        // the natural `(job) => Promise<void>` signature this whole
        // binding is designed around.
        handler: ThreadsafeFunction<Job, ErrorStrategy::Fatal>,
    ) -> napi::Result<()> {
        let consumer = EngineConsumer::<RawBytes>::with_pause_control(
            self.redis_url.clone(),
            self.cfg.clone(),
            self.pause.clone(),
        );
        let shutdown = (*self.shutdown).clone();
        let tsfn = Arc::new(handler);

        consumer
            .run(
                move |job: EngineJob<RawBytes>| {
                    let tsfn = tsfn.clone();
                    async move {
                        // One copy at the FFI boundary (engine `Bytes` →
                        // Node-managed `Buffer`). See
                        // `docs/phase3-napi-design.md` §4 — the
                        // throughput-path price for keeping the binding
                        // schema-agnostic.
                        let job_id_for_log = job.id.clone();
                        let js_job = Job {
                            id: job.id,
                            name: job.name,
                            payload: job.payload.0.to_vec(),
                            created_at_ms: clamp_u64_to_i64(job.created_at_ms),
                            attempt: job.attempt,
                            handle: job.handle,
                        };

                        // `call_async::<Promise<HandlerReturn>>` resolves
                        // the JS handler's returned Promise. The custom
                        // `HandlerReturn` decoder runs inside napi's
                        // `then` callback (on the JS thread) and copies
                        // the Buffer's bytes — if any — into a Vec, so
                        // we never carry napi pointers across threads.
                        // It accepts:
                        //   - Buffer        → `HandlerReturn::Bytes(...)`
                        //   - null / undef  → `HandlerReturn::None`
                        //   - anything else → `HandlerReturn::Skipped`
                        //                     (silent collapse to ack-only,
                        //                     parity with the Python shim)
                        // Real Promise rejections still take the catch
                        // path and route through `map_js_rejection`.
                        match tsfn.call_async::<Promise<HandlerReturn>>(js_job).await {
                            Ok(promise) => match promise.await {
                                Ok(HandlerReturn::Bytes(v)) => Ok(Bytes::from(v)),
                                Ok(HandlerReturn::None) => Ok(Bytes::new()),
                                Ok(HandlerReturn::Skipped) => {
                                    tracing::warn!(
                                        job_id = %job_id_for_log,
                                        "handler returned non-Buffer/non-nullish; result skipped"
                                    );
                                    Ok(Bytes::new())
                                }
                                Err(e) => Err(map_js_rejection(&e)),
                            },
                            Err(e) => Err(HandlerError::new(JsHandlerError(format!(
                                "TSFN call failed: {e}"
                            )))),
                        }
                    }
                },
                shutdown,
            )
            .await
            .map_err(map_engine_err)
    }

    /// Signal graceful shutdown. Idempotent; safe to call multiple times.
    /// The matching `run()` future resolves once the engine drains.
    #[napi]
    pub fn shutdown(&self) -> napi::Result<()> {
        self.shutdown.cancel();
        Ok(())
    }

    /// Pause this consumer's reader at the next batch boundary. In-flight
    /// jobs already handed to handlers run to completion; no new jobs are
    /// dispatched until `resume()`. Process-local (the `Worker.pause()`
    /// path) — does not write the cross-process Redis pause key.
    /// Idempotent; safe from any thread.
    #[napi]
    pub fn pause(&self) -> napi::Result<()> {
        self.pause.pause();
        Ok(())
    }

    /// Resume a paused reader. The parked reader wakes immediately (no
    /// poll-interval latency for the in-process path). Idempotent.
    #[napi]
    pub fn resume(&self) -> napi::Result<()> {
        self.pause.resume();
        Ok(())
    }

    /// Current in-process pause state. Does not reflect a cross-process
    /// pause set via `chasqui pause` / `Queue.pause()`.
    #[napi]
    pub fn is_paused(&self) -> napi::Result<bool> {
        Ok(self.pause.is_paused())
    }
}

fn build_consumer_config(opts: Option<ConsumerOpts>) -> napi::Result<ConsumerConfig> {
    let mut cfg = ConsumerConfig::default();
    if let Some(o) = opts {
        if let Some(v) = o.queue_name {
            cfg.queue_name = v;
        }
        if let Some(v) = o.group {
            cfg.group = v;
        }
        if let Some(v) = o.consumer_id {
            cfg.consumer_id = v;
        }
        if let Some(v) = o.batch {
            cfg.batch = v as usize;
        }
        if let Some(v) = o.block_ms {
            if v >= 0 {
                cfg.block_ms = v as u64;
            }
        }
        if let Some(v) = o.claim_min_idle_ms {
            if v >= 0 {
                cfg.claim_min_idle_ms = v as u64;
            }
        }
        if let Some(v) = o.concurrency {
            cfg.concurrency = (v as usize).max(1);
        }
        if let Some(v) = o.max_attempts {
            cfg.max_attempts = v;
        }
        if let Some(v) = o.ack_batch {
            cfg.ack_batch = v as usize;
        }
        if let Some(v) = o.ack_idle_ms {
            if v >= 0 {
                cfg.ack_idle_ms = v as u64;
            }
        }
        if let Some(v) = o.shutdown_deadline_secs {
            if v >= 0 {
                cfg.shutdown_deadline_secs = v as u64;
            }
        }
        if let Some(v) = o.max_payload_bytes {
            cfg.max_payload_bytes = v as usize;
        }
        if let Some(v) = o.delayed_enabled {
            cfg.delayed_enabled = v;
        }
        if let Some(v) = o.run_scheduler {
            cfg.run_scheduler = v;
        }
        if let Some(v) = o.scheduler_tick_ms {
            if v >= 0 {
                cfg.scheduler.tick_interval_ms = v as u64;
            }
        }
        if let Some(v) = o.store_results {
            cfg.store_results = v;
        }
        if let Some(v) = o.result_ttl_ms {
            // Reject 0 / negative explicitly; matches the Python shim's
            // PyRuntimeError. `undefined` (None) is still legitimate and
            // falls through to the engine default (3,600,000 ms).
            if v <= 0 {
                return Err(napi::Error::from_reason(format!(
                    "resultTtlMs must be > 0; got {v}"
                )));
            }
            // Engine TTL is in seconds; round up so a sub-second value
            // doesn't collapse to zero (which Redis rejects on `EX`).
            cfg.result_ttl_secs = (v as u64).div_ceil(1000);
        }
        if let Some(r) = o.retry {
            let mut rc = RetryConfig::default();
            if let Some(v) = r.initial_backoff_ms {
                if v >= 0 {
                    rc.initial_backoff_ms = v as u64;
                }
            }
            if let Some(v) = r.max_backoff_ms {
                if v >= 0 {
                    rc.max_backoff_ms = v as u64;
                }
            }
            if let Some(v) = r.multiplier {
                rc.multiplier = v;
            }
            if let Some(v) = r.jitter_ms {
                if v >= 0 {
                    rc.jitter_ms = v as u64;
                }
            }
            cfg.retry = rc;
        }
        if let Some(n) = o.reconnect_max_attempts {
            cfg.connection.reconnect_max_attempts = n;
        }
        if let Some(v) = o.log_max_len {
            cfg.log_max_stream_len = v as u64;
        }
        if let Some(v) = o.log_max_line_bytes {
            cfg.log_max_line_bytes = v as usize;
        }
        if let Some(v) = o.events_progress_enabled {
            cfg.events_progress_enabled = v;
        }
        if let Some(v) = o.stalled_detector_enabled {
            cfg.stalled_detector_enabled = v;
        }
        if let Some(v) = o.max_stalled_attempts {
            cfg.stalled_detector.max_stalled_attempts = v;
        }
        if let Some(v) = o.stalled_detector_tick_ms {
            if v < 0 {
                return Err(napi::Error::from_reason(format!(
                    "stalledDetectorTickMs must be non-negative; got {v}"
                )));
            }
            cfg.stalled_detector.tick_interval_ms = v as u64;
        }
        if let Some(v) = o.stalled_detector_idle_threshold_ms {
            if v < 0 {
                return Err(napi::Error::from_reason(format!(
                    "stalledDetectorIdleThresholdMs must be non-negative; got {v}"
                )));
            }
            cfg.stalled_detector.idle_threshold_ms = v as u64;
        }
        if let Some(v) = o.stalled_detector_scan_batch {
            cfg.stalled_detector.scan_batch = v as usize;
        }
    }
    Ok(cfg)
}

fn clamp_u64_to_i64(v: u64) -> i64 {
    // The engine stores `created_at_ms` as `u64`; JS reads `i64`. A future
    // timestamp past i64::MAX is impossible (year 292 million AD), but be
    // defensive.
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// Wrapper around a JS-side error message so the engine's `HandlerError`
/// (which expects `std::error::Error + Send + Sync + 'static`) has
/// something to box. The wrapped string is the raw `Display` of the JS
/// `Error`, including its `name` and `message`.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct JsHandlerError(String);

/// Decoded form of the JS handler's resolved Promise. The decoder runs
/// inside napi's `then` callback (on the JS thread) and copies any
/// Buffer's bytes into a `Vec<u8>` so the resulting value is `Send` —
/// no napi pointers are carried back to the tokio worker.
///
/// Variants:
/// - `Bytes(Vec<u8>)` — handler resolved with a `Buffer`.
/// - `None` — handler resolved with `null` or `undefined`.
/// - `Skipped` — handler resolved with anything else (number, object,
///   string, ...). Treated as "no result"; the consumer logs a warn
///   and ack-only's the job.
#[derive(Debug)]
enum HandlerReturn {
    Bytes(Vec<u8>),
    None,
    Skipped,
}

impl FromNapiValue for HandlerReturn {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let mut val_type = 0;
        let status = unsafe { sys::napi_typeof(env, napi_val, &mut val_type) };
        if status != sys::Status::napi_ok {
            return Ok(HandlerReturn::Skipped);
        }
        if val_type == sys::ValueType::napi_undefined || val_type == sys::ValueType::napi_null {
            return Ok(HandlerReturn::None);
        }
        let mut is_buffer = false;
        let st2 = unsafe { sys::napi_is_buffer(env, napi_val, &mut is_buffer) };
        if st2 != sys::Status::napi_ok || !is_buffer {
            return Ok(HandlerReturn::Skipped);
        }
        let mut data = ptr::null_mut();
        let mut len: usize = 0;
        let st3 = unsafe { sys::napi_get_buffer_info(env, napi_val, &mut data, &mut len) };
        if st3 != sys::Status::napi_ok {
            return Ok(HandlerReturn::Skipped);
        }
        if len == 0 || data.is_null() {
            return Ok(HandlerReturn::Bytes(Vec::new()));
        }
        // Copy into an owned Vec; the underlying Buffer storage is owned
        // by V8 and may be GC'd after the `then` callback returns.
        let slice = unsafe { std::slice::from_raw_parts(data as *const u8, len) };
        Ok(HandlerReturn::Bytes(slice.to_vec()))
    }
}

/// Translate a `napi::Error` produced by a rejected JS Promise into the
/// engine's `HandlerError`. When the rejection's stringified form starts
/// with `UnrecoverableError` (followed by `:` or end-of-string), this
/// returns `HandlerError::unrecoverable(...)` so the consumer routes the
/// job straight to the DLQ. Every other rejection follows the standard
/// retry-then-DLQ path via `HandlerError::new(...)`.
///
/// Why prefix-matching: `napi::Error::from(JsUnknown)` calls
/// `coerce_to_string` on the rejected value, which for a `Error` object
/// produces `"<error.name>: <error.message>"`. There's no reliable way
/// to read the JS `.name` property back out of a moved-out `napi::Error`
/// in a tokio context, so the prefix is the cheap, allocation-free way
/// to detect the marker class.
///
/// The carried error message is the user's `Error.message` only — the
/// `<ErrorName>: ` prefix added by `coerce_to_string` is stripped so the
/// `failedReason` surfaced on the events stream / `failed` event is the
/// raw user string (e.g. `new Error("smtp timeout")` → `"smtp timeout"`).
fn map_js_rejection(e: &napi::Error) -> HandlerError {
    let reason = &e.reason;
    let unrecoverable = is_unrecoverable_prefix(reason);
    let message = strip_js_error_name_prefix(reason);
    if unrecoverable {
        HandlerError::unrecoverable(JsHandlerError(message))
    } else {
        HandlerError::new(JsHandlerError(message))
    }
}

/// Strip the `"<ErrorName>: "` prefix that JS's `Error.toString()` (and
/// napi's `coerce_to_string`) adds to a rejected Error value. Returns the
/// suffix verbatim, or the input unchanged when no recognizable prefix
/// matches (e.g. the rejected value was a string or a non-Error object).
///
/// The prefix shape is `<JsIdentifier>: <message>` — match a leading
/// run of identifier-safe characters followed by `": "`. A bare error
/// name with no trailing `": "` (a `coerce_to_string` of `new Error()`
/// with no message) collapses to the empty string.
fn strip_js_error_name_prefix(s: &str) -> String {
    if let Some(idx) = s.find(": ") {
        let head = &s[..idx];
        if !head.is_empty() && head.chars().all(is_js_ident_char) {
            return s[idx + 2..].to_string();
        }
    }
    // Bare `"ErrorName"` (no message) — collapse to empty for parity with
    // `new Error("")` whose `.message` is `""`.
    if !s.is_empty() && s.chars().all(is_js_ident_char) {
        return String::new();
    }
    s.to_string()
}

fn is_js_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '$'
}

fn is_unrecoverable_prefix(s: &str) -> bool {
    const TAG: &str = "UnrecoverableError";
    if let Some(rest) = s.strip_prefix(TAG) {
        // Match either the standard `Error.toString()` form
        // (`"UnrecoverableError: <msg>"`) or a bare `Error` with no
        // message (`coerce_to_string` then yields `"UnrecoverableError"`).
        rest.is_empty() || rest.starts_with(':')
    } else {
        false
    }
}

#[cfg(test)]
mod prefix_tests {
    use super::is_unrecoverable_prefix;

    #[test]
    fn matches_standard_tostring_form() {
        assert!(is_unrecoverable_prefix("UnrecoverableError: boom"));
    }

    #[test]
    fn matches_bare_name_with_no_message() {
        assert!(is_unrecoverable_prefix("UnrecoverableError"));
    }

    #[test]
    fn rejects_other_named_errors() {
        assert!(!is_unrecoverable_prefix("Error: boom"));
        assert!(!is_unrecoverable_prefix("RateLimitError: too fast"));
        assert!(!is_unrecoverable_prefix("NotSupportedError: nope"));
    }

    #[test]
    fn rejects_substring_collisions() {
        // A user-named class containing the literal substring must not
        // match — the prefix must be the full token followed by `:` or end.
        assert!(!is_unrecoverable_prefix("MyUnrecoverableError: boom"));
        assert!(!is_unrecoverable_prefix("UnrecoverableErrorInfo: boom"));
    }
}

#[cfg(test)]
mod strip_prefix_tests {
    use super::strip_js_error_name_prefix;

    #[test]
    fn strips_standard_error_prefix() {
        assert_eq!(
            strip_js_error_name_prefix("Error: smtp timeout"),
            "smtp timeout"
        );
    }

    #[test]
    fn strips_custom_named_error() {
        assert_eq!(
            strip_js_error_name_prefix("UnrecoverableError: poison pill"),
            "poison pill"
        );
        assert_eq!(
            strip_js_error_name_prefix("RangeError: out of bounds"),
            "out of bounds"
        );
    }

    #[test]
    fn collapses_bare_error_name_to_empty() {
        // `new Error()` with no message → `coerce_to_string` yields just
        // `"Error"`. Match parity: `.message` is `""`.
        assert_eq!(strip_js_error_name_prefix("Error"), "");
        assert_eq!(strip_js_error_name_prefix("UnrecoverableError"), "");
    }

    #[test]
    fn passes_through_non_error_strings() {
        // A rejected string / non-Error value: leave verbatim.
        assert_eq!(
            strip_js_error_name_prefix("string rejection"),
            "string rejection"
        );
        assert_eq!(strip_js_error_name_prefix("foo: bar"), "bar");
        // No `": "` pair, no all-ident → passthrough.
        assert_eq!(strip_js_error_name_prefix("foo bar baz"), "foo bar baz");
    }

    #[test]
    fn message_with_colon_keeps_first_split() {
        // `new Error("a: b")` → `"Error: a: b"`. Strip only the first `: `.
        assert_eq!(strip_js_error_name_prefix("Error: a: b"), "a: b");
    }
}
