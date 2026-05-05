//! `NativeConsumer` — N-API wrapper over `chasquimq::Consumer<RawBytes>`.
//!
//! The hard part is the JS handler bridge: each engine worker, when it
//! pulls a `Job<RawBytes>` off the stream, hands it across the libuv
//! boundary via a `ThreadsafeFunction` (TSFN), awaits the JS-returned
//! `Promise<void>`, and translates resolution/rejection back into the
//! engine's `Result<(), HandlerError>` shape.
//!
//! Shutdown is signal-based: `NativeConsumer::shutdown` cancels a
//! `CancellationToken` shared with the engine. `run` resolves once the
//! engine's drain (workers, ack flusher, DLQ relocator, retry relocator,
//! optional in-process promoter) all settle.

use crate::payload::RawBytes;
use crate::producer::map_engine_err;
use chasquimq::config::{ConsumerConfig, RetryConfig};
use chasquimq::consumer::Consumer;
use chasquimq::{HandlerError, Job};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, UnknownReturnValue};
use napi_derive::napi;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[napi(object)]
pub struct NativeRetryOpts {
    pub initial_backoff_ms: Option<i64>,
    pub max_backoff_ms: Option<i64>,
    pub multiplier: Option<f64>,
    pub jitter_ms: Option<i64>,
}

#[napi(object)]
pub struct NativeConsumerOpts {
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
    pub retry: Option<NativeRetryOpts>,
    pub delayed_enabled: Option<bool>,
}

#[napi(object)]
pub struct NativeJob {
    pub id: String,
    /// Dispatch name from the source stream entry's `n` field. Empty when
    /// the entry had no `n` (legacy producers, delayed-path re-encodes,
    /// repeatable scheduler fires).
    pub name: String,
    pub payload: Buffer,
    /// `i64` so JS can read it as a regular `number` (safe up to 2^53-1
    /// ms ≈ year 287396; far past any realistic Job timestamp). Using
    /// BigInt here would force every JS handler to do `Number(ts)`
    /// arithmetic, which we'd rather not impose on the hot path.
    pub created_at_ms: i64,
    pub attempt: u32,
}

#[napi]
pub struct NativeConsumer {
    redis_url: String,
    cfg: ConsumerConfig,
    shutdown: Arc<CancellationToken>,
}

#[napi]
impl NativeConsumer {
    #[napi(constructor)]
    pub fn new(redis_url: String, opts: Option<NativeConsumerOpts>) -> napi::Result<Self> {
        let cfg = build_consumer_config(opts);
        Ok(Self {
            redis_url,
            cfg,
            shutdown: Arc::new(CancellationToken::new()),
        })
    }

    /// Run the consumer loop. Resolves once the engine drains.
    ///
    /// `handler` is a JS `(job: NativeJob) => Promise<void>`. A resolved
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
    #[napi(ts_args_type = "handler: (job: NativeJob) => Promise<void>")]
    pub async fn run(
        &self,
        // `ErrorStrategy::Fatal` — the JS handler is invoked with **only**
        // the job arg, *not* a Node-style `(err, job) => ...`. Conversion
        // failures from Rust to JS values would `panic!`, but our
        // `NativeJob` is a plain struct of strings / Buffer / numbers, so
        // there is no realistic conversion-failure path. The default
        // (`CalleeHandled`) would prepend a `null` error arg and break
        // the natural `(job) => Promise<void>` signature this whole
        // binding is designed around.
        handler: ThreadsafeFunction<NativeJob, ErrorStrategy::Fatal>,
    ) -> napi::Result<()> {
        let consumer = Consumer::<RawBytes>::new(self.redis_url.clone(), self.cfg.clone());
        let shutdown = (*self.shutdown).clone();
        let tsfn = Arc::new(handler);

        consumer
            .run(
                move |job: Job<RawBytes>| {
                    let tsfn = tsfn.clone();
                    async move {
                        // One copy at the FFI boundary (engine `Bytes` →
                        // Node-managed `Buffer`). See
                        // `docs/phase3-napi-design.md` §4 — the
                        // throughput-path price for keeping the binding
                        // schema-agnostic.
                        let js_job = NativeJob {
                            id: job.id,
                            name: job.name,
                            payload: Buffer::from(job.payload.0.to_vec()),
                            created_at_ms: clamp_u64_to_i64(job.created_at_ms),
                            attempt: job.attempt,
                        };

                        // `call_async::<Promise<UnknownReturnValue>>` says
                        // "the JS handler returns a Promise; I don't care
                        // what it resolves with, just whether it
                        // resolves." That matches `JobHandler = (job) =>
                        // Promise<void>` exactly.
                        //
                        // With `Fatal` strategy `call_async` takes the
                        // value directly (not `Result<T>`).
                        match tsfn.call_async::<Promise<UnknownReturnValue>>(js_job).await {
                            Ok(promise) => match promise.await {
                                Ok(_) => Ok(()),
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
}

fn build_consumer_config(opts: Option<NativeConsumerOpts>) -> ConsumerConfig {
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
    }
    cfg
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
fn map_js_rejection(e: &napi::Error) -> HandlerError {
    let reason = &e.reason;
    if is_unrecoverable_prefix(reason) {
        HandlerError::unrecoverable(JsHandlerError(format!("JS handler rejected: {reason}")))
    } else {
        HandlerError::new(JsHandlerError(format!("JS handler rejected: {reason}")))
    }
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
