use crate::config::RetryConfig;
use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::job::{BackoffKind, BackoffSpec, Job};
use crate::metrics::{self, MetricsSink, RetryScheduled};
use crate::redis::commands::{
    RETRY_RESCHEDULE_SCRIPT, eval_retry_args, evalsha_retry_args, script_load_args,
};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::IgnoredAny;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Latches `true` the first time a [`BackoffKind::Unknown`] variant is
/// observed by [`backoff_ms_from_spec`]. The warning then fires exactly
/// once per process — per-job logging would be too spammy on a busy
/// queue with mixed-version producers, but a single breadcrumb is the
/// right operator-visibility cost when a new producer SDK has rolled
/// out ahead of consumers.
static WARNED_UNKNOWN_KIND: AtomicBool = AtomicBool::new(false);

const RETRY_REDIS_ATTEMPTS: usize = 3;
const RETRY_REDIS_BASE_MS: u64 = 50;

#[derive(Debug)]
pub(crate) struct RetryRelocate {
    pub entry_id: StreamEntryId,
    pub job_bytes: Bytes,
    pub run_at_ms: i64,
    /// Attempt number the rescheduled job will run as
    /// (i.e. `previous_attempt + 1`). Carried so the metric event records
    /// "the retry that's about to happen" without having to decode the
    /// payload on the relocator hot path.
    pub attempt: u32,
    /// Backoff that was applied at enqueue time. Carried for the metric.
    pub backoff_ms: u64,
}

pub(crate) struct RetryRelocatorConfig {
    pub stream_key: String,
    pub delayed_key: String,
    pub group: String,
    pub metrics: Arc<dyn MetricsSink>,
    pub events: EventsWriter,
}

pub(crate) async fn enqueue(
    tx: &mpsc::Sender<RetryRelocate>,
    entry_id: StreamEntryId,
    job_bytes: Bytes,
    run_at_ms: i64,
    attempt: u32,
    backoff_ms: u64,
) {
    if tx
        .send(RetryRelocate {
            entry_id,
            job_bytes,
            run_at_ms,
            attempt,
            backoff_ms,
        })
        .await
        .is_err()
    {
        tracing::error!("retry relocator channel closed; retry dropped");
    }
}

pub(crate) async fn run_retry_relocator(
    client: Client,
    cfg: RetryRelocatorConfig,
    mut rx: mpsc::Receiver<RetryRelocate>,
) {
    let mut sha = match load_script(&client).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "retry relocator: SCRIPT LOAD failed; entries will reclaim via CLAIM");
            return;
        }
    };
    while let Some(relocate) = rx.recv().await {
        match reschedule_with_retry(&client, &cfg, &relocate, &mut sha).await {
            Ok(true) => {
                let event = RetryScheduled {
                    attempt: relocate.attempt,
                    backoff_ms: relocate.backoff_ms,
                };
                let sink = &*cfg.metrics;
                metrics::dispatch("retry_scheduled", || sink.retry_scheduled(event));
                // Mirror the MetricsSink gating: only emit on the actual
                // reschedule, never on a lost XACKDEL race. We need the
                // job id, which the relocator doesn't carry — decode it
                // from the encoded payload bytes. Cost: one rmp_serde
                // decode of just the `id` field via `IgnoredAny`. If the
                // bytes don't decode (corrupt payload), skip the event;
                // the metric still fires.
                if cfg.events.is_enabled() {
                    if let Some(job_id) = extract_job_id(&relocate.job_bytes) {
                        cfg.events
                            .emit_retry_scheduled(&job_id, relocate.attempt, relocate.backoff_ms)
                            .await;
                    }
                }
            }
            Ok(false) => {
                // Script returned 0: XACKDEL race lost — the entry was
                // already removed by a concurrent path (CLAIM, manual
                // ack, etc.). The gate did its job; no double-schedule.
                tracing::trace!(entry_id = %relocate.entry_id, "retry reschedule gated: entry already removed");
            }
            Err(e) => {
                tracing::error!(entry_id = %relocate.entry_id, error = %e, "retry reschedule failed permanently; entry remains pending and will be retried on next CLAIM tick");
            }
        }
    }
}

async fn reschedule_with_retry(
    client: &Client,
    cfg: &RetryRelocatorConfig,
    relocate: &RetryRelocate,
    sha: &mut String,
) -> Result<bool> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..RETRY_REDIS_ATTEMPTS {
        match reschedule_once(client, cfg, relocate, sha).await {
            Ok(rescheduled) => return Ok(rescheduled),
            Err(e) => {
                let backoff = RETRY_REDIS_BASE_MS << attempt;
                tracing::warn!(entry_id = %relocate.entry_id, attempt = attempt + 1, error = %e, backoff_ms = backoff, "retry reschedule failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Config("retry reschedule exhausted retries".into())))
}

async fn reschedule_once(
    client: &Client,
    cfg: &RetryRelocatorConfig,
    relocate: &RetryRelocate,
    sha: &mut String,
) -> Result<bool> {
    let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_retry_args(
        sha,
        &cfg.stream_key,
        &cfg.delayed_key,
        &cfg.group,
        relocate.entry_id.as_ref(),
        relocate.run_at_ms,
        relocate.job_bytes.clone(),
    );
    let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
    match res {
        Ok(v) => Ok(script_returned_one(&v)),
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            *sha = load_script(client).await?;
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args = eval_retry_args(
                RETRY_RESCHEDULE_SCRIPT,
                &cfg.stream_key,
                &cfg.delayed_key,
                &cfg.group,
                relocate.entry_id.as_ref(),
                relocate.run_at_ms,
                relocate.job_bytes.clone(),
            );
            let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
            Ok(script_returned_one(&v))
        }
        Err(e) => Err(Error::Redis(e)),
    }
}

/// `RETRY_RESCHEDULE_SCRIPT` returns Lua `1` (rescheduled) or `0` (gate fired
/// — XACKDEL found nothing to ack). Be defensive about how `fred` shapes the
/// `Value`: integers can come back as `Integer` or as a stringified payload
/// depending on protocol version. Anything not matching `1` is treated as
/// "did not reschedule" (the safe default — we'd rather miss the metric than
/// over-count it).
fn script_returned_one(v: &Value) -> bool {
    match v {
        Value::Integer(n) => *n == 1,
        Value::String(s) => s.as_bytes() == b"1",
        Value::Bytes(b) => b.as_ref() == b"1",
        _ => false,
    }
}

/// Decode a `JobId` from a msgpack-encoded `Job<T>` payload without
/// allocating for the (ignored) `payload` field. Returns `None` for malformed
/// bytes — used by the events emit on the retry-scheduled path so the
/// MetricsSink event still fires even if the events-stream emit can't.
/// Mirrors the helper of the same name in `promoter.rs`.
fn extract_job_id(bytes: &[u8]) -> Option<String> {
    let job: Job<IgnoredAny> = rmp_serde::from_slice(bytes).ok()?;
    Some(job.id)
}

async fn load_script(client: &Client) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(RETRY_RESCHEDULE_SCRIPT))
        .await
        .map_err(Error::Redis)?;
    match res {
        Value::String(s) => Ok(s.to_string()),
        Value::Bytes(b) => std::str::from_utf8(&b)
            .map(|s| s.to_string())
            .map_err(|_| Error::Config("SCRIPT LOAD returned non-utf8 sha".into())),
        other => Err(Error::Config(format!(
            "SCRIPT LOAD returned unexpected: {other:?}"
        ))),
    }
}

/// Compute backoff for the upcoming Nth retry. `attempt` is 1-indexed:
/// 1 = the first retry (after the initial failure), 2 = the second, etc.
///
/// Synthesizes a [`BackoffSpec`] from the queue-wide [`RetryConfig`] and
/// delegates to [`backoff_ms_from_spec`], so the per-job and queue-wide
/// paths share one implementation of the math.
pub(crate) fn backoff_ms(attempt: u32, cfg: &RetryConfig) -> u64 {
    let spec = BackoffSpec {
        kind: BackoffKind::Exponential,
        delay_ms: cfg.initial_backoff_ms,
        max_delay_ms: Some(cfg.max_backoff_ms),
        multiplier: Some(cfg.multiplier),
        jitter_ms: Some(cfg.jitter_ms),
    };
    backoff_ms_from_spec(attempt, &spec, cfg)
}

/// Compute backoff using a per-job [`BackoffSpec`], falling back to fields
/// of the queue-wide [`RetryConfig`] when the spec leaves them `None`.
///
/// `attempt` is 1-indexed exactly like [`backoff_ms`].
///
/// `kind` semantics:
///  - [`BackoffKind::Fixed`] returns `delay_ms` plus jitter, capped at
///    `max_delay_ms`. No multiplier is applied.
///  - [`BackoffKind::Exponential`] returns
///    `delay_ms * multiplier^(attempt-1)` plus jitter, capped at
///    `max_delay_ms`.
///  - [`BackoffKind::Unknown`] (variant emitted by a future SDK) is
///    routed through the same path as `Exponential`, so an unrecognised
///    `kind` doesn't hard-fail the consumer. Matches the previous
///    string-typed "unknown → exponential" behavior.
pub(crate) fn backoff_ms_from_spec(
    attempt: u32,
    spec: &BackoffSpec,
    fallback: &RetryConfig,
) -> u64 {
    // Operator-visibility breadcrumb: a `BackoffKind::Unknown` here means a
    // producer encoded a `kind` variant this consumer's `BackoffKind` enum
    // doesn't know about — typically because a newer SDK has been deployed
    // on the producer side ahead of consumers. The retry math silently
    // degrades to exponential below; without this log, operators would
    // see "weird retry timing" with no signal. `swap` returns the previous
    // value, so the second-and-later calls collapse to a no-op.
    if matches!(spec.kind, BackoffKind::Unknown)
        && !WARNED_UNKNOWN_KIND.swap(true, Ordering::Relaxed)
    {
        tracing::warn!(
            "consumer decoded BackoffKind::Unknown — likely a future-SDK variant; \
             retry math degraded to exponential. Upgrade consumer to silence this warning."
        );
    }

    let max_delay_ms = spec.max_delay_ms.unwrap_or(fallback.max_backoff_ms);
    let jitter_ms = spec.jitter_ms.unwrap_or(fallback.jitter_ms);

    let base = match spec.kind {
        BackoffKind::Fixed => spec.delay_ms as f64,
        // Exponential or Unknown → exponential semantics.
        BackoffKind::Exponential | BackoffKind::Unknown => {
            let multiplier = spec.multiplier.unwrap_or(fallback.multiplier);
            let exp = attempt.saturating_sub(1) as i32;
            (spec.delay_ms as f64) * multiplier.powi(exp)
        }
    };
    let capped = base.min(max_delay_ms as f64).max(0.0) as u64;
    let jitter = if jitter_ms == 0 {
        0
    } else {
        fastrand_jitter(jitter_ms)
    };
    capped.saturating_add(jitter)
}

fn fastrand_jitter(max: u64) -> u64 {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0);
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            let salt = SEED.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1);
            x = now ^ salt ^ 0xDEAD_BEEF_CAFE_F00D;
            if x == 0 {
                x = 1;
            }
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x % max
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_monotone_until_cap() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        assert_eq!(backoff_ms(1, &cfg), 100);
        assert_eq!(backoff_ms(2, &cfg), 200);
        assert_eq!(backoff_ms(3, &cfg), 400);
        assert_eq!(backoff_ms(4, &cfg), 800);
        assert_eq!(backoff_ms(5, &cfg), 1_000);
        assert_eq!(backoff_ms(10, &cfg), 1_000);
    }

    #[test]
    fn backoff_jitter_within_bounds() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 50,
        };
        for _ in 0..200 {
            let v = backoff_ms(2, &cfg);
            assert!((200..200 + 50).contains(&v), "out of range: {v}");
        }
    }

    #[test]
    fn backoff_zero_attempt_floors_to_initial() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        assert_eq!(backoff_ms(0, &cfg), 100);
    }

    #[test]
    fn backoff_from_spec_exponential_uses_spec_fields() {
        let fallback = RetryConfig {
            initial_backoff_ms: 999,
            max_backoff_ms: 100_000,
            multiplier: 99.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            kind: BackoffKind::Exponential,
            delay_ms: 100,
            max_delay_ms: Some(5_000),
            multiplier: Some(3.0),
            jitter_ms: Some(0),
        };
        // 100, 300, 900, 2700, capped at 5000.
        assert_eq!(backoff_ms_from_spec(1, &spec, &fallback), 100);
        assert_eq!(backoff_ms_from_spec(2, &spec, &fallback), 300);
        assert_eq!(backoff_ms_from_spec(3, &spec, &fallback), 900);
        assert_eq!(backoff_ms_from_spec(4, &spec, &fallback), 2_700);
        assert_eq!(backoff_ms_from_spec(5, &spec, &fallback), 5_000);
        assert_eq!(backoff_ms_from_spec(10, &spec, &fallback), 5_000);
    }

    #[test]
    fn backoff_from_spec_fixed_ignores_multiplier() {
        let fallback = RetryConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 100_000,
            multiplier: 99.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            kind: BackoffKind::Fixed,
            delay_ms: 50,
            max_delay_ms: None,
            multiplier: Some(99.0),
            jitter_ms: Some(0),
        };
        for attempt in 1..=10 {
            assert_eq!(backoff_ms_from_spec(attempt, &spec, &fallback), 50);
        }
    }

    #[test]
    fn backoff_from_spec_unknown_kind_degrades_to_exponential() {
        let fallback = RetryConfig {
            initial_backoff_ms: 999,
            max_backoff_ms: 100_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            // `Unknown` represents a future-SDK variant the engine doesn't
            // know about. It must route to the exponential path so the
            // consumer never hard-fails on an unfamiliar `kind`.
            kind: BackoffKind::Unknown,
            delay_ms: 100,
            max_delay_ms: None,
            multiplier: None, // pull from fallback (2.0)
            jitter_ms: Some(0),
        };
        assert_eq!(backoff_ms_from_spec(1, &spec, &fallback), 100);
        assert_eq!(backoff_ms_from_spec(2, &spec, &fallback), 200);
        assert_eq!(backoff_ms_from_spec(3, &spec, &fallback), 400);
    }

    /// Repeated calls with `Unknown` must keep returning identical math
    /// across invocations — the once-per-process operator-visibility
    /// `tracing::warn` is fire-and-forget and must not alter the return
    /// value or panic on the second-and-later call (the `AtomicBool::swap`
    /// has already latched `true`, so the warn is skipped — no observable
    /// behavior change).
    #[test]
    fn backoff_from_spec_unknown_kind_is_idempotent_across_calls() {
        let fallback = RetryConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 100_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            kind: BackoffKind::Unknown,
            delay_ms: 100,
            max_delay_ms: None,
            multiplier: None,
            jitter_ms: Some(0),
        };
        let first = backoff_ms_from_spec(2, &spec, &fallback);
        let second = backoff_ms_from_spec(2, &spec, &fallback);
        let third = backoff_ms_from_spec(2, &spec, &fallback);
        assert_eq!(first, 200);
        assert_eq!(second, 200);
        assert_eq!(third, 200);
    }

    #[test]
    fn backoff_from_spec_falls_back_to_retry_config_fields() {
        let fallback = RetryConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 250,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            kind: BackoffKind::Exponential,
            delay_ms: 100,
            max_delay_ms: None, // → fallback.max_backoff_ms = 250
            multiplier: None,   // → fallback.multiplier = 2.0
            jitter_ms: None,    // → fallback.jitter_ms = 0
        };
        // 100, 200, 400(capped to 250), 800(capped to 250)
        assert_eq!(backoff_ms_from_spec(1, &spec, &fallback), 100);
        assert_eq!(backoff_ms_from_spec(2, &spec, &fallback), 200);
        assert_eq!(backoff_ms_from_spec(3, &spec, &fallback), 250);
        assert_eq!(backoff_ms_from_spec(4, &spec, &fallback), 250);
    }

    #[test]
    fn backoff_from_spec_jitter_within_bounds() {
        let fallback = RetryConfig {
            initial_backoff_ms: 0,
            max_backoff_ms: 100_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        let spec = BackoffSpec {
            kind: BackoffKind::Fixed,
            delay_ms: 100,
            max_delay_ms: None,
            multiplier: None,
            jitter_ms: Some(25),
        };
        for _ in 0..200 {
            let v = backoff_ms_from_spec(1, &spec, &fallback);
            assert!((100..100 + 25).contains(&v), "out of range: {v}");
        }
    }

    /// `RetryConfig::backoff_ms` synthesizes a `BackoffSpec` and
    /// delegates to `backoff_ms_from_spec`. Pin equivalence so a future
    /// edit to one path can't silently drift from the other.
    #[test]
    fn backoff_ms_delegates_to_spec() {
        let cfg = RetryConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            multiplier: 2.0,
            jitter_ms: 0,
        };
        let synthesized = BackoffSpec {
            kind: BackoffKind::Exponential,
            delay_ms: cfg.initial_backoff_ms,
            max_delay_ms: Some(cfg.max_backoff_ms),
            multiplier: Some(cfg.multiplier),
            jitter_ms: Some(cfg.jitter_ms),
        };
        for attempt in 0u32..=10 {
            assert_eq!(
                backoff_ms(attempt, &cfg),
                backoff_ms_from_spec(attempt, &synthesized, &cfg),
                "drift between backoff_ms and backoff_ms_from_spec at attempt={attempt}"
            );
        }
    }

    #[test]
    fn script_returned_one_handles_value_shapes() {
        assert!(script_returned_one(&Value::Integer(1)));
        assert!(!script_returned_one(&Value::Integer(0)));
        assert!(!script_returned_one(&Value::Integer(2)));
        assert!(script_returned_one(&Value::String("1".into())));
        assert!(!script_returned_one(&Value::String("0".into())));
        assert!(script_returned_one(&Value::Bytes(
            bytes::Bytes::from_static(b"1")
        )));
        assert!(!script_returned_one(&Value::Null));
    }
}
