//! Shared scaffolding for leader-elected background loops.
//!
//! The promoter ([`crate::promoter`]), scheduler ([`crate::scheduler`]), and
//! stalled-detector ([`crate::stalled`]) all share the same shell:
//!
//!   1. `SET NX EX` a leader lock so only one replica drives the work.
//!   2. On every tick: acquire-or-wait, then either run the per-loop body
//!      or sleep one poll interval.
//!   3. Classify Redis errors as transient (sleep with capped backoff) vs
//!      permanent (surface up).
//!   4. On shutdown, best-effort release the lock so the next replica
//!      doesn't have to wait `lock_ttl_secs` for the lease to expire.
//!
//! This module isolates the helpers shared across all three loops:
//! script-load (with non-utf8 / unexpected-shape guards), `Value`-to-bool
//! / `Value`-to-u64 decoders that defend against `fred` shape drift, the
//! transient-error classifier, the exponential-backoff table, and a
//! `sleep_or_shutdown` future.
//!
//! The loops themselves still own their per-tick body (`promote_once` /
//! `tick_once` / `scan_once`) — sharing the shell wholesale would force
//! every loop through the same trait, and the three bodies are different
//! enough (return shapes, cleanup follow-ups, drain-vs-poll heuristics)
//! that a unified trait carries more friction than the three-helper
//! duplication it avoids. Promoter and scheduler retain their own local
//! copies of these helpers from before this slice — keeping their diff
//! to zero so this slice can land safely; consolidation into this
//! module is a follow-up refactor.

use crate::error::{Error, Result};
use crate::redis::commands::script_load_args;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Exponential backoff table for transient Redis errors during a leader
/// loop. Capped at 400ms so a leader on a flaky link still retries
/// promptly when the link recovers. Shared so the three loops can't
/// drift; the promoter / scheduler still keep their own copies until
/// their next refactor (see module docs).
pub(crate) const TRANSIENT_BACKOFF_MS: [u64; 4] = [50, 100, 200, 400];

/// Classify an error from the leader loop's `EVALSHA` / `XPENDING` /
/// lock-acquire as transient. Transients sleep with capped backoff and
/// retry; everything else is surfaced up so the loop can exit on a real
/// problem (auth failure, misconfigured URL, etc).
pub(crate) fn is_transient(err: &fred::error::Error) -> bool {
    use fred::error::ErrorKind;
    matches!(
        err.kind(),
        ErrorKind::Timeout | ErrorKind::IO | ErrorKind::Canceled
    )
}

/// Sleep for `d`, or return early if `shutdown` fires. Returns `true`
/// when the full sleep elapsed (continue), `false` when shutdown fired
/// (exit the loop).
pub(crate) async fn sleep_or_shutdown(d: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => true,
        _ = shutdown.cancelled() => false,
    }
}

/// One step of the exponential-backoff table. Returns the next index on
/// success (advancing into the table), `None` when shutdown fires during
/// the sleep — the caller should exit the loop on `None`.
pub(crate) async fn backoff_after(
    idx: usize,
    err: &fred::error::Error,
    op: &str,
    shutdown: &CancellationToken,
) -> Option<usize> {
    let wait_ms = TRANSIENT_BACKOFF_MS[idx.min(TRANSIENT_BACKOFF_MS.len() - 1)];
    tracing::warn!(error = %err, op = op, backoff_ms = wait_ms, "leader loop transient error");
    if !sleep_or_shutdown(Duration::from_millis(wait_ms), shutdown).await {
        return None;
    }
    Some(idx.saturating_add(1))
}

/// `SCRIPT LOAD` a script body and return its SHA1, decoded as a UTF-8
/// string. Returns `Error::Config` on the rare non-utf8 / unexpected-
/// shape reply (defensive — the SHA1 hex digest is always 40 ASCII
/// chars, but `fred`'s `Value::Bytes` vs `Value::String` framing varies
/// by RESP version).
pub(crate) async fn load_script(client: &Client, body: &str) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(body))
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

/// Defensive `Value` -> `bool` coercion for lock-acquire script returns.
/// Lua `1` / `0` is shaped as `Value::Integer(1|0)` over RESP2/RESP3, but
/// guard the other variants so a script-shape regression downgrades to
/// "lock not held" rather than panicking. `Value::Null` and unexpected
/// shapes are treated as `false` (the safe default — failing closed
/// means the loop sleeps instead of running a side-effect under a stale
/// lock).
pub(crate) fn value_as_bool(v: &Value) -> bool {
    match v {
        Value::Integer(n) => *n != 0,
        _ => false,
    }
}

/// Defensive `Value` -> `u64` coercion. Negative integers clamp to 0
/// (Lua should never return negative counts, but be defensive); non-
/// integer shapes return 0 so a malformed reply doesn't panic the loop.
pub(crate) fn value_as_u64(v: &Value) -> u64 {
    match v {
        Value::Integer(n) => (*n).max(0) as u64,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn value_as_bool_int_truthy() {
        assert!(value_as_bool(&Value::Integer(1)));
        assert!(value_as_bool(&Value::Integer(2)));
        assert!(!value_as_bool(&Value::Integer(0)));
        // Defensive: non-integer shapes treated as `false` (loop sleeps
        // instead of running under a stale lock).
        assert!(!value_as_bool(&Value::Null));
        assert!(!value_as_bool(&Value::String("1".into())));
        assert!(!value_as_bool(&Value::Bytes(Bytes::from_static(b"1"))));
    }

    #[test]
    fn value_as_u64_clamps_negative_and_handles_unexpected() {
        assert_eq!(value_as_u64(&Value::Integer(7)), 7);
        assert_eq!(value_as_u64(&Value::Integer(0)), 0);
        // Negative clamps to 0.
        assert_eq!(value_as_u64(&Value::Integer(-1)), 0);
        // Non-integer returns 0.
        assert_eq!(value_as_u64(&Value::Null), 0);
        assert_eq!(value_as_u64(&Value::String("42".into())), 0);
    }

    #[test]
    fn is_transient_classifies_io_timeout_cancelled() {
        use fred::error::{Error as FredError, ErrorKind};
        for kind in [ErrorKind::IO, ErrorKind::Timeout, ErrorKind::Canceled] {
            let label = format!("{kind:?}");
            let err = FredError::new(kind, "test");
            assert!(is_transient(&err), "{label} must be transient");
        }
        // A non-transient kind must NOT be classified transient.
        let err = FredError::new(ErrorKind::Auth, "bad creds");
        assert!(!is_transient(&err));
    }

    #[test]
    fn transient_backoff_table_caps_at_400ms() {
        // The last slot is the cap; saturating into the table beyond its
        // length must keep returning 400ms.
        assert_eq!(*TRANSIENT_BACKOFF_MS.last().unwrap(), 400);
        for idx in [0_usize, 1, 2, 3, 4, 99] {
            let ms = TRANSIENT_BACKOFF_MS[idx.min(TRANSIENT_BACKOFF_MS.len() - 1)];
            assert!(ms <= 400, "backoff at idx={idx} exceeds cap: {ms}");
        }
    }
}
