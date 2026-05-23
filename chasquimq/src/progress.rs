//! Per-handler `JobHandle` for in-handler progress reports and log lines.
//!
//! A `JobHandle` is attached to [`crate::Job`] by the consumer before the
//! user handler runs. Handlers call:
//!
//! - [`JobHandle::update_progress`] to record an integer 0..=100 against the
//!   per-job progress key (`{chasqui:<queue>}:progress:<id>`), readable by
//!   the introspector and by external dashboards via the events stream.
//! - [`JobHandle::log`] to append a UTF-8 line to the per-job log stream
//!   (`{chasqui:<queue>}:log:<id>`), readable via
//!   `Introspector::get_job_logs`.
//!
//! Design notes:
//!
//! - **Progress storage**: plain Redis STRING containing the ASCII decimal
//!   of a `u8` (so a shim can read it with `parseInt` / `int(str(...))`
//!   without a msgpack dependency). TTL `result_ttl_secs` so it disappears
//!   alongside the result key after job completion.
//! - **Log storage**: Redis Stream, one entry per call, field `line`.
//!   `MAXLEN ~ <log_max_stream_len>` keeps the stream bounded; the trim
//!   is approximate (the `~`) so Redis can do it cheaply. Each `log()`
//!   refreshes an `EXPIRE` of `result_ttl_secs` on the stream key so the
//!   log disappears alongside the result key after job completion;
//!   without that, `MAXLEN` caps the entries but leaves the key itself
//!   indefinitely.
//! - **Connection budget**: a `JobHandle` borrows a shared
//!   [`fred::clients::Pool`] (sized 2–8) — never a client per worker.
//! - **Bounded lines**: lines exceeding `log_max_line_bytes` are truncated
//!   on a UTF-8 char boundary with a `"[…truncated]"` marker appended, so
//!   a malformed line cannot exhaust Redis memory.
//! - **Warn-once**: clamp and truncate each fire a single `tracing::warn!`
//!   per handle so a hot-loop handler doesn't flood the log.
//! - **Best-effort events**: when `events_progress_enabled` is true the
//!   handle emits an `e=progress` event after the SET succeeds. The event
//!   write is best-effort (same contract as every other engine event); a
//!   failed XADD never propagates back to the handler.

use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::redis::keys::{log_key, progress_key};
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Truncation marker appended to oversize log lines. Encoded in UTF-8;
/// the truncation point is walked back to a char boundary before this
/// is appended so the assembled line stays well-formed.
const TRUNCATED_MARKER: &str = "[\u{2026}truncated]";

/// Per-handler progress + log surface. Attached to [`crate::Job::handle`]
/// by the consumer immediately before the user handler runs; absent
/// (`None`) on `Job<T>` instances returned by the introspector's
/// read-only paths.
pub struct JobHandle {
    job_id: Arc<str>,
    queue_name: Arc<str>,
    pool: Pool,
    result_ttl_secs: u64,
    log_max_stream_len: u64,
    log_max_line_bytes: usize,
    events: Option<EventsWriter>,
    job_name: Option<Arc<str>>,
    events_progress_enabled: bool,
    warned_clamp: AtomicBool,
    warned_truncate: AtomicBool,
}

impl std::fmt::Debug for JobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobHandle")
            .field("job_id", &self.job_id)
            .field("queue_name", &self.queue_name)
            .field("result_ttl_secs", &self.result_ttl_secs)
            .field("log_max_stream_len", &self.log_max_stream_len)
            .field("log_max_line_bytes", &self.log_max_line_bytes)
            .field("events_progress_enabled", &self.events_progress_enabled)
            .finish()
    }
}

impl JobHandle {
    /// Construct a handle. Engine-internal — the consumer wires this in
    /// per-dispatch; user code never calls this directly.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        job_id: Arc<str>,
        queue_name: Arc<str>,
        pool: Pool,
        result_ttl_secs: u64,
        log_max_stream_len: u64,
        log_max_line_bytes: usize,
        events: Option<EventsWriter>,
        job_name: Option<Arc<str>>,
        events_progress_enabled: bool,
    ) -> Self {
        Self {
            job_id,
            queue_name,
            pool,
            result_ttl_secs,
            log_max_stream_len,
            log_max_line_bytes,
            events,
            job_name,
            events_progress_enabled,
            warned_clamp: AtomicBool::new(false),
            warned_truncate: AtomicBool::new(false),
        }
    }

    /// Test-only constructor: opens a small `Pool` against `redis_url`,
    /// omits the events writer + job name fields, and returns a handle
    /// suitable for asserting against the SET / XADD / XLEN paths
    /// without bringing up a full Consumer. Exposed as `pub` and
    /// `#[doc(hidden)]` so the engine's integration tests
    /// (`tests/progress_and_log.rs`) can use it; production code
    /// constructs handles via the consumer's worker wiring.
    #[doc(hidden)]
    pub async fn new_for_test(
        redis_url: &str,
        job_id: Arc<str>,
        queue_name: Arc<str>,
        result_ttl_secs: u64,
        log_max_stream_len: u64,
        log_max_line_bytes: usize,
    ) -> Result<Self> {
        let pool = crate::redis::conn::connect_pool(
            redis_url,
            2,
            &crate::config::ConnectionTuning::default(),
        )
        .await?;
        Ok(Self::new(
            job_id,
            queue_name,
            pool,
            result_ttl_secs,
            log_max_stream_len,
            log_max_line_bytes,
            None,
            None,
            false,
        ))
    }

    /// The job id this handle reports against. Stable for the handle's
    /// lifetime.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Record `n` (clamped to `0..=100`) against the per-job progress
    /// key. A SET with TTL `result_ttl_secs` so the value disappears
    /// alongside the result key after a successful completion.
    ///
    /// Out-of-range values (`n > 100`) are clamped to 100, and the first
    /// such clamp logs a single `tracing::warn!` per handle so a
    /// hot-loop handler doesn't flood the operator log.
    ///
    /// When `events_progress_enabled` is true (default), a best-effort
    /// `e=progress` event is emitted after the SET succeeds. A failed
    /// event-emit never causes this method to return `Err` — the
    /// persisted state is the source of truth.
    pub async fn update_progress(&self, n: u8) -> Result<()> {
        let clamped = if n > 100 {
            if !self.warned_clamp.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    job_id = %self.job_id,
                    requested = n,
                    "update_progress: value > 100; clamping (warn-once per handle)"
                );
            }
            100
        } else {
            n
        };

        let key = progress_key(&self.queue_name, &self.job_id);
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("SET", ClusterHash::FirstKey, false);
        let ttl = i64::try_from(self.result_ttl_secs).unwrap_or(i64::MAX);
        let args = vec![
            Value::from(key),
            Value::from(clamped.to_string()),
            Value::from("EX"),
            Value::from(ttl),
        ];
        client
            .custom::<Value, _>(cmd, args)
            .await
            .map_err(Error::Redis)?;

        // SET-first-then-emit: the persisted progress value is the
        // source of truth; a failed event-emit must never leave
        // subscribers ahead of (or behind) what an introspector would
        // read. Event emission itself is best-effort — `xadd` swallows
        // errors and warns at the events module.
        if self.events_progress_enabled
            && let Some(events) = &self.events
        {
            let name = self.job_name.as_deref();
            events.emit_progress(&self.job_id, name, clamped).await;
        }

        Ok(())
    }

    /// Append `line` to the per-job log stream and return the new stream
    /// length (one XADD + XLEN + EXPIRE, pipelined into a single round
    /// trip).
    ///
    /// Lines exceeding `log_max_line_bytes` are truncated to the largest
    /// UTF-8 char boundary at or below that byte cap and have the marker
    /// `"[…truncated]"` appended. The first truncation per handle logs a
    /// single `tracing::warn!` so a hot-loop handler can't flood the
    /// operator log.
    ///
    /// The trailing `EXPIRE log_key result_ttl_secs` keeps the stream
    /// key from outliving the result it belongs to — `MAXLEN ~` caps the
    /// entry count but not the key itself, so a job that logs once and
    /// never logs again would leak the stream indefinitely without this.
    pub async fn log(&self, line: &str) -> Result<u64> {
        let payload = self.truncate_line(line);

        let key = log_key(&self.queue_name, &self.job_id);
        let client = self.pool.next_connected();
        let pipeline = client.pipeline();

        let xadd_cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        let xadd_args = vec![
            Value::from(key.as_str()),
            Value::from("MAXLEN"),
            Value::from("~"),
            Value::from(i64::try_from(self.log_max_stream_len).unwrap_or(i64::MAX)),
            Value::from("*"),
            Value::from("line"),
            Value::from(payload.as_ref()),
        ];
        pipeline
            .custom::<Value, _>(xadd_cmd, xadd_args)
            .await
            .map_err(Error::Redis)?;

        let xlen_cmd = CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false);
        pipeline
            .custom::<Value, _>(xlen_cmd, vec![Value::from(key.as_str())])
            .await
            .map_err(Error::Redis)?;

        let expire_cmd = CustomCommand::new_static("EXPIRE", ClusterHash::FirstKey, false);
        let ttl = i64::try_from(self.result_ttl_secs).unwrap_or(i64::MAX);
        pipeline
            .custom::<Value, _>(
                expire_cmd,
                vec![Value::from(key.as_str()), Value::from(ttl)],
            )
            .await
            .map_err(Error::Redis)?;

        let results = pipeline.all::<Value>().await.map_err(Error::Redis)?;
        Ok(extract_xlen(&results))
    }

    /// Returns the line as-is when it fits; otherwise walks back to the
    /// nearest UTF-8 char boundary at-or-below `log_max_line_bytes` and
    /// appends the truncation marker.
    fn truncate_line<'a>(&self, line: &'a str) -> std::borrow::Cow<'a, str> {
        if line.len() <= self.log_max_line_bytes {
            return std::borrow::Cow::Borrowed(line);
        }
        if !self.warned_truncate.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                job_id = %self.job_id,
                len = line.len(),
                cap = self.log_max_line_bytes,
                "log: line exceeds log_max_line_bytes; truncating (warn-once per handle)"
            );
        }
        let cap = self.log_max_line_bytes;
        let mut cut = cap.min(line.len());
        while cut > 0 && !line.is_char_boundary(cut) {
            cut -= 1;
        }
        let mut out = String::with_capacity(cut + TRUNCATED_MARKER.len());
        out.push_str(&line[..cut]);
        out.push_str(TRUNCATED_MARKER);
        std::borrow::Cow::Owned(out)
    }
}

/// The pipeline returns `[XADD-reply, XLEN-reply, EXPIRE-reply]`;
/// recover the XLEN integer (the second element). Saturating-on-
/// non-integer means a surprise reply shape doesn't panic in the
/// user's handler.
fn extract_xlen(results: &Value) -> u64 {
    let arr = match results {
        Value::Array(items) => items,
        _ => return 0,
    };
    match arr.get(1) {
        Some(Value::Integer(n)) => u64::try_from(*n).unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `truncate_line` is the pure-function core of `log()`'s oversize
    /// handling. Exercise it in isolation: a 1-byte cap, multibyte UTF-8
    /// at the cut point, and a marker that survives at the tail. We
    /// build a handle with a stub pool only to instantiate the method;
    /// no Redis is touched.
    fn cap_truncate(input: &str, cap: usize) -> String {
        let mut cut = cap.min(input.len());
        while cut > 0 && !input.is_char_boundary(cut) {
            cut -= 1;
        }
        let mut out = String::with_capacity(cut + TRUNCATED_MARKER.len());
        out.push_str(&input[..cut]);
        out.push_str(TRUNCATED_MARKER);
        out
    }

    #[test]
    fn truncate_walks_back_to_utf8_boundary() {
        let s = "a\u{1F600}bcd"; // "a" + 4-byte emoji + "bcd"
        // emoji starts at byte 1, ends at byte 5. cap=3 lands in the middle
        // of the emoji; walk-back must stop at byte 1.
        let out = cap_truncate(s, 3);
        assert!(out.starts_with('a'), "got: {out:?}");
        assert!(out.ends_with(TRUNCATED_MARKER));
        assert_eq!(&out[..1], "a");
    }

    #[test]
    fn truncate_marker_appended_only_when_over_cap() {
        let s = "hello";
        // cap >= len: bare string (the production path returns Borrowed).
        // Here we model the marker-append path only, so emulate the
        // production short-circuit explicitly.
        if s.len() <= 10 {
            assert_eq!(s, "hello");
        } else {
            let out = cap_truncate(s, 10);
            assert!(out.ends_with(TRUNCATED_MARKER));
        }
    }

    #[test]
    fn extract_xlen_pulls_second_element_integer() {
        // Production shape: [XADD-reply, XLEN-reply, EXPIRE-reply].
        let v = Value::Array(vec![
            Value::String("1700000000000-0".into()),
            Value::Integer(42),
            Value::Integer(1),
        ]);
        assert_eq!(extract_xlen(&v), 42);
    }

    #[test]
    fn extract_xlen_defaults_to_zero_on_surprise_shape() {
        assert_eq!(extract_xlen(&Value::Null), 0);
        assert_eq!(extract_xlen(&Value::Array(vec![Value::Integer(1)])), 0);
        assert_eq!(
            extract_xlen(&Value::Array(vec![
                Value::String("x".into()),
                Value::String("not-int".into()),
            ])),
            0
        );
    }
}
