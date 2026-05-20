//! Job introspection — bounded, on-demand read APIs across the stream,
//! delayed ZSET, DLQ, and result-key surfaces. Read-only. Adds zero work
//! to the producer / consumer / promoter / scheduler hot paths.
//!
//! Design notes (see `.claude/plans/introspection.md` for the full slice
//! plan; the gist):
//!
//! - **No secondary index.** Lookups are bounded scans.
//! - **No hot-path changes.** The XADD / XREADGROUP / XACK paths are not
//!   touched by this slice.
//! - **Live-state-first ordering.** `get_job_state` and `get_job` check
//!   pending → delayed → waiting → DLQ → result, in that order. A job
//!   that is both "in result key" (from a prior completion) and "back in
//!   stream" (from a manual DLQ replay) resolves as `Waiting`, not
//!   `Completed` — what matters for callers is the work the next worker
//!   tick is about to do.
//! - **Type-erased envelope walk.** We walk the msgpack array via
//!   `rmpv` so the inspector can pull `id` / `attempt` / `created_at_ms`
//!   from any payload the producer ever wrote, regardless of `T`. The
//!   inner payload bytes are returned to the caller as-is (re-encoded
//!   from the rmpv value tree, losslessly).
//! - **NOGROUP swallowed.** A fresh queue with no consumer group yet
//!   makes XPENDING return `NOGROUP`; we treat that as zero pending and
//!   continue. Pinned by `get_job_counts_with_no_consumer_group_yet`.
//! - **Decode-failure tolerance.** A poison entry on the stream skips
//!   with a `tracing::warn!`; pagination continues. Matches the
//!   `replay_dlq` pattern in `producer/dlq.rs::reset_attempts`.
//! - **Bounded `completed`.** `JobCounts::completed` is computed via a
//!   bounded SCAN with a cap (`CHASQUIMQ_COMPLETED_SCAN_CAP`, default
//!   10_000). When the cap is hit, `completed_is_capped` is set so the
//!   caller can present "≥ N" rather than the exact value.

use crate::error::{Error, Result};
use crate::payload::peek_envelope;
use crate::redis::keys::{
    delayed_index_key, delayed_key, dlq_key, paused_key, result_key, stream_key,
};
use crate::redis::parse::{XrangeEntry, parse_xrange_response};
use crate::{ConnectionTuning, config::ConsumerConfig};
use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;

/// State a `Job` can be in from the engine's POV at query time. Mirrors
/// the BullMQ shape so SDK callers can route to familiar branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Stream entry that has not yet been delivered to any consumer
    /// (no PEL membership). Producer-side `XADD` landed it here.
    Waiting,
    /// Stream entry that is in the consumer group's PEL — delivered to
    /// some worker, not yet acked. The worker may still be running it
    /// or may have crashed mid-job (the consumer's claim loop will pick
    /// it up either way).
    Active,
    /// Scheduled future job sitting in the delayed ZSET, waiting for
    /// the promoter to move it onto the stream.
    Delayed,
    /// Handler succeeded and the result key was written. Note that
    /// `result_ttl_secs` expiration eventually collapses this back to
    /// `Unknown`.
    Completed,
    /// Routed to the DLQ — retries exhausted, payload oversize, decode
    /// failure, or an `UnrecoverableError` short-circuit.
    Failed,
    /// Inspector found no trace of this id in any of the four surfaces.
    /// Could mean the id never existed, or the stream entry rolled off
    /// `max_stream_len`, or the result key already expired.
    Unknown,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Waiting => "waiting",
            JobState::Active => "active",
            JobState::Delayed => "delayed",
            JobState::Completed => "completed",
            JobState::Failed => "failed",
            JobState::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "waiting" => JobState::Waiting,
            "active" => JobState::Active,
            "delayed" => JobState::Delayed,
            "completed" => JobState::Completed,
            "failed" => JobState::Failed,
            "unknown" => JobState::Unknown,
            _ => return None,
        })
    }
}

/// Aggregate per-queue counts. Cheap to fetch (single round trip per
/// field, ~5 round trips total). `completed` is approximate above
/// `completed_scan_cap`; check `completed_is_capped`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobCounts {
    pub waiting: u64,
    pub active: u64,
    pub delayed: u64,
    pub completed: u64,
    pub failed: u64,
    /// 0 when the queue is not durably paused, 1 when it is. BullMQ-shape
    /// (a count column, not a bool) so this dict drops straight into the
    /// shim `getJobCounts` return shape.
    pub paused: u64,
    /// `true` when the SCAN over `result:*` keys hit the per-call cap
    /// (`CHASQUIMQ_COMPLETED_SCAN_CAP`, default 10_000) without
    /// exhausting the keyspace. When set, `completed` is a lower bound.
    pub completed_is_capped: bool,
}

/// Per-job snapshot returned by [`Introspector::get_job`].
///
/// `payload` is the opaque msgpack-encoded user data exactly as the
/// producer handed it in (the inner `payload` slot of `Job<T>`). The
/// engine doesn't decode it; the caller (or shim) does. `name` comes
/// from the `n` stream-entry field, not from the msgpack envelope.
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub id: String,
    pub name: String,
    pub payload: Bytes,
    pub attempt: u32,
    pub state: JobState,
    pub created_at_ms: u64,
    /// `Some(ms)` when the inspector can recover a "this entry first
    /// touched the stream at" timestamp from the stream entry id; `None`
    /// for delayed-only / DLQ-only / result-only lookups.
    pub processed_on_ms: Option<u64>,
    /// `Some(ms)` when the job is in the DLQ — derived from the DLQ
    /// stream entry id timestamp.
    pub finished_on_ms: Option<u64>,
    pub failure_reason: Option<String>,
    pub failure_detail: Option<String>,
    /// Set when the inspector found the entry on the wire but the
    /// msgpack envelope did not decode. `payload` holds the raw `d`
    /// field bytes; `attempt` / `created_at_ms` are 0. Lets callers
    /// surface "broken envelope" rather than silently dropping the
    /// match. Distinct from `state == Failed` (DLQ).
    pub decode_failed: bool,
}

/// One page of [`Introspector::get_jobs`] results. `next_cursor` is
/// `None` at the end of the range.
#[derive(Debug, Clone)]
pub struct JobsPage {
    pub jobs: Vec<JobInfo>,
    pub next_cursor: Option<String>,
}

/// Default cap on the per-query SCAN budget for `result:*` keys
/// when computing `JobCounts::completed`. Configurable via the
/// `CHASQUIMQ_COMPLETED_SCAN_CAP` env var.
const DEFAULT_COMPLETED_SCAN_CAP: u64 = 10_000;
/// Default per-iteration `COUNT` hint for SCAN.
const SCAN_COUNT_HINT: u64 = 256;
/// Default max XRANGE COUNT for stream scans during pagination — keeps
/// any single inspector call bounded.
const STREAM_SCAN_PAGE_DEFAULT: u64 = 1024;
/// Hard upper bound on a single get_jobs limit. Callers asking for more
/// page through `next_cursor`.
const STREAM_SCAN_PAGE_MAX: u64 = 10_000;

/// Read-only introspection API across the queue's stream, delayed ZSET,
/// DLQ, and result-key surfaces. Open one per (queue, consumer-group)
/// pair; safe to clone and share across tasks.
#[derive(Clone)]
pub struct Introspector {
    pool: Pool,
    queue_name: Arc<str>,
    stream_key: Arc<str>,
    delayed_key: Arc<str>,
    dlq_key: Arc<str>,
    paused_key: Arc<str>,
    group: Arc<str>,
    completed_scan_cap: u64,
}

impl Introspector {
    /// Connect against `redis_url` and bind to `queue_name` + `group`.
    ///
    /// `group` is the consumer-group name whose PEL the inspector reads
    /// for "active" state. Must match whatever the workers run under —
    /// `ConsumerConfig::group` defaults to `"default"`, which is what
    /// the shims pass when no explicit group is configured.
    ///
    /// One small pool by default (size 2) — introspection is bursty,
    /// not sustained; we don't want it competing with the producer's
    /// hot-path connections.
    pub async fn connect(
        redis_url: &str,
        queue_name: &str,
        tuning: &ConnectionTuning,
        group: Option<&str>,
    ) -> Result<Self> {
        let pool = crate::redis::conn::connect_pool(redis_url, 2, tuning).await?;
        let queue_name_arc: Arc<str> = Arc::from(queue_name);
        let stream_key_arc: Arc<str> = Arc::from(stream_key(queue_name));
        let delayed_key_arc: Arc<str> = Arc::from(delayed_key(queue_name));
        let dlq_key_arc: Arc<str> = Arc::from(dlq_key(queue_name));
        let paused_key_arc: Arc<str> = Arc::from(paused_key(queue_name));
        let group_arc: Arc<str> = Arc::from(group.unwrap_or(&ConsumerConfig::default().group));
        let completed_scan_cap = std::env::var("CHASQUIMQ_COMPLETED_SCAN_CAP")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_COMPLETED_SCAN_CAP);
        Ok(Self {
            pool,
            queue_name: queue_name_arc,
            stream_key: stream_key_arc,
            delayed_key: delayed_key_arc,
            dlq_key: dlq_key_arc,
            paused_key: paused_key_arc,
            group: group_arc,
            completed_scan_cap,
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.pool.quit().await.map_err(Error::Redis)
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// Aggregate counts. Each field is one round trip; total ~5+1.
    pub async fn get_job_counts(&self) -> Result<JobCounts> {
        let waiting_stream_len = xlen(&self.pool, &self.stream_key).await? as u64;
        let active = xpending_count(&self.pool, &self.stream_key, &self.group).await? as u64;
        let delayed = zcard(&self.pool, &self.delayed_key).await? as u64;
        let failed = xlen(&self.pool, &self.dlq_key).await? as u64;
        let paused = if exists(&self.pool, &self.paused_key).await? {
            1
        } else {
            0
        };
        let (completed, completed_is_capped) =
            scan_count_results(&self.pool, &self.queue_name, self.completed_scan_cap).await?;
        // Pending entries are still in the stream (XLEN counts them), so
        // waiting = total - active. Saturating arithmetic protects against
        // a TOCTOU race between XLEN and XPENDING (active grew between
        // calls).
        let waiting = waiting_stream_len.saturating_sub(active);
        Ok(JobCounts {
            waiting,
            active,
            delayed,
            completed,
            failed,
            paused,
            completed_is_capped,
        })
    }

    /// Live-state-first lookup. See module docs for ordering rationale.
    pub async fn get_job_state(&self, id: &str) -> Result<JobState> {
        if self.is_in_pel(id).await? {
            return Ok(JobState::Active);
        }
        if self.delayed_member(id).await?.is_some() {
            return Ok(JobState::Delayed);
        }
        if self.find_in_stream(id).await?.is_some() {
            return Ok(JobState::Waiting);
        }
        if self.find_in_dlq(id).await?.is_some() {
            return Ok(JobState::Failed);
        }
        if self.has_result_key(id).await? {
            return Ok(JobState::Completed);
        }
        Ok(JobState::Unknown)
    }

    /// Full per-id lookup. Returns `None` when the id doesn't match any
    /// surface. Each branch returns a `JobInfo` populated from whatever
    /// surface matched.
    pub async fn get_job(&self, id: &str) -> Result<Option<JobInfo>> {
        // 1. Active (PEL hit).
        if let Some(found) = self.lookup_in_pel(id).await? {
            return Ok(Some(found));
        }
        // 2. Delayed.
        if self.delayed_member(id).await?.is_some() {
            // Decode of delayed members happens elsewhere; we just
            // synthesize the info here from what we know.
            let info = self.lookup_in_delayed(id).await?;
            if info.is_some() {
                return Ok(info);
            }
        }
        // 3. Waiting.
        if let Some(found) = self.find_in_stream(id).await? {
            return Ok(Some(found));
        }
        // 4. Failed (DLQ).
        if let Some(found) = self.find_in_dlq(id).await? {
            return Ok(Some(found));
        }
        // 5. Completed.
        if self.has_result_key(id).await? {
            return Ok(Some(JobInfo {
                id: id.to_string(),
                name: String::new(),
                payload: Bytes::new(),
                attempt: 0,
                state: JobState::Completed,
                created_at_ms: 0,
                processed_on_ms: None,
                finished_on_ms: None,
                failure_reason: None,
                failure_detail: None,
                decode_failed: false,
            }));
        }
        Ok(None)
    }

    /// Paginated listing.
    ///
    /// - For `Waiting` / `Failed`: `cursor` is the last stream entry id
    ///   seen (exclusive). `offset` is honored only on the first call
    ///   (when `cursor` is `None`).
    /// - For `Delayed`: `cursor` is the last score seen as a stringified
    ///   millisecond timestamp (exclusive).
    /// - For `Completed`: `cursor` is the raw SCAN cursor (`"0"` resets
    ///   to the start).
    /// - For `Active`: paginates the PEL window with `offset` / `limit`.
    ///   Returns `next_cursor = None` after the first page (PEL is
    ///   bounded by `concurrency × read_count`).
    /// - For `Unknown`: returns an empty page.
    pub async fn get_jobs(
        &self,
        state: JobState,
        offset: u64,
        limit: u64,
        cursor: Option<String>,
    ) -> Result<JobsPage> {
        let limit = clamp_limit(limit);
        match state {
            JobState::Waiting => {
                self.paginate_stream(&self.stream_key, offset, limit, cursor, false)
                    .await
            }
            JobState::Failed => {
                self.paginate_stream(&self.dlq_key, offset, limit, cursor, true)
                    .await
            }
            JobState::Delayed => self.paginate_delayed(offset, limit, cursor).await,
            JobState::Active => self.paginate_active(offset, limit).await,
            JobState::Completed => self.paginate_completed(limit, cursor).await,
            JobState::Unknown => Ok(JobsPage {
                jobs: Vec::new(),
                next_cursor: None,
            }),
        }
    }

    // ---- Helpers ---------------------------------------------------------

    async fn pel_range(&self) -> Result<Option<(String, String, i64)>> {
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false);
        let args = vec![
            Value::from(self.stream_key.as_ref()),
            Value::from(self.group.as_ref()),
        ];
        let res = client.custom::<Value, _>(cmd, args).await;
        match res {
            Ok(v) => Ok(parse_xpending_summary(&v)),
            Err(e) if format!("{e}").contains("NOGROUP") => Ok(None),
            Err(e) => Err(Error::Redis(e)),
        }
    }

    async fn is_in_pel(&self, id: &str) -> Result<bool> {
        let Some((min_id, max_id, _)) = self.pel_range().await? else {
            return Ok(false);
        };
        let entries = xrange_window(
            &self.pool,
            &self.stream_key,
            &min_id,
            &max_id,
            STREAM_SCAN_PAGE_DEFAULT,
        )
        .await?;
        for entry in entries {
            if decoded_id_matches(&entry, id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn lookup_in_pel(&self, id: &str) -> Result<Option<JobInfo>> {
        let Some((min_id, max_id, _)) = self.pel_range().await? else {
            return Ok(None);
        };
        let entries = xrange_window(
            &self.pool,
            &self.stream_key,
            &min_id,
            &max_id,
            STREAM_SCAN_PAGE_DEFAULT,
        )
        .await?;
        for entry in entries {
            if let Some(info) = entry_to_info_if_match(&entry, id, JobState::Active) {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn find_in_stream(&self, id: &str) -> Result<Option<JobInfo>> {
        // For `find_in_stream` we don't know where the entry might be;
        // we have to walk. To keep this bounded we limit to a single
        // STREAM_SCAN_PAGE_DEFAULT chunk from `-`. Pagination is the
        // caller's escape hatch; this is the convenience "I know the id
        // is recent" path.
        let entries = xrange_window(
            &self.pool,
            &self.stream_key,
            "-",
            "+",
            STREAM_SCAN_PAGE_DEFAULT,
        )
        .await?;
        for entry in entries {
            if let Some(info) = entry_to_info_if_match(&entry, id, JobState::Waiting) {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn find_in_dlq(&self, id: &str) -> Result<Option<JobInfo>> {
        let entries = xrange_window(
            &self.pool,
            &self.dlq_key,
            "-",
            "+",
            STREAM_SCAN_PAGE_DEFAULT,
        )
        .await?;
        for entry in entries {
            if let Some(info) = dlq_entry_to_info_if_match(&entry, id) {
                return Ok(Some(info));
            }
        }
        Ok(None)
    }

    async fn delayed_member(&self, id: &str) -> Result<Option<Bytes>> {
        // Side-index key stores the exact encoded ZSET member. Reading
        // it directly avoids a ZSCAN. A miss here doesn't disprove
        // delayed presence (legacy schedules pre-idempotent path didn't
        // write the side index), but the inspector treats absence as
        // "not delayed for lookup purposes" — the engine's own delayed
        // path always uses the side index since slice 3.
        let key = delayed_index_key(self.queue_name.as_ref(), id);
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("GET", ClusterHash::FirstKey, false);
        let v: Value = client
            .custom(cmd, vec![Value::from(key)])
            .await
            .map_err(Error::Redis)?;
        Ok(value_as_bytes(&v))
    }

    async fn lookup_in_delayed(&self, id: &str) -> Result<Option<JobInfo>> {
        let Some(member) = self.delayed_member(id).await? else {
            return Ok(None);
        };
        // Decode the length-prefixed delayed-ZSET member: `n_len` u16 BE
        // | `n` UTF-8 | msgpack payload bytes.
        let (name, payload) = match decode_delayed_member(&member) {
            Some(parts) => parts,
            None => return Ok(None),
        };
        let (envelope_id, inner_payload, created_at_ms, attempt) = match peek_envelope(&payload) {
            Some(v) => v,
            None => (id.to_string(), payload.clone(), 0, 0),
        };
        Ok(Some(JobInfo {
            id: envelope_id,
            name,
            payload: inner_payload,
            attempt,
            state: JobState::Delayed,
            created_at_ms,
            processed_on_ms: None,
            finished_on_ms: None,
            failure_reason: None,
            failure_detail: None,
            decode_failed: false,
        }))
    }

    async fn has_result_key(&self, id: &str) -> Result<bool> {
        let key = result_key(self.queue_name.as_ref(), id);
        exists(&self.pool, &key).await
    }

    // ---- Pagination ------------------------------------------------------

    async fn paginate_stream(
        &self,
        key: &str,
        offset: u64,
        limit: u64,
        cursor: Option<String>,
        is_dlq: bool,
    ) -> Result<JobsPage> {
        let (lo, hi) = match cursor.as_deref() {
            Some(c) if !c.is_empty() => (format!("({c}"), "+".to_string()),
            _ => ("-".to_string(), "+".to_string()),
        };
        // Fetch one extra over `offset + limit` so we can both honor the
        // offset (first-page only) and detect "is there more". For
        // subsequent pages, cursor-based pagination already skips past
        // what we've seen — offset is ignored after the first page.
        let take_hint = if cursor.is_none() {
            offset.saturating_add(limit).saturating_add(1)
        } else {
            limit.saturating_add(1)
        };
        let entries = xrange_window(&self.pool, key, &lo, &hi, take_hint).await?;
        let mut iter = entries.into_iter();
        if cursor.is_none() {
            for _ in 0..offset {
                if iter.next().is_none() {
                    return Ok(JobsPage {
                        jobs: Vec::new(),
                        next_cursor: None,
                    });
                }
            }
        }
        let mut jobs: Vec<JobInfo> = Vec::with_capacity(limit as usize);
        let mut last_id: Option<String> = None;
        for entry in iter.by_ref().take(limit as usize) {
            let id_str = entry.id.clone();
            let info = if is_dlq {
                dlq_entry_to_info_any(&entry)
            } else {
                stream_entry_to_info_any(&entry, JobState::Waiting)
            };
            if let Some(info) = info {
                jobs.push(info);
            }
            last_id = Some(id_str);
        }
        let next_cursor = if iter.next().is_some() { last_id } else { None };
        Ok(JobsPage { jobs, next_cursor })
    }

    async fn paginate_delayed(
        &self,
        offset: u64,
        limit: u64,
        cursor: Option<String>,
    ) -> Result<JobsPage> {
        // Cursor encoding: `<score>:<offset_into_score>` so we can resume
        // mid-score when multiple delayed members share a fire-ms (cron
        // specs firing on the minute, or `add_in` calls with identical
        // delays). The plain `(score` cursor would skip every tied
        // member at that score — silent data loss.
        let (lo, score_skip) = match cursor.as_deref() {
            Some(c) if !c.is_empty() => match parse_delayed_cursor(c) {
                // Inclusive lower bound on ZRANGEBYSCORE is a plain
                // number (no `[` prefix — that's ZRANGEBYLEX syntax).
                Some((score, skip)) => (format!("{score}"), skip),
                None => {
                    // Backward-compat: a legacy bare-score cursor is
                    // honored as exclusive (same as before this fix).
                    (format!("({c}"), 0)
                }
            },
            _ => ("0".to_string(), 0),
        };
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("ZRANGEBYSCORE", ClusterHash::FirstKey, false);
        // First page honors offset; later pages use `score_skip` to step
        // past members at the boundary score we've already emitted.
        let zoffset = if cursor.is_none() {
            offset as i64
        } else {
            score_skip as i64
        };
        let take_hint = limit.saturating_add(1) as i64;
        let args = vec![
            Value::from(self.delayed_key.as_ref()),
            Value::from(lo),
            Value::from("+inf"),
            Value::from("WITHSCORES"),
            Value::from("LIMIT"),
            Value::from(zoffset),
            Value::from(take_hint),
        ];
        let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        let pairs = parse_zrange_withscores(&v);
        let mut jobs = Vec::with_capacity(limit as usize);
        let mut last_score: Option<i64> = None;
        // Count of consecutive same-score entries we've emitted at the
        // tail of this page — drives the `score_skip` in the next
        // cursor so a tied-score boundary doesn't drop members.
        let mut tail_same_score_count: u64 = 0;
        let mut over_by_one = false;
        for (idx, (member, score)) in pairs.into_iter().enumerate() {
            if idx >= limit as usize {
                over_by_one = true;
                break;
            }
            let (name, payload) = match decode_delayed_member(&member) {
                Some(parts) => parts,
                None => {
                    tracing::warn!(
                        score = score,
                        "introspect: skipping malformed delayed-ZSET member"
                    );
                    continue;
                }
            };
            let (id, inner_payload, created_at_ms, attempt) = match peek_envelope(&payload) {
                Some(v) => v,
                None => {
                    tracing::warn!(
                        score = score,
                        "introspect: delayed-ZSET member with undecodable envelope; skipping"
                    );
                    continue;
                }
            };
            jobs.push(JobInfo {
                id,
                name,
                payload: inner_payload,
                attempt,
                state: JobState::Delayed,
                created_at_ms,
                processed_on_ms: None,
                finished_on_ms: None,
                failure_reason: None,
                failure_detail: None,
                decode_failed: false,
            });
            match last_score {
                Some(prev) if prev == score => {
                    tail_same_score_count = tail_same_score_count.saturating_add(1);
                }
                _ => {
                    // Score boundary: reset the consecutive same-score
                    // counter. score_skip carried over from the cursor
                    // only applies to entries with that exact score.
                    let inherited = if last_score.is_none() {
                        score_skip
                    } else {
                        0
                    };
                    tail_same_score_count = inherited.saturating_add(1);
                }
            }
            last_score = Some(score);
        }
        let next_cursor = if over_by_one {
            last_score.map(|s| format!("{}:{}", s, tail_same_score_count))
        } else {
            None
        };
        Ok(JobsPage { jobs, next_cursor })
    }

    async fn paginate_active(&self, offset: u64, limit: u64) -> Result<JobsPage> {
        let Some((min_id, max_id, _)) = self.pel_range().await? else {
            return Ok(JobsPage {
                jobs: Vec::new(),
                next_cursor: None,
            });
        };
        let entries = xrange_window(
            &self.pool,
            &self.stream_key,
            &min_id,
            &max_id,
            STREAM_SCAN_PAGE_DEFAULT,
        )
        .await?;
        let mut iter = entries.into_iter().skip(offset as usize);
        let mut jobs = Vec::with_capacity(limit as usize);
        for entry in iter.by_ref().take(limit as usize) {
            if let Some(info) = stream_entry_to_info_any(&entry, JobState::Active) {
                jobs.push(info);
            }
        }
        Ok(JobsPage {
            jobs,
            next_cursor: None,
        })
    }

    async fn paginate_completed(&self, limit: u64, cursor: Option<String>) -> Result<JobsPage> {
        let cursor = cursor.unwrap_or_else(|| "0".to_string());
        let client = self.pool.next_connected();
        let cmd = CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false);
        let pattern = format!("{{chasqui:{}}}:result:*", self.queue_name);
        let args = vec![
            Value::from(cursor),
            Value::from("MATCH"),
            Value::from(pattern),
            Value::from("COUNT"),
            Value::from(limit.saturating_mul(2).max(64) as i64),
        ];
        let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        let (new_cursor, keys) = parse_scan_response(&v);
        let mut jobs = Vec::with_capacity(keys.len().min(limit as usize));
        for key in keys.into_iter().take(limit as usize) {
            if let Some(id) = extract_id_from_result_key(&key, &self.queue_name) {
                jobs.push(JobInfo {
                    id,
                    name: String::new(),
                    payload: Bytes::new(),
                    attempt: 0,
                    state: JobState::Completed,
                    created_at_ms: 0,
                    processed_on_ms: None,
                    finished_on_ms: None,
                    failure_reason: None,
                    failure_detail: None,
                    decode_failed: false,
                });
            }
        }
        let next_cursor = if new_cursor == "0" {
            None
        } else {
            Some(new_cursor)
        };
        Ok(JobsPage { jobs, next_cursor })
    }
}

// =========================================================================
// Helpers (pure functions over Redis values)
// =========================================================================

fn clamp_limit(limit: u64) -> u64 {
    if limit == 0 {
        STREAM_SCAN_PAGE_DEFAULT
    } else {
        limit.min(STREAM_SCAN_PAGE_MAX)
    }
}

async fn xlen(pool: &Pool, key: &str) -> Result<i64> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XLEN", ClusterHash::FirstKey, false);
    let v: Value = client
        .custom(cmd, vec![Value::from(key)])
        .await
        .map_err(Error::Redis)?;
    Ok(match v {
        Value::Integer(n) => n.max(0),
        _ => 0,
    })
}

async fn zcard(pool: &Pool, key: &str) -> Result<i64> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("ZCARD", ClusterHash::FirstKey, false);
    let v: Value = client
        .custom(cmd, vec![Value::from(key)])
        .await
        .map_err(Error::Redis)?;
    Ok(match v {
        Value::Integer(n) => n.max(0),
        _ => 0,
    })
}

async fn exists(pool: &Pool, key: &str) -> Result<bool> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("EXISTS", ClusterHash::FirstKey, false);
    let v: Value = client
        .custom(cmd, vec![Value::from(key)])
        .await
        .map_err(Error::Redis)?;
    Ok(matches!(v, Value::Integer(n) if n > 0))
}

async fn xpending_count(pool: &Pool, stream_key: &str, group: &str) -> Result<i64> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false);
    let res = client
        .custom::<Value, _>(cmd, vec![Value::from(stream_key), Value::from(group)])
        .await;
    match res {
        Ok(v) => Ok(parse_xpending_summary_count(&v)),
        Err(e) if format!("{e}").contains("NOGROUP") => Ok(0),
        Err(e) => Err(Error::Redis(e)),
    }
}

/// XPENDING summary: `[count, min_id, max_id, [[consumer, count], ...]]`.
fn parse_xpending_summary_count(v: &Value) -> i64 {
    if let Value::Array(items) = v {
        if let Some(Value::Integer(n)) = items.first() {
            return (*n).max(0);
        }
    }
    0
}

fn parse_xpending_summary(v: &Value) -> Option<(String, String, i64)> {
    let items = match v {
        Value::Array(items) => items,
        _ => return None,
    };
    let count = match items.first() {
        Some(Value::Integer(n)) => *n,
        _ => return None,
    };
    if count <= 0 {
        return None;
    }
    let min_id = value_as_string(items.get(1)?)?;
    let max_id = value_as_string(items.get(2)?)?;
    Some((min_id, max_id, count))
}

fn value_as_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.to_string()),
        Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
        _ => None,
    }
}

fn value_as_bytes(v: &Value) -> Option<Bytes> {
    match v {
        Value::Bytes(b) => Some(b.clone()),
        Value::String(s) => Some(Bytes::from(s.as_bytes().to_vec())),
        Value::Null => None,
        _ => None,
    }
}

async fn xrange_window(
    pool: &Pool,
    key: &str,
    lo: &str,
    hi: &str,
    count: u64,
) -> Result<Vec<XrangeEntry>> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false);
    let v: Value = client
        .custom(
            cmd,
            vec![
                Value::from(key),
                Value::from(lo),
                Value::from(hi),
                Value::from("COUNT"),
                Value::from(count as i64),
            ],
        )
        .await
        .map_err(Error::Redis)?;
    Ok(parse_xrange_response(&v))
}

fn entry_fields_payload_and_name(entry: &XrangeEntry) -> (Option<Bytes>, String) {
    let mut payload: Option<Bytes> = None;
    let mut name = String::new();
    for (k, v) in &entry.fields {
        match k.as_str() {
            "d" => payload = Some(v.as_bytes()),
            "n" => {
                if let Some(s) = v.as_string() {
                    name = s;
                }
            }
            _ => {}
        }
    }
    (payload, name)
}

fn decoded_id_matches(entry: &XrangeEntry, id: &str) -> bool {
    let (payload, _) = entry_fields_payload_and_name(entry);
    match payload {
        Some(bytes) => match peek_envelope(&bytes) {
            Some((entry_id, _, _, _)) => entry_id == id,
            None => {
                tracing::warn!(
                    entry_id = %entry.id,
                    "introspect: stream entry envelope did not decode; skipping"
                );
                false
            }
        },
        None => false,
    }
}

fn entry_to_info_if_match(entry: &XrangeEntry, id: &str, state: JobState) -> Option<JobInfo> {
    let (payload_bytes, name) = entry_fields_payload_and_name(entry);
    let payload_bytes = payload_bytes?;
    let (env_id, inner_payload, created_at_ms, attempt) = peek_envelope(&payload_bytes)?;
    if env_id != id {
        return None;
    }
    Some(JobInfo {
        id: env_id,
        name,
        payload: inner_payload,
        attempt,
        state,
        created_at_ms,
        processed_on_ms: stream_id_ms(&entry.id),
        finished_on_ms: None,
        failure_reason: None,
        failure_detail: None,
        decode_failed: false,
    })
}

fn stream_entry_to_info_any(entry: &XrangeEntry, state: JobState) -> Option<JobInfo> {
    let (payload_bytes, name) = entry_fields_payload_and_name(entry);
    let payload_bytes = payload_bytes?;
    match peek_envelope(&payload_bytes) {
        Some((id, inner_payload, created_at_ms, attempt)) => Some(JobInfo {
            id,
            name,
            payload: inner_payload,
            attempt,
            state,
            created_at_ms,
            processed_on_ms: stream_id_ms(&entry.id),
            finished_on_ms: None,
            failure_reason: None,
            failure_detail: None,
            decode_failed: false,
        }),
        None => {
            tracing::warn!(
                entry_id = %entry.id,
                "introspect: stream entry envelope did not decode; emitting decode_failed marker"
            );
            Some(JobInfo {
                id: String::new(),
                name,
                payload: payload_bytes,
                attempt: 0,
                state,
                created_at_ms: 0,
                processed_on_ms: stream_id_ms(&entry.id),
                finished_on_ms: None,
                failure_reason: None,
                failure_detail: None,
                decode_failed: true,
            })
        }
    }
}

fn dlq_entry_to_info_if_match(entry: &XrangeEntry, id: &str) -> Option<JobInfo> {
    let parsed = parse_dlq_fields(entry);
    if parsed.source_id != id {
        return None;
    }
    Some(dlq_parsed_to_info(parsed, entry))
}

fn dlq_entry_to_info_any(entry: &XrangeEntry) -> Option<JobInfo> {
    let parsed = parse_dlq_fields(entry);
    Some(dlq_parsed_to_info(parsed, entry))
}

fn dlq_parsed_to_info(parsed: DlqFields, entry: &XrangeEntry) -> JobInfo {
    // Try to walk the envelope to recover `attempt` / `created_at_ms`,
    // but a poison DLQ entry should still surface — the operator likely
    // wants to see it. Envelope-walk failure flips `decode_failed`.
    let (envelope_id, attempt, created_at_ms, decode_failed) = match peek_envelope(&parsed.payload)
    {
        Some((id, _, created, attempt)) => (id, attempt, created, false),
        None => (parsed.source_id.clone(), 0, 0, true),
    };
    JobInfo {
        id: envelope_id,
        name: parsed.name,
        payload: parsed.payload,
        attempt,
        state: JobState::Failed,
        created_at_ms,
        processed_on_ms: None,
        finished_on_ms: stream_id_ms(&entry.id),
        failure_reason: Some(parsed.reason),
        failure_detail: parsed.detail,
        decode_failed,
    }
}

struct DlqFields {
    source_id: String,
    reason: String,
    detail: Option<String>,
    payload: Bytes,
    name: String,
}

fn parse_dlq_fields(entry: &XrangeEntry) -> DlqFields {
    let mut source_id = String::new();
    let mut reason = String::new();
    let mut detail: Option<String> = None;
    let mut payload = Bytes::new();
    let mut name = String::new();
    for (k, v) in &entry.fields {
        match k.as_str() {
            "source_id" => {
                if let Some(s) = v.as_string() {
                    source_id = s;
                }
            }
            "reason" => {
                if let Some(s) = v.as_string() {
                    reason = s;
                }
            }
            "detail" => {
                detail = v.as_string();
            }
            "d" => payload = v.as_bytes(),
            "n" => {
                if let Some(s) = v.as_string() {
                    name = s;
                }
            }
            _ => {}
        }
    }
    DlqFields {
        source_id,
        reason,
        detail,
        payload,
        name,
    }
}

/// Decode the slice-3 length-prefixed delayed-ZSET member: `u32 LE name_len`
/// + `name` UTF-8 + msgpack payload.
///
/// `chasquimq::redis::delayed_member::decode_delayed_member` is the canonical
/// source-of-truth — this is the `Bytes`-flavored mirror used by the
/// inspector (so it doesn't allocate a `&[u8]` view-into-Bytes).
fn decode_delayed_member(bytes: &Bytes) -> Option<(String, Bytes)> {
    if bytes.len() < 4 {
        return None;
    }
    let name_len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if bytes.len() < 4 + name_len {
        return None;
    }
    let name = match std::str::from_utf8(&bytes[4..4 + name_len]) {
        Ok(s) => s.to_string(),
        Err(_) => return None,
    };
    let payload = bytes.slice(4 + name_len..);
    Some((name, payload))
}

/// Parse a Redis stream entry id `<ms>-<seq>` and recover the ms.
fn stream_id_ms(id: &str) -> Option<u64> {
    id.split_once('-')
        .and_then(|(ms, _)| ms.parse::<u64>().ok())
}

/// Parse a delayed-page cursor of the form `<score>:<offset_into_score>`.
/// Returns `(score, offset)` on success; `None` for legacy bare-score
/// cursors so callers can fall back to the old exclusive-bound shape.
fn parse_delayed_cursor(s: &str) -> Option<(i64, u64)> {
    let (score, off) = s.split_once(':')?;
    let score = score.parse::<i64>().ok()?;
    let off = off.parse::<u64>().ok()?;
    Some((score, off))
}

fn parse_zrange_withscores(v: &Value) -> Vec<(Bytes, i64)> {
    let items = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out = Vec::with_capacity(items.len() / 2);
    let mut iter = items.iter();
    while let (Some(member_v), Some(score_v)) = (iter.next(), iter.next()) {
        let member = match member_v {
            Value::Bytes(b) => b.clone(),
            Value::String(s) => Bytes::from(s.as_bytes().to_vec()),
            _ => continue,
        };
        // Score is RESP2 bulk string or RESP3 double; accept both.
        let score = match score_v {
            Value::Integer(n) => *n,
            Value::Double(d) => *d as i64,
            Value::String(s) => match s.parse::<f64>() {
                Ok(f) => f as i64,
                Err(_) => continue,
            },
            Value::Bytes(b) => match std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
            {
                Some(f) => f as i64,
                None => continue,
            },
            _ => continue,
        };
        out.push((member, score));
    }
    out
}

fn parse_scan_response(v: &Value) -> (String, Vec<String>) {
    let items = match v {
        Value::Array(items) if items.len() >= 2 => items,
        _ => return ("0".to_string(), Vec::new()),
    };
    let cursor = match &items[0] {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Integer(n) => n.to_string(),
        _ => "0".to_string(),
    };
    let keys: Vec<String> = match &items[1] {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|k| match k {
                Value::String(s) => Some(s.to_string()),
                Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    (cursor, keys)
}

fn extract_id_from_result_key(key: &str, queue: &str) -> Option<String> {
    let prefix = format!("{{chasqui:{queue}}}:result:");
    key.strip_prefix(&prefix).map(|s| s.to_string())
}

async fn scan_count_results(pool: &Pool, queue: &str, cap: u64) -> Result<(u64, bool)> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false);
    let pattern = format!("{{chasqui:{queue}}}:result:*");
    let mut cursor = "0".to_string();
    let mut count: u64 = 0;
    let mut first_call = true;
    loop {
        if !first_call && cursor == "0" {
            break;
        }
        first_call = false;
        let args = vec![
            Value::from(cursor.clone()),
            Value::from("MATCH"),
            Value::from(pattern.clone()),
            Value::from("COUNT"),
            Value::from(SCAN_COUNT_HINT as i64),
        ];
        let v: Value = client
            .custom(cmd.clone(), args)
            .await
            .map_err(Error::Redis)?;
        let (next, keys) = parse_scan_response(&v);
        count = count.saturating_add(keys.len() as u64);
        cursor = next;
        if count >= cap {
            return Ok((cap, true));
        }
    }
    Ok((count, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fred::types::Value;

    #[test]
    fn xpending_summary_parser_extracts_range() {
        // [count, min_id, max_id, [[consumer, count]]]
        let v = Value::Array(vec![
            Value::Integer(3),
            Value::String("1700000000000-0".into()),
            Value::String("1700000000002-0".into()),
            Value::Array(vec![]),
        ]);
        let parsed = parse_xpending_summary(&v).expect("Some");
        assert_eq!(parsed.0, "1700000000000-0");
        assert_eq!(parsed.1, "1700000000002-0");
        assert_eq!(parsed.2, 3);
    }

    #[test]
    fn xpending_summary_zero_count_is_none() {
        let v = Value::Array(vec![
            Value::Integer(0),
            Value::Null,
            Value::Null,
            Value::Null,
        ]);
        assert!(parse_xpending_summary(&v).is_none());
    }

    #[test]
    fn xpending_count_is_zero_for_unexpected_shape() {
        assert_eq!(parse_xpending_summary_count(&Value::Null), 0);
        assert_eq!(parse_xpending_summary_count(&Value::Integer(7)), 0);
    }

    #[test]
    fn stream_id_ms_parses_millis() {
        assert_eq!(stream_id_ms("1700000000123-0"), Some(1700000000123));
        assert_eq!(stream_id_ms("not-an-id"), None);
    }

    #[test]
    fn delayed_member_decoder_handles_named_and_unnamed() {
        // 4-byte LE length || name || payload (matches the engine's
        // canonical `delayed_member::encode_delayed_member`).
        let mut bytes = Vec::new();
        let name = "send-email";
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&[0x91, 0x01]); // msgpack [1]
        let (n, p) = decode_delayed_member(&Bytes::from(bytes)).expect("Some");
        assert_eq!(n, "send-email");
        assert_eq!(p.as_ref(), &[0x91, 0x01]);

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&[0x92, 0x02, 0x03]);
        let (n, p) = decode_delayed_member(&Bytes::from(bytes)).expect("Some");
        assert_eq!(n, "");
        assert_eq!(p.as_ref(), &[0x92, 0x02, 0x03]);
    }

    #[test]
    fn delayed_member_decoder_rejects_truncated() {
        // 3 bytes — can't even read the u32 length prefix.
        assert!(decode_delayed_member(&Bytes::from_static(&[0x00, 0x00, 0x00])).is_none());
        // 4 bytes claiming a 10-byte name but no name follows.
        assert!(decode_delayed_member(&Bytes::from_static(&[10, 0, 0, 0])).is_none());
    }

    #[test]
    fn extract_id_from_result_key_works() {
        assert_eq!(
            extract_id_from_result_key("{chasqui:demo}:result:abc123", "demo"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_id_from_result_key("{chasqui:other}:result:abc123", "demo"),
            None
        );
    }

    #[test]
    fn job_state_parse_round_trips() {
        for state in [
            JobState::Waiting,
            JobState::Active,
            JobState::Delayed,
            JobState::Completed,
            JobState::Failed,
            JobState::Unknown,
        ] {
            assert_eq!(JobState::parse(state.as_str()), Some(state));
        }
        assert!(JobState::parse("nope").is_none());
    }

    #[test]
    fn clamp_limit_floor_and_ceiling() {
        assert_eq!(clamp_limit(0), STREAM_SCAN_PAGE_DEFAULT);
        assert_eq!(clamp_limit(50), 50);
        assert_eq!(
            clamp_limit(STREAM_SCAN_PAGE_MAX + 100),
            STREAM_SCAN_PAGE_MAX
        );
    }

    #[test]
    fn delayed_cursor_parser_round_trips() {
        assert_eq!(parse_delayed_cursor("12345:3"), Some((12345, 3)));
        assert_eq!(parse_delayed_cursor("-1:0"), Some((-1, 0)));
        // Legacy bare-score cursors do not parse — the production
        // code falls back to the exclusive-bound interpretation.
        assert_eq!(parse_delayed_cursor("12345"), None);
        assert_eq!(parse_delayed_cursor(""), None);
        assert_eq!(parse_delayed_cursor(":3"), None);
        assert_eq!(parse_delayed_cursor("12345:"), None);
        assert_eq!(parse_delayed_cursor("abc:3"), None);
        assert_eq!(parse_delayed_cursor("12345:abc"), None);
    }
}
