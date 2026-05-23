//! Job maintenance — `remove`, `drain`, `clean`, `obliterate`.
//!
//! Operator / SDK surfaces for tearing jobs (or a whole queue) down. All
//! four adapt BullMQ-shaped semantics to a Redis Streams engine:
//!
//! - **`remove(job_id, group)`** — delete one job everywhere it could
//!   live: the delayed ZSET + its `didx`/`dlid` side-indexes, a waiting
//!   or active stream entry, the DLQ, and the result key. Idempotent;
//!   returns a [`RemovalReport`] flagging which surfaces actually had it.
//! - **`drain(group, opts)`** — clear waiting stream entries (those not in
//!   any consumer-group PEL) plus, by default, the delayed ZSET. In-flight
//!   (pending) jobs are left alone.
//! - **`clean(group, grace_ms, limit, state)`** — age- and state-filtered
//!   bulk delete. Removes up to `limit` jobs in the given [`JobState`]
//!   that are older than `now - grace_ms`. The age basis is the stream
//!   entry id for waiting / failed jobs and `created_at_ms` for delayed
//!   jobs; `grace_ms` is ignored for completed (result keys have no
//!   creation timestamp). Returns the removed job ids.
//! - **`obliterate(group)`** — nuke the entire `{chasqui:<queue>}`
//!   keyspace: stream + consumer groups, DLQ, delayed ZSET, every
//!   side-index, every result key, repeatable specs, the paused flag, the
//!   events stream, and the promoter / scheduler locks.
//!
//! ## Why a stream entry can't be matched by job id in Lua
//!
//! The stable [`JobId`] lives *inside* the msgpack `Job<T>` envelope (the
//! `d` field), not in the Redis stream entry id. Lua can't reliably
//! msgpack-decode, so `remove` / `clean` first run a **bounded `XRANGE`
//! scan** Rust-side to translate job ids into entry ids, then hand the
//! entry ids to an atomic Lua script. This mirrors the introspection
//! module's find-by-id design — no secondary index, bounded scans only.
//!
//! ```text
//! remove(job_id) ───┬─ delayed: GET didx ─ ZREM ─ DEL didx,dlid   (CANCEL_DELAYED_SCRIPT)
//!                   ├─ stream : XRANGE scan → entry id → XACKDEL/XDEL (REMOVE_STREAM_ENTRY_SCRIPT)
//!                   ├─ dlq    : XRANGE scan → dlq entry id → XDEL
//!                   └─ result : DEL result:<id>
//!                   = RemovalReport { delayed, stream, dlq, result }
//! ```

use crate::error::{Error, Result};
use crate::introspect::JobState;
use crate::job::{JobId, now_ms};
use crate::payload::peek_envelope;
use crate::redis::commands::{
    CANCEL_DELAYED_SCRIPT, CLEAN_STREAM_SCRIPT, DRAIN_STREAM_SCRIPT, REMOVE_STREAM_ENTRY_SCRIPT,
    eval_cancel_delayed_args, eval_clean_stream_args, eval_drain_stream_args,
    eval_remove_stream_entry_args, evalsha_cancel_delayed_args, evalsha_clean_stream_args,
    evalsha_drain_stream_args, evalsha_remove_stream_entry_args, script_load_args,
};
use crate::redis::keys::{dedup_marker_key, delayed_index_key, log_key, progress_key};
use crate::redis::parse::{XrangeEntry, parse_xrange_response};
use bytes::Bytes;
use fred::clients::{Client, Pool};
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};

/// Per-call cap on the `XRANGE` / `SCAN` window any maintenance scan
/// touches in one pass. Keeps a single `remove` / `clean` / `obliterate`
/// call bounded so it can never block Redis on an unbounded keyspace.
/// `drain` and `clean` additionally loop until a pass makes no progress.
pub(super) const MAINTENANCE_SCAN_PAGE: u64 = 1024;

/// Which surfaces a [`Producer::remove`](super::Producer::remove) call
/// actually deleted the job from. Every field is independent — a job that
/// was both DLQ'd and still had a result key reports `true` for both.
/// All-`false` means the id was not found anywhere (a no-op, not an error).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RemovalReport {
    /// Removed from the delayed ZSET (and its `didx` / `dlid`
    /// side-indexes).
    pub delayed: bool,
    /// Removed from the main stream — either a waiting entry or one that
    /// was pending in the consumer group (acked out of the PEL first).
    pub stream: bool,
    /// Removed from the dead-letter queue stream.
    pub dlq: bool,
    /// The per-job result key was deleted.
    pub result: bool,
}

impl RemovalReport {
    /// `true` when at least one surface had the job. `Producer::remove`'s
    /// idempotent contract means an all-`false` report is a valid result,
    /// not an error.
    pub fn removed_anything(&self) -> bool {
        self.delayed || self.stream || self.dlq || self.result
    }
}

/// Options for [`Producer::drain`](super::Producer::drain).
#[derive(Debug, Clone, Copy)]
pub struct DrainOptions {
    /// When `true` (the default), `drain` also empties the delayed ZSET
    /// and reaps its `didx` / `dlid` side-indexes. When `false`, scheduled
    /// future jobs survive the drain — matching BullMQ's `drain(false)`.
    pub delayed: bool,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self { delayed: true }
    }
}

// ---- internal helpers ---------------------------------------------------

/// Load a script and return its SHA, normalizing the reply across the
/// `String` / `Bytes` shapes `SCRIPT LOAD` can return depending on RESP
/// version. Same body as `producer::load_script_sha`, duplicated here only
/// to keep the maintenance module self-contained behind `pub(super)`.
async fn load_sha(client: &Client, body: &str) -> Result<String> {
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

fn lua_int(v: &Value) -> i64 {
    match v {
        Value::Integer(n) => *n,
        Value::String(s) => s.parse::<i64>().unwrap_or(0),
        Value::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        _ => 0,
    }
}

/// One `XRANGE key - + COUNT count` round trip, parsed into entries.
async fn xrange_scan(pool: &Pool, key: &str, count: u64) -> Result<Vec<XrangeEntry>> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false);
    let v: Value = client
        .custom(
            cmd,
            vec![
                Value::from(key),
                Value::from("-"),
                Value::from("+"),
                Value::from("COUNT"),
                Value::from(count as i64),
            ],
        )
        .await
        .map_err(Error::Redis)?;
    Ok(parse_xrange_response(&v))
}

/// Pull the `d` (payload) field bytes out of a stream entry.
fn entry_payload(entry: &XrangeEntry) -> Option<Bytes> {
    entry
        .fields
        .iter()
        .find(|(k, _)| k == "d")
        .map(|(_, v)| v.as_bytes())
}

/// The high 48 bits of a Redis stream id `<ms>-<seq>` are a millisecond
/// timestamp. Used as the age basis for `clean` on stream / DLQ entries.
fn stream_id_ms(id: &str) -> Option<u64> {
    id.split('-').next()?.parse::<u64>().ok()
}

/// Unlink the per-job `progress` + `log` keys for each id in `job_ids`,
/// pipelined into a single round trip per call. Mirrors the
/// `UNLINK progress + UNLINK log` tail of `remove(id)` so the
/// bulk-removal paths (`clean_stream` / `clean_delayed` /
/// `clean_completed`) leave no orphaned auxiliary keys behind. All
/// per-job keys share the `{chasqui:<queue>}` hash tag so the pipeline
/// is Cluster-correct. Best-effort: a Redis error is swallowed (with a
/// warn) so a transient blip never reverts the primary cleanup the
/// caller already did.
async fn unlink_progress_and_log(pool: &Pool, queue_name: &str, job_ids: &[String]) {
    if job_ids.is_empty() {
        return;
    }
    let client = pool.next_connected();
    let pipeline = client.pipeline();
    let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
    for id in job_ids {
        let p_key = progress_key(queue_name, id);
        let l_key = log_key(queue_name, id);
        if let Err(e) = pipeline
            .custom::<Value, _>(unlink_cmd.clone(), vec![Value::from(p_key)])
            .await
        {
            tracing::warn!(error = %e, "maintenance: queue UNLINK progress failed");
            return;
        }
        if let Err(e) = pipeline
            .custom::<Value, _>(unlink_cmd.clone(), vec![Value::from(l_key)])
            .await
        {
            tracing::warn!(error = %e, "maintenance: queue UNLINK log failed");
            return;
        }
    }
    if let Err(e) = pipeline.all::<Value>().await {
        tracing::warn!(error = %e, "maintenance: UNLINK progress+log pipeline failed");
    }
}

/// Locate the stream entry id whose msgpack envelope carries `job_id`,
/// scanning a single bounded `XRANGE` window. Returns `None` if the id is
/// not in the first `MAINTENANCE_SCAN_PAGE` entries — callers treat that
/// as "not present on this surface", consistent with the introspector.
fn find_entry_id(entries: &[XrangeEntry], job_id: &str) -> Option<String> {
    for entry in entries {
        let Some(payload) = entry_payload(entry) else {
            continue;
        };
        match peek_envelope(&payload) {
            Some((env_id, _, _, _)) if env_id == job_id => return Some(entry.id.clone()),
            Some(_) => {}
            None => {
                tracing::warn!(
                    entry_id = %entry.id,
                    "maintenance: stream entry envelope did not decode; skipping"
                );
            }
        }
    }
    None
}

/// Locate the DLQ entry id whose `source_id` field equals `job_id`.
fn find_dlq_entry_id(entries: &[XrangeEntry], job_id: &str) -> Option<String> {
    for entry in entries {
        let source = entry
            .fields
            .iter()
            .find(|(k, _)| k == "source_id")
            .and_then(|(_, v)| v.as_string());
        if source.as_deref() == Some(job_id) {
            return Some(entry.id.clone());
        }
    }
    None
}

/// `EVALSHA` runner with the `NOSCRIPT` self-heal the rest of the engine
/// uses: try `EVALSHA`, and on `NOSCRIPT` fall back to `EVAL` (which makes
/// Redis re-cache the body). `evalsha_args` carries the SHA, `eval_args`
/// carries the full script body — both are pre-built by the caller.
async fn eval_with_fallback(
    client: &Client,
    evalsha_args: Vec<Value>,
    eval_args: Vec<Value>,
) -> Result<Value> {
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, evalsha_args).await;
    match res {
        Ok(v) => Ok(v),
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            client.custom(cmd, eval_args).await.map_err(Error::Redis)
        }
        Err(e) => Err(Error::Redis(e)),
    }
}

// ---- remove -------------------------------------------------------------

/// Delete a single job everywhere it could live. See module docs.
pub(super) async fn remove(
    pool: &Pool,
    queue_name: &str,
    stream_key: &str,
    delayed_key: &str,
    dlq_key: &str,
    job_id: &JobId,
    group: &str,
) -> Result<RemovalReport> {
    let mut report = RemovalReport::default();

    // 1. Delayed ZSET — reuse CANCEL_DELAYED_SCRIPT verbatim. It GETs the
    //    didx side-index for the exact ZSET member, ZREMs it, and clears
    //    both didx and dlid. Returns 1 if the ZSET entry was removed.
    {
        let index_key = delayed_index_key(queue_name, job_id);
        let marker_key = dedup_marker_key(queue_name, job_id);
        let client = pool.next_connected();
        let sha = load_sha(client, CANCEL_DELAYED_SCRIPT).await?;
        let v = eval_with_fallback(
            client,
            evalsha_cancel_delayed_args(&sha, delayed_key, &index_key, &marker_key),
            eval_cancel_delayed_args(CANCEL_DELAYED_SCRIPT, delayed_key, &index_key, &marker_key),
        )
        .await?;
        report.delayed = lua_int(&v) == 1;
    }

    // 2. Main stream — the job id is inside the envelope, so scan a
    //    bounded window to translate it to a stream entry id, then run
    //    the atomic XACKDEL/XDEL script.
    {
        let entries = xrange_scan(pool, stream_key, MAINTENANCE_SCAN_PAGE).await?;
        if let Some(entry_id) = find_entry_id(&entries, job_id) {
            let client = pool.next_connected();
            let sha = load_sha(client, REMOVE_STREAM_ENTRY_SCRIPT).await?;
            let v = eval_with_fallback(
                client,
                evalsha_remove_stream_entry_args(&sha, stream_key, group, &entry_id),
                eval_remove_stream_entry_args(
                    REMOVE_STREAM_ENTRY_SCRIPT,
                    stream_key,
                    group,
                    &entry_id,
                ),
            )
            .await?;
            report.stream = lua_int(&v) == 1;
        }
    }

    // 3. DLQ — scan for the entry whose source_id matches, then XDEL.
    {
        let entries = xrange_scan(pool, dlq_key, MAINTENANCE_SCAN_PAGE).await?;
        if let Some(dlq_entry_id) = find_dlq_entry_id(&entries, job_id) {
            let client = pool.next_connected();
            let cmd = CustomCommand::new_static("XDEL", ClusterHash::FirstKey, false);
            let v: Value = client
                .custom(
                    cmd,
                    vec![Value::from(dlq_key), Value::from(dlq_entry_id.as_str())],
                )
                .await
                .map_err(Error::Redis)?;
            report.dlq = lua_int(&v) >= 1;
        }
    }

    // 4. Result key + progress + log stream — pipelined so all three
    //    Redis calls share one round trip. The result key keeps its DEL
    //    (so we can attribute its removal count to the report's
    //    `result` field, unchanged public contract); the progress key
    //    and the log stream ride along as `UNLINK` (async reclaim — a
    //    multi-MB log stream never stalls the call). All three keys
    //    share the `{chasqui:<queue>}` hash tag so the pipeline is
    //    Cluster-correct.
    {
        let r_key = crate::redis::keys::result_key(queue_name, job_id);
        let p_key = progress_key(queue_name, job_id);
        let l_key = log_key(queue_name, job_id);
        let client = pool.next_connected();

        let pipeline = client.pipeline();
        let del_cmd = CustomCommand::new_static("DEL", ClusterHash::FirstKey, false);
        pipeline
            .custom::<Value, _>(del_cmd, vec![Value::from(r_key.as_str())])
            .await
            .map_err(Error::Redis)?;
        let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
        pipeline
            .custom::<Value, _>(unlink_cmd.clone(), vec![Value::from(p_key)])
            .await
            .map_err(Error::Redis)?;
        pipeline
            .custom::<Value, _>(unlink_cmd, vec![Value::from(l_key)])
            .await
            .map_err(Error::Redis)?;
        let replies = pipeline.all::<Value>().await.map_err(Error::Redis)?;
        // First reply is the DEL count for the result key.
        report.result = match &replies {
            Value::Array(items) => items.first().map(lua_int).unwrap_or(0) >= 1,
            single => lua_int(single) >= 1,
        };
    }

    Ok(report)
}

// ---- drain --------------------------------------------------------------

/// Clear waiting stream entries (and, by default, the delayed ZSET).
/// In-flight (pending) jobs are left in place. Returns the total count of
/// stream + delayed entries removed.
pub(super) async fn drain(
    pool: &Pool,
    queue_name: &str,
    stream_key: &str,
    delayed_key: &str,
    group: &str,
    opts: DrainOptions,
) -> Result<u64> {
    let mut removed: u64 = 0;

    // Loop the bounded drain pass until a pass deletes nothing. Each pass
    // walks `XRANGE - + COUNT 1024`, skips the consumer-group PEL members
    // (Active jobs), and `XDEL`s the rest. The stop condition must be
    // "deleted zero", NOT "deleted < a full page": when Active entries are
    // interleaved near the front of the stream, a pass legitimately
    // deletes fewer than a full page while waiting jobs still remain
    // further back. Each `XDEL` from the front strictly shrinks the
    // waiting set and the Active set is bounded (concurrency x batch), so
    // the loop terminates once the scan window is all-Active. A hard
    // iteration cap is a belt-and-braces guard against a pathological
    // stream that somehow never converges.
    let mut passes: u32 = 0;
    const MAX_DRAIN_PASSES: u32 = 1_000_000;
    loop {
        let client = pool.next_connected();
        let sha = load_sha(client, DRAIN_STREAM_SCRIPT).await?;
        let v = eval_with_fallback(
            client,
            evalsha_drain_stream_args(&sha, stream_key, group, MAINTENANCE_SCAN_PAGE),
            eval_drain_stream_args(
                DRAIN_STREAM_SCRIPT,
                stream_key,
                group,
                MAINTENANCE_SCAN_PAGE,
            ),
        )
        .await?;
        let pass = lua_int(&v).max(0) as u64;
        removed += pass;
        passes += 1;
        if pass == 0 || passes >= MAX_DRAIN_PASSES {
            break;
        }
    }

    if opts.delayed {
        removed += drain_delayed(pool, queue_name, delayed_key).await?;
    }

    Ok(removed)
}

/// Empty the delayed ZSET and reap its `didx` / `dlid` side-indexes.
async fn drain_delayed(pool: &Pool, queue_name: &str, delayed_key: &str) -> Result<u64> {
    let client = pool.next_connected();
    // ZCARD before the DEL so we can report how many delayed jobs went.
    let zcard_cmd = CustomCommand::new_static("ZCARD", ClusterHash::FirstKey, false);
    let zcard: Value = client
        .custom(zcard_cmd, vec![Value::from(delayed_key)])
        .await
        .map_err(Error::Redis)?;
    let count = lua_int(&zcard).max(0) as u64;

    let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
    let _: Value = client
        .custom(unlink_cmd, vec![Value::from(delayed_key)])
        .await
        .map_err(Error::Redis)?;

    // Reap the per-job side-indexes. They TTL out on their own, but a
    // drain should leave the queue clean immediately.
    unlink_by_pattern(pool, &format!("{{chasqui:{queue_name}}}:didx:*")).await?;
    unlink_by_pattern(pool, &format!("{{chasqui:{queue_name}}}:dlid:*")).await?;

    Ok(count)
}

// ---- clean --------------------------------------------------------------

/// Age- and state-filtered bulk delete. See module docs for the per-state
/// age basis. Returns the job ids actually removed.
#[allow(clippy::too_many_arguments)]
pub(super) async fn clean(
    pool: &Pool,
    queue_name: &str,
    stream_key: &str,
    delayed_key: &str,
    dlq_key: &str,
    group: &str,
    grace_ms: u64,
    limit: usize,
    state: JobState,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let cutoff = now_ms().saturating_sub(grace_ms);
    let removed = match state {
        JobState::Waiting => clean_stream(pool, stream_key, group, cutoff, limit, false).await?,
        JobState::Failed => clean_stream(pool, dlq_key, group, cutoff, limit, true).await?,
        JobState::Delayed => clean_delayed(pool, queue_name, delayed_key, cutoff, limit).await?,
        JobState::Completed => clean_completed(pool, queue_name, limit).await?,
        // Active jobs are in-flight; removing one mid-execution is a
        // footgun. `remove(id)` is the deliberate per-job escape hatch.
        JobState::Active | JobState::Unknown => return Ok(Vec::new()),
    };
    // Mirror `remove(id)`'s tail: every removed job's per-job progress
    // key + log stream must also go. The per-state helpers above only
    // touch the primary surface (stream / DLQ / delayed ZSET / result
    // key); without this sweep the auxiliary keys would outlive the
    // job and only be reclaimed by `obliterate`.
    unlink_progress_and_log(pool, queue_name, &removed).await;
    Ok(removed)
}

/// `XPENDING <key> <group> - + <count>` → the set of entry ids currently
/// pending in the consumer group (Active jobs). A fresh queue with no
/// group raises `NOGROUP`; that is swallowed and the set is empty.
async fn pending_id_set(
    pool: &Pool,
    stream_key: &str,
    group: &str,
    count: u64,
) -> Result<std::collections::HashSet<String>> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false);
    let args = vec![
        Value::from(stream_key),
        Value::from(group),
        Value::from("-"),
        Value::from("+"),
        Value::from(count as i64),
    ];
    let res: std::result::Result<Value, _> = client.custom(cmd, args).await;
    let v = match res {
        Ok(v) => v,
        // A queue whose consumer group was never opened: no Active jobs.
        Err(e) if format!("{e}").contains("NOGROUP") => return Ok(Default::default()),
        Err(e) => return Err(Error::Redis(e)),
    };
    let mut set = std::collections::HashSet::new();
    if let Value::Array(items) = v {
        for item in items {
            // Each entry is `[id, consumer, idle_ms, deliveries]`.
            if let Value::Array(fields) = item {
                if let Some(id) = fields.first().and_then(|f| match f {
                    Value::String(s) => Some(s.to_string()),
                    Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
                    _ => None,
                }) {
                    set.insert(id);
                }
            }
        }
    }
    Ok(set)
}

/// Clean waiting (or DLQ) stream entries older than `cutoff`.
///
/// For the main stream (`is_dlq = false`) entries currently in the
/// consumer group's PEL are *Active*, not *Waiting* — `clean(Waiting)`
/// must leave them running, so they are subtracted from the candidate
/// set. The DLQ is a plain stream with no consumer group, so the filter
/// does not apply there.
async fn clean_stream(
    pool: &Pool,
    key: &str,
    group: &str,
    cutoff: u64,
    limit: usize,
    is_dlq: bool,
) -> Result<Vec<String>> {
    let entries = xrange_scan(pool, key, MAINTENANCE_SCAN_PAGE).await?;
    // Subtract Active (PEL) entries from the waiting-clean candidate set.
    let pending = if is_dlq {
        Default::default()
    } else {
        pending_id_set(pool, key, group, MAINTENANCE_SCAN_PAGE).await?
    };
    let mut entry_ids: Vec<String> = Vec::new();
    let mut job_ids: Vec<String> = Vec::new();
    for entry in &entries {
        if entry_ids.len() >= limit {
            break;
        }
        // Age basis: the stream entry id's millisecond prefix.
        match stream_id_ms(&entry.id) {
            Some(ms) if ms <= cutoff => {}
            _ => continue,
        }
        // An entry in the PEL is Active — skip it for clean(Waiting).
        if pending.contains(&entry.id) {
            continue;
        }
        let job_id = if is_dlq {
            entry
                .fields
                .iter()
                .find(|(k, _)| k == "source_id")
                .and_then(|(_, v)| v.as_string())
        } else {
            entry_payload(entry).and_then(|p| peek_envelope(&p).map(|(id, _, _, _)| id))
        };
        let Some(job_id) = job_id else {
            tracing::warn!(
                entry_id = %entry.id,
                "maintenance: clean could not recover a job id for entry; skipping"
            );
            continue;
        };
        entry_ids.push(entry.id.clone());
        job_ids.push(job_id);
    }
    if entry_ids.is_empty() {
        return Ok(Vec::new());
    }
    let client = pool.next_connected();
    let sha = load_sha(client, CLEAN_STREAM_SCRIPT).await?;
    let v = eval_with_fallback(
        client,
        evalsha_clean_stream_args(&sha, key, group, &entry_ids),
        eval_clean_stream_args(CLEAN_STREAM_SCRIPT, key, group, &entry_ids),
    )
    .await?;
    // The script returns the count removed; trust the ids we sent but cap
    // the reported list at that count so a concurrent delete is reflected.
    let removed = lua_int(&v).max(0) as usize;
    job_ids.truncate(removed.min(job_ids.len()));
    Ok(job_ids)
}

/// Clean delayed jobs whose **creation** is older than `cutoff`.
///
/// Age basis is the job's `created_at_ms` from the msgpack envelope — when
/// the job was first scheduled — not the ZSET score (run-at). That makes
/// `clean(Delayed, grace_ms, ...)` mean "drop scheduled jobs that were
/// created more than `grace_ms` ago", a stable, testable semantic
/// regardless of how far in the future the job was due.
///
/// Bounded by a single `ZRANGE 0 -1` page of `MAINTENANCE_SCAN_PAGE`
/// members; each member's envelope is decoded Rust-side to read
/// `created_at_ms`. Members whose envelope doesn't decode are skipped
/// (they can't be age-checked).
async fn clean_delayed(
    pool: &Pool,
    queue_name: &str,
    delayed_key: &str,
    cutoff: u64,
    limit: usize,
) -> Result<Vec<String>> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("ZRANGE", ClusterHash::FirstKey, false);
    let args = vec![
        Value::from(delayed_key),
        Value::from(0_i64),
        Value::from((MAINTENANCE_SCAN_PAGE as i64).saturating_sub(1)),
    ];
    let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
    let all_members: Vec<Bytes> = match v {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|item| match item {
                Value::Bytes(b) => Some(b),
                Value::String(s) => Some(Bytes::from(s.as_bytes().to_vec())),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    if all_members.is_empty() {
        return Ok(Vec::new());
    }
    // Decode each member, keep only those created at or before `cutoff`,
    // and stop once `limit` matches are collected.
    let mut members: Vec<Bytes> = Vec::new();
    let mut job_ids: Vec<String> = Vec::new();
    for member in all_members {
        if members.len() >= limit {
            break;
        }
        let Some((_, payload)) = crate::redis::delayed_member::decode_delayed_member(&member)
        else {
            tracing::warn!("maintenance: clean_delayed skipping malformed ZSET member");
            continue;
        };
        let Some((id, _, created_at_ms, _)) = peek_envelope(payload) else {
            tracing::warn!("maintenance: clean_delayed skipping member with undecodable envelope");
            continue;
        };
        if created_at_ms > cutoff {
            continue;
        }
        members.push(member.clone());
        job_ids.push(id);
    }
    if members.is_empty() {
        return Ok(Vec::new());
    }
    // ZREM the exact members in one call.
    let zrem_cmd = CustomCommand::new_static("ZREM", ClusterHash::FirstKey, false);
    let mut zrem_args: Vec<Value> = Vec::with_capacity(1 + members.len());
    zrem_args.push(Value::from(delayed_key));
    for m in &members {
        zrem_args.push(Value::Bytes(m.clone()));
    }
    let removed: Value = client
        .custom(zrem_cmd, zrem_args)
        .await
        .map_err(Error::Redis)?;
    let removed = lua_int(&removed).max(0) as usize;

    // Reap the side-indexes for the removed ids (best-effort).
    for id in &job_ids {
        if id.is_empty() {
            continue;
        }
        let didx = delayed_index_key(queue_name, id);
        let dlid = dedup_marker_key(queue_name, id);
        let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
        let _: std::result::Result<Value, _> = client
            .custom(unlink_cmd, vec![Value::from(didx), Value::from(dlid)])
            .await;
    }
    job_ids.truncate(removed.min(job_ids.len()));
    Ok(job_ids)
}

/// Clean completed jobs — delete up to `limit` result keys. `grace_ms` is
/// not applied: a result key has no creation timestamp, and its own
/// `result_ttl_secs` already handles age-based expiry. Documented in
/// `docs/engine.md`.
async fn clean_completed(pool: &Pool, queue_name: &str, limit: usize) -> Result<Vec<String>> {
    let pattern = format!("{{chasqui:{queue_name}}}:result:*");
    let prefix = format!("{{chasqui:{queue_name}}}:result:");
    let mut cursor = "0".to_string();
    let mut removed: Vec<String> = Vec::new();
    loop {
        if removed.len() >= limit {
            break;
        }
        let client = pool.next_connected();
        let cmd = CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false);
        let v: Value = client
            .custom(
                cmd,
                vec![
                    Value::from(cursor.as_str()),
                    Value::from("MATCH"),
                    Value::from(pattern.as_str()),
                    Value::from("COUNT"),
                    Value::from(256_i64),
                ],
            )
            .await
            .map_err(Error::Redis)?;
        let (next, keys) = parse_scan(&v);
        for key in keys {
            if removed.len() >= limit {
                break;
            }
            let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
            let del: Value = client
                .custom(unlink_cmd, vec![Value::from(key.as_str())])
                .await
                .map_err(Error::Redis)?;
            if lua_int(&del) >= 1 {
                if let Some(id) = key.strip_prefix(prefix.as_str()) {
                    removed.push(id.to_string());
                }
            }
        }
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    Ok(removed)
}

// ---- obliterate ---------------------------------------------------------

/// Nuke the entire `{chasqui:<queue>}` keyspace. Rust-orchestrated
/// `SCAN MATCH {chasqui:<queue>}:* ` in batches, each batch `UNLINK`ed
/// (async reclaim — a multi-GB stream never stalls Redis). Not atomic;
/// obliterate is a destructive admin op where a crash mid-teardown is
/// fully recoverable by re-running (the SCAN finds the remainder).
pub(super) async fn obliterate(pool: &Pool, queue_name: &str, _group: &str) -> Result<u64> {
    let pattern = format!("{{chasqui:{queue_name}}}:*");
    unlink_by_pattern(pool, &pattern).await
}

/// `SCAN MATCH <pattern>` in 256-key batches, `UNLINK` each batch. Returns
/// the total count of keys removed. Shared by `obliterate` and the
/// side-index reaping in `drain` / `clean`.
async fn unlink_by_pattern(pool: &Pool, pattern: &str) -> Result<u64> {
    let mut cursor = "0".to_string();
    let mut removed: u64 = 0;
    loop {
        let client = pool.next_connected();
        let cmd = CustomCommand::new_static("SCAN", ClusterHash::FirstKey, false);
        let v: Value = client
            .custom(
                cmd,
                vec![
                    Value::from(cursor.as_str()),
                    Value::from("MATCH"),
                    Value::from(pattern),
                    Value::from("COUNT"),
                    Value::from(256_i64),
                ],
            )
            .await
            .map_err(Error::Redis)?;
        let (next, keys) = parse_scan(&v);
        if !keys.is_empty() {
            let unlink_cmd = CustomCommand::new_static("UNLINK", ClusterHash::FirstKey, false);
            let mut args: Vec<Value> = Vec::with_capacity(keys.len());
            for k in &keys {
                args.push(Value::from(k.as_str()));
            }
            let del: Value = client
                .custom(unlink_cmd, args)
                .await
                .map_err(Error::Redis)?;
            removed += lua_int(&del).max(0) as u64;
        }
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    Ok(removed)
}

/// Parse a `SCAN` reply `[cursor, [key, key, ...]]` into `(cursor, keys)`.
fn parse_scan(v: &Value) -> (String, Vec<String>) {
    let items = match v {
        Value::Array(items) => items,
        _ => return ("0".to_string(), Vec::new()),
    };
    let cursor = items
        .first()
        .and_then(|c| match c {
            Value::String(s) => Some(s.to_string()),
            Value::Bytes(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
            Value::Integer(n) => Some(n.to_string()),
            _ => None,
        })
        .unwrap_or_else(|| "0".to_string());
    let keys = match items.get(1) {
        Some(Value::Array(ks)) => ks
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removal_report_removed_anything() {
        assert!(!RemovalReport::default().removed_anything());
        assert!(
            RemovalReport {
                stream: true,
                ..Default::default()
            }
            .removed_anything()
        );
        assert!(
            RemovalReport {
                delayed: true,
                stream: true,
                dlq: true,
                result: true,
            }
            .removed_anything()
        );
    }

    #[test]
    fn drain_options_default_includes_delayed() {
        assert!(DrainOptions::default().delayed);
    }

    #[test]
    fn stream_id_ms_parses_prefix() {
        assert_eq!(stream_id_ms("1700000000000-0"), Some(1_700_000_000_000));
        assert_eq!(stream_id_ms("1700000000000-7"), Some(1_700_000_000_000));
        assert_eq!(stream_id_ms("not-an-id"), None);
        assert_eq!(stream_id_ms(""), None);
    }

    #[test]
    fn parse_scan_handles_string_and_bytes() {
        let reply = Value::Array(vec![
            Value::String("42".into()),
            Value::Array(vec![
                Value::String("{chasqui:q}:result:a".into()),
                Value::Bytes(Bytes::from_static(b"{chasqui:q}:result:b")),
            ]),
        ]);
        let (cursor, keys) = parse_scan(&reply);
        assert_eq!(cursor, "42");
        assert_eq!(keys, vec!["{chasqui:q}:result:a", "{chasqui:q}:result:b"]);
    }

    #[test]
    fn parse_scan_terminal_cursor() {
        let reply = Value::Array(vec![Value::Integer(0), Value::Array(vec![])]);
        let (cursor, keys) = parse_scan(&reply);
        assert_eq!(cursor, "0");
        assert!(keys.is_empty());
    }
}
