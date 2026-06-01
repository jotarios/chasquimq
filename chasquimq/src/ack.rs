use crate::error::{Error, Result};
use crate::job::JobId;
use crate::redis::commands::{
    JOB_OK_SCRIPT, eval_job_ok_args, evalsha_job_ok_args, script_load_args, xackdel_args,
};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use fred::clients::Client;
use fred::error::Error as FredError;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

const ACK_RETRY_ATTEMPTS: usize = 4;
const ACK_RETRY_BASE_MS: u64 = 25;

pub(crate) struct AckFlusherConfig {
    pub stream_key: String,
    pub group: String,
    pub batch: usize,
    pub idle: Duration,
}

pub(crate) async fn run_ack_flusher(
    client: Client,
    cfg: AckFlusherConfig,
    mut rx: mpsc::Receiver<StreamEntryId>,
) {
    let mut buf: Vec<StreamEntryId> = Vec::with_capacity(cfg.batch);
    loop {
        if buf.is_empty() {
            match rx.recv().await {
                Some(id) => buf.push(id),
                None => return,
            }
        }

        let deadline = Instant::now() + cfg.idle;
        loop {
            if buf.len() >= cfg.batch {
                break;
            }
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                break;
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(id)) => buf.push(id),
                Ok(None) => {
                    flush_with_retry(&client, &cfg, &buf).await;
                    return;
                }
                Err(_) => break,
            }
        }

        flush_with_retry(&client, &cfg, &buf).await;
        buf.clear();
    }
}

async fn flush_with_retry(client: &Client, cfg: &AckFlusherConfig, ids: &[StreamEntryId]) {
    if ids.is_empty() {
        return;
    }
    for attempt in 0..ACK_RETRY_ATTEMPTS {
        match flush_once(client, cfg, ids).await {
            Ok(()) => return,
            Err(e) => {
                let backoff = ACK_RETRY_BASE_MS << attempt;
                tracing::warn!(error = %e, count = ids.len(), attempt = attempt + 1, backoff_ms = backoff, "xackdel batch failed; retrying");
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
        }
    }
    tracing::error!(
        count = ids.len(),
        "xackdel batch failed after retries; entries will reclaim via CLAIM (handler may run again)"
    );
}

async fn flush_once(
    client: &Client,
    cfg: &AckFlusherConfig,
    ids: &[StreamEntryId],
) -> std::result::Result<(), fred::error::Error> {
    let args = xackdel_args(&cfg.stream_key, &cfg.group, ids);
    let cmd = CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false);
    client.custom::<fred::types::Value, _>(cmd, args).await?;
    Ok(())
}

/// One handler outcome destined for the result-backend writer. Sibling to
/// the `StreamEntryId` flowing on `ack_tx`: this carries the per-job result
/// bytes, ttl, and bookkeeping needed for the per-entry `JOB_OK_SCRIPT`
/// invocation. `result_bytes` is opaque — every shim msgpack-encodes user
/// values before they cross the FFI boundary.
#[derive(Debug)]
pub(crate) struct JobOk {
    pub entry_id: StreamEntryId,
    pub job_id: JobId,
    pub result_bytes: Bytes,
    pub ttl_secs: u64,
}

pub(crate) struct OkResultWriterConfig {
    pub stream_key: String,
    pub queue_name: String,
    pub group: String,
    /// Max `JobOk` entries collected into one pipelined flush. Mirrors
    /// `AckFlusherConfig::batch`.
    pub batch: usize,
    /// Idle deadline before a partial buffer flushes. Mirrors
    /// `AckFlusherConfig::idle`.
    pub idle: Duration,
}

/// Sibling of [`run_ack_flusher`] for the result-backend opt-in path. Each
/// `JobOk` invokes `JOB_OK_SCRIPT` (XACKDEL + conditional SET); distinct
/// keys/argv per entry rule out a single-command batch (no `XACKDEL`
/// multi-result-key form), so we coalesce calls into a single fred
/// `Pipeline` and flush the whole window in one round trip.
///
/// Shape matches [`run_ack_flusher`]: a bounded `Vec<JobOk>` buffer of cap
/// `cfg.batch`, drained from `rx` until either the buffer is full or the
/// `cfg.idle` deadline elapses, then flushed with [`flush_pipeline`].
/// `Ok(None)` from `rx.recv()` flushes any pending entries and returns.
///
/// Per-pipeline failure contract (per-element via `pipeline.try_all`):
/// - Any element returning `NOSCRIPT` → reload the SHA and rebuild the
///   *whole* pipeline as inline `EVAL`s, single retry. Scripts are
///   per-server: if one element NOSCRIPTs, every element on that
///   connection will too, so re-running the whole batch is correct.
/// - Per-element non-NOSCRIPT error → error-log with the entry id; leave
///   that one entry pending so `XCLAIM` reclaims it. Other elements'
///   outcomes are still applied. The committed-SET concern from the
///   single-call path doesn't apply: `try_all` reports per-element
///   results, so we never re-execute a successful element.
/// - Per-element value `1` = ack+SET committed.
/// - Per-element value `0` = race lost (entry was already removed via
///   CLAIM or a prior delivery), debug-log only.
/// - Per-element value `-1` = entry not found in the stream (CLAIM
///   already moved it; XACKDEL is an idempotent no-op). Documented
///   `JOB_OK_SCRIPT` return; debug-log only.
pub(crate) async fn run_ok_result_writer(
    client: Client,
    cfg: OkResultWriterConfig,
    mut rx: mpsc::Receiver<JobOk>,
) {
    let mut sha = match load_job_ok_script(&client).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "ok-result writer: SCRIPT LOAD failed; falling back to inline EVAL until next successful load");
            // Fall back to inline EVAL of `JOB_OK_SCRIPT` on each call
            // until the next successful SCRIPT LOAD; behavior is correct,
            // just slower per-entry.
            String::new()
        }
    };
    let mut buf: Vec<JobOk> = Vec::with_capacity(cfg.batch);
    loop {
        if buf.is_empty() {
            match rx.recv().await {
                Some(item) => buf.push(item),
                None => return,
            }
        }

        let deadline = Instant::now() + cfg.idle;
        loop {
            if buf.len() >= cfg.batch {
                break;
            }
            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                break;
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(item)) => buf.push(item),
                Ok(None) => {
                    flush_pipeline(&client, &cfg, &buf, &mut sha).await;
                    return;
                }
                Err(_) => break,
            }
        }

        flush_pipeline(&client, &cfg, &buf, &mut sha).await;
        buf.clear();
    }
}

/// Pipelines one `EVALSHA` (or `EVAL` fallback) per buffered `JobOk` and
/// awaits the whole window with `pipeline.try_all` to surface per-element
/// outcomes. Caller resets `buf` on return — this function never
/// partial-acks at the buffer level.
async fn flush_pipeline(
    client: &Client,
    cfg: &OkResultWriterConfig,
    buf: &[JobOk],
    sha: &mut String,
) {
    if buf.is_empty() {
        return;
    }

    // First attempt: EVALSHA if we have a SHA, otherwise straight EVAL.
    if !sha.is_empty() {
        let results = run_evalsha_pipeline(client, cfg, buf, sha).await;
        if !pipeline_has_noscript(&results) {
            report_pipeline_outcomes(buf, &results);
            return;
        }
        // Any element NOSCRIPT'd. Reload SHA and fall through to inline
        // EVAL for the whole batch — scripts are per-server, so all
        // elements on the same connection are equally affected.
        match load_job_ok_script(client).await {
            Ok(s) => *sha = s,
            Err(le) => {
                tracing::warn!(error = %le, "ok-result writer: SCRIPT LOAD on NOSCRIPT recovery failed; falling through to inline EVAL");
                sha.clear();
            }
        }
    }

    // Inline EVAL fallback: either we never had a SHA, or NOSCRIPT just
    // forced a rebuild. One retry, then per-element outcomes determine
    // which entries leave pending.
    let results = run_eval_pipeline(client, cfg, buf).await;
    report_pipeline_outcomes(buf, &results);
}

async fn run_evalsha_pipeline(
    client: &Client,
    cfg: &OkResultWriterConfig,
    buf: &[JobOk],
    sha: &str,
) -> Vec<std::result::Result<Value, FredError>> {
    let pipeline = client.pipeline();
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    for item in buf {
        let result_key = crate::redis::keys::result_key(&cfg.queue_name, &item.job_id);
        let stall_key = crate::redis::keys::stall_counter_key(&cfg.queue_name, &item.job_id);
        let args = evalsha_job_ok_args(
            sha,
            &cfg.stream_key,
            &result_key,
            &stall_key,
            &cfg.group,
            item.entry_id.as_ref(),
            item.result_bytes.clone(),
            item.ttl_secs,
        );
        // Enqueueing into a pipeline is in-memory; the `await` returns
        // immediately. Errors here are buffer-allocation level and would
        // also break `try_all`; surface them as a per-element error so
        // the report path handles it uniformly.
        if let Err(e) = pipeline.custom::<Value, _>(evalsha_cmd.clone(), args).await {
            return std::iter::repeat_with(|| Err(e.clone()))
                .take(buf.len())
                .collect();
        }
    }
    pipeline.try_all::<Value>().await
}

async fn run_eval_pipeline(
    client: &Client,
    cfg: &OkResultWriterConfig,
    buf: &[JobOk],
) -> Vec<std::result::Result<Value, FredError>> {
    let pipeline = client.pipeline();
    let eval_cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    for item in buf {
        let result_key = crate::redis::keys::result_key(&cfg.queue_name, &item.job_id);
        let stall_key = crate::redis::keys::stall_counter_key(&cfg.queue_name, &item.job_id);
        let args = eval_job_ok_args(
            JOB_OK_SCRIPT,
            &cfg.stream_key,
            &result_key,
            &stall_key,
            &cfg.group,
            item.entry_id.as_ref(),
            item.result_bytes.clone(),
            item.ttl_secs,
        );
        if let Err(e) = pipeline.custom::<Value, _>(eval_cmd.clone(), args).await {
            return std::iter::repeat_with(|| Err(e.clone()))
                .take(buf.len())
                .collect();
        }
    }
    pipeline.try_all::<Value>().await
}

fn pipeline_has_noscript(results: &[std::result::Result<Value, FredError>]) -> bool {
    results
        .iter()
        .any(|r| matches!(r, Err(e) if format!("{e}").contains("NOSCRIPT")))
}

fn report_pipeline_outcomes(buf: &[JobOk], results: &[std::result::Result<Value, FredError>]) {
    if results.len() != buf.len() {
        tracing::error!(
            count = buf.len(),
            returned = results.len(),
            "ok-result pipeline returned unexpected element count; entries left pending and will reclaim via CLAIM",
        );
        return;
    }
    for (item, r) in buf.iter().zip(results.iter()) {
        match r {
            Ok(v) => match parse_lua_int(v) {
                1 => {}
                // `0` = race lost (entry already removed for this group);
                // `-1` = XACKDEL no-op (entry never pending). Both are
                // documented `JOB_OK_SCRIPT` returns — debug only.
                0 | -1 => {
                    tracing::debug!(entry_id = %item.entry_id, job_id = %item.job_id, returned = parse_lua_int(v), "ok-result write gated: entry already removed");
                }
                other => {
                    tracing::error!(entry_id = %item.entry_id, job_id = %item.job_id, returned = other, value = ?v, "ok-result write returned unexpected value; entry left pending");
                }
            },
            Err(e) => {
                tracing::error!(
                    entry_id = %item.entry_id,
                    job_id = %item.job_id,
                    error = %e,
                    "ok-result write failed for entry; will reclaim via CLAIM",
                );
            }
        }
    }
}

async fn load_job_ok_script(client: &Client) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(JOB_OK_SCRIPT))
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

fn parse_lua_int(v: &Value) -> i64 {
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
