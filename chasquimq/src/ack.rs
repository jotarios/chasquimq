use crate::error::{Error, Result};
use crate::job::JobId;
use crate::redis::commands::{
    JOB_OK_SCRIPT, eval_job_ok_args, evalsha_job_ok_args, script_load_args, xackdel_args,
};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use fred::clients::Client;
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
/// Per-pipeline failure contract:
/// - Whole-pipeline `NOSCRIPT` → reload the SHA and rebuild the same
///   pipeline as inline `EVAL`s, single retry.
/// - Any other whole-pipeline error → error-log with the buffered count
///   and leave the entries pending. No automatic retry: a partial
///   server-side success would re-execute already-committed SETs.
///   `XCLAIM` from the same consumer group reclaims the entries on the
///   next idle sweep.
/// - `Ok(values)` → pair each `Value` with its `JobOk`; `1` = ack+SET
///   committed, `0` = race lost (entry was already removed via CLAIM or
///   a prior delivery), debug-log only; anything else is defensive-logged
///   and the entry left pending.
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
/// awaits the whole window with `pipeline.all()`. Caller resets `buf` on
/// return — this function never partial-acks.
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
        match run_evalsha_pipeline(client, cfg, buf, sha).await {
            Ok(values) => {
                report_pipeline_outcomes(buf, &values);
                return;
            }
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                // Script flushed between LOAD and flush (or mid-pipeline
                // on a follower). Reload + retry inline below.
                match load_job_ok_script(client).await {
                    Ok(s) => *sha = s,
                    Err(le) => {
                        tracing::warn!(error = %le, "ok-result writer: SCRIPT LOAD on NOSCRIPT recovery failed; falling through to inline EVAL");
                        sha.clear();
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    count = buf.len(),
                    "ok-result pipeline failed; entries remain pending and will reclaim via CLAIM (no retry — would re-execute committed SETs)",
                );
                return;
            }
        }
    }

    // Inline EVAL fallback: either we never had a SHA, or NOSCRIPT just
    // forced a rebuild. One retry, then leave pending on failure.
    match run_eval_pipeline(client, cfg, buf).await {
        Ok(values) => report_pipeline_outcomes(buf, &values),
        Err(e) => {
            tracing::error!(
                error = %e,
                count = buf.len(),
                "ok-result EVAL pipeline failed; entries remain pending and will reclaim via CLAIM",
            );
        }
    }
}

async fn run_evalsha_pipeline(
    client: &Client,
    cfg: &OkResultWriterConfig,
    buf: &[JobOk],
    sha: &str,
) -> std::result::Result<Vec<Value>, fred::error::Error> {
    let pipeline = client.pipeline();
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    for item in buf {
        let result_key = crate::redis::keys::result_key(&cfg.queue_name, &item.job_id);
        let args = evalsha_job_ok_args(
            sha,
            &cfg.stream_key,
            &result_key,
            &cfg.group,
            item.entry_id.as_ref(),
            item.result_bytes.clone(),
            item.ttl_secs,
        );
        let _: () = pipeline.custom(evalsha_cmd.clone(), args).await?;
    }
    pipeline.all().await
}

async fn run_eval_pipeline(
    client: &Client,
    cfg: &OkResultWriterConfig,
    buf: &[JobOk],
) -> std::result::Result<Vec<Value>, fred::error::Error> {
    let pipeline = client.pipeline();
    let eval_cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    for item in buf {
        let result_key = crate::redis::keys::result_key(&cfg.queue_name, &item.job_id);
        let args = eval_job_ok_args(
            JOB_OK_SCRIPT,
            &cfg.stream_key,
            &result_key,
            &cfg.group,
            item.entry_id.as_ref(),
            item.result_bytes.clone(),
            item.ttl_secs,
        );
        let _: () = pipeline.custom(eval_cmd.clone(), args).await?;
    }
    pipeline.all().await
}

fn report_pipeline_outcomes(buf: &[JobOk], values: &[Value]) {
    if values.len() != buf.len() {
        tracing::error!(
            count = buf.len(),
            returned = values.len(),
            "ok-result pipeline returned unexpected element count; entries left pending and will reclaim via CLAIM",
        );
        return;
    }
    for (item, v) in buf.iter().zip(values.iter()) {
        match parse_lua_int(v) {
            1 => {}
            0 => {
                tracing::debug!(entry_id = %item.entry_id, job_id = %item.job_id, "ok-result write gated: entry already removed");
            }
            other => {
                tracing::error!(entry_id = %item.entry_id, job_id = %item.job_id, returned = other, value = ?v, "ok-result write returned unexpected value; entry left pending");
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
