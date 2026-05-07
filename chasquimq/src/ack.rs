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
}

/// Sibling of [`run_ack_flusher`] for the result-backend opt-in path. Each
/// `JobOk` invokes `JOB_OK_SCRIPT` (XACKDEL + conditional SET) per-entry
/// — distinct keys/argv per entry rule out batching. Pipelining via the
/// connection-level fred client is the practical optimization.
///
/// Falls back to `EVAL` once on `NOSCRIPT` and retries the SHA path on
/// the next entry. A script return of `0` (the entry was already gone via
/// CLAIM or manual ack) is logged at debug and silently dropped — no
/// retry, no block; this matches the retry-relocator's gate behavior.
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
    while let Some(item) = rx.recv().await {
        match write_once(&client, &cfg, &item, &mut sha).await {
            Ok(acked) => {
                if !acked {
                    tracing::debug!(entry_id = %item.entry_id, job_id = %item.job_id, "ok-result write gated: entry already removed");
                }
            }
            Err(e) => {
                tracing::error!(entry_id = %item.entry_id, job_id = %item.job_id, error = %e, "ok-result write failed; entry remains pending and will be retried on next CLAIM tick");
            }
        }
    }
}

/// Returns `true` when the script wrote (XACKDEL == 1), `false` when the
/// entry was already gone (race lost — debug-log; do not retry).
async fn write_once(
    client: &Client,
    cfg: &OkResultWriterConfig,
    item: &JobOk,
    sha: &mut String,
) -> Result<bool> {
    let result_key = crate::redis::keys::result_key(&cfg.queue_name, &item.job_id);
    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    if !sha.is_empty() {
        let args = evalsha_job_ok_args(
            sha,
            &cfg.stream_key,
            &result_key,
            &cfg.group,
            item.entry_id.as_ref(),
            item.result_bytes.clone(),
            item.ttl_secs,
        );
        let res: std::result::Result<Value, fred::error::Error> =
            client.custom(evalsha_cmd, args).await;
        match res {
            Ok(v) => return Ok(parse_lua_int(&v) == 1),
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                *sha = load_job_ok_script(client).await?;
            }
            Err(e) => return Err(Error::Redis(e)),
        }
    }
    let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
    let args = eval_job_ok_args(
        JOB_OK_SCRIPT,
        &cfg.stream_key,
        &result_key,
        &cfg.group,
        item.entry_id.as_ref(),
        item.result_bytes.clone(),
        item.ttl_secs,
    );
    let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
    Ok(parse_lua_int(&v) == 1)
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
