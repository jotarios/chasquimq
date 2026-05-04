use crate::error::{Error, Result};
use crate::job::Job;
use crate::redis::commands::{
    REPLAY_DLQ_SCRIPT, eval_replay_args, evalsha_replay_args, script_load_args, xrange_args,
};
use crate::redis::keys::NAME_FIELD;
use crate::redis::parse::{XrangeEntry, parse_xrange_response};
use bytes::Bytes;
use fred::clients::Pool;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::DlqEntry;

pub(super) async fn xrange_dlq(
    pool: &Pool,
    dlq_key: &str,
    limit: usize,
) -> Result<Vec<XrangeEntry>> {
    let client = pool.next_connected();
    let cmd = CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, xrange_args(dlq_key, limit))
        .await
        .map_err(Error::Redis)?;
    Ok(parse_xrange_response(&res))
}

pub(super) fn parse_dlq_entry(entry: XrangeEntry) -> DlqEntry {
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
            "d" => {
                payload = v.as_bytes();
            }
            // Match the `n` field via the canonical constant so a future rename
            // of `NAME_FIELD` reaches this site automatically.
            k if k == NAME_FIELD => {
                if let Some(s) = v.as_string() {
                    name = s;
                }
            }
            _ => {}
        }
    }
    DlqEntry {
        dlq_id: entry.id,
        source_id,
        reason,
        detail,
        payload,
        name,
    }
}

pub(super) async fn replay<T>(
    pool: &Pool,
    dlq_key: &str,
    stream_key: &str,
    max_stream_len: u64,
    limit: usize,
) -> Result<usize>
where
    T: Serialize + DeserializeOwned,
{
    if limit == 0 {
        return Ok(0);
    }
    let entries = xrange_dlq(pool, dlq_key, limit).await?;
    if entries.is_empty() {
        return Ok(0);
    }

    let triples = reset_attempts::<T>(entries)?;
    if triples.is_empty() {
        return Ok(0);
    }

    let client = pool.next_connected();
    let load_cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let sha: Value = client
        .custom(load_cmd, script_load_args(REPLAY_DLQ_SCRIPT))
        .await
        .map_err(Error::Redis)?;
    let sha = match sha {
        Value::String(s) => s.to_string(),
        Value::Bytes(b) => std::str::from_utf8(&b)
            .map_err(|_| Error::Config("SCRIPT LOAD returned non-utf8 sha".into()))?
            .to_string(),
        other => {
            return Err(Error::Config(format!(
                "SCRIPT LOAD returned unexpected: {other:?}"
            )));
        }
    };

    let evalsha_cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_replay_args(&sha, dlq_key, stream_key, max_stream_len, &triples);
    let res: std::result::Result<Value, _> = client.custom(evalsha_cmd, args).await;
    let count_value = match res {
        Ok(v) => v,
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args = eval_replay_args(
                REPLAY_DLQ_SCRIPT,
                dlq_key,
                stream_key,
                max_stream_len,
                &triples,
            );
            client.custom(cmd, args).await.map_err(Error::Redis)?
        }
        Err(e) => return Err(Error::Redis(e)),
    };
    match count_value {
        Value::Integer(n) => Ok(n.max(0) as usize),
        _ => Ok(triples.len()),
    }
}

fn reset_attempts<T>(entries: Vec<XrangeEntry>) -> Result<Vec<(String, Bytes, String)>>
where
    T: Serialize + DeserializeOwned,
{
    let mut triples: Vec<(String, Bytes, String)> = Vec::with_capacity(entries.len());
    for entry in entries {
        let dlq_id = entry.id.clone();
        let parsed = parse_dlq_entry(entry);
        let mut job: Job<T> = match rmp_serde::from_slice(&parsed.payload) {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(
                    dlq_id = %dlq_id,
                    source_id = %parsed.source_id,
                    error = %e,
                    "replay_dlq: skipping entry with undecodable payload"
                );
                continue;
            }
        };
        // Reset only the attempt counter so the replay gets a fresh retry
        // budget. Per-job `retry` overrides are preserved verbatim — a
        // replayed job should still respect whatever `JobRetryOverride`
        // the producer attached, not silently revert to queue-wide config.
        job.attempt = 0;
        let bytes = Bytes::from(rmp_serde::to_vec(&job)?);
        triples.push((dlq_id, bytes, parsed.name));
    }
    Ok(triples)
}
