use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::metrics::{self, DlqRouted, MetricsSink};
use crate::redis::commands::{
    RELOCATE_DLQ_SCRIPT, eval_relocate_dlq_args, evalsha_relocate_dlq_args, script_load_args,
};
use crate::redis::parse::StreamEntryId;
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;
use tokio::sync::mpsc;

const DLQ_RETRY_ATTEMPTS: usize = 3;
const DLQ_RETRY_BASE_MS: u64 = 50;

// Re-export the public observability type for the consumer module's internal
// use, so call sites can keep saying `dlq::DlqReason` without reaching into
// `crate::metrics`.
pub(crate) use crate::metrics::DlqReason;

#[derive(Debug)]
pub(crate) struct DlqRelocate {
    /// The job's stable id, plumbed from the upstream call site so the
    /// relocator hot path doesn't have to msgpack-decode `payload` just
    /// to read the id field. Carried for the events-stream `dlq` emit;
    /// placed first so debug formatting and any field-by-field logging
    /// surface the most recognisable handle. May be empty (`""`) for
    /// reader-side DLQ routes where the payload never decoded — see the
    /// emit-side comment in `run_relocator` for the contract.
    pub job_id: String,
    pub entry_id: StreamEntryId,
    pub payload: Bytes,
    pub reason: DlqReason,
    /// Attempt count that just gave up. `0` for arrival-side DLQ paths
    /// (malformed / oversize / decode-fail) where the handler never ran.
    pub attempt: u32,
    /// Dispatch name plumbed from the source stream entry's `n` field, so
    /// the DLQ entry preserves it as a sibling field. Empty for reader-side
    /// routes where the entry was malformed or had no `n` to begin with.
    pub name: String,
}

pub(crate) struct DlqRelocatorConfig {
    pub stream_key: String,
    pub dlq_key: String,
    pub group: String,
    pub producer_id: Arc<str>,
    pub max_stream_len: u64,
    pub metrics: Arc<dyn MetricsSink>,
    pub events: EventsWriter,
}

pub(crate) async fn enqueue(
    dlq_tx: &mpsc::Sender<DlqRelocate>,
    job_id: String,
    entry_id: StreamEntryId,
    payload: Bytes,
    reason: DlqReason,
    attempt: u32,
    name: String,
) {
    if dlq_tx
        .send(DlqRelocate {
            job_id,
            entry_id,
            payload,
            reason,
            attempt,
            name,
        })
        .await
        .is_err()
    {
        tracing::error!("dlq relocator channel closed; relocation dropped");
    }
}

pub(crate) async fn run_relocator(
    client: Client,
    cfg: DlqRelocatorConfig,
    mut rx: mpsc::Receiver<DlqRelocate>,
) {
    let mut sha = match load_script(&client).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "dlq relocator: SCRIPT LOAD failed; entries will reclaim via CLAIM");
            return;
        }
    };
    while let Some(relocate) = rx.recv().await {
        match relocate_with_retry(&client, &cfg, &relocate, &mut sha).await {
            Ok(true) => {
                let event = DlqRouted {
                    reason: relocate.reason,
                    attempt: relocate.attempt,
                    name: relocate.name.clone(),
                };
                let sink = &*cfg.metrics;
                metrics::dispatch("dlq_routed", move || sink.dlq_routed(event));
                // Cross-process `dlq` event mirrors the metric. Reader-side
                // routes (malformed / oversize / decode-fail) carry an
                // empty payload and an empty job id — the event still
                // fires so subscribers can count "DLQ-routed entries"
                // without losing the malformed bucket. The event id will
                // be empty in that case; consumers should treat empty id
                // as "decode-side reject, no recoverable id". The id is
                // plumbed in on the `DlqRelocate` so the relocator hot
                // path doesn't have to msgpack-decode `payload` just to
                // read the id field. The name is plumbed in the same way
                // (preserved verbatim from the source entry's `n` field).
                if cfg.events.is_enabled() {
                    cfg.events
                        .emit_dlq(
                            &relocate.job_id,
                            &relocate.name,
                            relocate.reason.as_str(),
                            relocate.attempt,
                        )
                        .await;
                }
            }
            Ok(false) => {
                // Script returned 0: the XACKDEL gate found nothing to ack —
                // a concurrent CLAIM or manual ack already removed the entry,
                // so no DLQ write happened. The gate did its job; emitting
                // `DlqRouted` here would over-count a relocation that this
                // task did not perform. Mirrors the retry relocator's
                // `Ok(false)` no-op branch.
                tracing::trace!(entry_id = %relocate.entry_id, "dlq relocation gated: entry already removed");
            }
            Err(e) => {
                tracing::error!(entry_id = %relocate.entry_id, reason = %relocate.reason.as_str(), error = %e, "DLQ relocation failed permanently; entry remains pending and will be retried on next CLAIM tick");
            }
        }
    }
}

/// Returns `Ok(true)` when the entry was relocated into the DLQ, `Ok(false)`
/// when the XACKDEL gate found nothing to ack (a concurrent path already
/// removed it — no DLQ write happened).
async fn relocate_with_retry(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
    sha: &mut String,
) -> Result<bool> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..DLQ_RETRY_ATTEMPTS {
        match relocate_once(client, cfg, relocate, sha).await {
            Ok(relocated) => return Ok(relocated),
            Err(e) => {
                let backoff = DLQ_RETRY_BASE_MS << attempt;
                tracing::warn!(entry_id = %relocate.entry_id, attempt = attempt + 1, error = %e, backoff_ms = backoff, "DLQ relocation failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| Error::Config("DLQ relocation exhausted retries".into())))
}

/// Runs [`RELOCATE_DLQ_SCRIPT`] via EVALSHA with a cached SHA, falling back to
/// EVAL (and refreshing the cache) on `NOSCRIPT`. The script does the
/// XACKDEL-gate-then-XADD move atomically, so a crash or dropped connection
/// can never leave the entry both in the DLQ and pending on the main stream.
async fn relocate_once(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
    sha: &mut String,
) -> Result<bool> {
    let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
    let args = evalsha_relocate_dlq_args(
        sha,
        &cfg.stream_key,
        &cfg.dlq_key,
        &cfg.group,
        relocate.entry_id.as_ref(),
        &cfg.producer_id,
        relocate.entry_id.as_ref(),
        relocate.payload.clone(),
        relocate.reason.as_str(),
        cfg.max_stream_len,
        &relocate.name,
        relocate.reason.detail(),
    );
    let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
    match res {
        Ok(v) => Ok(script_returned_one(&v)),
        Err(e) if format!("{e}").contains("NOSCRIPT") => {
            *sha = load_script(client).await?;
            let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
            let args = eval_relocate_dlq_args(
                RELOCATE_DLQ_SCRIPT,
                &cfg.stream_key,
                &cfg.dlq_key,
                &cfg.group,
                relocate.entry_id.as_ref(),
                &cfg.producer_id,
                relocate.entry_id.as_ref(),
                relocate.payload.clone(),
                relocate.reason.as_str(),
                cfg.max_stream_len,
                &relocate.name,
                relocate.reason.detail(),
            );
            let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
            Ok(script_returned_one(&v))
        }
        Err(e) => Err(Error::Redis(e)),
    }
}

/// [`RELOCATE_DLQ_SCRIPT`] returns Lua `1` (relocated) or `0` (gate lost).
/// `fred` may shape the integer as `Integer`, `String`, or `Bytes` depending
/// on protocol version; anything not matching `1` is treated as "did not
/// relocate" — the safe default (we'd rather miss the metric than over-count).
fn script_returned_one(v: &Value) -> bool {
    match v {
        Value::Integer(n) => *n == 1,
        Value::String(s) => s.as_bytes() == b"1",
        Value::Bytes(b) => b.as_ref() == b"1",
        _ => false,
    }
}

async fn load_script(client: &Client) -> Result<String> {
    let cmd = CustomCommand::new_static("SCRIPT", ClusterHash::FirstKey, false);
    let res: Value = client
        .custom(cmd, script_load_args(RELOCATE_DLQ_SCRIPT))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `enqueue` is the only construction site for `DlqRelocate` outside
    /// of test code. Pin the contract that the caller-supplied `job_id`
    /// is plumbed onto the struct verbatim so the relocator hot path can
    /// consume `relocate.job_id` directly without a second msgpack decode.
    #[tokio::test]
    async fn enqueue_plumbs_job_id_onto_relocate() {
        let (tx, mut rx) = mpsc::channel::<DlqRelocate>(1);
        let entry_id: StreamEntryId = std::sync::Arc::from("1700000000000-0");
        enqueue(
            &tx,
            "job-xyz-789".to_string(),
            entry_id.clone(),
            Bytes::from_static(b"opaque"),
            DlqReason::RetriesExhausted,
            5,
            "send-email".to_string(),
        )
        .await;
        let received = rx.recv().await.expect("relocate sent");
        assert_eq!(received.job_id, "job-xyz-789");
        assert_eq!(received.entry_id, entry_id);
        assert_eq!(received.attempt, 5);
        assert_eq!(received.name, "send-email");
        assert!(matches!(received.reason, DlqReason::RetriesExhausted));
    }

    /// Reader-side DLQ routes (malformed / oversize / decode-fail) plumb
    /// `String::new()` as the job id because the payload never decoded
    /// into a `Job<T>`. The struct must accept the empty string verbatim
    /// — the events-stream emit treats `""` as "decode-side reject, no
    /// recoverable id", and that contract relies on the field being
    /// untouched in transit.
    #[tokio::test]
    async fn enqueue_accepts_empty_job_id_for_reader_side_routes() {
        let (tx, mut rx) = mpsc::channel::<DlqRelocate>(1);
        let entry_id: StreamEntryId = std::sync::Arc::from("1700000000000-0");
        enqueue(
            &tx,
            String::new(),
            entry_id,
            Bytes::new(),
            DlqReason::Malformed {
                reason: "missing payload field",
            },
            0,
            String::new(),
        )
        .await;
        let received = rx.recv().await.expect("relocate sent");
        assert_eq!(received.job_id, "");
        assert_eq!(received.attempt, 0);
        assert_eq!(received.name, "");
    }
}
