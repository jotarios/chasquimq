use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::metrics::{self, DlqRouted, MetricsSink};
use crate::redis::commands::{xackdel_args, xadd_dlq_args};
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
    while let Some(relocate) = rx.recv().await {
        match relocate_with_retry(&client, &cfg, &relocate).await {
            Ok(()) => {
                let event = DlqRouted {
                    reason: relocate.reason,
                    attempt: relocate.attempt,
                };
                let sink = &*cfg.metrics;
                metrics::dispatch("dlq_routed", || sink.dlq_routed(event));
                // Cross-process `dlq` event mirrors the metric. Reader-side
                // routes (malformed / oversize / decode-fail) carry an
                // empty payload and an empty job id — the event still
                // fires so subscribers can count "DLQ-routed entries"
                // without losing the malformed bucket. The event id will
                // be empty in that case; consumers should treat empty id
                // as "decode-side reject, no recoverable id". The id is
                // plumbed in on the `DlqRelocate` so the relocator hot
                // path doesn't have to msgpack-decode `payload` just to
                // read the id field.
                if cfg.events.is_enabled() {
                    cfg.events
                        .emit_dlq(&relocate.job_id, relocate.reason.as_str(), relocate.attempt)
                        .await;
                }
            }
            Err(e) => {
                tracing::error!(entry_id = %relocate.entry_id, reason = %relocate.reason.as_str(), error = %e, "DLQ relocation failed permanently; entry remains pending and will be retried on next CLAIM tick");
            }
        }
    }
}

async fn relocate_with_retry(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
) -> Result<()> {
    let mut last_err: Option<Error> = None;
    for attempt in 0..DLQ_RETRY_ATTEMPTS {
        match relocate_once(client, cfg, relocate).await {
            Ok(()) => return Ok(()),
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

async fn relocate_once(
    client: &Client,
    cfg: &DlqRelocatorConfig,
    relocate: &DlqRelocate,
) -> Result<()> {
    let pipeline = client.pipeline();
    let xadd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
    let xackdel = CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false);

    let xadd_args = xadd_dlq_args(
        &cfg.dlq_key,
        &cfg.producer_id,
        relocate.entry_id.as_ref(),
        relocate.payload.clone(),
        relocate.reason.as_str(),
        relocate.reason.detail(),
        cfg.max_stream_len,
        &relocate.name,
    );
    let xackdel_args = xackdel_args(
        &cfg.stream_key,
        &cfg.group,
        std::slice::from_ref(&relocate.entry_id),
    );

    let _: () = pipeline
        .custom(xadd, xadd_args)
        .await
        .map_err(Error::Redis)?;
    let _: () = pipeline
        .custom(xackdel, xackdel_args)
        .await
        .map_err(Error::Redis)?;
    let _: Vec<Value> = pipeline.all().await.map_err(Error::Redis)?;
    Ok(())
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
