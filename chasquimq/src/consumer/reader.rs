use crate::config::ConsumerConfig;
use crate::consumer::dlq::{self, DlqReason, DlqRelocate};
use crate::consumer::worker::DispatchedJob;
use crate::error::Result;
use crate::job::Job;
use crate::redis::commands::xreadgroup_args;
use crate::redis::parse::{EntryShape, parse_xreadgroup_response};
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) struct ReadState<T> {
    pub reader: Client,
    pub stream_key: Arc<str>,
    pub cfg: ConsumerConfig,
    pub job_tx: async_channel::Sender<DispatchedJob<T>>,
    pub dlq_tx: mpsc::Sender<DlqRelocate>,
    pub shutdown: CancellationToken,
}

pub(crate) async fn reader_loop<T>(state: ReadState<T>) -> Result<()>
where
    T: DeserializeOwned + Send + 'static,
{
    let ReadState {
        reader,
        stream_key,
        cfg,
        job_tx,
        dlq_tx,
        shutdown,
    } = state;

    let cmd = CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false);
    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let args = xreadgroup_args(
            &cfg.group,
            &cfg.consumer_id,
            cfg.batch,
            cfg.block_ms,
            cfg.claim_min_idle_ms,
            &stream_key,
        );

        let response = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            r = reader.custom::<Value, _>(cmd.clone(), args) => r,
        };

        let value = match response {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "XREADGROUP failed; backing off 200ms");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        let entries = parse_xreadgroup_response(&value);
        if entries.is_empty() {
            continue;
        }

        for shape in entries {
            if dispatch_one::<T>(shape, &cfg, &job_tx, &dlq_tx)
                .await
                .is_break()
            {
                return Ok(());
            }
        }
    }

    Ok(())
}

enum DispatchFlow {
    Continue,
    Break,
}

impl DispatchFlow {
    fn is_break(&self) -> bool {
        matches!(self, DispatchFlow::Break)
    }
}

async fn dispatch_one<T>(
    shape: EntryShape,
    cfg: &ConsumerConfig,
    job_tx: &async_channel::Sender<DispatchedJob<T>>,
    dlq_tx: &mpsc::Sender<DlqRelocate>,
) -> DispatchFlow
where
    T: DeserializeOwned + Send + 'static,
{
    let entry = match shape {
        EntryShape::Ok(e) => e,
        EntryShape::MalformedWithId { id, reason } => {
            tracing::warn!(entry_id = %id, reason, "malformed stream entry; routing to DLQ");
            dlq::enqueue(dlq_tx, id, Bytes::new(), DlqReason::Malformed { reason }).await;
            return DispatchFlow::Continue;
        }
        EntryShape::Unrecoverable => {
            tracing::error!(
                "XREADGROUP returned an entry with no recoverable id; cannot DLQ — skipping"
            );
            return DispatchFlow::Continue;
        }
    };

    let attempt: u32 = u32::try_from(entry.delivery_count.saturating_add(1)).unwrap_or(u32::MAX);
    if attempt > cfg.max_attempts {
        dlq::enqueue(dlq_tx, entry.id, entry.payload, DlqReason::RetriesExhausted).await;
        return DispatchFlow::Continue;
    }

    if entry.payload.len() > cfg.max_payload_bytes {
        tracing::warn!(entry_id = %entry.id, size = entry.payload.len(), max = cfg.max_payload_bytes, "payload exceeds max_payload_bytes; routing to DLQ");
        dlq::enqueue(dlq_tx, entry.id, entry.payload, DlqReason::OversizePayload).await;
        return DispatchFlow::Continue;
    }

    match rmp_serde::from_slice::<Job<T>>(&entry.payload) {
        Ok(job) => {
            let dispatched = DispatchedJob {
                entry_id: entry.id,
                job,
            };
            if job_tx.send(dispatched).await.is_err() {
                return DispatchFlow::Break;
            }
        }
        Err(decode_err) => {
            tracing::warn!(entry_id = %entry.id, error = %decode_err, "decode failed; routing to DLQ");
            dlq::enqueue(dlq_tx, entry.id, entry.payload, DlqReason::DecodeFailed).await;
        }
    }
    DispatchFlow::Continue
}
