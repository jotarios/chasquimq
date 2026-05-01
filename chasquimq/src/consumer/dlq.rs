use crate::error::{Error, Result};
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

#[derive(Debug)]
pub(crate) enum DlqReason {
    RetriesExhausted,
    DecodeFailed,
    Malformed { reason: &'static str },
    OversizePayload,
}

impl DlqReason {
    fn as_str(&self) -> &'static str {
        match self {
            DlqReason::RetriesExhausted => "retries_exhausted",
            DlqReason::DecodeFailed => "decode_failed",
            DlqReason::Malformed { .. } => "malformed",
            DlqReason::OversizePayload => "oversize_payload",
        }
    }

    fn detail(&self) -> Option<&'static str> {
        if let DlqReason::Malformed { reason } = self {
            Some(reason)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(crate) struct DlqRelocate {
    pub entry_id: StreamEntryId,
    pub payload: Bytes,
    pub reason: DlqReason,
}

pub(crate) struct DlqRelocatorConfig {
    pub stream_key: String,
    pub dlq_key: String,
    pub group: String,
    pub producer_id: Arc<str>,
    pub max_stream_len: u64,
}

pub(crate) async fn enqueue(
    dlq_tx: &mpsc::Sender<DlqRelocate>,
    entry_id: StreamEntryId,
    payload: Bytes,
    reason: DlqReason,
) {
    if dlq_tx
        .send(DlqRelocate {
            entry_id,
            payload,
            reason,
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
        if let Err(e) = relocate_with_retry(&client, &cfg, &relocate).await {
            tracing::error!(entry_id = %relocate.entry_id, reason = %relocate.reason.as_str(), error = %e, "DLQ relocation failed permanently; entry remains pending and will be retried on next CLAIM tick");
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
