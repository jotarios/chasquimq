use crate::error::{Error, Result};
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
    pub entry_id: StreamEntryId,
    pub payload: Bytes,
    pub reason: DlqReason,
    /// Attempt count that just gave up. `0` for arrival-side DLQ paths
    /// (malformed / oversize / decode-fail) where the handler never ran.
    pub attempt: u32,
}

pub(crate) struct DlqRelocatorConfig {
    pub stream_key: String,
    pub dlq_key: String,
    pub group: String,
    pub producer_id: Arc<str>,
    pub max_stream_len: u64,
    pub metrics: Arc<dyn MetricsSink>,
}

pub(crate) async fn enqueue(
    dlq_tx: &mpsc::Sender<DlqRelocate>,
    entry_id: StreamEntryId,
    payload: Bytes,
    reason: DlqReason,
    attempt: u32,
) {
    if dlq_tx
        .send(DlqRelocate {
            entry_id,
            payload,
            reason,
            attempt,
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
