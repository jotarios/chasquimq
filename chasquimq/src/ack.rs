use crate::consumer::StreamEntryId;
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
    tracing::error!(count = ids.len(), "xackdel batch failed after retries; entries will reclaim via CLAIM (handler may run again)");
}

async fn flush_once(
    client: &Client,
    cfg: &AckFlusherConfig,
    ids: &[StreamEntryId],
) -> std::result::Result<(), fred::error::Error> {
    let mut args: Vec<Value> = Vec::with_capacity(4 + ids.len());
    args.push(Value::from(cfg.stream_key.as_str()));
    args.push(Value::from(cfg.group.as_str()));
    args.push(Value::from("IDS"));
    args.push(Value::from(ids.len() as i64));
    for id in ids {
        args.push(Value::from(id.as_ref()));
    }
    let cmd = CustomCommand::new_static("XACKDEL", ClusterHash::FirstKey, false);
    client.custom::<Value, _>(cmd, args).await?;
    Ok(())
}
