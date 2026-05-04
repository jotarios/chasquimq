use crate::config::ConsumerConfig;
use crate::consumer::dlq::{self, DlqReason, DlqRelocate};
use crate::consumer::worker::DispatchedJob;
use crate::error::Result;
use crate::events::EventsWriter;
use crate::job::Job;
use crate::metrics::{self, MetricsSink, ReaderBatch};
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
    pub metrics: Arc<dyn MetricsSink>,
    pub events: EventsWriter,
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
        metrics: metrics_sink,
        events,
    } = state;

    let cmd = CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false);
    // `drained` event firing rule: only on the full -> empty transition.
    // `last_was_non_empty` tracks the previous non-empty batch so we don't
    // emit `drained` on every blocking-poll timeout (which would be tens
    // per second with a 50ms `block_ms`).
    let mut last_was_non_empty = false;
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
            // Transition guard: we only emit `drained` when the *previous*
            // non-empty batch is followed by an empty one. A consumer that
            // starts up against an empty queue does not emit (initial state
            // is already drained). Without this guard a `block_ms = 50` /
            // idle queue would emit ~20 events/s/consumer.
            if last_was_non_empty && events.is_enabled() {
                events.emit_drained().await;
            }
            last_was_non_empty = false;
            continue;
        }
        last_was_non_empty = true;

        // Emit ReaderBatch on every non-empty response. `size` is the raw
        // count from Redis (including entries that are about to be DLQ-routed
        // for being malformed / oversize / undecodable). `reclaimed` counts
        // entries with delivery_count > 1, which is the CLAIM-recovery
        // signal — only `EntryShape::Ok` carries a parsed delivery_count;
        // malformed/unrecoverable variants don't, and aren't included in
        // the reclaimed count even if Redis bumped their delivery counter
        // (their cardinality is dominated by the malformed signal anyway).
        let size = entries.len() as u64;
        let reclaimed = entries
            .iter()
            .filter(|e| match e {
                EntryShape::Ok(p) => p.delivery_count > 1,
                _ => false,
            })
            .count() as u64;
        let batch = ReaderBatch { size, reclaimed };
        let sink = &*metrics_sink;
        metrics::dispatch("reader_batch", || sink.reader_batch(batch));

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
            // Reader-side DLQ: handler never ran, so attempt is 0. No
            // recoverable job id (the entry never decoded into a `Job<T>`),
            // so plumb the empty string — the event-emit contract treats
            // `""` as "decode-side reject". Same for the name: a malformed
            // entry has no recoverable `n` field, plumb empty.
            dlq::enqueue(
                dlq_tx,
                String::new(),
                id,
                Bytes::new(),
                DlqReason::Malformed { reason },
                0,
                String::new(),
            )
            .await;
            return DispatchFlow::Continue;
        }
        EntryShape::Unrecoverable => {
            tracing::error!(
                "XREADGROUP returned an entry with no recoverable id; cannot DLQ — skipping"
            );
            return DispatchFlow::Continue;
        }
    };

    if entry.payload.len() > cfg.max_payload_bytes {
        tracing::warn!(entry_id = %entry.id, size = entry.payload.len(), max = cfg.max_payload_bytes, "payload exceeds max_payload_bytes; routing to DLQ");
        // Oversize: the payload was never decoded into a `Job<T>`
        // (we'd be doing the work the size cap exists to prevent), so
        // plumb the empty job id — the event-emit contract treats `""`
        // as "decode-side reject". The `n` field is plumbed verbatim
        // since it lives outside the payload bytes; preserving it on
        // the DLQ entry keeps "route by name" tooling correct even
        // for oversize-rejected jobs.
        dlq::enqueue(
            dlq_tx,
            String::new(),
            entry.id,
            entry.payload,
            DlqReason::OversizePayload,
            0,
            entry.name,
        )
        .await;
        return DispatchFlow::Continue;
    }

    let mut job: Job<T> = match rmp_serde::from_slice(&entry.payload) {
        Ok(j) => j,
        Err(decode_err) => {
            tracing::warn!(entry_id = %entry.id, error = %decode_err, "decode failed; routing to DLQ");
            // Decode failed: by definition there's no recoverable job id.
            dlq::enqueue(
                dlq_tx,
                String::new(),
                entry.id,
                entry.payload,
                DlqReason::DecodeFailed,
                0,
                String::new(),
            )
            .await;
            return DispatchFlow::Continue;
        }
    };
    // The `n` stream-entry field is the source of truth for `Job::name` in
    // slice 1 — the field is `#[serde(skip)]` on `Job<T>` so msgpack-decode
    // hands us `name = ""` regardless of what was on the wire. Forward-compat
    // with old producers (no `n` field) is automatic: the parser returns
    // `String::new()` for missing fields.
    job.name = entry.name.clone();

    // Pick the larger of the in-payload attempt counter and the CLAIM-derived
    // delivery_count. The retry-relocator path increments job.attempt explicitly;
    // the CLAIM safety-net path bumps delivery_count when a worker crashes mid-
    // handler. Either route should trigger DLQ once max_attempts is exhausted.
    let claim_seen = u32::try_from(entry.delivery_count.saturating_sub(1)).unwrap_or(0);
    let prior_attempts = job.attempt.max(claim_seen);
    let next_attempt = prior_attempts.saturating_add(1);
    // Per-job override: `Job::retry.max_attempts` wins over the queue-wide
    // `cfg.max_attempts` when set. Mirrors the logic in `worker::on_handler_failure`
    // so the arrival-time DLQ gate and the post-handler DLQ gate use the same budget.
    let max_attempts = job
        .retry
        .as_ref()
        .and_then(|r| r.max_attempts)
        .unwrap_or(cfg.max_attempts);
    if next_attempt > max_attempts {
        // Retries-exhausted-on-arrival: carry the prior attempt count so
        // operators can see how many tries the job got before being shed.
        // The job decoded successfully, so plumb its id directly — the
        // relocator no longer needs to re-decode `payload` on the hot path.
        dlq::enqueue(
            dlq_tx,
            job.id.clone(),
            entry.id,
            entry.payload,
            DlqReason::RetriesExhausted,
            prior_attempts,
            job.name.clone(),
        )
        .await;
        return DispatchFlow::Continue;
    }

    let dispatched = DispatchedJob {
        entry_id: entry.id,
        job,
    };
    if job_tx.send(dispatched).await.is_err() {
        return DispatchFlow::Break;
    }
    DispatchFlow::Continue
}
