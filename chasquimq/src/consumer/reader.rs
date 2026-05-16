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
use fred::interfaces::{ClientLike, KeysInterface};
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub(crate) struct ReadState<T> {
    pub reader: Client,
    pub stream_key: Arc<str>,
    pub paused_key: Arc<str>,
    pub cfg: ConsumerConfig,
    pub job_tx: async_channel::Sender<DispatchedJob<T>>,
    pub dlq_tx: mpsc::Sender<DlqRelocate>,
    pub shutdown: CancellationToken,
    pub pause_rx: watch::Receiver<bool>,
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
        paused_key,
        cfg,
        job_tx,
        dlq_tx,
        shutdown,
        pause_rx,
        metrics: metrics_sink,
        events,
    } = state;

    let cmd = CustomCommand::new_static("XREADGROUP", ClusterHash::FirstKey, false);
    // `drained` event firing rule: only on the full -> empty transition.
    // `last_was_non_empty` tracks the previous non-empty batch so we don't
    // emit `drained` on every blocking-poll timeout (which would be tens
    // per second with a 50ms `block_ms`).
    let mut last_was_non_empty = false;

    // Pause gate state. `PauseGate` owns the cross-process-key bookkeeping
    // so the not-paused hot path is one `watch::borrow()` (atomic) plus
    // one time comparison per batch — never a Redis round trip and never
    // per-job. `last_redis_check` is seeded in the past so a consumer
    // started against an already-paused queue parks before its first read.
    let mut gate = PauseGate::new(
        pause_rx,
        Arc::clone(&paused_key),
        Duration::from_millis(cfg.pause_poll_ms),
    );

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Park here (batch boundary) while either the in-process switch or
        // the cross-process Redis key says paused. In-flight jobs from the
        // previous XREADGROUP have already been dispatched and drain via
        // the still-running worker/ack/relocator pipeline; producers and
        // the promoter are unaffected. Returns `false` only when shutdown
        // was observed while parked, in which case we break to drain.
        if !gate.wait_until_runnable(&reader, &shutdown).await {
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

/// Batch-boundary pause gate.
///
/// ```text
///  every loop iteration (batch boundary, BEFORE XREADGROUP):
///    wait_until_runnable():
///      ┌─ in-proc paused? (watch::borrow, atomic, ~ns) ──┐
///      │                                                  │
///      ├─ cross-proc paused? ── time-gated EXISTS ────────┤
///      │   (only when pause_poll_ms elapsed since last     │
///      │    check; cached otherwise → zero round trips     │
///      │    on the not-paused hot path)                    │
///      │                                                  ▼
///      └─ neither → return true (proceed to XREADGROUP) ──►
///         either  → park:
///            select! { shutdown      => return false (drain),
///                      pause changed => re-evaluate,
///                      sleep(poll)   => recheck Redis key }
/// ```
///
/// The not-paused common path costs one `watch::Receiver::borrow()` and,
/// at most once per `pause_poll_ms`, one `EXISTS`. It is never per-job and
/// never on the produce path, satisfying the "no new hot-path round trip"
/// constraint.
struct PauseGate {
    pause_rx: watch::Receiver<bool>,
    paused_key: Arc<str>,
    poll: Duration,
    /// Last time the cross-process Redis key was queried. Seeded one full
    /// poll interval in the past so the first batch boundary always does a
    /// real check (a consumer started against an already-paused queue
    /// parks before its first XREADGROUP).
    last_redis_check: Instant,
    /// Cached result of the last `EXISTS`. Retained verbatim on a Redis
    /// error so a transient connection blip neither crashes the reader nor
    /// spuriously unpauses it.
    redis_paused: bool,
}

impl PauseGate {
    fn new(pause_rx: watch::Receiver<bool>, paused_key: Arc<str>, poll: Duration) -> Self {
        Self {
            pause_rx,
            paused_key,
            poll,
            last_redis_check: Instant::now()
                .checked_sub(poll)
                .unwrap_or_else(Instant::now),
            redis_paused: false,
        }
    }

    /// Time-gated cross-process pause-key probe. Issues a single `EXISTS`
    /// only when at least `poll` has elapsed since the previous probe;
    /// otherwise returns the cached value. On a Redis error the cached
    /// value is retained (debug-logged, never flipped) — the in-process
    /// switch is unaffected because it never touches Redis.
    async fn refresh_redis(&mut self, reader: &Client) -> bool {
        if self.last_redis_check.elapsed() < self.poll {
            return self.redis_paused;
        }
        self.last_redis_check = Instant::now();
        match reader.exists::<bool, _>(&*self.paused_key).await {
            Ok(exists) => {
                self.redis_paused = exists;
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    key = %self.paused_key,
                    "pause-key EXISTS failed; retaining last known cross-process pause state"
                );
            }
        }
        self.redis_paused
    }

    /// Block until the reader may issue its next XREADGROUP. Returns
    /// `true` when runnable, `false` when shutdown was observed while
    /// parked (caller breaks to drain).
    async fn wait_until_runnable(&mut self, reader: &Client, shutdown: &CancellationToken) -> bool {
        loop {
            let in_proc_paused = *self.pause_rx.borrow();
            let redis_paused = self.refresh_redis(reader).await;
            if !in_proc_paused && !redis_paused {
                return true;
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return false,
                changed = self.pause_rx.changed() => {
                    // All PauseControl senders dropped: no further
                    // in-process pause signals are possible. Re-evaluate
                    // against the Redis key (which may still pause us) and
                    // proceed if it doesn't — never busy-loop on the
                    // closed channel.
                    if changed.is_err() && !self.refresh_redis(reader).await {
                        return true;
                    }
                }
                _ = tokio::time::sleep(self.poll) => {
                    // Force a fresh probe on the next refresh_redis call
                    // so a cross-process resume (key DEL) is observed
                    // within `poll` even while parked.
                    self.last_redis_check = Instant::now()
                        .checked_sub(self.poll)
                        .unwrap_or_else(Instant::now);
                }
            }
        }
    }
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
