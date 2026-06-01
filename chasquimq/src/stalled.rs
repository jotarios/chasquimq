//! Stalled-job detector (slice 12).
//!
//! Today, ChasquiMQ recovers stuck deliveries passively: the reader's
//! `XREADGROUP ... CLAIM <claim_min_idle_ms>` call re-claims any PEL
//! entry idle for longer than the threshold and dispatches it again.
//! That works for "worker fell over once" but doesn't bound the
//! "worker keeps crashing on this entry" case — the entry just keeps
//! getting re-delivered, with `delivery_count` rising forever.
//!
//! This module adds an **active** detector: a leader-elected
//! background task spawned alongside the promoter and scheduler that
//! scans the consumer group's PEL on a tick, INCRs a per-job stall
//! counter, and — when a job has been observed sitting idle past the
//! threshold for `max_stalled_attempts` consecutive ticks —
//! atomically relocates the entry to the DLQ as
//! [`crate::DlqReason::Stalled`].
//!
//! ## Invariants
//!
//! - **Reader owns CLAIM.** This module never issues `XCLAIM` and
//!   never calls `XREADGROUP ... CLAIM`. The reader's CLAIM-on-read
//!   path is the only path that bumps `delivery_count`; the detector
//!   reads idle-ms but never modifies it.
//! - **One INCR per scan tick per entry.** The detector's unit of
//!   measurement is "this entry has now been observed sitting idle
//!   past the threshold for the N-th consecutive scan." When the
//!   reader CLAIMs the entry between scans and the handler succeeds,
//!   the entry leaves the PEL; the next scan doesn't see it; the
//!   counter TTLs out. When the worker keeps crashing, the entry
//!   keeps reappearing, the counter climbs, and at threshold we
//!   relocate.
//! - **Detector tick == idle threshold.** Decoupling these breaks the
//!   per-crash counting invariant (a 5s tick on a 30s threshold INCRs
//!   6x per crash). Validation rejects `tick_interval_ms <
//!   idle_threshold_ms`; the embedded spawn inherits both fields from
//!   `ConsumerConfig::claim_min_idle_ms` so the common path is
//!   always valid.
//! - **DLQ write reuses the relocator.** The Lua script only handles
//!   INCR + EXPIRE + the XACKDEL gate; the DLQ `XADD` travels the
//!   existing [`crate::consumer::dlq::run_relocator`] pipeline so the
//!   IDMP-XADD wire shape, the `e=dlq` emit, and dlq_inflight
//!   backpressure are all reused.
//! - **Successful ack / DLQ replay reset.** `JOB_OK_SCRIPT` and
//!   `REPLAY_DLQ_SCRIPT` both `DEL` the stall counter in the same
//!   Lua call so a one-off stall followed by success starts a fresh
//!   streak.

use crate::config::StalledDetectorConfig;
use crate::consumer::dlq::{self, DlqRelocate};
use crate::error::{Error, Result};
use crate::events::EventsWriter;
use crate::job::Job;
use crate::leader_task::{
    backoff_after, is_transient, load_script, sleep_or_shutdown, value_as_bool, value_as_u64,
};
use crate::metrics::{DlqReason, LockOutcome, StalledTick, dispatch};
use crate::redis::commands::{
    ACQUIRE_LOCK_SCRIPT, RELEASE_LOCK_SCRIPT, STALLED_SCAN_SCRIPT, eval_acquire_lock_args,
    eval_release_lock_args, eval_stalled_scan_args, evalsha_acquire_lock_args,
    evalsha_stalled_scan_args, xpending_idle_args, xrange_id_args,
};
use crate::redis::conn::connect;
use crate::redis::keys::{stalled_lock_key, stream_key};
use crate::redis::parse::parse_xrange_response;
use bytes::Bytes;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use serde::de::IgnoredAny;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Public scaffold for the standalone detector. The hot work happens
/// inside [`StalledDetector::run`]; the constructor surface mirrors
/// [`crate::Promoter`] (a `new` for standalone use plus a
/// `with_shared_*` constructor for the embedded path so the detector
/// reuses the consumer's already-spawned `dlq::run_relocator` channel
/// and shared `EventsWriter`).
pub struct StalledDetector {
    redis_url: String,
    cfg: StalledDetectorConfig,
    group: String,
    stream_key: String,
    lock_key: String,
    /// Shared events writer handed in by the embedding `Consumer`.
    /// When `Some`, the detector emits through this writer instead of
    /// opening a second Redis connection.
    shared_events: Option<Arc<EventsWriter>>,
    /// Shared `dlq_tx` channel handed in by the embedding `Consumer`.
    /// When `Some`, the detector reuses the consumer's already-spawned
    /// `run_relocator` instead of spawning its own — critical so the
    /// `dlq_inflight` budget and IDMP-XADD producer_id stay coherent
    /// across all DLQ-bound paths. When `None` (standalone), the
    /// detector emits a structured warn and counter-only continues:
    /// without a relocator channel the detector can still increment
    /// counters and emit `e=stalled` for observability, but it cannot
    /// move entries to the DLQ. The standalone path is intended for
    /// operator-driven observability use; production should always
    /// embed via `Consumer::run`.
    shared_dlq_tx: Option<mpsc::Sender<DlqRelocate>>,
}

impl StalledDetector {
    /// Construct a standalone detector. `group` is the consumer-group
    /// the detector scans; must match whatever the workers use.
    pub fn new(
        redis_url: impl Into<String>,
        cfg: StalledDetectorConfig,
        group: impl Into<String>,
    ) -> Self {
        let stream_key = stream_key(&cfg.queue_name);
        let lock_key = stalled_lock_key(&cfg.queue_name);
        Self {
            redis_url: redis_url.into(),
            cfg,
            group: group.into(),
            stream_key,
            lock_key,
            shared_events: None,
            shared_dlq_tx: None,
        }
    }

    /// Hand the detector a shared `EventsWriter` so it can emit
    /// `e=stalled` events through the consumer's events connection
    /// instead of opening its own. Used by `Consumer::spawn_stalled_detector`.
    pub(crate) fn with_shared_events(mut self, events: Arc<EventsWriter>) -> Self {
        self.shared_events = Some(events);
        self
    }

    /// Hand the detector a `dlq_tx` channel so threshold-hit relocates
    /// flow through the consumer's existing `run_relocator` instead of
    /// being dropped.
    pub(crate) fn with_shared_dlq_tx(mut self, tx: mpsc::Sender<DlqRelocate>) -> Self {
        self.shared_dlq_tx = Some(tx);
        self
    }

    /// Run the detector loop. Resolves once the engine drains
    /// (`shutdown` fires).
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        tracing::debug!(
            queue = %self.cfg.queue_name,
            holder_id = %self.cfg.holder_id,
            tick_ms = self.cfg.tick_interval_ms,
            idle_ms = self.cfg.idle_threshold_ms,
            max_stalled_attempts = self.cfg.max_stalled_attempts,
            "stalled detector run entry"
        );
        let client = connect(&self.redis_url, &self.cfg.connection).await?;
        // Events writer: clone the shared `Arc<EventsWriter>` when
        // embedded; standalone uses `EventsWriter::disabled()` since we
        // don't own a queue-events knob here (standalone observability
        // setups can wire one up in a future slice).
        let events = match &self.shared_events {
            Some(shared) => EventsWriter::clone(shared),
            None => EventsWriter::disabled(),
        };
        let mut scan_sha = load_script(&client, STALLED_SCAN_SCRIPT).await?;
        let mut lock_sha = load_script(&client, ACQUIRE_LOCK_SCRIPT).await?;
        let outcome = self
            .loop_until_shutdown(&client, &events, &mut scan_sha, &mut lock_sha, &shutdown)
            .await;
        self.release_lock_best_effort(&client).await;
        outcome
    }

    async fn loop_until_shutdown(
        &self,
        client: &Client,
        events: &EventsWriter,
        scan_sha: &mut String,
        lock_sha: &mut String,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        let tick = Duration::from_millis(self.cfg.tick_interval_ms.max(1));
        let mut backoff_idx: usize = 0;
        let mut last_outcome: Option<LockOutcome> = None;

        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let acquired = match self.acquire_lock(client, lock_sha).await {
                Ok(v) => v,
                Err(LockError::NoScript) => {
                    *lock_sha = load_script(client, ACQUIRE_LOCK_SCRIPT).await?;
                    self.acquire_lock_via_eval(client).await?
                }
                Err(LockError::Transient(e)) => {
                    match backoff_after(backoff_idx, &e, "stalled.acquire_lock", shutdown).await {
                        Some(next) => backoff_idx = next,
                        None => return Ok(()),
                    }
                    continue;
                }
                Err(LockError::Permanent(e)) => return Err(Error::Redis(e)),
            };

            let outcome = if acquired {
                LockOutcome::Acquired
            } else {
                LockOutcome::Held
            };
            // Reuse the promoter_lock_outcome channel for parity with
            // the other leader loops — a dedicated `stalled_lock_outcome`
            // would force every downstream sink to special-case it
            // without adding signal. Operators that need to disambiguate
            // can filter on the `holder_id` shape (`sd-<uuid>`).
            if last_outcome != Some(outcome) {
                let sink = &*self.cfg.metrics;
                dispatch("stalled_lock_outcome", || {
                    sink.promoter_lock_outcome(outcome)
                });
                last_outcome = Some(outcome);
            }

            if !acquired {
                if !sleep_or_shutdown(tick, shutdown).await {
                    return Ok(());
                }
                continue;
            }

            match self.scan_once(client, events, scan_sha).await {
                Ok(ScanResult { tick: t, drain }) => {
                    let sink = &*self.cfg.metrics;
                    dispatch("stalled_tick", || sink.stalled_tick(t));
                    backoff_idx = 0;
                    if drain {
                        // Likely more idle PEL entries; iterate immediately.
                        continue;
                    }
                    if !sleep_or_shutdown(tick, shutdown).await {
                        return Ok(());
                    }
                }
                Err(ScanError::NoScript) => {
                    *scan_sha = load_script(client, STALLED_SCAN_SCRIPT).await?;
                    // Fall through to the next iteration — the next acquire-
                    // lock attempt will be cheap (we still hold the lease).
                }
                Err(ScanError::Transient(e)) => {
                    match backoff_after(backoff_idx, &e, "stalled.scan", shutdown).await {
                        Some(next) => backoff_idx = next,
                        None => return Ok(()),
                    }
                }
                Err(ScanError::Permanent(e)) => return Err(Error::Redis(e)),
            }
        }
    }

    async fn acquire_lock(
        &self,
        client: &Client,
        sha: &str,
    ) -> std::result::Result<bool, LockError> {
        let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_acquire_lock_args(
            sha,
            &self.lock_key,
            &self.cfg.holder_id,
            self.cfg.lock_ttl_secs,
        );
        let res: std::result::Result<Value, fred::error::Error> = client.custom(cmd, args).await;
        match res {
            Ok(v) => Ok(value_as_bool(&v)),
            Err(e) => Err(classify_lock_error(e)),
        }
    }

    async fn acquire_lock_via_eval(&self, client: &Client) -> Result<bool> {
        let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
        let args = eval_acquire_lock_args(
            ACQUIRE_LOCK_SCRIPT,
            &self.lock_key,
            &self.cfg.holder_id,
            self.cfg.lock_ttl_secs,
        );
        let v: Value = client.custom(cmd, args).await.map_err(Error::Redis)?;
        Ok(value_as_bool(&v))
    }

    async fn release_lock_best_effort(&self, client: &Client) {
        let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
        let args = eval_release_lock_args(RELEASE_LOCK_SCRIPT, &self.lock_key, &self.cfg.holder_id);
        let _: std::result::Result<Value, _> = client.custom(cmd, args).await;
    }

    /// One detector tick:
    ///
    /// 1. `XPENDING ... IDLE <idle> - + <scan_batch>` to find pending
    ///    entries that have sat idle past the threshold.
    /// 2. Pipelined `XRANGE id id` per entry to msgpack-decode the
    ///    envelope and recover `(job_id, name)` so the script can
    ///    synthesize the per-job stall counter key.
    /// 3. `STALLED_SCAN_SCRIPT` does INCR + EXPIRE + threshold-gate
    ///    XACKDEL for every entry, returning two index lists:
    ///    `incremented_meta` (count rose, still under threshold) and
    ///    `threshold_hit_meta` (count reached threshold AND XACKDEL
    ///    gate held — Rust side must relocate to DLQ).
    /// 4. For each `incremented_meta`, emit `e=stalled` best-effort.
    /// 5. For each `threshold_hit_meta`, send onto `dlq_tx` with
    ///    `DlqReason::Stalled`; the existing `run_relocator` does the
    ///    IDMP-XADD + `e=dlq` emit.
    ///
    /// Returns the metrics tick + whether to drain (when the scan
    /// returned a full batch or any threshold hits, iterate immediately
    /// in case more idle entries are queued).
    async fn scan_once(
        &self,
        client: &Client,
        events: &EventsWriter,
        sha: &str,
    ) -> std::result::Result<ScanResult, ScanError> {
        // 1) XPENDING ... IDLE - + <batch>
        let pending = match self.xpending_idle(client).await {
            Ok(p) => p,
            Err(e) => {
                // NOGROUP-on-fresh-queue: treat as empty scan, sleep
                // tick. The detector ran before any worker — totally
                // normal on a brand-new queue.
                if format!("{e}").contains("NOGROUP") {
                    return Ok(ScanResult {
                        tick: StalledTick::default(),
                        drain: false,
                    });
                }
                if is_transient(&e) {
                    return Err(ScanError::Transient(e));
                }
                return Err(ScanError::Permanent(e));
            }
        };
        if pending.is_empty() {
            return Ok(ScanResult {
                tick: StalledTick::default(),
                drain: false,
            });
        }

        // 2) Pipelined XRANGE id id for each entry — one round trip for
        // the whole batch (every key shares the queue's hash tag).
        let entries: Vec<EntryProbe> = self
            .pipelined_envelope_decode(client, &pending)
            .await
            .map_err(|e| {
                if is_transient(&e) {
                    ScanError::Transient(e)
                } else {
                    ScanError::Permanent(e)
                }
            })?;

        let mut valid: Vec<EntryProbe> = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry.job_id.is_empty() {
                // Envelope decode failed — leave it for the reader's
                // existing CLAIM-then-DLQ-decode-failed path to handle.
                tracing::trace!(
                    entry_id = %entry.entry_id,
                    "stalled detector: skipping entry with undecodable envelope (reader DLQ-decode-failed path)"
                );
                continue;
            }
            valid.push(entry);
        }

        if valid.is_empty() {
            return Ok(ScanResult {
                tick: StalledTick {
                    scanned: pending.len() as u64,
                    incremented: 0,
                    relocated: 0,
                },
                drain: false,
            });
        }

        // 3) STALLED_SCAN_SCRIPT
        let ttl_secs =
            stall_counter_ttl_secs(self.cfg.idle_threshold_ms, self.cfg.max_stalled_attempts);
        let script_pairs: Vec<(String, String)> = valid
            .iter()
            .map(|e| (e.entry_id.clone(), e.job_id.clone()))
            .collect();
        let reply = match self
            .invoke_stalled_scan(client, sha, ttl_secs, &script_pairs)
            .await
        {
            Ok(v) => v,
            Err(e) if format!("{e}").contains("NOSCRIPT") => return Err(ScanError::NoScript),
            Err(e) => {
                if is_transient(&e) {
                    return Err(ScanError::Transient(e));
                }
                return Err(ScanError::Permanent(e));
            }
        };
        let parsed = parse_stalled_reply(&reply);

        // 4) Emit e=stalled for the incremented batch (best-effort).
        if events.is_enabled() {
            for (idx, n) in &parsed.incremented {
                let i = *idx as usize;
                if let Some(entry) = valid.get(i) {
                    events.emit_stalled(&entry.job_id, &entry.name, *n).await;
                }
            }
        }

        // 5) Hand threshold-hit entries to the shared dlq_tx. The
        // relocator does the IDMP XADD + e=dlq emit.
        let mut relocated_count: u64 = 0;
        if !parsed.threshold_hits.is_empty() {
            if let Some(tx) = self.shared_dlq_tx.as_ref() {
                for (idx, n) in &parsed.threshold_hits {
                    let i = *idx as usize;
                    let Some(entry) = valid.get(i) else { continue };
                    // `pre_acked = true` — the STALLED_SCAN_SCRIPT already
                    // XACKDEL'd this entry out of the PEL at the threshold-
                    // hit branch. Without this flag, the relocator's own
                    // XACKDEL gate inside RELOCATE_DLQ_SCRIPT would return
                    // 0 ("gate lost") and silently skip the DLQ write —
                    // orphan ack, no DLQ entry.
                    dlq::enqueue_with_mode(
                        tx,
                        entry.job_id.clone(),
                        entry.entry_id.clone().into(),
                        entry.payload.clone(),
                        DlqReason::Stalled,
                        *n,
                        entry.name.clone(),
                        true,
                    )
                    .await;
                    relocated_count += 1;
                }
            } else {
                // Standalone path with no shared dlq_tx. The XACKDEL gate
                // inside the script already removed the entry from the
                // PEL, so the entry is acked — but no DLQ write happens.
                // This is a misconfiguration; surface it loudly so the
                // operator notices.
                tracing::error!(
                    queue = %self.cfg.queue_name,
                    threshold_hits = parsed.threshold_hits.len(),
                    "stalled detector observed threshold-hit entries but no dlq_tx is wired — \
                     entries were XACKDEL'd by the script but not relocated to the DLQ. \
                     Run the detector embedded via Consumer::run to wire the DLQ relocator."
                );
            }
        }

        let scanned = pending.len() as u64;
        let incremented = parsed.incremented.len() as u64;
        let drain = relocated_count > 0 || scanned >= self.cfg.scan_batch as u64;
        Ok(ScanResult {
            tick: StalledTick {
                scanned,
                incremented,
                relocated: relocated_count,
            },
            drain,
        })
    }

    async fn xpending_idle(
        &self,
        client: &Client,
    ) -> std::result::Result<Vec<String>, fred::error::Error> {
        let cmd = CustomCommand::new_static("XPENDING", ClusterHash::FirstKey, false);
        let args = xpending_idle_args(
            &self.stream_key,
            &self.group,
            self.cfg.idle_threshold_ms,
            self.cfg.scan_batch as u64,
        );
        let v: Value = client.custom(cmd, args).await?;
        Ok(parse_xpending_idle_response(&v))
    }

    async fn pipelined_envelope_decode(
        &self,
        client: &Client,
        entry_ids: &[String],
    ) -> std::result::Result<Vec<EntryProbe>, fred::error::Error> {
        let pipeline = client.pipeline();
        let cmd = CustomCommand::new_static("XRANGE", ClusterHash::FirstKey, false);
        for id in entry_ids {
            let args = xrange_id_args(&self.stream_key, id);
            // `pipeline.custom` enqueues — errors here are buffer-level
            // and would also break `try_all`.
            let _: std::result::Result<Value, _> = pipeline.custom(cmd.clone(), args).await;
        }
        let results = pipeline.try_all::<Value>().await;
        let mut out: Vec<EntryProbe> = Vec::with_capacity(entry_ids.len());
        for (id, r) in entry_ids.iter().zip(results) {
            let entry_id = id.clone();
            let probe = match r {
                Ok(v) => parse_entry_probe(&v, entry_id.clone()),
                Err(e) => {
                    tracing::trace!(entry_id = %entry_id, error = %e, "stalled detector: XRANGE failed for pending entry");
                    EntryProbe {
                        entry_id,
                        job_id: String::new(),
                        name: String::new(),
                        payload: Bytes::new(),
                    }
                }
            };
            out.push(probe);
        }
        Ok(out)
    }

    async fn invoke_stalled_scan(
        &self,
        client: &Client,
        sha: &str,
        ttl_secs: u64,
        pairs: &[(String, String)],
    ) -> std::result::Result<Value, fred::error::Error> {
        let cmd = CustomCommand::new_static("EVALSHA", ClusterHash::FirstKey, false);
        let args = evalsha_stalled_scan_args(
            sha,
            &self.stream_key,
            &self.group,
            self.cfg.max_stalled_attempts,
            ttl_secs,
            pairs,
        );
        match client.custom(cmd, args).await {
            Ok(v) => Ok(v),
            Err(e) if format!("{e}").contains("NOSCRIPT") => {
                let cmd = CustomCommand::new_static("EVAL", ClusterHash::FirstKey, false);
                let args = eval_stalled_scan_args(
                    STALLED_SCAN_SCRIPT,
                    &self.stream_key,
                    &self.group,
                    self.cfg.max_stalled_attempts,
                    ttl_secs,
                    pairs,
                );
                client.custom(cmd, args).await
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
struct ScanResult {
    tick: StalledTick,
    drain: bool,
}

#[derive(Debug)]
enum ScanError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
}

enum LockError {
    NoScript,
    Transient(fred::error::Error),
    Permanent(fred::error::Error),
}

fn classify_lock_error(err: fred::error::Error) -> LockError {
    if format!("{err}").contains("NOSCRIPT") {
        return LockError::NoScript;
    }
    if is_transient(&err) {
        LockError::Transient(err)
    } else {
        LockError::Permanent(err)
    }
}

#[derive(Debug)]
struct EntryProbe {
    entry_id: String,
    /// Empty when the envelope didn't decode.
    job_id: String,
    name: String,
    payload: Bytes,
}

/// Parse the `XPENDING ... IDLE - + <count>` extended-form reply into a
/// flat list of entry ids. Each pending entry is shaped as
/// `[id, consumer, idle_ms, delivery_count]`; we only need the id at
/// this stage (the script INCRs by `(entry_id, job_id)` pair).
pub(crate) fn parse_xpending_idle_response(v: &Value) -> Vec<String> {
    let entries = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out: Vec<String> = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Array(parts) = entry else { continue };
        let id = match parts.first() {
            Some(Value::String(s)) => s.to_string(),
            Some(Value::Bytes(b)) => match std::str::from_utf8(b) {
                Ok(s) => s.to_string(),
                Err(_) => continue,
            },
            _ => continue,
        };
        out.push(id);
    }
    out
}

/// Parse an `XRANGE <key> <id> <id>` reply into a probe carrying
/// `(entry_id, job_id, name, payload)`. The `d` field is msgpack-decoded
/// as `Job<IgnoredAny>` so the payload type isn't needed here (the
/// detector is type-erased — it scans whatever the consumer is
/// processing).
fn parse_entry_probe(v: &Value, entry_id: String) -> EntryProbe {
    let entries = parse_xrange_response(v);
    let Some(entry) = entries.into_iter().next() else {
        return EntryProbe {
            entry_id,
            job_id: String::new(),
            name: String::new(),
            payload: Bytes::new(),
        };
    };
    let mut payload: Option<Bytes> = None;
    let mut name = String::new();
    for (k, val) in &entry.fields {
        match k.as_str() {
            "d" => payload = Some(val.as_bytes()),
            "n" => {
                if let Some(s) = val.as_string() {
                    name = s;
                }
            }
            _ => {}
        }
    }
    let Some(payload_bytes) = payload else {
        return EntryProbe {
            entry_id,
            job_id: String::new(),
            name,
            payload: Bytes::new(),
        };
    };
    match extract_job_id_from_payload(&payload_bytes) {
        Some(job_id) => EntryProbe {
            entry_id,
            job_id,
            name,
            payload: payload_bytes,
        },
        None => EntryProbe {
            entry_id,
            job_id: String::new(),
            name,
            payload: payload_bytes,
        },
    }
}

/// One-shot msgpack decode of a `Job<IgnoredAny>` envelope to recover
/// the `id` field. Returns `None` for an undecodable envelope — the
/// reader's existing CLAIM-then-DLQ-decode-failed path will handle it.
pub(crate) fn extract_job_id_from_payload(bytes: &[u8]) -> Option<String> {
    let job: Job<IgnoredAny> = rmp_serde::from_slice(bytes).ok()?;
    Some(job.id)
}

#[derive(Debug, Default)]
pub(crate) struct ParsedStalledReply {
    /// `(arg_index, n)` for each entry whose stall counter rose but
    /// stayed below `max_stalled_attempts`. Rust uses `arg_index` to
    /// correlate back to the `(entry_id, job_id, name)` triple it sent.
    pub incremented: Vec<(u32, u32)>,
    /// `(arg_index, n)` for each entry that hit threshold AND the
    /// script's XACKDEL gate held.
    pub threshold_hits: Vec<(u32, u32)>,
}

/// Decode the `STALLED_SCAN_SCRIPT` reply into Rust-side metadata.
/// Shape: `{scanned, incremented_meta, threshold_hit_meta}` where each
/// meta entry is `{arg_index, n}`. Defensive against shape drift —
/// unexpected shapes return an empty `ParsedStalledReply` so a future
/// script regression downgrades to "no work done" rather than panicking
/// the detector.
pub(crate) fn parse_stalled_reply(v: &Value) -> ParsedStalledReply {
    let items = match v {
        Value::Array(items) => items,
        _ => return ParsedStalledReply::default(),
    };
    let incremented = items.get(1).map(parse_index_n_pairs).unwrap_or_default();
    let threshold_hits = items.get(2).map(parse_index_n_pairs).unwrap_or_default();
    ParsedStalledReply {
        incremented,
        threshold_hits,
    }
}

fn parse_index_n_pairs(v: &Value) -> Vec<(u32, u32)> {
    let entries = match v {
        Value::Array(items) => items,
        _ => return Vec::new(),
    };
    let mut out: Vec<(u32, u32)> = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Array(pair) = entry else { continue };
        let idx = pair.first().map(value_as_u64).unwrap_or(0);
        let n = pair.get(1).map(value_as_u64).unwrap_or(0);
        out.push((idx as u32, n as u32));
    }
    out
}

/// Compute the per-stall-counter TTL in seconds: `idle_threshold_ms *
/// max_stalled_attempts * 2`, converted to seconds with
/// `div_ceil(1000).max(1)`. The doubling gives headroom for one extra
/// tick before eviction; the `max(1)` floor guards against
/// sub-millisecond test configurations collapsing to 0.
pub(crate) fn stall_counter_ttl_secs(idle_threshold_ms: u64, max_stalled_attempts: u32) -> u64 {
    let total_ms = idle_threshold_ms
        .saturating_mul(max_stalled_attempts.max(1) as u64)
        .saturating_mul(2);
    total_ms.div_ceil(1000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stall_counter_ttl_floor_at_one_second() {
        assert_eq!(stall_counter_ttl_secs(100, 1), 1);
        assert_eq!(stall_counter_ttl_secs(0, 1), 1);
    }

    #[test]
    fn stall_counter_ttl_doubles_and_rounds_up() {
        // 30_000ms * 1 * 2 = 60_000ms = 60s.
        assert_eq!(stall_counter_ttl_secs(30_000, 1), 60);
        // 30_000ms * 3 * 2 = 180_000ms = 180s.
        assert_eq!(stall_counter_ttl_secs(30_000, 3), 180);
        // 1_500ms * 2 * 2 = 6_000ms = 6s.
        assert_eq!(stall_counter_ttl_secs(1_500, 2), 6);
    }

    #[test]
    fn parse_stalled_reply_well_formed() {
        let reply = Value::Array(vec![
            Value::Integer(3),
            Value::Array(vec![Value::Array(vec![
                Value::Integer(0),
                Value::Integer(1),
            ])]),
            Value::Array(vec![
                Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
                Value::Array(vec![Value::Integer(2), Value::Integer(3)]),
            ]),
        ]);
        let parsed = parse_stalled_reply(&reply);
        assert_eq!(parsed.incremented, vec![(0, 1)]);
        assert_eq!(parsed.threshold_hits, vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn parse_stalled_reply_empty_returns_default() {
        let reply = Value::Array(vec![
            Value::Integer(0),
            Value::Array(vec![]),
            Value::Array(vec![]),
        ]);
        let parsed = parse_stalled_reply(&reply);
        assert!(parsed.incremented.is_empty());
        assert!(parsed.threshold_hits.is_empty());
    }

    #[test]
    fn parse_stalled_reply_unexpected_shape_is_empty() {
        let parsed = parse_stalled_reply(&Value::Null);
        assert!(parsed.incremented.is_empty());
        assert!(parsed.threshold_hits.is_empty());
        let parsed = parse_stalled_reply(&Value::Integer(0));
        assert!(parsed.incremented.is_empty());
        assert!(parsed.threshold_hits.is_empty());
    }

    #[test]
    fn parse_stalled_reply_short_array_returns_empty_pairs() {
        let reply = Value::Array(vec![Value::Integer(0)]);
        let parsed = parse_stalled_reply(&reply);
        assert!(parsed.incremented.is_empty());
        assert!(parsed.threshold_hits.is_empty());
    }

    #[test]
    fn parse_xpending_idle_extracts_ids() {
        // Each row is [id, consumer, idle_ms, delivery_count].
        let v = Value::Array(vec![
            Value::Array(vec![
                Value::String("1700-0".into()),
                Value::String("c-1".into()),
                Value::Integer(45_000),
                Value::Integer(3),
            ]),
            Value::Array(vec![
                Value::Bytes(Bytes::from_static(b"1701-0")),
                Value::String("c-2".into()),
                Value::Integer(60_000),
                Value::Integer(2),
            ]),
        ]);
        let ids = parse_xpending_idle_response(&v);
        assert_eq!(ids, vec!["1700-0".to_string(), "1701-0".to_string()]);
    }

    #[test]
    fn parse_xpending_idle_unexpected_shape_returns_empty() {
        assert!(parse_xpending_idle_response(&Value::Null).is_empty());
        assert!(parse_xpending_idle_response(&Value::Integer(0)).is_empty());
    }

    #[test]
    fn extract_job_id_round_trips_through_msgpack() {
        let job = crate::job::Job::with_id("my-id".to_string(), 42_u32);
        let bytes = rmp_serde::to_vec(&job).expect("encode");
        assert_eq!(
            extract_job_id_from_payload(&bytes).as_deref(),
            Some("my-id")
        );
    }

    #[test]
    fn extract_job_id_returns_none_for_garbage() {
        assert!(extract_job_id_from_payload(b"not msgpack").is_none());
    }
}
