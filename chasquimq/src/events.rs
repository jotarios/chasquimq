//! Cross-process events stream.
//!
//! Writes engine-internal transitions (waiting / active / completed / failed /
//! retry-scheduled / delayed / dlq / drained) to a per-queue Redis Stream so
//! external subscribers (BullMQ-style `QueueEvents`, dashboards, custom
//! tooling) can observe them with a plain `XREAD`. Sibling to
//! [`crate::metrics::MetricsSink`]: `MetricsSink` is in-process and zero-cost
//! when noop'd; the events stream is cross-process and best-effort.
//!
//! # Design
//!
//! - One `XADD` per event with `MAXLEN ~ <cap>` to keep the stream bounded.
//!   Trim is approximate (the `~`) so Redis can do it cheaply.
//! - Each entry's fields are ASCII strings: `e=<event>`, `id=<job_id>`,
//!   `ts=<unix_ms>`, plus event-specific fields. Numeric fields are decimal
//!   strings, not binary. The wire format is intentionally trivial so a Node
//!   subscriber can decode it without a MessagePack dependency.
//! - **Best-effort**: writes go through [`tracing::warn`] on failure and
//!   never propagate. The events emit lives outside the ack/retry gate, so
//!   a network blip on the events stream cannot cause a job to be retried.
//! - **Disabled**: `EventsWriter::is_enabled()` returns `false` for the
//!   no-op writer used when `ConsumerConfig::events_enabled = false`. Each
//!   `emit_*` method short-circuits before constructing args, so the
//!   disabled path is one branch + return.
//!
//! # Schema (event names and fields)
//!
//! All events carry `e`, `id` (UTF-8 strings), and `ts` (unix-ms decimal).
//! Per-job events also carry `n` (the dispatch name; empty → field omitted),
//! same convention as the main stream's `n` field. Event-specific fields:
//!
//! | event              | fields                                     |
//! |--------------------|--------------------------------------------|
//! | `waiting`          | `n` (opt)                                  |
//! | `active`           | `attempt`, `n` (opt)                       |
//! | `completed`        | `attempt`, `duration_us`, `n` (opt)        |
//! | `failed`           | `attempt`, `reason`, `duration_us` (opt), `n` (opt) |
//! | `retry-scheduled`  | `attempt` (next attempt), `backoff_ms`, `n` (opt) |
//! | `delayed`          | `delay_ms`, `n` (opt)                      |
//! | `dlq`              | `attempt`, `reason`, `n` (opt)             |
//! | `drained`          | (no `id`/`n` — emitted with `id=""`)       |
//!
//! `attempt` is 1-indexed (matches `JobOutcome.attempt`). Numeric values are
//! always decimal strings. The `n` field is the slice-5 payoff: subscribers
//! see job kind without msgpack-decoding payload bytes (the architectural
//! justification for picking Option B over Option D in
//! `docs/name-on-wire-design.md`). Empty name → `n` is omitted entirely
//! (same byte-saving rule as the main stream, see
//! [`crate::redis::commands::xadd_args`]).

use crate::job::now_ms;
use crate::redis::keys::events_key;
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};
use std::sync::Arc;

/// Best-effort writer for the per-queue events stream.
///
/// Cheap to clone: holds an `Arc` to the underlying client and `Arc<str>`
/// for the stream key. A `Disabled` writer (constructed when the engine's
/// `events_enabled` knob is `false`) skips all `XADD` calls — every
/// `emit_*` method becomes a single branch.
#[derive(Clone)]
pub(crate) struct EventsWriter {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    /// Writes are no-ops; the engine's `events_enabled` knob is `false`.
    Disabled,
    Enabled {
        client: Client,
        stream_key: Arc<str>,
        max_stream_len: u64,
    },
}

impl EventsWriter {
    /// Construct an enabled writer that XADDs to `{chasqui:<queue>}:events`.
    ///
    /// `client` is shared by reference-count; the writer owns its `Arc`-style
    /// share but does not establish a new connection.
    pub(crate) fn new(client: Client, queue_name: &str, max_stream_len: u64) -> Self {
        Self {
            inner: Inner::Enabled {
                client,
                stream_key: Arc::from(events_key(queue_name)),
                max_stream_len,
            },
        }
    }

    /// Construct a no-op writer. Every `emit_*` method short-circuits.
    pub(crate) fn disabled() -> Self {
        Self {
            inner: Inner::Disabled,
        }
    }

    /// Whether `XADD`s will actually be issued. Used by the reader's
    /// drained-transition tracker to skip extra work entirely when the
    /// writer is no-op.
    pub(crate) fn is_enabled(&self) -> bool {
        matches!(self.inner, Inner::Enabled { .. })
    }

    /// Emit a `waiting` event (a delayed job was just promoted into the
    /// stream and is now eligible for `XREADGROUP`).
    pub(crate) async fn emit_waiting(&self, id: &str, name: &str) {
        self.xadd("waiting", id, name, &[]).await;
    }

    /// Emit an `active` event (handler is about to run).
    pub(crate) async fn emit_active(&self, id: &str, name: &str, attempt: u32) {
        let attempt_s = attempt.to_string();
        self.xadd("active", id, name, &[("attempt", &attempt_s)])
            .await;
    }

    /// Emit a `completed` event (handler returned `Ok`).
    pub(crate) async fn emit_completed(
        &self,
        id: &str,
        name: &str,
        attempt: u32,
        duration_us: u64,
    ) {
        let attempt_s = attempt.to_string();
        let duration_s = duration_us.to_string();
        self.xadd(
            "completed",
            id,
            name,
            &[("attempt", &attempt_s), ("duration_us", &duration_s)],
        )
        .await;
    }

    /// Emit a `failed` event. `duration_us` is `None` for failures that
    /// happen before the handler ran (panic before `await` in theory, though
    /// the worker currently always has a duration; future reader-side
    /// failure paths might not).
    pub(crate) async fn emit_failed(
        &self,
        id: &str,
        name: &str,
        attempt: u32,
        reason: &str,
        duration_us: Option<u64>,
    ) {
        let attempt_s = attempt.to_string();
        match duration_us {
            Some(us) => {
                let duration_s = us.to_string();
                self.xadd(
                    "failed",
                    id,
                    name,
                    &[
                        ("attempt", &attempt_s),
                        ("reason", reason),
                        ("duration_us", &duration_s),
                    ],
                )
                .await;
            }
            None => {
                self.xadd(
                    "failed",
                    id,
                    name,
                    &[("attempt", &attempt_s), ("reason", reason)],
                )
                .await;
            }
        }
    }

    /// Emit a `retry-scheduled` event (the retry relocator atomically
    /// rescheduled this job; `attempt` is the run number the retry will
    /// execute as).
    pub(crate) async fn emit_retry_scheduled(
        &self,
        id: &str,
        name: &str,
        attempt: u32,
        backoff_ms: u64,
    ) {
        let attempt_s = attempt.to_string();
        let backoff_s = backoff_ms.to_string();
        self.xadd(
            "retry-scheduled",
            id,
            name,
            &[("attempt", &attempt_s), ("backoff_ms", &backoff_s)],
        )
        .await;
    }

    /// Emit a `delayed` event. **Currently unused on the producer hot path**
    /// to keep `add_in` / `add_at` lean — see the v1 decision in
    /// `tests/events_stream.rs::events_delayed_emit_skipped_v1`. Wired here
    /// so a future caller (or a Node binding that wants to mirror BullMQ's
    /// `delayed` event) can opt in without re-plumbing the writer.
    #[allow(dead_code)]
    pub(crate) async fn emit_delayed(&self, id: &str, name: &str, delay_ms: u64) {
        let delay_s = delay_ms.to_string();
        self.xadd("delayed", id, name, &[("delay_ms", &delay_s)])
            .await;
    }

    /// Emit a `dlq` event after a successful DLQ relocate.
    pub(crate) async fn emit_dlq(&self, id: &str, name: &str, reason: &str, attempt: u32) {
        let attempt_s = attempt.to_string();
        self.xadd(
            "dlq",
            id,
            name,
            &[("attempt", &attempt_s), ("reason", reason)],
        )
        .await;
    }

    /// Emit a `drained` event (the consumer just observed an empty
    /// `XREADGROUP`). Caller should rate-limit so this only fires on the
    /// full -> empty transition, not every empty poll.
    pub(crate) async fn emit_drained(&self) {
        // No job id and no name — drained is queue-scoped, not per-job.
        self.xadd("drained", "", "", &[]).await;
    }

    async fn xadd(&self, event_name: &str, id: &str, name: &str, extra: &[(&str, &str)]) {
        let (client, stream_key, max_stream_len) = match &self.inner {
            Inner::Disabled => return,
            Inner::Enabled {
                client,
                stream_key,
                max_stream_len,
            } => (client, stream_key, *max_stream_len),
        };

        let now = now_ms();
        // Capacity: stream key + MAXLEN ~ <cap> + * + e/id/ts (3 pairs = 6
        // values) + optional n (2 values) + extras (2 per pair). Slight
        // over-alloc is fine.
        let mut args: Vec<Value> = Vec::with_capacity(8 + 6 + 2 + extra.len() * 2);
        args.push(Value::from(stream_key.as_ref()));
        args.push(Value::from("MAXLEN"));
        args.push(Value::from("~"));
        args.push(Value::from(max_stream_len as i64));
        args.push(Value::from("*"));
        args.push(Value::from("e"));
        args.push(Value::from(event_name));
        args.push(Value::from("id"));
        args.push(Value::from(id));
        args.push(Value::from("ts"));
        args.push(Value::from(now.to_string()));
        // Empty name: omit the field entirely (zero bytes on the wire). Same
        // convention as the main stream's `n`.
        if !name.is_empty() {
            args.push(Value::from("n"));
            args.push(Value::from(name));
        }
        for (k, v) in extra {
            args.push(Value::from(*k));
            args.push(Value::from(*v));
        }

        let cmd = CustomCommand::new_static("XADD", ClusterHash::FirstKey, false);
        let res: std::result::Result<Value, _> = client.custom(cmd, args).await;
        if let Err(e) = res {
            // Best-effort: events are observability, never block the hot
            // path. A network blip here must not cause a job to be retried.
            tracing::warn!(
                event = event_name,
                stream_key = %stream_key,
                error = %e,
                "events: XADD failed; event dropped"
            );
        }
    }
}
