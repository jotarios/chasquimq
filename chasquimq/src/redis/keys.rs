pub(crate) const PAYLOAD_FIELD: &str = "d";

/// Stream entry field carrying the optional UTF-8 job `name` alongside the
/// msgpack-encoded payload in `d`. Producer omits the field entirely for
/// unnamed jobs; consumer treats absent and empty as equivalent.
pub(crate) const NAME_FIELD: &str = "n";

pub fn stream_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:stream")
}

pub fn dlq_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:dlq")
}

pub fn delayed_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:delayed")
}

pub fn promoter_lock_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:promoter:lock")
}

/// Per-queue, per-job-id dedup marker key. Used by the idempotent delayed
/// scheduling path (`Producer::add_in_with_id` / `add_at_with_id` /
/// `add_in_bulk_with_ids`) so a network-driven caller retry doesn't double
/// the scheduled job. Same `{chasqui:<queue>}` hash tag as the delayed ZSET
/// so they always co-locate on the same Redis Cluster slot.
pub fn dedup_marker_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:dlid:{job_id}")
}

/// Per-queue cross-process events stream. The engine writes engine-internal
/// transitions (waiting / active / completed / failed / retry-scheduled /
/// delayed / dlq / drained) here as Redis Stream entries; subscribers in any
/// process can `XREAD` to observe them. This is a sibling to `MetricsSink`,
/// not a replacement: `MetricsSink` is in-process (zero IPC), the events
/// stream is cross-process (subscribable by an external dashboard or the
/// Node bindings' `QueueEvents` class). Both fire on the same hot-path
/// occurrences. Same `{chasqui:<queue>}` hash tag so it co-locates with the
/// other queue keys on a single Redis Cluster slot.
pub fn events_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:events")
}

/// Per-queue ZSET tracking repeatable specs by next fire time. Score =
/// `next_fire_ms`, member = `RepeatableSpec::resolved_key()`. The
/// `Scheduler` (slice 10) tails this with `ZRANGEBYSCORE -inf <now>` to
/// find specs whose next fire time has elapsed.
pub fn repeat_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:repeat")
}

/// Per-queue, per-spec-key hash storing the full repeatable spec
/// (`pattern`, `payload`, `limit`, etc.) under field `spec` as
/// msgpack-encoded [`crate::repeat::StoredSpec`]. Separate from the ZSET so
/// the scheduler tick only hydrates due specs, not the entire catalog.
pub fn repeat_spec_key(queue_name: &str, spec_key: &str) -> String {
    format!("{{chasqui:{queue_name}}}:repeat:spec:{spec_key}")
}

/// Per-queue scheduler leader-election lock key. Independent from the
/// `promoter:lock` so a deployment can run scheduler and promoter on
/// disjoint replicas if it chooses.
pub fn scheduler_lock_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:scheduler:lock")
}

/// Per-queue, per-job-id result-backend key. Stores the handler's
/// return value (opaque bytes — every shim msgpack-encodes the user's
/// native value before the bytes cross the FFI boundary) with a
/// configurable TTL set by [`crate::config::ConsumerConfig::result_ttl_secs`].
/// Written by `JOB_OK_SCRIPT` in the same Lua round trip as the
/// `XACKDEL` so the result write is gated on a successful ack — no
/// orphan results when a concurrent CLAIM removed the entry first.
/// Same `{chasqui:<queue>}` hash tag as the rest of the queue's
/// keyspace so the result key always co-locates on a single Redis
/// Cluster slot.
pub fn result_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:result:{job_id}")
}

/// Per-queue, per-job-id side-index key used by `Producer::cancel_delayed`.
/// Stores the exact encoded ZSET member so cancel can `ZREM` precisely
/// without a slow `ZRANGE` scan. Written by the idempotent schedule path
/// alongside the dedup marker, with the same TTL — after natural expiration
/// (or post-cancel `DEL`) the key disappears on its own; the promoter never
/// has to clean it up because the cancel script is already correct in the
/// "GET hits, ZREM misses (already promoted)" race. Same `{chasqui:<queue>}`
/// hash tag so it co-locates on the same Cluster slot as the ZSET.
pub fn delayed_index_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:didx:{job_id}")
}

/// Per-queue cross-process pause flag. When this key exists, every
/// consumer of the queue parks its stream reader at the next batch
/// boundary (in-flight jobs still drain; producers are unaffected).
/// Written by `chasqui pause` / `Queue.pause()` (SET), removed by
/// `chasqui resume` / `Queue.resume()` (DEL). No TTL — pause is durable
/// operator intent that persists until an explicit resume, and survives
/// consumer restarts (a fresh consumer parks before its first
/// `XREADGROUP` if the key is present). The reader checks this key with
/// a single `EXISTS` only at batch boundaries, time-gated by
/// `ConsumerConfig::pause_poll_ms`, so it is never on the per-job hot
/// path. Same `{chasqui:<queue>}` hash tag so it co-locates on the same
/// Cluster slot as the stream.
pub fn paused_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:paused")
}

/// Per-queue, per-job-id progress key. Holds the latest progress value
/// (`0..=100`, written as a plain ASCII decimal string by the engine so
/// every shim can `parseInt` / `int(str(...))` it without a msgpack
/// dependency) with TTL `result_ttl_secs` so it disappears alongside the
/// result key after a successful job completes. Written by
/// [`crate::JobHandle::update_progress`] from inside a worker; read by
/// the introspector and by shim-side `Job.progress` / `Queue.getJob`
/// callers. Same `{chasqui:<queue>}` hash tag so it co-locates on the
/// same Redis Cluster slot as the stream and result key.
pub fn progress_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:progress:{job_id}")
}

/// Per-queue, per-job-id log stream. Each call to
/// [`crate::JobHandle::log`] appends one entry under field `line`. The
/// stream is `MAXLEN ~`-trimmed by `ConsumerConfig::log_max_stream_len`
/// so a chatty handler can't grow Redis unbounded. Read back via
/// `Introspector::get_job_logs` (XRANGE / XREVRANGE + XLEN). Same
/// `{chasqui:<queue>}` hash tag so it co-locates on the same Redis
/// Cluster slot as the stream and result key.
pub fn log_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:log:{job_id}")
}

/// Per-queue, per-job-id stall counter. Written by the stalled-job
/// detector's `STALLED_SCAN_SCRIPT` (INCR + EXPIRE) when the entry has
/// sat idle past the detector's threshold; cleared by `JOB_OK_SCRIPT`
/// on successful ack and by `REPLAY_DLQ_SCRIPT` on replay. The detector
/// relocates the entry to the DLQ as `DlqReason::Stalled` once the
/// counter reaches `max_stalled_attempts`. TTL is sliding:
/// `idle_threshold_ms * max_stalled_attempts * 2` ms, applied on every
/// INCR, so a job that genuinely keeps stalling doesn't have its
/// counter evicted between ticks. Same `{chasqui:<queue>}` hash tag so
/// the counter co-locates with the stream / result / progress keys on
/// the same Cluster slot — `STALLED_SCAN_SCRIPT` reaches it from the
/// same Lua call as the `XACKDEL` on the source stream.
pub fn stall_counter_key(queue_name: &str, job_id: &str) -> String {
    format!("{{chasqui:{queue_name}}}:stalls:{job_id}")
}

/// Per-queue stalled-detector leader-election lock. Sibling of
/// `promoter_lock_key` and `scheduler_lock_key`; uses the shared
/// `ACQUIRE_LOCK_SCRIPT` / `RELEASE_LOCK_SCRIPT` primitives. TTL =
/// `StalledDetectorConfig::lock_ttl_secs` (5s default).
pub fn stalled_lock_key(queue_name: &str) -> String {
    format!("{{chasqui:{queue_name}}}:stalled:lock")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every per-queue key must carry the `{chasqui:<queue>}` hash tag
    /// so Redis Cluster routes the whole keyspace to a single slot. The
    /// engine's CustomCommand calls use `ClusterHash::FirstKey`, which
    /// relies on this invariant — losing it would scatter a queue's keys
    /// across slots and break atomic multi-key Lua.
    #[test]
    fn progress_and_log_keys_share_queue_hash_tag() {
        let pk = progress_key("demo", "job-abc");
        let lk = log_key("demo", "job-abc");
        assert_eq!(pk, "{chasqui:demo}:progress:job-abc");
        assert_eq!(lk, "{chasqui:demo}:log:job-abc");

        // The hash tag content (between the braces) must match the other
        // per-queue keys exactly — otherwise Cluster places them on
        // different slots than the stream / result key.
        let stream = stream_key("demo");
        let tag = |s: &str| {
            let start = s.find('{').unwrap();
            let end = s.find('}').unwrap();
            s[start..=end].to_string()
        };
        assert_eq!(tag(&pk), tag(&stream));
        assert_eq!(tag(&lk), tag(&stream));
        assert_eq!(tag(&pk), tag(&result_key("demo", "job-abc")));
    }

    /// The stalled-detector keys share the same `{chasqui:<queue>}` hash
    /// tag as the rest of the queue's keyspace. Critical because the
    /// `STALLED_SCAN_SCRIPT` synthesizes `stalls:<job_id>` keys from the
    /// stream KEYS[1] hash tag inside Lua and `XACKDEL`s the source
    /// stream entry in the same call — both keys must live on the same
    /// Redis Cluster slot.
    #[test]
    fn stalled_keys_share_queue_hash_tag() {
        let sc = stall_counter_key("demo", "job-abc");
        let lk = stalled_lock_key("demo");
        assert_eq!(sc, "{chasqui:demo}:stalls:job-abc");
        assert_eq!(lk, "{chasqui:demo}:stalled:lock");
        let stream = stream_key("demo");
        let tag = |s: &str| {
            let start = s.find('{').unwrap();
            let end = s.find('}').unwrap();
            s[start..=end].to_string()
        };
        assert_eq!(tag(&sc), tag(&stream));
        assert_eq!(tag(&lk), tag(&stream));
    }

    /// Job ids with `:` characters (legacy slug ids, user-supplied ids
    /// from `add_with_id`) must not interfere with the hash tag or the
    /// `progress:` / `log:` prefix — the tag is closed before the id is
    /// concatenated, and the prefix is fixed.
    #[test]
    fn job_ids_with_colons_are_safe() {
        let pk = progress_key("q", "user:42:retry");
        let lk = log_key("q", "user:42:retry");
        assert_eq!(pk, "{chasqui:q}:progress:user:42:retry");
        assert_eq!(lk, "{chasqui:q}:log:user:42:retry");
        assert!(pk.starts_with("{chasqui:q}:progress:"));
        assert!(lk.starts_with("{chasqui:q}:log:"));
    }
}
