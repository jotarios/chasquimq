pub(crate) const PAYLOAD_FIELD: &str = "d";

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
