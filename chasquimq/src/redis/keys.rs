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
