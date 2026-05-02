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
