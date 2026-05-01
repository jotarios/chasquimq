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
