//! `Job<T>` — the wire-level message envelope.

use serde::{Deserialize, Serialize};

pub type JobId = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T> {
    pub id: JobId,
    pub payload: T,
    pub created_at_ms: u64,
}
