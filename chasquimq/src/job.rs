use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub type JobId = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T> {
    pub id: JobId,
    pub payload: T,
    pub created_at_ms: u64,
}

impl<T> Job<T> {
    pub fn new(payload: T) -> Self {
        Self::with_id(ulid::Ulid::new().to_string(), payload)
    }

    pub fn with_id(id: JobId, payload: T) -> Self {
        let created_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            id,
            payload,
            created_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct Nested {
        n: i64,
        s: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct Sample {
        name: String,
        count: u32,
        nested: Nested,
    }

    #[test]
    fn round_trip_messagepack() {
        let job = Job::new(Sample {
            name: "hello".into(),
            count: 7,
            nested: Nested {
                n: -42,
                s: "world".into(),
            },
        });
        let bytes = rmp_serde::to_vec(&job).expect("encode");
        let decoded: Job<Sample> = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(job.id, decoded.id);
        assert_eq!(job.payload, decoded.payload);
        assert_eq!(job.created_at_ms, decoded.created_at_ms);
    }

    #[test]
    fn new_populates_id_and_timestamp() {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let job = Job::new("payload");
        ulid::Ulid::from_string(&job.id).expect("id is a valid ULID");
        assert!(job.created_at_ms >= now_ms.saturating_sub(1_000));
        assert!(job.created_at_ms <= now_ms + 1_000);
    }
}
