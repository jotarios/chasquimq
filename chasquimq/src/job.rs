use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub type JobId = String;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Per-job retry overrides carried in the `Job<T>` payload. Set via the
/// producer's `*_with_options` family. When fields here are `Some`, they
/// override the queue-wide defaults from [`crate::config::ConsumerConfig`]
/// for this specific job.
///
/// BullMQ-shaped: `max_attempts` mirrors `attempts`; [`BackoffSpec`] mirrors
/// the `backoff` option.
///
/// **Encoding note**: `rmp-serde` encodes structs as positional arrays by
/// default, which means `skip_serializing_if` on a *middle* `Option` would
/// shift trailing fields onto the wrong slot at decode time. We therefore
/// only apply `skip_serializing_if` on the outer `Job::retry` (which is
/// the *last* field of `Job<T>` — safe to omit), and leave the inner
/// fields here always present in the encoded form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobRetryOverride {
    /// If set, overrides `ConsumerConfig::max_attempts` for this job.
    pub max_attempts: Option<u32>,
    /// If set, overrides `ConsumerConfig::retry` for this job.
    pub backoff: Option<BackoffSpec>,
}

/// Per-job backoff spec. Mirrors the BullMQ `backoff` option. Unknown
/// `kind` strings degrade to `"exponential"` so a forward-compat
/// payload (e.g. `"linear"` from a future SDK) doesn't cause a hard
/// failure on the consumer.
///
/// **Encoding note**: see [`JobRetryOverride`] — the trailing optional
/// fields here are always serialized (no `skip_serializing_if`) because
/// `rmp-serde`'s array encoding is positional.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackoffSpec {
    /// `"fixed"` | `"exponential"`. Anything else is treated as
    /// `"exponential"`.
    pub kind: String,
    /// Base delay in milliseconds. Required.
    pub delay_ms: u64,
    /// Cap. Defaults to `RetryConfig::max_backoff_ms` when `None`.
    pub max_delay_ms: Option<u64>,
    /// Multiplier for exponential. Defaults to `RetryConfig::multiplier`
    /// when `None`. Ignored for `"fixed"`.
    pub multiplier: Option<f64>,
    /// Defaults to `RetryConfig::jitter_ms` when `None`.
    pub jitter_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job<T> {
    pub id: JobId,
    pub payload: T,
    pub created_at_ms: u64,
    #[serde(default)]
    pub attempt: u32,
    /// Per-job retry overrides. Producer-set, consumer-honored. `None`
    /// means "fall back to queue-wide `ConsumerConfig` retry settings".
    /// Skipped on serialization when absent so the wire format is
    /// identical to the pre-slice-8 layout for jobs that don't use
    /// overrides — old consumers can still decode new payloads, and
    /// new consumers default `retry` to `None` for old payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<JobRetryOverride>,
}

impl<T> Job<T> {
    pub fn new(payload: T) -> Self {
        Self::with_id(ulid::Ulid::new().to_string(), payload)
    }

    pub fn with_id(id: JobId, payload: T) -> Self {
        Self {
            id,
            payload,
            created_at_ms: now_ms(),
            attempt: 0,
            retry: None,
        }
    }

    /// Builder-style setter for the per-job retry override.
    pub fn with_retry(mut self, retry: JobRetryOverride) -> Self {
        self.retry = Some(retry);
        self
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

    /// Old-shape `Job<T>` used to verify forward-compat: a payload that
    /// pre-dates the `retry` field decodes cleanly into the new struct
    /// with `retry == None`.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct LegacyJob<T> {
        id: JobId,
        payload: T,
        created_at_ms: u64,
        #[serde(default)]
        attempt: u32,
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
        assert_eq!(decoded.retry, None);
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
        assert_eq!(job.retry, None);
    }

    #[test]
    fn round_trip_with_retry_override() {
        let job = Job::new(Sample {
            name: "x".into(),
            count: 1,
            nested: Nested {
                n: 0,
                s: "y".into(),
            },
        })
        .with_retry(JobRetryOverride {
            max_attempts: Some(7),
            backoff: Some(BackoffSpec {
                kind: "exponential".into(),
                delay_ms: 250,
                max_delay_ms: Some(10_000),
                multiplier: Some(3.0),
                jitter_ms: Some(50),
            }),
        });
        let bytes = rmp_serde::to_vec(&job).expect("encode");
        let decoded: Job<Sample> = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(decoded.retry, job.retry);
    }

    /// Forward-compat: encode a `LegacyJob<T>` (no `retry` field) and
    /// confirm `Job<T>` decodes it with `retry == None`. This is the
    /// guarantee that lets new consumers run alongside old producers.
    #[test]
    fn forward_compat_legacy_payload_decodes_with_retry_none() {
        let legacy = LegacyJob {
            id: "legacy-1".to_string(),
            payload: Sample {
                name: "old".into(),
                count: 3,
                nested: Nested {
                    n: 1,
                    s: "z".into(),
                },
            },
            created_at_ms: 1_700_000_000_000,
            attempt: 2,
        };
        let bytes = rmp_serde::to_vec(&legacy).expect("encode legacy");
        let decoded: Job<Sample> = rmp_serde::from_slice(&bytes).expect("decode into new shape");
        assert_eq!(decoded.id, "legacy-1");
        assert_eq!(decoded.attempt, 2);
        assert_eq!(decoded.payload.name, "old");
        assert_eq!(decoded.retry, None);
    }

    /// Reverse compat: a new payload (with `retry: None`) decodes into the
    /// legacy struct without error — `rmp-serde` tolerates extra fields
    /// at the end. With `skip_serializing_if = Option::is_none` the
    /// `retry` field is omitted entirely when unset, so the wire shape
    /// is identical to the legacy shape on the common path.
    #[test]
    fn new_payload_without_override_decodes_in_legacy_consumer() {
        let job: Job<Sample> = Job::new(Sample {
            name: "new".into(),
            count: 1,
            nested: Nested { n: 0, s: "".into() },
        });
        let bytes = rmp_serde::to_vec(&job).expect("encode new");
        let decoded: LegacyJob<Sample> =
            rmp_serde::from_slice(&bytes).expect("decode into legacy shape");
        assert_eq!(decoded.id, job.id);
        assert_eq!(decoded.attempt, 0);
    }
}
