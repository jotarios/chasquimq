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
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobRetryOverride {
    /// If set, overrides `ConsumerConfig::max_attempts` for this job.
    pub max_attempts: Option<u32>,
    /// If set, overrides `ConsumerConfig::retry` for this job.
    pub backoff: Option<BackoffSpec>,
}

/// Backoff strategy for [`BackoffSpec::kind`].
///
/// Unit-only enum encoded by serde as the variant name as a plain string
/// (rmp-serde renders `BackoffKind::Exponential` as the msgpack fixstr
/// `"exponential"`). The `#[serde(rename_all = "lowercase")]` attribute
/// makes the on-wire variant tag byte-identical to the lowercase string
/// values that the previous `kind: String` field carried, so payloads
/// produced by the prior release decode unchanged into the new shape —
/// pinned by `legacy_backoff_kind_string_decodes_into_enum` below.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BackoffKind {
    /// Returns `delay_ms` plus jitter every retry, capped at
    /// `max_delay_ms`. No multiplier applied.
    Fixed,
    /// Returns `delay_ms * multiplier^(attempt-1)` plus jitter, capped at
    /// `max_delay_ms`.
    Exponential,
    /// Forward-compat sink for unknown variants emitted by a future SDK
    /// (e.g. `"linear"`). Routed to the same backoff math as
    /// [`BackoffKind::Exponential`] at the consumer so a strange `kind`
    /// from a newer producer doesn't hard-fail an older consumer. Matches
    /// the previous `kind: String` "unknown → exponential" behavior.
    #[serde(other)]
    Unknown,
}

/// Per-job backoff spec. Unknown `kind` variants from a future SDK
/// degrade to `BackoffKind::Unknown` and are routed through the
/// exponential math at the consumer, so a strange `kind` doesn't cause
/// a hard failure.
///
/// **Wire format**: unchanged from the prior `kind: String` shape because
/// [`BackoffKind`]'s lowercase variant tags (`"fixed"`, `"exponential"`)
/// are byte-identical to the strings the previous release wrote.
///
/// **Encoding note**: see [`JobRetryOverride`] — the trailing optional
/// fields here are always serialized (no `skip_serializing_if`) because
/// `rmp-serde`'s array encoding is positional.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackoffSpec {
    /// Strategy. See [`BackoffKind`].
    pub kind: BackoffKind,
    /// Base delay in milliseconds. Required.
    pub delay_ms: u64,
    /// Cap. Defaults to `RetryConfig::max_backoff_ms` when `None`.
    pub max_delay_ms: Option<u64>,
    /// Multiplier for exponential. Defaults to `RetryConfig::multiplier`
    /// when `None`. Ignored for [`BackoffKind::Fixed`].
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
                kind: BackoffKind::Exponential,
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
    /// `BackoffKind::Exponential` and `BackoffKind::Fixed` must encode as
    /// the plain msgpack strings `"exponential"` / `"fixed"` — no enum
    /// wrapping, no externally-tagged outer object — so the wire format
    /// matches the legacy `kind: String` representation byte-for-byte.
    #[test]
    fn backoff_kind_serializes_as_lowercase_string() {
        let exp = rmp_serde::to_vec(&BackoffKind::Exponential).expect("encode exp");
        let from_str = rmp_serde::to_vec(&"exponential".to_string()).expect("encode str");
        assert_eq!(
            exp, from_str,
            "BackoffKind::Exponential must encode as msgpack string \"exponential\""
        );

        let fixed = rmp_serde::to_vec(&BackoffKind::Fixed).expect("encode fixed");
        let from_str = rmp_serde::to_vec(&"fixed".to_string()).expect("encode str");
        assert_eq!(
            fixed, from_str,
            "BackoffKind::Fixed must encode as msgpack string \"fixed\""
        );
    }

    /// Legacy `BackoffSpec` with `kind: String` shape must still decode
    /// into the new enum-typed `BackoffSpec`. This is the load-bearing
    /// wire-format-back-compat guarantee — encoded jobs already in flight
    /// in Redis at upgrade time must round-trip unchanged.
    #[test]
    fn legacy_backoff_kind_string_decodes_into_enum() {
        #[derive(Serialize, Deserialize)]
        struct LegacyBackoffSpec {
            kind: String,
            delay_ms: u64,
            max_delay_ms: Option<u64>,
            multiplier: Option<f64>,
            jitter_ms: Option<u64>,
        }

        for (legacy_kind, want) in [
            ("exponential", BackoffKind::Exponential),
            ("fixed", BackoffKind::Fixed),
        ] {
            let legacy = LegacyBackoffSpec {
                kind: legacy_kind.to_string(),
                delay_ms: 250,
                max_delay_ms: Some(10_000),
                multiplier: Some(3.0),
                jitter_ms: Some(50),
            };
            let bytes = rmp_serde::to_vec(&legacy).expect("encode legacy");
            let decoded: BackoffSpec =
                rmp_serde::from_slice(&bytes).expect("decode legacy into new shape");
            assert_eq!(decoded.kind, want, "wire-format mismatch for {legacy_kind}");
            assert_eq!(decoded.delay_ms, 250);
            assert_eq!(decoded.max_delay_ms, Some(10_000));
            assert_eq!(decoded.multiplier, Some(3.0));
            assert_eq!(decoded.jitter_ms, Some(50));
        }
    }

    /// An unknown `kind` variant from a future SDK (e.g. `"linear"`) must
    /// decode into `BackoffKind::Unknown` rather than erroring. This is
    /// the `#[serde(other)]` forward-compat guarantee.
    #[test]
    fn legacy_backoff_kind_unknown_string_decodes_as_unknown_variant() {
        #[derive(Serialize, Deserialize)]
        struct LegacyBackoffSpec {
            kind: String,
            delay_ms: u64,
            max_delay_ms: Option<u64>,
            multiplier: Option<f64>,
            jitter_ms: Option<u64>,
        }

        let legacy = LegacyBackoffSpec {
            kind: "linear".to_string(),
            delay_ms: 100,
            max_delay_ms: None,
            multiplier: None,
            jitter_ms: None,
        };
        let bytes = rmp_serde::to_vec(&legacy).expect("encode legacy");
        let decoded: BackoffSpec =
            rmp_serde::from_slice(&bytes).expect("decode unknown kind into new shape");
        assert_eq!(decoded.kind, BackoffKind::Unknown);
        assert_eq!(decoded.delay_ms, 100);
    }

    /// `BackoffSpec` round-trips through msgpack with the new enum field.
    #[test]
    fn backoff_spec_round_trips_with_enum_kind() {
        let spec = BackoffSpec {
            kind: BackoffKind::Fixed,
            delay_ms: 80,
            max_delay_ms: Some(1_000),
            multiplier: Some(2.0),
            jitter_ms: Some(10),
        };
        let bytes = rmp_serde::to_vec(&spec).expect("encode");
        let decoded: BackoffSpec = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(decoded, spec);
    }

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
