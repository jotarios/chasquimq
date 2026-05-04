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
    /// Per-job retry overrides. Producer-set, consumer-honored.
    ///
    /// `None` means "fall back to queue-wide `ConsumerConfig` retry settings".
    ///
    /// **Wire-format invariant**: this is the LAST field of `Job<T>` and is
    /// `skip_serializing_if = Option::is_none`, so payloads with `retry = None`
    /// encode identically to the pre-slice-8 4-field shape — old consumers
    /// decode them transparently.
    ///
    /// **Deploy-order requirement**: a payload with `retry = Some(...)`
    /// encodes as a 5-element array, which **cannot** be decoded by a
    /// pre-slice-8 consumer (rmp-serde positional decode rejects array length
    /// mismatches). When rolling out per-job retry overrides:
    /// 1. Deploy the new consumer everywhere first.
    /// 2. Then deploy producers that emit `retry = Some(...)`.
    ///
    /// Producing `retry = Some(...)` while a stale consumer is still running
    /// will route those jobs to the DLQ (decode failure on read).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<JobRetryOverride>,
    /// Optional handler-dispatch name carried as a separate `n` field on the
    /// Redis Stream entry, NOT inside the msgpack-encoded `Job<T>` envelope.
    ///
    /// `#[serde(skip)]` on this field is load-bearing: putting `name` inside
    /// the positional msgpack array alongside the trailing-optional `retry`
    /// would create wire-format ambiguity (slot N could be a `retry`-shaped
    /// struct or a `name` string depending on which trailing-skip fired).
    /// Keeping it out of the envelope means the rmp-serde decode path stays
    /// the existing 4-or-5-element shape pinned by slice 8, and `name`
    /// travels via the `n` stream-entry field — readable into a metric label
    /// or events-stream field without msgpack-decoding the payload bytes.
    ///
    /// Set on the read path by the consumer's reader (from the entry's `n`
    /// field). `String::new()` for unnamed jobs.
    ///
    /// **Where `name` is preserved (slice 1 + PR #56 fixups):**
    /// - Producer→consumer hot path (`Producer::add_with_options` /
    ///   `add_bulk_with_options` / `add_bulk_named` → `XADD` with `n` →
    ///   `XREADGROUP` parser populates `Job::name`).
    /// - DLQ relocate (`xadd_dlq_args` carries `n` verbatim from the source
    ///   entry).
    /// - DLQ peek (`Producer::peek_dlq` returns `DlqEntry::name`).
    /// - DLQ replay (`Producer::replay_dlq` re-emits `n` on the new XADD).
    ///
    /// **Where `name` is dropped today (slice 4 will close):**
    /// - Automatic retry-via-delayed-ZSET re-encode. The consumer's retry
    ///   path re-encodes `Job<T>` with `attempt + 1` and `ZADD`s onto the
    ///   delayed sorted set; the encoded bytes are the only thing the
    ///   promoter has, and it `XADD`s without an `n` field. Replay the
    ///   resulting consumer event and `Job::name == ""` even if the
    ///   original arrived with a name.
    /// - Repeatable-spec scheduler-fire. `RepeatableSpec` carries
    ///   `job_name`, but the scheduler's `XADD` / `ZADD` path doesn't
    ///   thread it onto the stream entry's `n` field yet.
    ///
    /// In both drop cases, the original arrival sees `name`; only re-emits
    /// after a handler error or a repeatable fire lose it.
    #[serde(default, skip)]
    pub name: String,
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
            name: String::new(),
        }
    }

    /// Builder-style setter for the per-job retry override.
    pub fn with_retry(mut self, retry: JobRetryOverride) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Builder-style setter for the dispatch name. Same parser-populated
    /// field set on the read path; this is just a convenience for tests and
    /// for consumers that want to construct a `Job<T>` by hand.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
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
        assert_eq!(decoded.name, "");
    }

    /// `Job::name` is `#[serde(skip)]` — encoding a job with a non-empty name
    /// must not grow the msgpack envelope; decoding always defaults to `""`.
    /// This is what keeps the wire shape identical to the pre-slice-1 form
    /// and makes the `n` stream-entry field the single source of truth on
    /// the read path.
    #[test]
    fn name_field_is_not_serialized() {
        let job_named: Job<u32> = Job::with_id("test".into(), 42).with_name("send-email");
        let job_unnamed: Job<u32> = Job::with_id("test".into(), 42);
        let bytes_named = rmp_serde::to_vec(&job_named).expect("encode named");
        let bytes_unnamed = rmp_serde::to_vec(&job_unnamed).expect("encode unnamed");
        assert_eq!(
            bytes_named, bytes_unnamed,
            "name must be #[serde(skip)] — encoded bytes must match the unnamed shape"
        );
        let decoded: Job<u32> = rmp_serde::from_slice(&bytes_named).expect("decode");
        assert_eq!(
            decoded.name, "",
            "decoded name defaults to empty; the n stream-entry field is the source of truth"
        );
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

    /// Deploy-order regression: a new payload with `retry = Some(...)` is a
    /// 5-element msgpack array. A pre-slice-8 4-field decode path cannot
    /// consume it — `rmp-serde`'s positional decode rejects array-length
    /// mismatches. This pins the asymmetry so we don't accidentally start
    /// claiming "old consumers can decode new payloads" in docs again. The
    /// rollout requires deploying the new consumer before any producer that
    /// emits `retry = Some(...)`.
    #[test]
    fn legacy_consumer_fails_on_new_payload_with_retry_some() {
        // Mimic the pre-slice-8 4-field Job shape.
        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct LegacyJob {
            id: String,
            payload: u32,
            created_at_ms: u64,
            attempt: u32,
        }

        // Produce a current-shape Job with retry = Some(...).
        let job = Job::with_id("test".into(), 42_u32).with_retry(JobRetryOverride {
            max_attempts: Some(7),
            backoff: None,
        });
        let bytes = rmp_serde::to_vec(&job).expect("encode");

        // Legacy decode path must fail with an array-length mismatch.
        let res: std::result::Result<LegacyJob, _> = rmp_serde::from_slice(&bytes);
        assert!(
            res.is_err(),
            "expected legacy 4-field decode to fail on 5-field payload, got {res:?}"
        );
        let msg = format!("{:?}", res.unwrap_err());
        assert!(
            msg.contains("incorrect length") || msg.contains("4") || msg.contains("array"),
            "unexpected error message: {msg}"
        );
    }

    /// Companion to the "fails" case above: same `LegacyJob<u32>` shape, but
    /// the new payload has `retry = None`, which is `skip_serializing_if =
    /// Option::is_none` — so the wire shape collapses back to 4 fields and
    /// the legacy consumer decodes it transparently.
    #[test]
    fn legacy_consumer_decodes_new_payload_with_retry_none() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct LegacyJob {
            id: String,
            payload: u32,
            created_at_ms: u64,
            attempt: u32,
        }

        let job = Job::with_id("test".into(), 42_u32);
        assert!(job.retry.is_none());
        let bytes = rmp_serde::to_vec(&job).expect("encode");

        let decoded: LegacyJob = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(decoded.id, "test");
        assert_eq!(decoded.payload, 42);
        assert_eq!(decoded.attempt, 0);
    }

    /// Sanity: an `Option::Some` override whose inner `max_attempts` and
    /// `backoff` are both `None` is *inert* at the consumer level — it's
    /// equivalent to `retry = None` for retry-decision purposes — but the
    /// wire shape still grows to 5 elements because `skip_serializing_if`
    /// is on the outer `Job::retry`, not the inner fields. Documents the
    /// distinction so a user reading the encoded bytes doesn't think they
    /// found a bug.
    #[test]
    fn empty_override_with_no_inner_fields_set_is_inert() {
        let opts = JobRetryOverride {
            max_attempts: None,
            backoff: None,
        };
        let job: Job<u32> = Job::with_id("x".into(), 0).with_retry(opts);
        let bytes = rmp_serde::to_vec(&job).expect("encode");

        // Round-trips through Job<u32> with retry intact and inner fields
        // both still None — the encode path doesn't drop them.
        let back: Job<u32> = rmp_serde::from_slice(&bytes).expect("round-trip");
        assert!(back.retry.is_some(), "outer retry must survive encode");
        let r = back.retry.unwrap();
        assert!(
            r.max_attempts.is_none(),
            "empty override stays empty after round-trip"
        );
        assert!(
            r.backoff.is_none(),
            "empty override stays empty after round-trip"
        );
    }
}
