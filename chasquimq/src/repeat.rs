//! Repeatable job specs.
//!
//! BullMQ has two flavors of recurring jobs:
//! - **cron**: a 5-field (or 6-field with seconds) cron expression, optionally
//!   with a timezone, that yields fire times via the `croner` crate.
//! - **every**: a fixed millisecond interval.
//!
//! ChasquiMQ stores specs in the `{chasqui:<queue>}:repeat` ZSET keyed by
//! [`RepeatableSpec::key`] with `score = next_fire_ms`. The full spec lives
//! in the `{chasqui:<queue>}:repeat:spec:<key>` hash so the scheduler tick
//! only deserializes due specs (not every spec on every tick).
//!
//! Key generation: if the user does not supply a stable `key`, we derive one
//! as `<job_name>::<pattern_signature>` where `pattern_signature` is
//! `cron:<expr>:<tz>` (or `cron:<expr>:UTC` if no tz) for cron patterns and
//! `every:<interval_ms>` for fixed intervals. **This deliberately diverges
//! from BullMQ's repeat-key scheme**, which is an md5 hash over the
//! `(name, key, endDate, tz, cron|every)` tuple — that format is tightly
//! coupled to BullMQ internals (key field, endDate, hash construction) and
//! locking it in here would block future BullMQ-compat shims from supplying
//! their own key. The BullMQ-compat layer (Phase 3+) is expected to compute
//! BullMQ-shaped keys upstream and pass them in via the `key` field.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Pattern that drives when a [`RepeatableSpec`] fires.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepeatPattern {
    /// Cron expression. Accepts both 5-field (minute hour dom month dow) and
    /// 6-field (with leading seconds) syntax via the `croner` crate. The
    /// `tz` field is parsed by `chrono_tz`-style names (e.g. `"America/New_York"`);
    /// `None` means UTC.
    Cron {
        expression: String,
        tz: Option<String>,
    },
    /// Fixed millisecond interval between fires.
    Every { interval_ms: u64 },
}

impl RepeatPattern {
    /// Stable signature used to derive a default key when the caller doesn't
    /// supply one. Two specs with the same `job_name` and the same signature
    /// are considered the same recurring job — re-upserting overwrites.
    pub fn signature(&self) -> String {
        match self {
            RepeatPattern::Cron { expression, tz } => {
                let tz_str = tz.as_deref().unwrap_or("UTC");
                format!("cron:{expression}:{tz_str}")
            }
            RepeatPattern::Every { interval_ms } => format!("every:{interval_ms}"),
        }
    }
}

/// A recurring job spec.
///
/// `T` is the payload type — same constraint as `Producer<T>`. Stored on
/// Redis as msgpack inside the `{chasqui:<queue>}:repeat:spec:<key>` hash
/// under field `spec`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepeatableSpec<T> {
    /// Stable identifier. Auto-derived from `job_name::pattern_signature` if
    /// left empty. See module docs for the divergence from BullMQ's hashing
    /// scheme.
    pub key: String,
    /// User-facing job name carried into each fired `Job<T>`.
    pub job_name: String,
    /// What schedules the next fire time.
    pub pattern: RepeatPattern,
    /// The payload that every fire of this spec emits. Cloned and re-encoded
    /// per fire (the spec itself is encoded once at upsert).
    pub payload: T,
    /// Maximum number of fires; `None` means unlimited. Decremented in the
    /// same Lua tick that schedules the next fire.
    pub limit: Option<u64>,
    /// Earliest fire time in epoch milliseconds. Fires before this are
    /// skipped.
    pub start_after_ms: Option<u64>,
    /// Latest fire time in epoch milliseconds. Once `next_fire_ms >
    /// end_before_ms` the spec is removed from the repeat ZSET.
    pub end_before_ms: Option<u64>,
}

impl<T> RepeatableSpec<T> {
    /// Resolve the effective key for this spec, deriving a default if the
    /// user supplied an empty string.
    pub fn resolved_key(&self) -> String {
        if self.key.is_empty() {
            format!("{}::{}", self.job_name, self.pattern.signature())
        } else {
            self.key.clone()
        }
    }
}

/// Light-weight metadata returned by [`Producer::list_repeatable`] —
/// deliberately omits the payload so listing thousands of specs doesn't
/// pull thousands of msgpack-encoded bodies across the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepeatableMeta {
    pub key: String,
    pub job_name: String,
    pub pattern: RepeatPattern,
    pub next_fire_ms: u64,
    pub limit: Option<u64>,
    pub start_after_ms: Option<u64>,
    pub end_before_ms: Option<u64>,
}

/// Wire-format spec stored in the repeat:spec:<key> hash. Separate from the
/// public [`RepeatableSpec<T>`] only because we need a generic-erased shape
/// to decode into when listing specs without paying for the payload.
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredSpec {
    pub key: String,
    pub job_name: String,
    pub pattern: RepeatPattern,
    /// Raw msgpack-encoded payload bytes. Stored once per spec; the
    /// scheduler clones these into each fired job's encoded `Job<T>` body.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
    pub limit: Option<u64>,
    pub start_after_ms: Option<u64>,
    pub end_before_ms: Option<u64>,
    /// Number of times the spec has fired so far. Used to enforce `limit`.
    #[serde(default)]
    pub fired: u64,
}

// Hand-rolled `serde_bytes`-compatible module — we don't want to take the
// `serde_bytes` crate as a dep just for one field. msgpack already encodes
// `Vec<u8>` efficiently as a binary blob, but the explicit module makes
// the intent obvious.
mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let v: serde_bytes_helper::ByteBuf = Deserialize::deserialize(d)?;
        Ok(v.0)
    }

    mod serde_bytes_helper {
        use serde::de::{Deserialize, Deserializer, Visitor};
        use std::fmt;

        pub struct ByteBuf(pub Vec<u8>);

        impl<'de> Deserialize<'de> for ByteBuf {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = ByteBuf;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("byte array")
                    }
                    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
                        Ok(ByteBuf(v.to_vec()))
                    }
                    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                        Ok(ByteBuf(v))
                    }
                    fn visit_seq<A: serde::de::SeqAccess<'de>>(
                        self,
                        mut seq: A,
                    ) -> Result<Self::Value, A::Error> {
                        let mut out: Vec<u8> = Vec::new();
                        while let Some(b) = seq.next_element::<u8>()? {
                            out.push(b);
                        }
                        Ok(ByteBuf(out))
                    }
                }
                d.deserialize_byte_buf(V)
            }
        }
    }
}

/// Compute the next fire time strictly **after** `now_ms` for a given pattern.
/// Returns `None` if the pattern can never fire again (cron expression with
/// no future matches, or `interval_ms == 0`).
///
/// `start_after_ms` clamps the floor: if the pattern's natural next fire is
/// before `start_after_ms`, the result is bumped up to `start_after_ms`.
pub(crate) fn next_fire_after(
    pattern: &RepeatPattern,
    now_ms: u64,
    start_after_ms: Option<u64>,
) -> Result<Option<u64>> {
    let floor = start_after_ms.unwrap_or(0);
    let from_ms = now_ms.max(floor);
    let raw = match pattern {
        RepeatPattern::Cron { expression, tz } => {
            next_cron_after(expression, tz.as_deref(), from_ms)?
        }
        RepeatPattern::Every { interval_ms } => {
            if *interval_ms == 0 {
                return Ok(None);
            }
            // For `every`, the "next fire" is `now_ms + interval_ms` modulo
            // any floor — this matches BullMQ semantics where every:N triggers
            // the first fire one interval after the spec is created (no
            // immediate fire).
            Some(from_ms.saturating_add(*interval_ms))
        }
    };
    Ok(raw)
}

fn next_cron_after(expression: &str, tz: Option<&str>, from_ms: u64) -> Result<Option<u64>> {
    use chrono::{TimeZone, Utc};
    use croner::parser::{CronParser, Seconds};

    // Accept both 5-field (`m h dom mon dow`) and 6-field (with leading
    // seconds) cron expressions. `Seconds::Optional` lets the parser pick
    // based on token count.
    let cron = CronParser::builder()
        .seconds(Seconds::Optional)
        .build()
        .parse(expression)
        .map_err(|e| Error::Config(format!("invalid cron expression {expression:?}: {e}")))?;

    let from_secs = (from_ms / 1000) as i64;
    let from_nsec = ((from_ms % 1000) * 1_000_000) as u32;

    // Resolve the offset. We accept three forms today:
    // - `None` or `"UTC"` → UTC offset.
    // - `"+HH:MM"` / `"-HH:MM"` / `"+HHMM"` → parsed as a fixed offset.
    // Named IANA zones (`"America/New_York"`) are deliberately not supported
    // yet because that would pull in `chrono-tz` (~600 KB compiled in). The
    // BullMQ-compat layer (Phase 3+) can convert IANA names to the spec's
    // current offset upstream, or this can be added behind a feature flag.
    let offset = parse_tz_offset(tz)?;

    let utc = Utc
        .timestamp_opt(from_secs, from_nsec)
        .single()
        .ok_or_else(|| Error::Config("invalid from-time".into()))?;
    let from = utc.with_timezone(&offset);

    let next = cron
        .find_next_occurrence(&from, false)
        .map_err(|e| Error::Config(format!("cron scheduling failed: {e}")))?;

    Ok(Some(next.timestamp_millis().max(0) as u64))
}

fn parse_tz_offset(tz: Option<&str>) -> Result<chrono::FixedOffset> {
    use chrono::FixedOffset;
    let raw = match tz {
        None => return Ok(FixedOffset::east_opt(0).expect("UTC")),
        Some(s) => s,
    };
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("UTC") || trimmed == "Z" {
        return Ok(FixedOffset::east_opt(0).expect("UTC"));
    }
    // Accept `+HH:MM` / `-HH:MM` / `+HHMM` / `-HHMM`.
    let (sign, rest) = match trimmed.as_bytes().first() {
        Some(b'+') => (1, &trimmed[1..]),
        Some(b'-') => (-1, &trimmed[1..]),
        _ => {
            return Err(Error::Config(format!(
                "unsupported timezone {raw:?}: expected UTC, +HH:MM, or -HH:MM (named IANA zones are not yet supported)"
            )));
        }
    };
    let (hh, mm) = if let Some((h, m)) = rest.split_once(':') {
        (h, m)
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else {
        return Err(Error::Config(format!(
            "malformed timezone offset {raw:?}: expected +HH:MM or +HHMM"
        )));
    };
    let h: i32 = hh
        .parse()
        .map_err(|_| Error::Config(format!("malformed timezone offset {raw:?}")))?;
    let m: i32 = mm
        .parse()
        .map_err(|_| Error::Config(format!("malformed timezone offset {raw:?}")))?;
    let total = sign * (h * 3600 + m * 60);
    FixedOffset::east_opt(total)
        .ok_or_else(|| Error::Config(format!("timezone offset {raw:?} out of range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_distinguishes_cron_and_every() {
        let a = RepeatPattern::Cron {
            expression: "* * * * *".into(),
            tz: None,
        };
        let b = RepeatPattern::Every { interval_ms: 1000 };
        assert_ne!(a.signature(), b.signature());
    }

    #[test]
    fn signature_includes_tz() {
        let a = RepeatPattern::Cron {
            expression: "0 0 * * *".into(),
            tz: None,
        };
        let b = RepeatPattern::Cron {
            expression: "0 0 * * *".into(),
            tz: Some("America/New_York".into()),
        };
        assert_ne!(a.signature(), b.signature());
    }

    #[test]
    fn resolved_key_uses_supplied_when_present() {
        let s = RepeatableSpec::<u32> {
            key: "explicit".into(),
            job_name: "my-job".into(),
            pattern: RepeatPattern::Every { interval_ms: 1000 },
            payload: 0,
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
        };
        assert_eq!(s.resolved_key(), "explicit");
    }

    #[test]
    fn resolved_key_falls_back_to_signature() {
        let s = RepeatableSpec::<u32> {
            key: String::new(),
            job_name: "my-job".into(),
            pattern: RepeatPattern::Every { interval_ms: 1000 },
            payload: 0,
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
        };
        assert_eq!(s.resolved_key(), "my-job::every:1000");
    }

    #[test]
    fn next_fire_after_every() {
        let pat = RepeatPattern::Every { interval_ms: 500 };
        let next = next_fire_after(&pat, 1000, None).expect("ok");
        assert_eq!(next, Some(1500));
    }

    #[test]
    fn next_fire_after_every_zero_returns_none() {
        let pat = RepeatPattern::Every { interval_ms: 0 };
        assert_eq!(next_fire_after(&pat, 1000, None).unwrap(), None);
    }

    #[test]
    fn next_fire_after_every_respects_start_after() {
        let pat = RepeatPattern::Every { interval_ms: 100 };
        let next = next_fire_after(&pat, 1000, Some(5_000)).unwrap();
        assert_eq!(next, Some(5_100));
    }

    #[test]
    fn next_fire_after_cron_minute_resolution() {
        let pat = RepeatPattern::Cron {
            expression: "* * * * *".into(),
            tz: None,
        };
        // Pin a specific instant: 2030-01-01 00:00:30 UTC = some epoch ms
        let from_ms: u64 = 1_893_456_030_000;
        let next = next_fire_after(&pat, from_ms, None)
            .expect("ok")
            .expect("some");
        // Next minute boundary after 00:00:30 is 00:01:00 = +30s
        assert_eq!(next, 1_893_456_060_000_u64);
    }

    #[test]
    fn next_fire_after_cron_invalid_expression_errors() {
        let pat = RepeatPattern::Cron {
            expression: "not a cron".into(),
            tz: None,
        };
        let res = next_fire_after(&pat, 0, None);
        assert!(res.is_err(), "got {res:?}");
    }
}
