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
//! Catch-up: when the scheduler returns from extended downtime and finds a
//! spec whose `next_fire_ms` is many cadences in the past, the per-spec
//! [`MissedFiresPolicy`] decides whether to drop the missed windows
//! ([`MissedFiresPolicy::Skip`], default), fire one job to represent them
//! ([`MissedFiresPolicy::FireOnce`]), or replay every missed window up to a
//! configured cap ([`MissedFiresPolicy::FireAll`]).
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

/// What the scheduler does when it finds a spec whose `next_fire_ms` is more
/// than one cadence in the past — i.e. the scheduler was down (or partitioned
/// off the leader lock) long enough to miss multiple windows.
///
/// The default is [`MissedFiresPolicy::Skip`] because for most workloads
/// (email blasts, billing cron, alert fan-out) replaying a backlog of missed
/// windows after restart is at best confusing and at worst a real incident.
/// Opt into [`MissedFiresPolicy::FireOnce`] when downstream needs to know a
/// window was missed but doesn't care how many; opt into
/// [`MissedFiresPolicy::FireAll`] when each window stands on its own (e.g.
/// per-window aggregation jobs that idempotently no-op for empty windows).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum MissedFiresPolicy {
    /// Default. Drop all missed fires and advance `next_fire_ms` to the
    /// first future fire strictly after `now`. Safe — no thundering herd.
    #[default]
    Skip,
    /// Fire one job to represent the missed window(s), then advance to
    /// first-future. Use when downstream needs to know a window was
    /// missed but doesn't care how many.
    FireOnce,
    /// Fire every missed window in this tick. Bounded by `max_catchup`
    /// to avoid pathological replays after very long outages. After
    /// `max_catchup` fires, advance to first-future and log a warning.
    FireAll { max_catchup: u32 },
}

/// Pattern that drives when a [`RepeatableSpec`] fires.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RepeatPattern {
    /// Cron expression. Accepts both 5-field (minute hour dom month dow) and
    /// 6-field (with leading seconds) syntax via the `croner` crate.
    ///
    /// The `tz` field accepts:
    /// - `None` or `"UTC"` / `"Z"` → UTC.
    /// - Fixed offsets `"+HH:MM"` / `"-HH:MM"` / `"+HHMM"` / `"-HHMM"`.
    /// - Any IANA timezone name (e.g. `"America/New_York"`, `"Europe/London"`,
    ///   `"US/Eastern"`) resolved via the `chrono-tz` database.
    ///
    /// IANA names are **DST-aware**: `0 2 * * *` in `America/New_York` fires
    /// at 02:00 local on both sides of spring-forward / fall-back, with the
    /// underlying UTC instant shifting by one hour. Fixed offsets are not.
    ///
    /// On fall-back ambiguous local times (e.g. New York `01:30` on the
    /// November DST end, which exists twice — once as EDT and once as EST),
    /// the **earlier** of the two UTC instants is selected.
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
    ///
    /// IANA aliases (e.g. `US/Eastern` and `America/New_York`) produce
    /// distinct signatures even though they resolve to the same zone — pass
    /// an explicit `key` if you need alias-collapse.
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
///
/// Catch-up after scheduler downtime is governed by
/// [`RepeatableSpec::missed_fires`]; see [`MissedFiresPolicy`] for the three
/// options. The default ([`MissedFiresPolicy::Skip`]) drops missed windows
/// and resumes on the first future fire — safe for any payload, no
/// thundering herd on restart.
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
    /// What to do with windows that elapsed while the scheduler was down.
    /// Defaults to [`MissedFiresPolicy::Skip`] — drop missed windows and
    /// resume on the first future fire. See [`MissedFiresPolicy`] for the
    /// other options.
    ///
    /// Trailing optional with `skip_serializing_if` so the field is omitted
    /// from the encoded msgpack when set to the default — pre-existing
    /// stored specs (no `missed_fires` field on the wire) continue to decode
    /// unchanged into the new shape with `Skip`.
    #[serde(default, skip_serializing_if = "is_default_missed_fires_policy")]
    pub missed_fires: MissedFiresPolicy,
}

fn is_default_missed_fires_policy(p: &MissedFiresPolicy) -> bool {
    matches!(p, MissedFiresPolicy::Skip)
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
///
/// **Encoding note**: `rmp-serde` encodes structs as positional arrays.
/// Trailing optional fields with `#[serde(default, skip_serializing_if =
/// ...)]` are safe to add — pre-existing encoded specs decode cleanly into
/// the new shape (the missing trailing slots fall through to `Default`).
/// **Adding a non-trailing field, or removing the `skip_serializing_if`,
/// would shift positions and break decode of every spec already in Redis.**
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
    /// Catch-up policy. Trailing-optional with `skip_serializing_if` so
    /// pre-existing specs (encoded before this field existed) decode
    /// cleanly with `Skip` (the default). See [`MissedFiresPolicy`].
    #[serde(default, skip_serializing_if = "is_default_missed_fires_policy")]
    pub missed_fires: MissedFiresPolicy,
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

/// Hard cap on iterations of [`first_future_fire`]'s advance loop. Protects
/// against pathological `every:1ms`-with-multi-day-outage replays that would
/// otherwise spin for tens of millions of iterations on a hot path. At
/// `every:1s`, 100k iterations covers ~28 hours of catch-up; at `every:1m`,
/// ~70 days; at `every:1h`, ~11 years — well past any realistic outage.
/// If exceeded, the function returns `Ok(None)` with a tracing warning and
/// the scheduler treats the spec as "no future fire" (drops it).
const FIRST_FUTURE_FIRE_ITERATION_CAP: u32 = 100_000;

/// Walk `next_fire_after` from `fire_at_ms` forward until the result is
/// strictly greater than `now_ms` (or there is no next fire). Used by the
/// scheduler's catch-up logic to advance past every missed window in one
/// step without dispatching jobs for them.
///
/// Returns `Ok(None)` if the pattern has no future fire (cron with no
/// match, `every:0`, or the iteration cap was hit — see
/// [`FIRST_FUTURE_FIRE_ITERATION_CAP`]). Caller should treat that as
/// "remove this spec".
pub(crate) fn first_future_fire(
    pattern: &RepeatPattern,
    now_ms: u64,
    fire_at_ms: u64,
    start_after_ms: Option<u64>,
) -> Result<Option<u64>> {
    let mut at = fire_at_ms;
    for _ in 0..FIRST_FUTURE_FIRE_ITERATION_CAP {
        match next_fire_after(pattern, at, start_after_ms)? {
            Some(next) => {
                if next > now_ms {
                    return Ok(Some(next));
                }
                if next <= at {
                    // Defensive: pattern didn't advance (shouldn't happen
                    // for any valid `RepeatPattern`, but bail rather than
                    // spin if it does).
                    return Ok(Some(next));
                }
                at = next;
            }
            None => return Ok(None),
        }
    }
    tracing::warn!(
        cap = FIRST_FUTURE_FIRE_ITERATION_CAP,
        "first_future_fire iteration cap reached; treating pattern as exhausted",
    );
    Ok(None)
}

/// Resolved timezone for a cron spec. Carries either a fixed UTC offset (so
/// `"+05:30"` and `"UTC"` keep working with no `chrono-tz` lookup) or a
/// named IANA zone (so `"America/New_York"` honors DST transitions —
/// occurrences land at the correct local wall-clock minute on both sides
/// of spring-forward / fall-back).
#[derive(Clone, Copy)]
pub(crate) enum TzKind {
    Fixed(chrono::FixedOffset),
    Named(chrono_tz::Tz),
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

    let utc = Utc
        .timestamp_opt(from_secs, from_nsec)
        .single()
        .ok_or_else(|| Error::Config("invalid from-time".into()))?;

    // Dispatch once on the resolved zone kind so each branch instantiates
    // `find_next_occurrence` with a concrete `TimeZone` impl — no dynamic
    // dispatch on the hot path. `next_in_zone` does the per-zone work.
    let next_ms = match parse_tz(tz)? {
        TzKind::Fixed(off) => next_in_zone(&cron, utc, off)?,
        TzKind::Named(zone) => next_in_zone(&cron, utc, zone)?,
    };

    Ok(Some(next_ms))
}

/// Convert a UTC instant into the given zone, ask croner for the next
/// occurrence, and project back to epoch ms. Generic over `TimeZone` so
/// both `FixedOffset` and `chrono_tz::Tz` callers monomorphize cleanly.
///
/// `chrono` handles DST gaps/overlaps internally — the local time croner
/// returns is always a real wall-clock moment in `tz`, so the round-trip
/// back to UTC is total (no `LocalResult::None`/`Ambiguous` handling
/// required here).
fn next_in_zone<Tz: chrono::TimeZone>(
    cron: &croner::Cron,
    from_utc: chrono::DateTime<chrono::Utc>,
    tz: Tz,
) -> Result<u64> {
    use chrono::Utc;
    let from_local = from_utc.with_timezone(&tz);
    let next_local = cron
        .find_next_occurrence(&from_local, false)
        .map_err(|e| Error::Config(format!("cron scheduling failed: {e}")))?;
    Ok(next_local.with_timezone(&Utc).timestamp_millis().max(0) as u64)
}

/// Resolve a user-supplied tz string to a [`TzKind`]. DST-aware for IANA
/// names; snapshot for fixed offsets.
///
/// Accepted forms:
/// - `None` or `"UTC"` / `"Z"` → UTC.
/// - `"+HH:MM"` / `"-HH:MM"` / `"+HHMM"` / `"-HHMM"` → fixed offset.
/// - Any IANA timezone name parseable by `chrono-tz` (e.g.
///   `"America/New_York"`, `"Europe/London"`, `"US/Eastern"`).
fn parse_tz(tz: Option<&str>) -> Result<TzKind> {
    use chrono::FixedOffset;

    let raw = match tz {
        None => return Ok(TzKind::Fixed(FixedOffset::east_opt(0).expect("UTC"))),
        Some(s) => s,
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("UTC") || trimmed == "Z" {
        return Ok(TzKind::Fixed(FixedOffset::east_opt(0).expect("UTC")));
    }

    // Fixed-offset forms always start with `+` or `-`. Anything else is
    // either an IANA name or an outright error.
    if trimmed.starts_with('+') || trimmed.starts_with('-') {
        return parse_fixed_offset(trimmed, raw).map(TzKind::Fixed);
    }

    // IANA lookup. `chrono_tz::Tz: FromStr` returns a static error string
    // on failure; surface a friendlier message that lists every accepted
    // form so the operator knows what to try next.
    trimmed
        .parse::<chrono_tz::Tz>()
        .map(TzKind::Named)
        .map_err(|_| {
            Error::Config(format!(
                "unsupported timezone {raw:?}: expected UTC, +HH:MM, or any IANA timezone name (e.g. \"America/New_York\")"
            ))
        })
}

fn parse_fixed_offset(trimmed: &str, raw: &str) -> Result<chrono::FixedOffset> {
    use chrono::FixedOffset;
    let (sign, rest) = match trimmed.as_bytes().first() {
        Some(b'+') => (1, &trimmed[1..]),
        Some(b'-') => (-1, &trimmed[1..]),
        _ => unreachable!("parse_fixed_offset called without leading sign"),
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
            missed_fires: MissedFiresPolicy::Skip,
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
            missed_fires: MissedFiresPolicy::Skip,
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

    #[test]
    fn next_fire_after_cron_fixed_offset_still_works() {
        // Sanity: pre-existing fixed-offset path is unchanged.
        // Cron `0 2 * * *` in `+05:30`, asked from 2026-01-15 00:00 UTC.
        // Local time then = 05:30 IST. Next 02:00 IST is 2026-01-16 02:00
        // IST = 2026-01-15 20:30 UTC.
        let pat = RepeatPattern::Cron {
            expression: "0 2 * * *".into(),
            tz: Some("+05:30".into()),
        };
        let from_ms: u64 = 1_768_435_200_000; // 2026-01-15 00:00 UTC
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        // 2026-01-15 20:30 UTC
        assert_eq!(next, 1_768_509_000_000_u64);
    }

    #[test]
    fn next_fire_after_cron_named_iana_zone() {
        // `0 2 * * *` in America/New_York. From 2026-01-15 00:00 UTC, NYC
        // local time is 2026-01-14 19:00 EST (UTC-5, no DST). Next 02:00
        // local fire = 2026-01-15 02:00 EST = 2026-01-15 07:00 UTC.
        let pat = RepeatPattern::Cron {
            expression: "0 2 * * *".into(),
            tz: Some("America/New_York".into()),
        };
        let from_ms: u64 = 1_768_435_200_000; // 2026-01-15 00:00 UTC
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        assert_eq!(next, 1_768_460_400_000_u64); // 2026-01-15 07:00 UTC
    }

    #[test]
    fn next_fire_after_cron_iana_zone_dst_transition() {
        // The headline test: `0 2 * * *` in America/New_York, asked from
        // 2026-03-09 00:00 UTC (after the 2026-03-08 spring-forward in
        // NYC, which jumps 02:00 EST → 03:00 EDT). Local time then is
        // 2026-03-08 20:00 EDT (UTC-4). The next 02:00 local fire is
        // 2026-03-09 02:00 EDT = 2026-03-09 06:00 UTC — *not* 07:00 UTC,
        // which is what a snapshot fixed-offset would give. This proves
        // the resolver honors live DST rules instead of pinning the
        // offset at upsert time.
        let pat = RepeatPattern::Cron {
            expression: "0 2 * * *".into(),
            tz: Some("America/New_York".into()),
        };
        let from_ms: u64 = 1_773_014_400_000; // 2026-03-09 00:00 UTC
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        assert_eq!(next, 1_773_036_000_000_u64); // 2026-03-09 06:00 UTC (EDT)
    }

    #[test]
    fn next_fire_after_cron_iana_alias() {
        // chrono-tz exposes pre-1993 POSIX-style aliases like `US/Eastern`.
        // Should resolve to the same zone as `America/New_York`.
        let pat = RepeatPattern::Cron {
            expression: "0 2 * * *".into(),
            tz: Some("US/Eastern".into()),
        };
        let from_ms: u64 = 1_768_435_200_000; // 2026-01-15 00:00 UTC
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        assert_eq!(next, 1_768_460_400_000_u64); // 2026-01-15 07:00 UTC
    }

    #[test]
    fn next_fire_after_cron_invalid_iana_zone_errors() {
        let pat = RepeatPattern::Cron {
            expression: "0 2 * * *".into(),
            tz: Some("Mars/Olympus_Mons".into()),
        };
        let res = next_fire_after(&pat, 0, None);
        assert!(res.is_err(), "got {res:?}");
        // The error message should help the operator: list the accepted
        // forms so they don't have to read the source.
        let msg = format!("{}", res.unwrap_err());
        assert!(msg.contains("UTC"), "msg={msg}");
        assert!(msg.contains("IANA"), "msg={msg}");
    }

    #[test]
    fn next_fire_after_cron_europe_london_bst() {
        // `0 9 * * *` in Europe/London during BST (UTC+1). From
        // 2026-06-15 00:00 UTC, London is at 01:00 BST. Next 09:00 BST
        // = 2026-06-15 09:00 BST = 2026-06-15 08:00 UTC.
        let pat = RepeatPattern::Cron {
            expression: "0 9 * * *".into(),
            tz: Some("Europe/London".into()),
        };
        let from_ms: u64 = 1_781_481_600_000; // 2026-06-15 00:00 UTC
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        assert_eq!(next, 1_781_510_400_000_u64); // 2026-06-15 08:00 UTC
    }

    #[test]
    fn next_fire_after_cron_iana_dst_overlap_picks_consistent_instant() {
        // Behavior pin: on the NYC fall-back transition, the local time
        // `01:30` happens twice — once at 01:30 EDT (UTC-4) and again at
        // 01:30 EST (UTC-5). For 2026, that transition is on
        // 2026-11-01 02:00 EDT → 01:00 EST.
        //
        //   First occurrence:  01:30 EDT = 2026-11-01 05:30 UTC = 1_793_511_000_000 ms
        //   Second occurrence: 01:30 EST = 2026-11-01 06:30 UTC = 1_793_514_600_000 ms
        //
        // Whichever of the two croner picks, we pin it so a future bump
        // doesn't silently flip the behavior.
        let pat = RepeatPattern::Cron {
            expression: "30 1 * * *".into(),
            tz: Some("America/New_York".into()),
        };
        // 2026-11-01 04:00 UTC = 2026-11-01 00:00 EDT (well before any
        // 01:30 fire on the transition day).
        let from_ms: u64 = 1_793_505_600_000;
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();

        let first_edt: u64 = 1_793_511_000_000; // 01:30 EDT (first wall-clock pass)
        let second_est: u64 = 1_793_514_600_000; // 01:30 EST (second wall-clock pass)

        assert!(
            next == first_edt || next == second_est,
            "expected {first_edt} or {second_est}, got {next}",
        );
        // Pinned: croner picks the **earlier** of the two ambiguous UTC
        // instants (01:30 EDT, the first wall-clock occurrence in local
        // time). If a croner upgrade flips this, this assertion fires
        // and the doc on `RepeatPattern::Cron` needs updating in lockstep.
        assert_eq!(next, first_edt);
    }

    #[test]
    fn next_fire_after_cron_iana_dst_gap_skips_to_valid_local_time() {
        // Behavior-pinning: `30 2 * * *` (02:30 every day) in
        // America/New_York, on a spring-forward day. 2026-03-08 02:30 EST
        // does not exist (clocks jump 02:00 → 03:00). croner picks the
        // next *real* match. Whatever it picks, the result must:
        //   1. Be strictly greater than the from-time.
        //   2. Be a valid epoch ms (no panic, no overflow).
        //   3. Decode back to 02:30 local on a date that isn't 2026-03-08.
        // We're not picky about whether croner jumps to 03:30 EDT same
        // day or skips to 02:30 EDT the following day — we just need it
        // to not crash and not return the impossible local time.
        use chrono::{TimeZone, Timelike};
        let pat = RepeatPattern::Cron {
            expression: "30 2 * * *".into(),
            tz: Some("America/New_York".into()),
        };
        // 2026-03-08 06:00 UTC = 2026-03-08 01:00 EST (one hour before
        // the spring-forward).
        let from_ms: u64 = 1_772_949_600_000;
        let next = next_fire_after(&pat, from_ms, None).unwrap().unwrap();
        assert!(next > from_ms, "next={next} should be after from={from_ms}");

        let zone: chrono_tz::Tz = "America/New_York".parse().unwrap();
        let local = zone
            .timestamp_millis_opt(next as i64)
            .single()
            .expect("local time exists");
        // Must not be the nonexistent 02:30 on 2026-03-08.
        let bad_date = chrono::NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        assert!(
            !(local.date_naive() == bad_date
                && local.time().hour() == 2
                && local.time().minute() == 30),
            "got nonexistent local time {local}",
        );
    }

    #[test]
    fn missed_fires_policy_default_is_skip() {
        let p: MissedFiresPolicy = Default::default();
        assert_eq!(p, MissedFiresPolicy::Skip);
    }

    #[test]
    fn missed_fires_policy_msgpack_round_trip() {
        for p in [
            MissedFiresPolicy::Skip,
            MissedFiresPolicy::FireOnce,
            MissedFiresPolicy::FireAll { max_catchup: 7 },
        ] {
            let bytes = rmp_serde::to_vec(&p).expect("encode");
            let decoded: MissedFiresPolicy = rmp_serde::from_slice(&bytes).expect("decode");
            assert_eq!(decoded, p);
        }
    }

    #[test]
    fn first_future_fire_every_skips_to_strictly_after_now() {
        let pat = RepeatPattern::Every {
            interval_ms: 60_000,
        };
        // fire_at_ms = 1_000_000 (1000s into epoch). now_ms = 1_300_000 (5
        // missed minutes, the 5th in the past). first_future = 1_360_000
        // (the 6th, strictly > now).
        let next = first_future_fire(&pat, 1_300_000, 1_000_000, None)
            .unwrap()
            .unwrap();
        assert_eq!(next, 1_360_000);
        assert!(next > 1_300_000);
    }

    #[test]
    fn first_future_fire_every_zero_returns_none() {
        let pat = RepeatPattern::Every { interval_ms: 0 };
        assert_eq!(first_future_fire(&pat, 1_000, 0, None).unwrap(), None);
    }

    #[test]
    fn first_future_fire_when_already_future_returns_first_step() {
        // If the pattern's natural next fire from `fire_at_ms` is already
        // > now_ms, no looping required — return it directly.
        let pat = RepeatPattern::Every {
            interval_ms: 60_000,
        };
        let next = first_future_fire(&pat, 100, 1_000, None).unwrap().unwrap();
        assert_eq!(next, 61_000); // 1_000 + 60_000
    }

    /// Pre-this-PR shape: the legacy `StoredSpec` without the
    /// `missed_fires` field. Verifies that an existing rmp-serde-encoded
    /// spec already in Redis decodes cleanly into the new shape with
    /// `missed_fires = Skip` (the default). This is the deploy-safety test
    /// — if it ever fails, in-place upgrades will lose all live recurring
    /// jobs.
    #[derive(Serialize)]
    struct LegacyStoredSpec {
        key: String,
        job_name: String,
        pattern: RepeatPattern,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
        limit: Option<u64>,
        start_after_ms: Option<u64>,
        end_before_ms: Option<u64>,
        fired: u64,
    }

    #[test]
    fn legacy_storedspec_decodes_with_default_policy() {
        let legacy = LegacyStoredSpec {
            key: "legacy-key".into(),
            job_name: "legacy-job".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: vec![0xC0], // msgpack nil
            limit: None,
            start_after_ms: None,
            end_before_ms: Some(9_999_999_999),
            fired: 42,
        };
        let bytes = rmp_serde::to_vec(&legacy).expect("encode legacy");
        let decoded: StoredSpec = rmp_serde::from_slice(&bytes).expect("decode into new shape");
        assert_eq!(decoded.key, "legacy-key");
        assert_eq!(decoded.job_name, "legacy-job");
        assert_eq!(decoded.fired, 42);
        assert_eq!(decoded.missed_fires, MissedFiresPolicy::Skip);
    }

    #[test]
    fn new_storedspec_with_default_policy_omits_field_on_wire() {
        // skip_serializing_if + Default::default keeps the encoded shape
        // byte-compatible with the legacy form when the policy is Skip.
        // This is what makes the back-compat work in *both* directions:
        // an old reader sees no extra trailing field on a default-policy
        // spec, and a new reader sees no field at all and falls back to
        // Default.
        let new_default = StoredSpec {
            key: "k".into(),
            job_name: "j".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: vec![0xC0],
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            fired: 0,
            missed_fires: MissedFiresPolicy::Skip,
        };
        let legacy_eq = LegacyStoredSpec {
            key: "k".into(),
            job_name: "j".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: vec![0xC0],
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            fired: 0,
        };
        let new_bytes = rmp_serde::to_vec(&new_default).expect("encode new");
        let legacy_bytes = rmp_serde::to_vec(&legacy_eq).expect("encode legacy");
        assert_eq!(
            new_bytes, legacy_bytes,
            "default-policy StoredSpec must encode identically to the legacy 8-field shape",
        );
    }

    #[test]
    fn new_storedspec_with_non_default_policy_round_trips() {
        let s = StoredSpec {
            key: "k".into(),
            job_name: "j".into(),
            pattern: RepeatPattern::Every { interval_ms: 1_000 },
            payload: vec![0xC0],
            limit: None,
            start_after_ms: None,
            end_before_ms: None,
            fired: 0,
            missed_fires: MissedFiresPolicy::FireAll { max_catchup: 12 },
        };
        let bytes = rmp_serde::to_vec(&s).expect("encode");
        let decoded: StoredSpec = rmp_serde::from_slice(&bytes).expect("decode");
        assert_eq!(
            decoded.missed_fires,
            MissedFiresPolicy::FireAll { max_catchup: 12 }
        );
    }
}
