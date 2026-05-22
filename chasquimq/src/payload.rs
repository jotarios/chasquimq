//! Engine-private msgpack envelope walker used by the introspection
//! module.
//!
//! `Job<T>` encodes via rmp-serde as a positional msgpack array. The
//! first slot is `id` (string), the second is `payload` (whatever shape
//! `T` serialized to), then `created_at_ms` (u64), `attempt` (u32), and
//! an optional `retry` slot. The introspector needs to recover the
//! first, third, and fourth slots without knowing `T`.
//!
//! `peek_envelope` walks the msgpack array via `rmpv` (a value-tree
//! decoder) and returns `(id, payload_bytes, created_at_ms, attempt)`,
//! where `payload_bytes` is the re-encoded msgpack of the payload slot
//! exactly as it sat on the wire. The re-encode is correct because
//! `rmpv::Value` round-trips through `rmp_serde::to_vec` losslessly.
//!
//! Decode-failure tolerance: if the slice is not a valid msgpack array
//! or doesn't have the expected positional shape, `peek_envelope`
//! returns `None`. Callers treat that as "broken envelope" and surface
//! `decode_failed = true` on the resulting `JobInfo` rather than
//! propagating the error.

use bytes::Bytes;

/// Walk a msgpack-encoded `Job<T>` envelope and return the engine-known
/// fields plus the raw payload bytes. Returns `None` for any shape that
/// isn't a positional msgpack array with the expected `[id, payload,
/// created_at_ms, attempt, ...]` layout.
pub(crate) fn peek_envelope(bytes: &[u8]) -> Option<(String, Bytes, u64, u32)> {
    let value: rmpv::Value = rmpv::decode::read_value(&mut std::io::Cursor::new(bytes)).ok()?;
    let arr = match value {
        rmpv::Value::Array(arr) => arr,
        _ => return None,
    };
    if arr.len() < 4 {
        return None;
    }
    let id = match &arr[0] {
        rmpv::Value::String(s) => s.as_str()?.to_string(),
        _ => return None,
    };
    // Slot 1: the encoded payload. For the shim path (`RawBytes`-shaped
    // payloads), the on-wire shape is a msgpack `bin` whose *inner*
    // bytes are the shim's own msgpack-encoded user value — we hand
    // those inner bytes back so the shim's `decode_payload` matches.
    // For the engine-typed path (e.g. `Producer<UserData>`), slot 1 is
    // whatever shape `T` serialized to (array, map, int, …); we
    // re-encode the `rmpv::Value` losslessly.
    let payload_bytes = match &arr[1] {
        rmpv::Value::Binary(b) => b.clone(),
        other => rmp_serde::to_vec(other).ok()?,
    };
    let created_at_ms = arr[2].as_u64()?;
    let attempt = arr[3].as_u64().map(|n| n as u32)?;
    Some((id, Bytes::from(payload_bytes), created_at_ms, attempt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct UserData {
        name: String,
        count: u32,
    }

    /// Engine-typed `Producer<UserData>` writes `UserData` as its native
    /// msgpack shape (a positional 2-element array). The inspector must
    /// recover `id` / `created_at_ms` / `attempt` from the outer
    /// envelope regardless.
    #[test]
    fn peek_envelope_recovers_typed_job_fields() {
        let job = Job::new(UserData {
            name: "round-trip".into(),
            count: 7,
        });
        let envelope = rmp_serde::to_vec(&job).expect("encode");
        let (id, payload, created_at_ms, attempt) =
            peek_envelope(&envelope).expect("peek envelope");
        assert_eq!(id, job.id);
        assert_eq!(created_at_ms, job.created_at_ms);
        assert_eq!(attempt, 0);
        // Payload round-trips back into UserData.
        let recovered: UserData = rmp_serde::from_slice(&payload).expect("decode user data");
        assert_eq!(
            recovered,
            UserData {
                name: "round-trip".into(),
                count: 7
            }
        );
    }

    /// A shim-typed producer wraps the user data in a single msgpack
    /// `bin` (matches `chasquimq-node::RawBytes`). The inspector still
    /// recovers the envelope fields and the payload bytes survive.
    #[test]
    fn peek_envelope_recovers_bin_payload() {
        // Hand-build a Job<X> where X serializes via serialize_bytes.
        struct RawBytesLike(Bytes);
        impl serde::Serialize for RawBytesLike {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(self.0.as_ref())
            }
        }
        let inner_msgpack = rmp_serde::to_vec(&UserData {
            name: "shim-path".into(),
            count: 11,
        })
        .expect("encode inner");
        let job = Job::new(RawBytesLike(Bytes::copy_from_slice(&inner_msgpack)));
        let envelope = rmp_serde::to_vec(&job).expect("encode envelope");
        let (id, payload, created_at_ms, attempt) =
            peek_envelope(&envelope).expect("peek envelope");
        assert_eq!(id, job.id);
        assert_eq!(created_at_ms, job.created_at_ms);
        assert_eq!(attempt, 0);
        // Re-encoded `bin` round-trips into the original bytes.
        let recovered: UserData = rmp_serde::from_slice(&payload).expect("decode user data");
        assert_eq!(
            recovered,
            UserData {
                name: "shim-path".into(),
                count: 11
            }
        );
    }

    #[test]
    fn peek_envelope_returns_none_for_garbage() {
        assert!(peek_envelope(&[0xff, 0x00, 0x01]).is_none());
        assert!(peek_envelope(&[]).is_none());
    }

    /// A msgpack array shorter than 4 elements (the minimum encoded
    /// `Job` shape) is treated as undecodable.
    #[test]
    fn peek_envelope_rejects_too_short_array() {
        // msgpack fixarray of length 2 with two integers.
        let bytes = vec![0x92, 0x01, 0x02];
        assert!(peek_envelope(&bytes).is_none());
    }
}
