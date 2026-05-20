//! Engine-private opaque payload wrapper used by the introspection module
//! to decode `Job<T>` envelopes off the wire without knowing the inner
//! user type.
//!
//! Why a new type instead of reusing `chasquimq-node::RawBytes`: the shim
//! crate's `RawBytes` lives in `chasquimq-node`, which the engine crate
//! must not depend on (it would invert the dependency direction and pull
//! napi-rs into a pure-Rust library). Both types have identical wire shape
//! — a single msgpack `bin` value — so a `Job<OpaqueBytes>` decodes any
//! payload a `Producer<T>` ever wrote, regardless of whether `T` was
//! `RawBytes` (Node shim), the engine's own opaque-bytes path (Python
//! shim), or a strongly-typed `T: Serialize` (engine integration tests).
//!
//! `OpaqueBytes` is engine-private (`pub(crate)`) — it's an implementation
//! detail of the inspector. External callers receive `bytes::Bytes`
//! directly via the `Introspector` API surface.

use bytes::Bytes;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Opaque payload bytes that survive a `Job<OpaqueBytes>` round trip
/// through `rmp-serde` as a single msgpack `bin` value.
///
/// Mirrors the wire shape of `chasquimq-node::RawBytes` and the python
/// shim's bytes path — encoded jobs from any producer surface decode
/// here unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OpaqueBytes(pub(crate) Bytes);

impl OpaqueBytes {
    pub(crate) fn into_inner(self) -> Bytes {
        self.0
    }
}

impl Serialize for OpaqueBytes {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for OpaqueBytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_bytes(OpaqueBytesVisitor)
    }
}

struct OpaqueBytesVisitor;

impl<'de> Visitor<'de> for OpaqueBytesVisitor {
    type Value = OpaqueBytes;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a msgpack `bin` (or compatible byte buffer)")
    }

    fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<OpaqueBytes, E> {
        Ok(OpaqueBytes(Bytes::copy_from_slice(v)))
    }

    fn visit_byte_buf<E: DeError>(self, v: Vec<u8>) -> Result<OpaqueBytes, E> {
        Ok(OpaqueBytes(Bytes::from(v)))
    }

    fn visit_borrowed_bytes<E: DeError>(self, v: &'de [u8]) -> Result<OpaqueBytes, E> {
        Ok(OpaqueBytes(Bytes::copy_from_slice(v)))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<OpaqueBytes, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut buf = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(b) = seq.next_element::<u8>()? {
            buf.push(b);
        }
        Ok(OpaqueBytes(Bytes::from(buf)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct UserData {
        name: String,
        count: u32,
    }

    /// A `Job<UserData>` written by a typed producer decodes cleanly as
    /// `Job<OpaqueBytes>`, with the inner payload bytes recoverable as
    /// the originally-encoded `UserData` msgpack representation.
    #[test]
    fn typed_job_decodes_as_opaque() {
        let original = UserData {
            name: "round-trip".into(),
            count: 7,
        };
        let typed_job = Job::new(original.clone());
        let envelope = rmp_serde::to_vec(&typed_job).expect("encode");

        let opaque: Job<OpaqueBytes> = rmp_serde::from_slice(&envelope).expect("decode opaque");
        assert_eq!(opaque.id, typed_job.id);
        assert_eq!(opaque.attempt, 0);

        // The inner payload bytes are the msgpack-encoded UserData.
        let recovered: UserData =
            rmp_serde::from_slice(opaque.payload.0.as_ref()).expect("decode user data");
        assert_eq!(recovered, original);
    }

    /// A `Job<OpaqueBytes(buf)>` round-trips through msgpack with the
    /// inner bytes preserved verbatim — the property the shim layers
    /// depend on.
    #[test]
    fn opaque_round_trip_preserves_inner() {
        let inner: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        let job = Job::new(OpaqueBytes(Bytes::copy_from_slice(&inner)));
        let envelope = rmp_serde::to_vec(&job).expect("encode");
        let decoded: Job<OpaqueBytes> = rmp_serde::from_slice(&envelope).expect("decode");
        assert_eq!(decoded.payload.0.as_ref(), inner.as_slice());
    }
}
