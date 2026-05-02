//! Opaque-Buffer payload type for the JS / Rust seam.
//!
//! `RawBytes(Bytes)` serializes via `serialize_bytes` and deserializes via
//! `deserialize_bytes`, so a `Job<RawBytes>` written through `rmp-serde`
//! produces the wire shape:
//!
//! ```text
//! msgpack({ id, payload: bin(<bytes>), created_at_ms, attempt })
//! ```
//!
//! This is **Option A** of the Phase 3 design: the JS BullMQ-compat layer
//! msgpack-encodes its user data to a `Buffer`, hands the buffer to
//! `NativeProducer.add(buf)`, and the native binding embeds those bytes as
//! the `payload` slot of the `Job` envelope. The two wire-format invariants
//! that follow:
//!
//! 1. `RawBytes` round-trips byte-for-byte through `Job<RawBytes>` —
//!    encode and decode preserve the inner `Bytes` exactly.
//! 2. The encoded `Job<RawBytes>` is wire-compatible with the engine's
//!    `Producer<T>` for any `T: Serialize + DeserializeOwned` whose
//!    msgpack form is the same `bin` shape (in practice: any user type
//!    whose data the JS layer pre-encoded as msgpack and handed in as
//!    `Buffer`).
//!
//! See `lib.rs::tests::raw_bytes_round_trip` for the load-bearing test.

use bytes::Bytes;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Opaque payload bytes that survive a `Job<RawBytes>` round trip through
/// `rmp-serde` as a single msgpack `bin` value.
///
/// We don't reach for `serde_bytes::ByteBuf` because that pulls a separate
/// crate just for the impl, and the engine already uses `bytes::Bytes`
/// everywhere in the producer / DLQ paths — keeping the JS-facing payload
/// type as `Bytes` means the FFI boundary copy goes straight from the
/// engine's `Bytes` into a Node `Buffer` without an intermediate `Vec`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawBytes(pub Bytes);

impl RawBytes {
    pub fn new(b: Bytes) -> Self {
        Self(b)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl From<Bytes> for RawBytes {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<Vec<u8>> for RawBytes {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl Serialize for RawBytes {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // `serialize_bytes` produces a msgpack `bin` value via `rmp-serde` —
        // *not* an `array of u8`. Confirmed by the round-trip test in
        // `lib.rs::tests`.
        s.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for RawBytes {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_bytes(RawBytesVisitor)
    }
}

struct RawBytesVisitor;

impl<'de> Visitor<'de> for RawBytesVisitor {
    type Value = RawBytes;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a msgpack `bin` (or compatible byte buffer)")
    }

    fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<RawBytes, E> {
        Ok(RawBytes(Bytes::copy_from_slice(v)))
    }

    fn visit_byte_buf<E: DeError>(self, v: Vec<u8>) -> Result<RawBytes, E> {
        Ok(RawBytes(Bytes::from(v)))
    }

    fn visit_borrowed_bytes<E: DeError>(self, v: &'de [u8]) -> Result<RawBytes, E> {
        Ok(RawBytes(Bytes::copy_from_slice(v)))
    }

    // Tolerate the `seq of u8` shape that some serde-format combinations
    // produce for "bytes". msgpack-via-rmp doesn't hit this path for
    // `serialize_bytes`, but accepting it costs nothing and keeps us
    // robust if the wire shape ever drifts (e.g. a JSON-fallback test).
    fn visit_seq<A>(self, mut seq: A) -> Result<RawBytes, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut buf = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(b) = seq.next_element::<u8>()? {
            buf.push(b);
        }
        Ok(RawBytes(Bytes::from(buf)))
    }
}
