//! Opaque-bytes payload type for the Python / Rust seam — mirror of
//! `chasquimq-node`'s `RawBytes`. Kept duplicated rather than re-used from
//! the Node crate because the two FFI surfaces are otherwise independent
//! and we don't want a cross-crate dep just for a tiny newtype.

use bytes::Bytes;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawBytes(pub Bytes);

impl RawBytes {
    pub fn as_slice(&self) -> &[u8] {
        self.0.as_ref()
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
