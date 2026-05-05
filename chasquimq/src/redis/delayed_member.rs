//! Encoding helpers for the delayed-jobs ZSET member format.
//!
//! The ZSET member is a separate wire surface from the stream-entry shape:
//! the consumer hot path reads `Job<T>` out of an `XADD`'s `d` field and a
//! parallel `n` stream-entry field, but the delayed path's only handle is
//! the ZSET member bytes. To preserve `Job::name` across the delayed path
//! (producer-supplied delayed adds, retry-via-delayed-ZSET, and repeatable
//! scheduler fires) the member is length-prefixed:
//!
//! ```text
//! [4 bytes: name length, little-endian u32]
//! [N bytes: name as UTF-8]
//! [M bytes: msgpack-encoded Job<T> payload]
//! ```
//!
//! When `name` is empty the prefix is `[0u32 LE]` followed directly by the
//! msgpack bytes. The promoter splits the prefix off and re-emits the name
//! as the stream entry's `n` field, mirroring the immediate-XADD path.
//!
//! BREAKING: the legacy delayed-ZSET shape (raw msgpack with no prefix) is
//! not parseable under this format. Pre-1.0 — drain delayed jobs (or
//! FLUSHDB) before deploy.

use bytes::Bytes;

/// Encode a delayed-ZSET member: 4-byte LE name length + name UTF-8 + payload.
/// `name` may be empty (encoded as `[0u32 LE] || payload`).
pub(crate) fn encode_delayed_member(name: &str, payload: &[u8]) -> Bytes {
    let name_bytes = name.as_bytes();
    let mut out = Vec::with_capacity(4 + name_bytes.len() + payload.len());
    out.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(name_bytes);
    out.extend_from_slice(payload);
    Bytes::from(out)
}

/// Decode a delayed-ZSET member produced by [`encode_delayed_member`]. Returns
/// `None` when `member` is too short to carry the prefix or the declared name
/// length runs past the buffer — both of which indicate either a pre-slice-3
/// member that escaped the migration or outright corruption. The caller logs
/// and skips in either case.
#[allow(dead_code)]
pub(crate) fn decode_delayed_member(member: &[u8]) -> Option<(String, &[u8])> {
    if member.len() < 4 {
        return None;
    }
    let name_len = u32::from_le_bytes([member[0], member[1], member[2], member[3]]) as usize;
    if member.len() < 4 + name_len {
        return None;
    }
    let name = std::str::from_utf8(&member[4..4 + name_len])
        .ok()?
        .to_string();
    let payload = &member[4 + name_len..];
    Some((name, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_decode_round_trips_named() {
        let payload = b"\x91\x01"; // msgpack array of one
        let member = encode_delayed_member("send-email", payload);
        let (name, rest) = decode_delayed_member(&member).expect("decode");
        assert_eq!(name, "send-email");
        assert_eq!(rest, payload);
    }

    #[test]
    fn encode_then_decode_round_trips_empty_name() {
        let payload = b"opaque-body";
        let member = encode_delayed_member("", payload);
        let (name, rest) = decode_delayed_member(&member).expect("decode");
        assert_eq!(name, "");
        assert_eq!(rest, payload);
        // Empty name must encode as 4 LE-zero bytes + payload, byte-for-byte.
        assert_eq!(&member[..4], &[0u8, 0, 0, 0]);
        assert_eq!(&member[4..], payload);
    }

    #[test]
    fn encode_layout_is_le_u32_prefix_plus_name_plus_payload() {
        let member = encode_delayed_member("ab", b"XY");
        assert_eq!(&member[..4], &[2u8, 0, 0, 0]);
        assert_eq!(&member[4..6], b"ab");
        assert_eq!(&member[6..], b"XY");
    }

    #[test]
    fn decode_short_input_returns_none() {
        assert!(decode_delayed_member(b"").is_none());
        assert!(decode_delayed_member(b"\x00\x00\x00").is_none());
    }

    #[test]
    fn decode_truncated_name_returns_none() {
        // name length claims 10 bytes but only 2 follow.
        let bad = [10u8, 0, 0, 0, b'a', b'b'];
        assert!(decode_delayed_member(&bad).is_none());
    }

    #[test]
    fn decode_legacy_msgpack_member_returns_none() {
        // Pre-slice-3 ZSET members were raw msgpack starting with a fixarray
        // marker (0x91..0x9F). The prefix decode reads the first 4 bytes as
        // a name length — for a small msgpack envelope the resulting "name
        // length" is much larger than the available buffer, so decode
        // returns None. Pin that the legacy path doesn't accidentally
        // succeed and produce garbage names.
        let legacy: &[u8] = &[0x95, 0xa3, b'x', b'y', b'z']; // 5-array, 3-string "xyz", ...
        assert!(decode_delayed_member(legacy).is_none());
    }

    #[test]
    fn decode_unicode_name_round_trips() {
        let member = encode_delayed_member("発送-email", b"body");
        let (name, rest) = decode_delayed_member(&member).expect("decode");
        assert_eq!(name, "発送-email");
        assert_eq!(rest, b"body");
    }
}
