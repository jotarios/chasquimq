#![deny(clippy::all)]
//! N-API bindings for ChasquiMQ. Exposes `NativeProducer`, `NativeConsumer`,
//! and `NativePromoter` to JS as the lower-level engine API. The
//! high-level `Queue` / `Worker` / `Job` shim sits *on top* of these classes
//! and lives in TypeScript — it is **not** part of this crate.
//!
//! See `docs/phase3-napi-design.md` for the load-bearing decisions:
//! - §4: opaque-Buffer payload (Option A)
//! - §5: TSFN-based handler dispatch
//! - §6: error mapping (rejection → retry → DLQ)

mod consumer;
mod payload;
mod producer;
mod promoter;
mod repeat;
mod scheduler;

pub use consumer::{NativeConsumer, NativeConsumerOpts, NativeJob, NativeRetryOpts};
pub use payload::RawBytes;
pub use producer::{NativeDlqEntry, NativeProducer, NativeProducerOpts};
pub use promoter::{NativePromoter, NativePromoterOpts};
pub use repeat::{
    NativeMissedFiresPolicy, NativeRepeatPattern, NativeRepeatableMeta, NativeRepeatableSpec,
};
pub use scheduler::{NativeScheduler, NativeSchedulerOpts};

use napi_derive::napi;

/// Returns the version of this binding crate. The npm package version
/// tracks this 1:1 (see `docs/phase3-napi-design.md` §7).
#[napi]
pub fn engine_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[cfg(test)]
mod tests {
    //! Wire-format invariant tests.
    //!
    //! These are the load-bearing tests for the high-level shim that
    //! will sit on top of this binding. If any of these fail, **do not**
    //! ship — the upper layer's msgpack-on-JS-side encoding will not
    //! survive the round trip.

    use super::payload::RawBytes;
    use bytes::Bytes;
    use chasquimq::Job;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct UserData {
        name: String,
        count: u32,
    }

    /// **The wire-format invariant.**
    ///
    /// The JS high-level shim will:
    /// 1. msgpack-encode `UserData` to a `Buffer` via `@msgpack/msgpack`
    /// 2. hand the `Buffer` to `NativeProducer.add(buf)`
    /// 3. the binding wraps it as `Job<RawBytes(buf)>` and msgpack-encodes
    ///    that whole envelope before XADD'ing.
    ///
    /// On the consume side, the engine decodes back to `Job<RawBytes>`
    /// and JS receives `payload: Buffer` — which must be the **exact
    /// same bytes** the producer originally handed in, so JS can
    /// `@msgpack/msgpack`.decode` them back into `UserData`.
    ///
    /// This test simulates that whole loop.
    #[test]
    fn raw_bytes_round_trip_preserves_inner_msgpack() {
        let original = UserData {
            name: "round-trip".into(),
            count: 7,
        };

        // Step 1: JS-side msgpack-encode the user data.
        let inner_bytes = rmp_serde::to_vec(&original).expect("encode user data");

        // Step 2: Native producer wraps as Job<RawBytes(buf)> and encodes.
        let job = Job::new(RawBytes(Bytes::copy_from_slice(&inner_bytes)));
        let job_id = job.id.clone();
        let job_attempt = job.attempt;
        let job_created = job.created_at_ms;
        let envelope = rmp_serde::to_vec(&job).expect("encode job envelope");

        // Step 3: Engine decodes back to Job<RawBytes>. The inner Buffer
        // bytes the JS consumer sees must equal `inner_bytes`.
        let decoded: Job<RawBytes> = rmp_serde::from_slice(&envelope).expect("decode job envelope");
        assert_eq!(decoded.id, job_id);
        assert_eq!(decoded.attempt, job_attempt);
        assert_eq!(decoded.created_at_ms, job_created);
        assert_eq!(
            decoded.payload.as_slice(),
            inner_bytes.as_slice(),
            "RawBytes did not preserve the inner msgpack bytes verbatim"
        );

        // Step 4: JS-side @msgpack/msgpack.decode recovers UserData.
        let recovered: UserData =
            rmp_serde::from_slice(decoded.payload.as_slice()).expect("decode inner");
        assert_eq!(recovered, original);
    }

    /// A `RawBytes(empty_buf)` must survive the round trip too — the
    /// high-level shim may send empty payloads (jobs with no data).
    #[test]
    fn raw_bytes_round_trip_empty_buffer() {
        let job = Job::new(RawBytes(Bytes::new()));
        let envelope = rmp_serde::to_vec(&job).expect("encode");
        let decoded: Job<RawBytes> = rmp_serde::from_slice(&envelope).expect("decode");
        assert!(decoded.payload.as_slice().is_empty());
    }

    /// A binary-blob payload (image bytes, protobuf, etc.) — i.e. a JS
    /// Buffer that is *not* msgpack-encoded — must also survive verbatim.
    /// The binding doesn't care what's inside the buffer; that's the
    /// whole point of the opaque-Buffer choice.
    #[test]
    fn raw_bytes_round_trip_arbitrary_bytes() {
        let raw: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        let job = Job::new(RawBytes(Bytes::copy_from_slice(&raw)));
        let envelope = rmp_serde::to_vec(&job).expect("encode");
        let decoded: Job<RawBytes> = rmp_serde::from_slice(&envelope).expect("decode");
        assert_eq!(decoded.payload.as_slice(), raw.as_slice());
    }
}
