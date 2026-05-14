//! `JsCredentialProvider` — bridges a JS async callback to fred's
//! `CredentialProvider` trait so users can plug a rotating-token auth
//! source (ElastiCache IAM, Vault, Secrets Manager, ...) into the engine's
//! `Producer` / `Consumer` / `Promoter` / `Scheduler`.
//!
//! Shape on the JS side (the high-level shim sets this on
//! `ConnectionOptions.credentialProvider`):
//!
//! ```ts
//! credentialProvider?: (
//!   host: string | null,
//! ) => Promise<{ username?: string; password?: string }>
//! ```
//!
//! The native binding accepts a `ThreadsafeFunction<Option<String>,
//! ErrorStrategy::Fatal>` — mirroring the consumer's handler shape so the
//! JS callback receives the host arg directly rather than the
//! Node-style `(err, host)`. The callback is invoked off the engine's
//! tokio runtime, on the libuv thread, and resolves to a
//! `Promise<CredentialResponseJs>` whose `.await` napi schedules back
//! onto the tokio side.
//!
//! Errors map to `fred::error::Error { kind: Auth, .. }` so fred's
//! reconnect-on-auth-error policy treats them as transient and re-fetches
//! on the next reconnect attempt. The TSFN-level call_async failure and
//! the JS-promise-rejection path are distinguished only in the embedded
//! error message — both surface to fred identically.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use fred::error::{Error as FredError, ErrorKind as FredErrorKind};
use fred::types::config::{CredentialProvider, Server};
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi_derive::napi;

/// Resolved shape returned by the JS `credentialProvider` callback.
///
/// Both fields are optional — `None` carries through to fred as "no
/// `AUTH` / `HELLO` arg supplied," matching Redis's positional-arg
/// semantics (omitted username keeps the default `default` user).
#[napi(object)]
pub struct CredentialResponseJs {
    pub username: Option<String>,
    pub password: Option<String>,
}

/// `ThreadsafeFunction` flavor accepted by the producer / consumer / promoter
/// / scheduler factory functions. Fatal-strategy because the JS callback
/// shape is `(host) => Promise<...>` — single arg in, single Promise out;
/// the default `CalleeHandled` strategy would prepend a `null` error and
/// break that signature. Aliased here so every napi factory in this crate
/// refers to one canonical type — keeps the cross-cutting parameter type
/// from drifting if napi-rs ever changes the const-generic syntax.
pub type CredentialProviderTsfn =
    ThreadsafeFunction<Option<String>, ErrorStrategy::Fatal>;

/// Wraps a JS-side TSFN as something fred can call on every reconnect /
/// `AUTH` cycle. The TSFN handle is reference-counted; cloning is cheap
/// (napi's internal `Arc`), so we keep one per provider instance.
pub struct JsCredentialProvider {
    tsfn: CredentialProviderTsfn,
}

impl fmt::Debug for JsCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The TSFN handle is opaque; emit a stable string so the engine's
        // `Debug` impl for `ConnectionTuning` stays useful in logs.
        f.write_str("JsCredentialProvider { <js callback> }")
    }
}

#[async_trait]
impl CredentialProvider for JsCredentialProvider {
    async fn fetch(
        &self,
        server: Option<&Server>,
    ) -> Result<(Option<String>, Option<String>), FredError> {
        let host = server.map(|s| format!("{}:{}", s.host, s.port));

        // Two await points: (1) call_async resolves once napi has posted
        // the call to the libuv thread and the JS callback returned a
        // Promise; (2) Promise::await resolves once that Promise settles
        // (resolve / reject) on the JS side. A failure at either point
        // maps to `FredErrorKind::Auth` so fred's
        // `reconnect_on_auth_error` policy retries on the next attempt.
        let promise = self
            .tsfn
            .call_async::<Promise<CredentialResponseJs>>(host)
            .await
            .map_err(|e| {
                FredError::new(
                    FredErrorKind::Auth,
                    format!("credentialProvider TSFN call failed: {e}"),
                )
            })?;
        let resp = promise.await.map_err(|e| {
            FredError::new(
                FredErrorKind::Auth,
                format!("credentialProvider promise rejected: {e}"),
            )
        })?;

        Ok((resp.username, resp.password))
    }
}

/// Wrap an inbound TSFN as an `Arc<dyn CredentialProvider>` suitable for
/// stashing on `ConnectionTuning::credential_provider`. Called from each
/// of the four native factory entry points (producer / consumer / promoter
/// / scheduler) so the `Option<TSFN> -> Option<Arc<...>>` conversion lives
/// in exactly one place.
pub fn build_js_credential_provider(
    tsfn: CredentialProviderTsfn,
) -> Arc<dyn CredentialProvider> {
    Arc::new(JsCredentialProvider { tsfn })
}
