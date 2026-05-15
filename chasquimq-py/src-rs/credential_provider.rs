//! `PyCredentialProvider` — bridge from fred's `CredentialProvider` trait
//! to a user-supplied Python `async def fetch_credentials(host: str | None)
//! -> tuple[str | None, str | None]` coroutine.
//!
//! Use case: short-lived auth tokens — most notably AWS ElastiCache IAM
//! tokens, which expire roughly every 15 minutes. `fred` calls
//! [`fetch`](fred::types::config::CredentialProvider::fetch) before every
//! `AUTH` / `HELLO` (initial connect + every reconnect), so a long-lived
//! pool stays authenticated through token rotation without rebuilding.
//!
//! ## TaskLocals capture
//!
//! fred drives this callback from inside its background router task on
//! tokio worker threads — those threads have no associated asyncio loop.
//! We therefore capture the user's running asyncio loop and contextvars
//! at construction time (`Producer.__init__` / `Consumer.__init__` /
//! `Scheduler.__init__`) via [`TaskLocals::with_running_loop`] +
//! [`TaskLocals::copy_context`], store the locals on the provider, and
//! hand each `fetch()` call back to *that* loop via
//! [`pyo3_async_runtimes::into_future_with_locals`]. Without this hop,
//! `asyncio.get_running_loop()` would fail inside the awaited coroutine
//! and the future would be dropped before completion — same trap the
//! consumer's per-job handler dispatch avoids.
//!
//! ## Error mapping
//!
//! Any failure path (call-time `PyErr`, awaited coroutine raises, return
//! value isn't a 2-tuple of `Optional[str]`) maps to
//! [`FredError::new(ErrorKind::Auth, ...)`]. Combined with the engine's
//! default `reconnect_on_auth_error = true`, this lets fred retry the
//! handshake on the next reconnect — the user gets one shot to surface a
//! fresh token per AUTH/HELLO attempt.

use std::fmt;

use async_trait::async_trait;
use fred::error::{Error as FredError, ErrorKind as FredErrorKind};
use fred::types::config::{CredentialProvider, Server};
use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;

pub struct PyCredentialProvider {
    callable: Py<PyAny>,
    locals: TaskLocals,
}

impl PyCredentialProvider {
    /// Construct from a Python callable and the currently-running asyncio
    /// loop. Fails with `PyRuntimeError("no running event loop")` (raised
    /// by `asyncio.get_running_loop()`) when called outside an asyncio
    /// context — match the per-job handler dispatch behaviour in
    /// `Consumer::run`. Callers should construct inside `asyncio.run(...)`
    /// or an equivalent loop.
    pub fn new(py: Python<'_>, callable: Py<PyAny>) -> PyResult<Self> {
        let locals = TaskLocals::with_running_loop(py)?.copy_context(py)?;
        Ok(Self { callable, locals })
    }
}

impl fmt::Debug for PyCredentialProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PyCredentialProvider { <py async callable> }")
    }
}

#[async_trait]
impl CredentialProvider for PyCredentialProvider {
    async fn fetch(
        &self,
        server: Option<&Server>,
    ) -> Result<(Option<String>, Option<String>), FredError> {
        // Shape the host argument as `Option<"host:port">` — symmetric to
        // what the Node shim does and simpler than constructing a Python
        // dict every reconnect. The user can split on `:` if they need
        // host / port separately.
        let host = server.map(|s| format!("{}:{}", s.host, s.port));
        let locals = self.locals.clone();

        // Step 1: call into Python under the GIL to obtain the coroutine,
        // then convert it to a rust future. The future itself is `Send`
        // and can be awaited on any tokio worker.
        let fut = Python::attach(|py| -> PyResult<_> {
            let coro = self.callable.bind(py).call1((host,))?;
            pyo3_async_runtimes::into_future_with_locals(&locals, coro)
        })
        .map_err(|e| {
            FredError::new(
                FredErrorKind::Auth,
                format!("credential_provider call failed: {e}"),
            )
        })?;

        // Step 2: await the coroutine. The Python loop runs it; fred's
        // router task parks on the oneshot receiver until it resolves.
        let result = fut.await.map_err(|e| {
            FredError::new(
                FredErrorKind::Auth,
                format!("credential_provider await failed: {e}"),
            )
        })?;

        // Step 3: extract the result. Accept any 2-tuple shape that
        // coerces to `(Option<String>, Option<String>)` — i.e. plain
        // strings, `None`, or a mix. Anything else (a single value, a
        // dict, a 3-tuple, non-string entries) raises an extract-time
        // error which we map to `Auth` so fred surfaces it on the next
        // handshake attempt.
        Python::attach(|py| result.extract::<(Option<String>, Option<String>)>(py)).map_err(|e| {
            FredError::new(
                FredErrorKind::Auth,
                format!("credential_provider must return tuple[str | None, str | None]; got: {e}"),
            )
        })
    }
}
