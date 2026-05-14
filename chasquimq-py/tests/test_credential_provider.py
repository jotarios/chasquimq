"""Integration tests for the ``credential_provider`` kwarg.

The kwarg surfaces fred's `CredentialProvider` trait as a Python async
callable. We can't replay an ElastiCache IAM token rotation locally, so
the strategy is:

* Build a stub callback that returns the standard `(None, None)` shape
  loopback Redis accepts (no AUTH required) and assert it is **invoked**
  at least once during initial connect. Counts the loop hand-off.
* Assert that a callback raising an exception surfaces as a
  connect-time error (fred raises an `Auth` error on the first
  handshake; the engine's `Producer::connect` propagates it).
* Assert the high-level `Queue` shim forwards the kwarg through to the
  native `Producer` (deferred-construction path).
* Assert the native `Consumer` and `Scheduler` accept the kwarg without
  raising when constructed inside a running loop (smoke).
"""

from __future__ import annotations

import asyncio
import os
from typing import Optional, Tuple

import pytest

from chasquimq import Consumer, Producer, Queue, Scheduler


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


def _make_counter() -> tuple[list[Optional[str]], "Callback"]:
    """Return a (calls, callable) pair. ``calls`` accumulates one entry
    per invocation — captures the ``host:port`` argument fred passes."""
    calls: list[Optional[str]] = []

    async def callback(host: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        calls.append(host)
        # Loopback Redis has no AUTH requirement, so returning the
        # "no credentials" sentinel is accepted and lets the handshake
        # complete normally — fred's HELLO command sends an empty AUTH
        # payload, which Redis interprets as "no AUTH this connection".
        return (None, None)

    return calls, callback


@pytest.mark.asyncio
async def test_native_producer_invokes_credential_provider_on_first_call(
    redis_url: str, queue_name: str
) -> None:
    """When a ``credential_provider`` is supplied, construction is
    deferred (see :class:`Producer.ProducerState::Deferred` in
    ``producer.rs``) — the first awaited method triggers connect and,
    in turn, the AUTH/HELLO callback. By the time ``add`` resolves the
    callback must have been invoked at least once."""
    calls, callback = _make_counter()

    producer = Producer(redis_url, queue_name, credential_provider=callback)
    # Sync accessors work pre-connect (key shapes are deterministic
    # from the queue name).
    assert producer.stream_key().endswith(":stream")
    # No calls yet — connect is deferred.
    assert len(calls) == 0

    # Triggering the first awaited method connects the pool, which
    # runs AUTH/HELLO on each connection.
    await producer.add(b"\x80")  # empty msgpack map
    assert len(calls) >= 1, (
        "credential_provider was never called; expected at least one "
        f"invocation during first connect. Calls so far: {calls!r}"
    )
    # All entries should be either ``None`` (fred couldn't resolve a
    # specific endpoint) or a ``"host:port"`` string.
    for host in calls:
        assert host is None or (isinstance(host, str) and ":" in host), (
            f"unexpected host arg shape: {host!r}"
        )

    await producer.shutdown()


@pytest.mark.asyncio
async def test_high_level_queue_threads_credential_provider(
    redis_url: str, queue_name: str
) -> None:
    """The high-level ``Queue`` defers producer construction to first
    use. The kwarg must reach the native Producer when ``add`` (or any
    method that triggers ``_get_producer``) is awaited."""
    calls, callback = _make_counter()

    async with Queue(
        queue_name, redis_url=redis_url, credential_provider=callback
    ) as queue:
        # Trigger the lazy producer construction by enqueuing a job.
        await queue.add("hello", {"to": "ada"})
        assert len(calls) >= 1


@pytest.mark.asyncio
async def test_credential_provider_failure_propagates_as_connect_error(
    redis_url: str, queue_name: str
) -> None:
    """A callback that raises must cause the first awaited method to
    fail with an auth-flavoured error instead of silently hanging or
    succeeding without authentication."""

    async def boom(host: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        raise RuntimeError("simulated IAM token fetch failure")

    producer = Producer(redis_url, queue_name, credential_provider=boom)

    with pytest.raises(Exception) as excinfo:
        await producer.add(b"\x80")

    # fred wraps the auth failure; the engine surfaces it as a generic
    # connection error. The error message should reference auth or
    # surface the underlying RuntimeError text so the user can debug.
    msg = str(excinfo.value).lower()
    assert (
        "auth" in msg
        or "simulated iam" in msg
        or "credential" in msg
        or "connect" in msg
    ), f"expected auth/credential-flavoured error, got: {excinfo.value!r}"


@pytest.mark.asyncio
async def test_credential_provider_wrong_return_shape_surfaces(
    redis_url: str, queue_name: str
) -> None:
    """A callback that returns something other than ``tuple[Optional[str],
    Optional[str]]`` must surface as an Auth/connect error rather than
    panicking the binding."""

    async def wrong_shape(host: Optional[str]) -> str:  # type: ignore[override]
        # Intentionally wrong: returning a plain string instead of a
        # 2-tuple. The native bridge should map this to a fred Auth
        # error during the handshake.
        return "not-a-tuple"

    producer = Producer(redis_url, queue_name, credential_provider=wrong_shape)

    with pytest.raises(Exception) as excinfo:
        await producer.add(b"\x80")

    msg = str(excinfo.value).lower()
    assert (
        "credential_provider" in msg
        or "tuple" in msg
        or "auth" in msg
        or "connect" in msg
    ), f"expected tuple-shape / auth error, got: {excinfo.value!r}"


@pytest.mark.asyncio
async def test_native_consumer_accepts_credential_provider_kwarg(
    redis_url: str, queue_name: str
) -> None:
    """``Consumer.__init__`` only captures the asyncio loop — it does
    not connect until ``run()``. Smoke: constructing inside an asyncio
    test with the kwarg should not raise. Calling ``shutdown()`` before
    ``run()`` is a no-op."""
    _, callback = _make_counter()
    consumer = Consumer(
        redis_url,
        queue_name,
        concurrency=1,
        credential_provider=callback,
    )
    consumer.shutdown()


@pytest.mark.asyncio
async def test_native_scheduler_accepts_credential_provider_kwarg(
    redis_url: str, queue_name: str
) -> None:
    _, callback = _make_counter()
    scheduler = Scheduler(redis_url, queue_name, credential_provider=callback)
    scheduler.shutdown()


def test_native_consumer_credential_provider_requires_running_loop(
    redis_url: str, queue_name: str
) -> None:
    """Constructing the native Consumer with a credential_provider from
    a sync (non-asyncio) context must fail — the binding can't capture
    the running loop fred will dispatch the callback back to. Surfaces
    as ``RuntimeError`` ("no running event loop")."""

    async def cb(_: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        return (None, None)

    with pytest.raises(RuntimeError) as excinfo:
        Consumer(redis_url, queue_name, credential_provider=cb)

    assert "running" in str(excinfo.value).lower() or "loop" in str(
        excinfo.value
    ).lower()
