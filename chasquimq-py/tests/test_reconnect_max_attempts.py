"""Tests for the ``reconnect_max_attempts`` kwarg.

The kwarg surfaces the engine's ``ConnectionTuning::reconnect_max_attempts``
field. A positive value caps fred's exponential reconnect loop so a
permanently rejecting ``credential_provider`` gives up instead of
looping forever; ``0`` / ``None`` (the engine default) keeps the
unbounded behaviour.

We can't drive fred's reconnect loop from loopback Redis without a
flapping server, so — mirroring ``test_credential_provider.py`` — these
are wiring/acceptance tests:

* The native ``Producer`` / ``Consumer`` constructors accept the kwarg.
* The high-level ``Queue`` / ``Worker`` thread it through and the happy
  path still works.
* ``0`` and ``None`` are both accepted (the unbounded default).
* The kwarg combines with a ``credential_provider`` without raising.
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

import pytest

from chasquimq import Consumer, Producer, Queue


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_native_producer_accepts_reconnect_max_attempts(
    redis_url: str, queue_name: str
) -> None:
    """A positive cap is accepted by the native Producer and the
    produce path still works (no credential_provider → eager connect)."""
    producer = Producer(redis_url, queue_name, reconnect_max_attempts=5)
    assert producer.stream_key().endswith(":stream")
    await producer.add(b"\x80")  # empty msgpack map
    await producer.shutdown()


@pytest.mark.asyncio
async def test_native_producer_accepts_zero_unbounded(
    redis_url: str, queue_name: str
) -> None:
    """``0`` (the engine default = unbounded) is a legal explicit value,
    not rejected like an out-of-range arg."""
    producer = Producer(redis_url, queue_name, reconnect_max_attempts=0)
    await producer.add(b"\x80")
    await producer.shutdown()


@pytest.mark.asyncio
async def test_high_level_queue_threads_reconnect_max_attempts(
    redis_url: str, queue_name: str
) -> None:
    """The high-level ``Queue`` must forward the kwarg through to the
    native Producer. Enqueuing a job triggers lazy construction +
    connect; if the kwarg were mis-threaded the native constructor
    would raise here."""
    async with Queue(
        queue_name, redis_url=redis_url, reconnect_max_attempts=10
    ) as queue:
        await queue.add("hello", {"to": "ada"})


@pytest.mark.asyncio
async def test_high_level_queue_none_is_default(
    redis_url: str, queue_name: str
) -> None:
    """``None`` (the default) must behave exactly as if the kwarg were
    never passed — unbounded reconnect, no error."""
    async with Queue(
        queue_name, redis_url=redis_url, reconnect_max_attempts=None
    ) as queue:
        await queue.add("hello", {"to": "ada"})


@pytest.mark.asyncio
async def test_native_consumer_accepts_reconnect_max_attempts(
    redis_url: str, queue_name: str
) -> None:
    """``Consumer.__init__`` does not connect until ``run()``. Smoke:
    constructing with the kwarg must not raise; ``shutdown()`` before
    ``run()`` is a no-op."""
    consumer = Consumer(
        redis_url,
        queue_name,
        concurrency=1,
        reconnect_max_attempts=3,
    )
    consumer.shutdown()


@pytest.mark.asyncio
async def test_reconnect_max_attempts_combines_with_credential_provider(
    redis_url: str, queue_name: str
) -> None:
    """The two connection knobs must be independently settable on the
    same Producer. With a credential_provider the producer takes the
    deferred-construction path; reconnect_max_attempts is applied to
    cfg.connection *before* that branch, so it must land there too."""

    async def cb(host: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        # Loopback Redis has no AUTH requirement; the no-credentials
        # sentinel lets the handshake complete normally.
        return (None, None)

    producer = Producer(
        redis_url,
        queue_name,
        reconnect_max_attempts=4,
        credential_provider=cb,
    )
    # Deferred path: first awaited method connects.
    await producer.add(b"\x80")
    await producer.shutdown()
