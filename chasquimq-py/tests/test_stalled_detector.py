"""Cross-FFI tests for the slice-12 stalled-job detector + Python
wiring of ``Worker(max_stalled_attempts=...)``.

The hung-handler end-to-end relocate scenario lives in the engine
integration tests (``chasquimq/tests/stalled_detection.rs``); this
file pins the Python-shim invariants:
 1. ``worker.on('stalled', cb)`` lazily spawns the internal
    :class:`QueueEvents` subscriber and routes the event payload
    through.
 2. ``Worker(max_stalled_attempts=N)`` routes to engine
    ``max_stalled_attempts`` (slice-12). Pre-slice the Python shim
    had no equivalent field — this test pins the new wiring.
 3. ``max_stalled_attempts`` does NOT short-circuit total handler
    attempts (which live under ``max_attempts``). A handler that
    always fails respects ``max_attempts``, not
    ``max_stalled_attempts``.
"""
from __future__ import annotations

import asyncio

import pytest

from chasquimq.queue import Queue
from chasquimq.worker import Worker


pytestmark = pytest.mark.asyncio


async def test_max_stalled_attempts_routes_to_stalled_detector(
    redis_url, queue_name, cleanup_keys
):
    """Regression: ``max_stalled_attempts`` must control the stalled-
    detector ceiling, NOT total handler attempts. An always-failing
    (non-hanging) handler should respect ``max_attempts``, not
    ``max_stalled_attempts``.

    Pre-slice the Python shim did not have a ``max_stalled_attempts``
    field at all; this test pins the new wiring routes to the
    correct engine field.
    """
    attempts = 0

    async def handler(job):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("always-fail")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        # Pre-slice this would have been the only retry-cap knob.
        # Post-slice it goes to the stalled-detector (which a
        # fail-throwing handler never trips). The bound on attempts
        # comes from `max_attempts` below.
        max_stalled_attempts=2,
        max_attempts=5,
        read_block_ms=100,
    )
    worker.on("error", lambda _: None)
    worker.on("failed", lambda *_a: None)
    run_task = asyncio.create_task(worker.run())

    try:
        queue = Queue(queue_name, redis_url=redis_url)
        try:
            await queue.add("regular", {"value": 1})
            # Wait for max_attempts (5) handler invocations.
            deadline = asyncio.get_event_loop().time() + 30
            while attempts < 5 and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.05)
            assert attempts >= 5, f"expected >=5, saw {attempts}"
            # Bounded by max_attempts + minor CLAIM jitter.
            assert attempts <= 8
        finally:
            await queue.close()
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5)
        except (asyncio.TimeoutError, BaseException):
            pass


async def test_stalled_listener_wires_up_lazily(
    redis_url, queue_name, cleanup_keys
):
    """``Worker.on('stalled', cb)`` must lazily spawn the internal
    QueueEvents subscriber, mirroring the ``drained`` / ``progress``
    shape. Sanity: the worker still processes normal jobs after the
    listener attaches.
    """
    seen = []

    def on_stalled(job_id: str, prev: str) -> None:
        seen.append((job_id, prev))

    async def handler(job):
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=100,
    )
    # Attaching `stalled` listener must not raise and must construct
    # the internal subscriber lazily.
    worker.on("stalled", on_stalled)
    # Private attribute peek: the subscriber must now exist (lazy
    # spawn at first ``on`` call).
    assert worker._internal_events is not None  # type: ignore[attr-defined]

    run_task = asyncio.create_task(worker.run())
    try:
        queue = Queue(queue_name, redis_url=redis_url)
        try:
            await queue.add("ok", {"value": 1})
            # Give the handler a beat to process; we're only proving
            # the wiring doesn't crash on normal jobs.
            await asyncio.sleep(0.5)
        finally:
            await queue.close()
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5)
        except (asyncio.TimeoutError, BaseException):
            pass


async def test_stalled_detector_disabled_via_constructor(
    redis_url, queue_name, cleanup_keys, redis_client
):
    """``Worker(stalled_detector_enabled=False)`` must skip the
    embedded detector spawn — pinned by checking that the detector
    lock key never appears in Redis even after the worker has been
    running.
    """

    async def handler(job):
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=100,
        stalled_detector_enabled=False,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        queue = Queue(queue_name, redis_url=redis_url)
        try:
            await queue.add("ok", {"value": 1})
            # Process a job to make sure the worker is up.
            await asyncio.sleep(0.5)
            # Detector lock key must never appear when disabled.
            lock_exists = await redis_client.exists(
                f"{{chasqui:{queue_name}}}:stalled:lock"
            )
            assert lock_exists == 0, (
                f"detector lock key must not exist when disabled (got {lock_exists})"
            )
        finally:
            await queue.close()
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5)
        except (asyncio.TimeoutError, BaseException):
            pass
