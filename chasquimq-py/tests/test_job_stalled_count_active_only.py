"""``Job.stalled_count`` is populated by the introspector only on the
Active state branch.

The engine ships ``JobInfo.stalled_count`` as an Active-only probe
(see ``chasquimq/src/introspect.rs``). This test pins the Python shim's
end-to-end plumbing:

 1. A Job synthesized from ``Queue.get_job`` of a freshly-added job
    (Waiting state, no stall counter exists yet) has ``stalled_count is None``.
 2. After the stalled-job detector has INCR'd the counter for an
    Active (hung) job, ``Queue.get_job`` surfaces the integer count.

The end-to-end DLQ relocate at ``max_stalled_attempts`` is covered by
the engine integration tests; here we only verify the field flows
across the FFI.
"""

from __future__ import annotations

import asyncio

import pytest

from chasquimq.queue import Queue
from chasquimq.worker import Worker


pytestmark = pytest.mark.asyncio


async def test_stalled_count_none_for_waiting_job(redis_url, queue_name, cleanup_keys):
    """A freshly-added job that has never been picked up by a worker
    sits in the Waiting state with no stall counter — ``get_job`` must
    surface ``stalled_count is None``, not zero (zero would mislead a
    caller into thinking the detector has run and observed it).
    """
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        added = await queue.add("waiting-one", {"value": 1})
        fetched = await queue.get_job(added.id)
        assert fetched is not None
        assert fetched.stalled_count is None
    finally:
        await queue.close()


async def test_stalled_count_surfaces_for_active_hung_job(
    redis_url, queue_name, cleanup_keys
):
    """A handler that hangs past ``idle_threshold_ms`` lets the
    embedded stalled detector INCR the counter — ``get_job`` on that
    Active job must surface a positive integer ``stalled_count``.

    We hold the handler hostage with ``asyncio.Future`` so the entry
    stays in the consumer group's PEL while the detector ticks. The
    very-low ``claim_min_idle_ms`` (and therefore detector tick + idle
    threshold, since the embedded spawn inherits both) lets the
    detector fire quickly. ``max_stalled_attempts=10`` keeps the
    threshold well above what we'll observe so the entry never gets
    DLQ-relocated mid-test.
    """
    held = asyncio.Future()

    async def hang(job):
        await held

    worker = Worker(
        queue_name,
        hang,
        redis_url=redis_url,
        concurrency=1,
        # Drive the detector fast: claim_min_idle_ms = 500ms means the
        # embedded detector ticks every 500ms and considers entries
        # idle past 500ms as stall candidates.
        claim_min_idle_ms=500,
        # High ceiling so the detector INCRs the counter without
        # relocating the job to the DLQ before we read it back.
        max_stalled_attempts=10,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = asyncio.create_task(worker.run())

    try:
        added = await queue.add("hung-one", {"value": 1})
        # Wait for the detector to INCR at least once. With a 500ms
        # tick we expect this inside ~2s; pad generously for CI.
        deadline = asyncio.get_event_loop().time() + 20
        observed = None
        while asyncio.get_event_loop().time() < deadline:
            fetched = await queue.get_job(added.id)
            if (
                fetched is not None
                and fetched.stalled_count is not None
                and fetched.stalled_count >= 1
            ):
                observed = fetched.stalled_count
                break
            await asyncio.sleep(0.1)
        assert observed is not None and observed >= 1, (
            f"expected stalled_count >= 1 on Active hung job; saw {observed!r}"
        )
    finally:
        if not held.done():
            held.set_exception(asyncio.CancelledError())
        await queue.close()
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5)
        except (asyncio.TimeoutError, BaseException):
            pass
