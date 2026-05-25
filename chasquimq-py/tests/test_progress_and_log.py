"""Integration tests for Job.update_progress / Job.log / Queue.get_job_logs.

Mirrors ``chasquimq-node/__test__/progress-and-log.test.ts``.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from chasquimq import Job, Queue, QueueEvents, Worker


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


async def _run_worker(worker: Worker) -> asyncio.Task[None]:
    """Spawn the worker's ``run()`` loop and return the task. Swallows
    cancellation so the test's ``finally`` cleanup is quiet.
    """

    async def _run() -> None:
        try:
            await worker.run()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    return asyncio.ensure_future(_run())


async def _wait_for(predicate, timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate() and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.025)
    if not predicate():
        raise AssertionError(f"predicate did not become true within {timeout}s")


@pytest.mark.asyncio
async def test_handler_update_progress_persists_and_get_job_reads_it_back(
    redis_url: str, queue_name: str
) -> None:
    """Handler ``await job.update_progress(50)`` →
    ``Queue.get_job(id).progress == 50``.
    """
    handler_done = asyncio.Event()

    async def handler(job: Job) -> None:
        await job.update_progress(50)
        handler_done.set()
        # Hold the handler so the job stays Active while we introspect —
        # a completed job with no stored result key disappears from the
        # introspector's view, so we'd race the ack vs the read.
        await asyncio.sleep(0.5)

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        added = await queue.add("progress-one", {"value": 1})
        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        fetched = await queue.get_job(added.id)
        assert fetched is not None
        assert fetched.progress == 50
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_queue_events_emits_per_id_progress_channel(
    redis_url: str, queue_name: str
) -> None:
    """``QueueEvents.on('progress:<id>', cb)`` receives the payload."""
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)

    handler_done = asyncio.Event()

    async def handler(job: Job) -> None:
        # Wait until the per-id subscriber is attached before emitting,
        # so the subscription window covers the engine's XADD.
        await asyncio.sleep(0.1)
        await job.update_progress(33)
        handler_done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)

    run_task = await _run_worker(worker)
    try:
        added = await queue.add("emits-progress", {"value": 1})
        targeted_calls: list[dict] = []
        broadcast_calls: list[dict] = []
        events.on(
            f"progress:{added.id}",
            lambda payload, eid: targeted_calls.append(payload),
        )
        events.on(
            "progress", lambda payload, eid: broadcast_calls.append(payload)
        )
        await events.wait_until_ready()

        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        await _wait_for(
            lambda: targeted_calls and broadcast_calls, timeout=10.0
        )
        payload = targeted_calls[0]
        assert payload["jobId"] == added.id
        assert payload["name"] == "emits-progress"
        assert payload["progress"] == 33
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_worker_progress_listener_fires_once_per_call(
    redis_url: str, queue_name: str
) -> None:
    """Worker EE ``progress`` listener fires once per
    ``update_progress`` call with ``(job, n)``.
    """
    handler_done = asyncio.Event()
    progress_calls: list[tuple[Job, int]] = []

    async def handler(job: Job) -> None:
        await job.update_progress(10)
        await job.update_progress(50)
        await job.update_progress(100)
        handler_done.set()
        # Keep the handler alive briefly so the events forwarder has
        # time to dispatch all three before the inflight entry drops.
        await asyncio.sleep(0.5)

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    worker.on(
        "progress", lambda job, n: progress_calls.append((job, n))
    )
    queue = Queue(queue_name, redis_url=redis_url)

    run_task = await _run_worker(worker)
    try:
        await queue.add("three-updates", {"value": 1})
        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        await _wait_for(lambda: len(progress_calls) >= 3, timeout=10.0)
        assert len(progress_calls) == 3
        assert [n for _, n in progress_calls] == [10, 50, 100]
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_job_log_appends_and_get_job_logs_reads_them_back(
    redis_url: str, queue_name: str
) -> None:
    """``await job.log("A"); await job.log("B"); Queue.get_job_logs(id)
    == (["A", "B"], 2)``.
    """
    handler_done = asyncio.Event()

    async def handler(job: Job) -> None:
        await job.log("A")
        await job.log("B")
        handler_done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        added = await queue.add("logs-two", {"value": 1})
        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        lines, count = await queue.get_job_logs(added.id)
        assert lines == ["A", "B"]
        assert count == 2
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_logs_pagination_returns_slice_in_order(
    redis_url: str, queue_name: str
) -> None:
    """10 lines, fetch ``start=2 end=4 asc=True`` → 3 lines in order."""
    handler_done = asyncio.Event()

    async def handler(job: Job) -> None:
        for i in range(10):
            await job.log(f"L{i}")
        handler_done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        added = await queue.add("logs-ten", {"value": 1})
        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        lines, count = await queue.get_job_logs(
            added.id, start=2, end=4, asc=True
        )
        assert lines == ["L2", "L3", "L4"]
        assert count == 10
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_logs_out_of_range_clamps_without_exception(
    redis_url: str, queue_name: str
) -> None:
    """An ``end`` beyond the current XLEN clamps to the tail; no
    exception raised, returned ``count`` is the real XLEN.
    """
    handler_done = asyncio.Event()

    async def handler(job: Job) -> None:
        for i in range(3):
            await job.log(f"L{i}")
        handler_done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        added = await queue.add("logs-clamp", {"value": 1})
        await asyncio.wait_for(handler_done.wait(), timeout=10.0)
        lines, count = await queue.get_job_logs(
            added.id, start=0, end=100, asc=True
        )
        assert lines == ["L0", "L1", "L2"]
        assert count == 3
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_read_only_job_raises_on_update_progress_and_log(
    redis_url: str, queue_name: str
) -> None:
    """``Queue.get_job(id).update_progress(50)`` raises with the
    read-only message; same for ``.log()``.
    """
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        # No worker — the job sits waiting; get_job synthesizes a
        # read-only Job (no native handle).
        added = await queue.add("read-only", {"value": 1})
        fetched = await queue.get_job(added.id)
        assert fetched is not None
        with pytest.raises(RuntimeError, match="read-only"):
            await fetched.update_progress(50)
        with pytest.raises(RuntimeError, match="read-only"):
            await fetched.log("nope")
    finally:
        await queue.close()
