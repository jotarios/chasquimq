"""Tests for :meth:`Job.wait_for_result` (slice 5d)."""

from __future__ import annotations

import asyncio

import pytest

from chasquimq import Job, Queue, Worker


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_wait_for_result_happy_path(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)

    async def handler(job: Job):
        return {"ok": 42, "echo": job.data}

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("compute", {"x": 1})
        result = await job.wait_for_result(timeout=10.0)
        assert result == {"ok": 42, "echo": {"x": 1}}
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_timeout_with_no_worker(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add("orphan", {"x": 1})
        with pytest.raises(asyncio.TimeoutError):
            await job.wait_for_result(timeout=0.3, poll_interval=0.05)
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_void_handler_times_out(
    redis_url: str, queue_name: str
) -> None:
    """``store_results=True`` + handler returns ``None`` → no result key
    written → ``wait_for_result`` times out (documented behavior)."""
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(_job: Job):
        seen.set()
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("void", {"x": 1})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        with pytest.raises(asyncio.TimeoutError):
            await job.wait_for_result(timeout=0.5, poll_interval=0.05)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_store_results_disabled_times_out(
    redis_url: str, queue_name: str
) -> None:
    """Worker with ``store_results=False`` (default) → no result key
    written → ``wait_for_result`` times out (documented behavior)."""
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(_job: Job):
        seen.set()
        return {"ok": 1}

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        # store_results omitted -> defaults to False
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("no-store", {"x": 1})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        with pytest.raises(asyncio.TimeoutError):
            await job.wait_for_result(timeout=0.5, poll_interval=0.05)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_cancellation_propagates(
    redis_url: str, queue_name: str
) -> None:
    """Cancelling the awaiting task surfaces as
    :class:`asyncio.CancelledError` — the inner ``asyncio.wait_for`` /
    ``asyncio.sleep`` chain is cancellation-safe."""
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add("cancel-me", {"x": 1})

        async def waiter():
            await job.wait_for_result(timeout=30.0, poll_interval=0.05)

        task = asyncio.create_task(waiter())
        # Let the loop spin once.
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_without_queue_ref_raises(
    redis_url: str, queue_name: str
) -> None:
    """Constructing a ``Job`` manually (no queue ref, like the
    Worker-side projection) means ``wait_for_result`` has no producer
    to poll. Surface that as a clear ``RuntimeError`` rather than
    silently timing out."""
    del redis_url, queue_name
    job = Job(id="manual", name="m", data={}, attempt=0, created_at_ms=0)
    with pytest.raises(RuntimeError, match=r"requires a Queue reference"):
        await job.wait_for_result(timeout=1.0)


@pytest.mark.asyncio
async def test_wait_for_result_bulk_jobs_have_queue_ref(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)

    async def handler(job: Job):
        return {"ok": job.data["i"] + 100}

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=4,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        jobs = await queue.add_bulk(
            [{"name": "b", "data": {"i": i}} for i in range(2)]
        )
        results = await asyncio.gather(
            *(j.wait_for_result(timeout=10.0) for j in jobs)
        )
        assert results == [{"ok": 100}, {"ok": 101}]
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


def test_invalid_timeout_rejected() -> None:
    job = Job(id="x", name="x", data={}, attempt=0, created_at_ms=0)
    # Create a barebones queue ref so we hit the validation before the
    # missing-queue branch.

    class _StubQueue:
        async def get_job_result(self, _job_id: str):
            return None

    job._queue = _StubQueue()  # type: ignore[assignment]
    with pytest.raises(ValueError, match=r"timeout must be a positive"):
        asyncio.run(job.wait_for_result(timeout=0))
    with pytest.raises(ValueError, match=r"poll_interval must be a positive"):
        asyncio.run(job.wait_for_result(timeout=1.0, poll_interval=0))


@pytest.mark.asyncio
async def test_wait_for_result_warns_when_store_results_disabled(
    redis_url: str, queue_name: str
) -> None:
    """Worker without ``store_results=True`` → polling never finds a key
    → after >1s wait_for_result emits a one-shot RuntimeWarning that
    points at the most likely cause."""
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(_job: Job):
        seen.set()
        return {"ok": 1}

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        # store_results omitted -> defaults to False
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("no-store", {"x": 1})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        with pytest.warns(RuntimeWarning, match=r"store_results=True"):
            with pytest.raises(asyncio.TimeoutError):
                await job.wait_for_result(timeout=1.5, poll_interval=0.05)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_wait_for_result_does_not_warn_on_fast_success(
    redis_url: str, queue_name: str
) -> None:
    """Result lands inside the first 1s heuristic window → no warning."""
    import warnings

    queue = Queue(queue_name, redis_url=redis_url)

    async def handler(job: Job):
        return {"ok": True, "echo": job.data}

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=50,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("fast", {"x": 1})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = await job.wait_for_result(timeout=10.0, poll_interval=0.05)
            assert result == {"ok": True, "echo": {"x": 1}}
            assert not any(
                issubclass(w.category, RuntimeWarning)
                and "store_results=True" in str(w.message)
                for w in caught
            ), f"unexpected warnings: {[str(w.message) for w in caught]}"
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()
