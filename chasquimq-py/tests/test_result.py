"""Result-backend integration tests for the high-level shim (slice 5c)."""

from __future__ import annotations

import asyncio
import os

import msgpack
import pytest

from chasquimq import Job, Queue, Worker
from chasquimq._native import Consumer, Producer


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


async def _wait_for_result(queue: Queue, job_id: str, timeout_s: float = 10.0):
    """Poll `queue.get_job_result` until non-None or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while asyncio.get_event_loop().time() < deadline:
        result = await queue.get_job_result(job_id)
        if result is not None:
            return result
        await asyncio.sleep(0.05)
    return None


@pytest.mark.asyncio
async def test_store_results_round_trip(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(job: Job):
        seen.set()
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
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        result = await _wait_for_result(queue, job.id)
        assert result == {"ok": 42, "echo": {"x": 1}}
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_store_results_disabled_returns_none(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(job: Job):
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
        # store_results omitted — defaults to False.
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("compute", {"x": 1})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        # Give the engine a beat to flush the ack so we know the
        # handler-completion path ran (not a "still in flight" None).
        await asyncio.sleep(0.3)
        assert await queue.get_job_result(job.id) is None
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_result_ttl_expires(redis_url: str, queue_name: str) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    seen = asyncio.Event()

    async def handler(job: Job):
        seen.set()
        return {"v": "expires"}

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
        result_ttl_ms=1000,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add("compute", {"x": 1})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        first = await _wait_for_result(queue, job.id, timeout_s=5.0)
        assert first == {"v": "expires"}
        await asyncio.sleep(2.0)
        assert await queue.get_job_result(job.id) is None
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_failed_handler_writes_no_result(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    invocations = 0
    done = asyncio.Event()

    async def handler(job: Job):
        nonlocal invocations
        invocations += 1
        if invocations >= 2:
            # Final attempt after a single retry — let it settle.
            done.set()
        raise RuntimeError("nope")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=2,
        read_block_ms=200,
        delayed_enabled=True,
        run_scheduler=False,
        store_results=True,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        job = await queue.add(
            "compute",
            {"x": 1},
            attempts=2,
            backoff=10,
        )
        await asyncio.wait_for(done.wait(), timeout=15.0)
        # Settle: let the DLQ relocator move the entry.
        await asyncio.sleep(0.5)
        assert await queue.get_job_result(job.id) is None
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_multiple_jobs_resolve_independently(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    n = 3
    counter = 0
    all_done = asyncio.Event()

    async def handler(job: Job):
        nonlocal counter
        counter += 1
        if counter >= n:
            all_done.set()
        return {"echo": job.data["i"], "doubled": job.data["i"] * 2}

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
            [{"name": "compute", "data": {"i": i}} for i in range(n)]
        )
        await asyncio.wait_for(all_done.wait(), timeout=10.0)
        ids = [j.id for j in jobs]

        # Poll get_job_result_bulk until all resolve.
        deadline = asyncio.get_event_loop().time() + 10.0
        results = []
        while asyncio.get_event_loop().time() < deadline:
            results = await queue.get_job_result_bulk(ids)
            if all(r is not None for r in results):
                break
            await asyncio.sleep(0.05)

        assert len(results) == n
        for i, r in enumerate(results):
            assert r == {"echo": i, "doubled": i * 2}
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


def test_result_ttl_ms_zero_rejected_at_construction(redis_url: str, queue_name: str) -> None:
    """The native ``Consumer`` rejects ``result_ttl_ms <= 0`` up-front
    (parity with the Node binding's ``resultTtlMs`` validation)."""
    with pytest.raises(RuntimeError, match=r"result_ttl_ms.*positive"):
        Consumer(
            redis_url,
            queue_name,
            store_results=True,
            result_ttl_ms=0,
        )

    with pytest.raises(RuntimeError, match=r"result_ttl_ms.*positive"):
        Consumer(
            redis_url,
            queue_name,
            store_results=True,
            result_ttl_ms=-1,
        )


def test_result_ttl_ms_omitted_uses_engine_default(
    redis_url: str, queue_name: str
) -> None:
    """``result_ttl_ms=None`` (the default) is legitimate — the engine
    default applies. Construction must not raise."""
    Consumer(
        redis_url,
        queue_name,
        store_results=True,
    )


@pytest.mark.asyncio
async def test_native_handler_returns_empty_bytes_writes_no_result(
    redis_url: str, queue_name: str
) -> None:
    """Pin the engine's `JOB_OK_SCRIPT` `#ARGV[3] > 0` gate end-to-end.

    When the handler returns ``b""`` (zero-length bytes), the engine
    must short-circuit to the ack-only path and write no result key.
    Uses the native ``Consumer`` / ``Producer`` so we control the exact
    bytes returned (the high-level :class:`Worker` shim msgpack-encodes
    results, so an empty user value would still encode to non-empty bytes).
    """
    producer = Producer(redis_url, queue_name)
    consumer = Consumer(
        redis_url,
        queue_name,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
    )

    seen = asyncio.Event()

    async def handler(_job) -> bytes:
        seen.set()
        return b""

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        job_id = await producer.add(msgpack.packb({"v": 1}, use_bin_type=True))
        await asyncio.wait_for(seen.wait(), timeout=10.0)
        # Let the engine's ack-flush + script call settle.
        await asyncio.sleep(0.3)
        assert await producer.get_result(job_id) is None
    finally:
        consumer.shutdown()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
