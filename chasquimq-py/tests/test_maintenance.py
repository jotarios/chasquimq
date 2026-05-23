"""Job maintenance API coverage for the Python shim.

Mirrors the Node ``maintenance.test.ts`` suite: ``Queue.remove`` /
``drain`` / ``clean`` / ``obliterate`` against a live loopback Redis.
"""

import asyncio

import pytest

from chasquimq import Queue, Worker


async def _wait_for(pred, timeout=10.0):
    loop = asyncio.get_event_loop()
    start = loop.time()
    while not pred():
        if loop.time() - start > timeout:
            raise AssertionError("wait_for timed out")
        await asyncio.sleep(0.05)


# ---- remove ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_waiting_job(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add("w", {"msg": "remove-me"})
    await queue.add("w", {"msg": "keep-me"})

    count = await queue.remove(job.id)
    assert count == 1

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 1


@pytest.mark.asyncio
async def test_remove_missing_is_idempotent(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    count = await queue.remove("does-not-exist")
    assert count == 0

    report = await queue.remove_report("does-not-exist")
    assert report == {
        "delayed": False,
        "stream": False,
        "dlq": False,
        "result": False,
    }


@pytest.mark.asyncio
async def test_remove_delayed_job(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "d", {"msg": "delayed"}, delay_ms=3_600_000, job_id="delayed-target"
    )
    counts = await queue.get_job_counts()
    assert counts["delayed"] == 1

    report = await queue.remove_report(job.id)
    assert report["delayed"] is True

    counts = await queue.get_job_counts()
    assert counts["delayed"] == 0


# ---- drain ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_clears_waiting_and_delayed(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(6):
        await queue.add("w", {"n": n})
    for n in range(3):
        await queue.add("d", {"n": n}, delay_ms=3_600_000)

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 6
    assert counts["delayed"] == 3

    removed = await queue.drain()
    assert removed == 9

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 0
    assert counts["delayed"] == 0


@pytest.mark.asyncio
async def test_drain_keeps_delayed_when_flag_off(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    await queue.add("w", {"msg": "w"})
    await queue.add("d", {"msg": "d"}, delay_ms=3_600_000)

    removed = await queue.drain(delayed=False)
    assert removed == 1

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 0
    assert counts["delayed"] == 1


@pytest.mark.asyncio
async def test_drain_empty_queue_is_noop(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    removed = await queue.drain()
    assert removed == 0


# ---- clean ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_waiting_removes_old(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(4):
        await queue.add("w", {"n": n})

    removed = await queue.clean(0, 100, "waiting")
    assert len(removed) == 4

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 0


@pytest.mark.asyncio
async def test_clean_grace_excludes_recent(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(3):
        await queue.add("w", {"n": n})

    removed = await queue.clean(3_600_000, 100, "waiting")
    assert removed == []

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 3


@pytest.mark.asyncio
async def test_clean_limit_caps_removals(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(8):
        await queue.add("w", {"n": n})

    removed = await queue.clean(0, 3, "waiting")
    assert len(removed) == 3

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 5


@pytest.mark.asyncio
async def test_clean_delayed(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(3):
        await queue.add("d", {"n": n}, delay_ms=3_600_000)

    removed = await queue.clean(0, 100, "delayed")
    assert len(removed) == 3

    counts = await queue.get_job_counts()
    assert counts["delayed"] == 0


@pytest.mark.asyncio
async def test_clean_failed_removes_dlq(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)

    async def handler(_job) -> None:
        raise RuntimeError("intentional failure")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=1,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        for n in range(3):
            await queue.add("f", {"n": n})

        async def dlq_full() -> bool:
            counts = await queue.get_job_counts()
            return counts["failed"] >= 3

        loop = asyncio.get_event_loop()
        start = loop.time()
        while not await dlq_full():
            if loop.time() - start > 10.0:
                raise AssertionError("DLQ never populated")
            await asyncio.sleep(0.05)
    finally:
        worker.pause()
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass

    removed = await queue.clean(0, 100, "failed")
    assert len(removed) == 3

    counts = await queue.get_job_counts()
    assert counts["failed"] == 0


# ---- obliterate -----------------------------------------------------------


@pytest.mark.asyncio
async def test_obliterate_nukes_keyspace(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    for n in range(5):
        await queue.add("w", {"n": n})
    await queue.add("d", {"msg": "d"}, delay_ms=3_600_000)

    removed = await queue.obliterate()
    assert removed >= 2

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 0
    assert counts["delayed"] == 0
    assert counts["failed"] == 0


@pytest.mark.asyncio
async def test_obliterate_then_reuse(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    await queue.add("w", {"msg": "before"})
    await queue.obliterate()

    job = await queue.add("w", {"msg": "after"})
    assert job.id

    counts = await queue.get_job_counts()
    assert counts["waiting"] == 1


@pytest.mark.asyncio
async def test_obliterate_empty_is_idempotent(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    assert await queue.obliterate() == 0
    assert await queue.obliterate() == 0
