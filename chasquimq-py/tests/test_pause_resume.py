"""Pause / resume coverage for the Python shim.

Two surfaces, mirroring the Node suite:
  - ``Worker.pause()/resume()/is_paused()`` — process-local in-memory
    switch on the native Consumer. Stops dispatch at the next batch
    boundary; in-flight jobs drain; producers keep enqueueing.
  - ``Queue.pause()/resume()/is_paused()`` — durable cross-process Redis
    flag observed by every consumer of the queue.
Behavioural where loopback Redis makes that deterministic, wiring-level
otherwise (matching the repo's FFI test depth).
"""

import asyncio

import pytest

from chasquimq import Queue, Worker
from chasquimq import _native


@pytest.mark.asyncio
async def test_worker_pause_stops_dispatch_then_resume_drains(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    processed = 0

    async def handler(_job) -> None:
        nonlocal processed
        processed += 1

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=4,
        max_attempts=3,
        # Tight block so the in-flight read at pause time resolves fast
        # and the next batch boundary (gate check) comes quickly.
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        for n in range(10):
            await queue.add("seed", {"n": n})

        async def wait_for(pred, timeout=5.0):
            loop = asyncio.get_event_loop()
            start = loop.time()
            while not pred():
                if loop.time() - start > timeout:
                    raise AssertionError("wait_for timed out")
                await asyncio.sleep(0.05)

        await wait_for(lambda: processed >= 10)

        assert worker.is_paused() is False
        worker.pause()
        assert worker.is_paused() is True

        # Let any in-flight job drain, then snapshot once stable.
        prev = -1
        while prev != processed:
            prev = processed
            await asyncio.sleep(0.3)
        before = processed

        # Producer must keep working while paused.
        for n in range(100, 115):
            await queue.add("while-paused", {"n": n})
        await asyncio.sleep(0.6)
        assert processed == before, "no new jobs may dispatch while paused"

        worker.resume()
        assert worker.is_paused() is False
        await wait_for(lambda: processed >= before + 15)
        assert processed >= before + 15
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_worker_double_pause_resume_idempotent(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    async def handler(_job) -> None:
        pass

    worker = Worker(queue_name, handler, redis_url=redis_url)
    worker.pause()
    worker.pause()
    assert worker.is_paused() is True
    worker.resume()
    worker.resume()
    assert worker.is_paused() is False
    await worker.close()


@pytest.mark.asyncio
async def test_queue_durable_pause_is_observed_by_worker(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    processed = 0

    async def handler(_job) -> None:
        nonlocal processed
        processed += 1

    assert await queue.is_paused() is False
    await queue.pause()
    assert await queue.is_paused() is True

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        for n in range(8):
            await queue.add("seed", {"n": n})
        # Durable flag set → worker must not dispatch.
        await asyncio.sleep(0.8)
        assert processed == 0

        await queue.resume()
        assert await queue.is_paused() is False

        loop = asyncio.get_event_loop()
        start = loop.time()
        while processed < 8:
            if loop.time() - start > 5.0:
                raise AssertionError("worker did not resume after key delete")
            await asyncio.sleep(0.05)
        assert processed >= 8
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()


@pytest.mark.asyncio
async def test_queue_pause_idempotent(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        await queue.pause()
        await queue.pause()
        assert await queue.is_paused() is True
        await queue.resume()
        await queue.resume()
        assert await queue.is_paused() is False
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_native_consumer_pause_surface(redis_url: str, queue_name: str) -> None:
    c = _native.Consumer(redis_url, queue_name)
    assert c.is_paused() is False
    c.pause()
    assert c.is_paused() is True
    c.resume()
    assert c.is_paused() is False
