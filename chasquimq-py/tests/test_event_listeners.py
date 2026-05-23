"""Integration tests for Worker / QueueEvents listener API + Job.wait_until_finished.

Mirrors ``chasquimq-node/__test__/event-listeners.test.ts``.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from chasquimq import (
    Job,
    Queue,
    QueueEvents,
    WaitUntilFinishedTimeoutError,
    Worker,
)


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


# --- Worker.on('drained') -----------------------------------------------------


@pytest.mark.asyncio
async def test_worker_emits_drained_after_full_to_empty_transition(
    redis_url: str, queue_name: str
) -> None:
    drained_evt = asyncio.Event()

    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=4,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    worker.on("drained", lambda *_a: drained_evt.set())

    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        await queue.add("one", {"x": 1})
        await asyncio.wait_for(drained_evt.wait(), timeout=10.0)
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_worker_does_not_spawn_drained_subscriber_when_no_listener(
    redis_url: str, queue_name: str
) -> None:
    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=4,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        await queue.add("one", {"x": 1})
        # Give the worker a beat to process; no drained listener
        # attached, so the embedded subscriber was never created.
        await asyncio.sleep(0.2)
        # Close should be fast — no embedded QueueEvents to drain.
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        await worker.close()
        elapsed = loop.time() - t0
        # Bound generously: native consumer shutdown is fast (<1s); a
        # live QueueEvents would add up to ~5s before close completes.
        assert elapsed < 3.0
    finally:
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


# --- Worker.on('paused' / 'resumed') -----------------------------------------


@pytest.mark.asyncio
async def test_worker_emits_paused_and_resumed(
    redis_url: str, queue_name: str
) -> None:
    paused_calls: list[None] = []
    resumed_calls: list[None] = []

    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    worker.on("paused", lambda: paused_calls.append(None))
    worker.on("resumed", lambda: resumed_calls.append(None))

    run_task = await _run_worker(worker)
    try:
        # Let it start
        await asyncio.sleep(0.05)
        worker.pause()
        assert len(paused_calls) == 1
        worker.resume()
        assert len(resumed_calls) == 1
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)


# --- Worker.on('completed' / 'failed') ---------------------------------------


@pytest.mark.asyncio
async def test_worker_emits_completed_with_result(
    redis_url: str, queue_name: str
) -> None:
    completed: list[tuple[Job, object]] = []

    async def handler(job: Job) -> int:
        return job.data["v"] * 3

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    worker.on("completed", lambda job, result: completed.append((job, result)))

    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        await queue.add("triple", {"v": 7})
        deadline = asyncio.get_event_loop().time() + 5.0
        while not completed and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert len(completed) == 1
        assert completed[0][1] == 21
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_worker_emits_failed_with_exception(
    redis_url: str, queue_name: str
) -> None:
    failed: list[tuple[Job, BaseException]] = []

    async def handler(job: Job) -> None:
        raise RuntimeError("boom")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    worker.on("failed", lambda job, err: failed.append((job, err)))

    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        await queue.add("fail-me", {"v": 0})
        deadline = asyncio.get_event_loop().time() + 5.0
        while not failed and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert len(failed) >= 1
        assert isinstance(failed[0][1], RuntimeError)
        assert "boom" in str(failed[0][1])
    finally:
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


# --- QueueEvents listener API + per-id channels ------------------------------


@pytest.mark.asyncio
async def test_queue_events_per_id_completed_channel(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)

    async def handler(job: Job) -> int:
        return job.data["v"]

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)

    run_task = await _run_worker(worker)
    try:
        # Add job; capture id, then attach per-id listener AFTER
        # subscription is ready.
        job = await queue.add("compute", {"v": 99})
        targeted_calls: list[dict] = []
        broadcast_calls: list[dict] = []

        events.on(f"completed:{job.id}", lambda payload, eid: targeted_calls.append(payload))
        events.on("completed", lambda payload, eid: broadcast_calls.append(payload))
        await events.wait_until_ready()

        # Re-add to fire after the subscription is open (the earlier
        # job may have completed before the subscriber connected).
        job2 = await queue.add("compute", {"v": 100})
        # Targeted listener was for `job` (the earlier one), so wire
        # one more for job2 too.
        events.on(f"completed:{job2.id}", lambda payload, eid: targeted_calls.append(payload))

        deadline = asyncio.get_event_loop().time() + 8.0
        while (
            not targeted_calls or not broadcast_calls
        ) and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)

        assert broadcast_calls, "broadcast `completed` listener should fire"
        assert targeted_calls, "per-id `completed:<jobId>` listener should fire"
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_queue_events_supports_async_callbacks(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)
    seen = asyncio.Event()

    async def on_completed(payload: dict, _event_id: str) -> None:
        # Async callback — small await to prove the dispatcher
        # schedules it on the loop.
        await asyncio.sleep(0)
        seen.set()

    events.on("completed", on_completed)
    await events.wait_until_ready()

    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        await queue.add("async-cb", {})
        await asyncio.wait_for(seen.wait(), timeout=10.0)
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


# --- Job.wait_until_finished -------------------------------------------------


@pytest.mark.asyncio
async def test_job_wait_until_finished_resolves_with_stored_result(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)
    # Force the subscriber loop to start so `wait_until_ready` is
    # awaitable.
    events.on("completed", lambda *_a: None)
    await events.wait_until_ready()

    async def handler(job: Job) -> int:
        return job.data["v"] * 2

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
        store_results=True,
        result_ttl_ms=60_000,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        job = await queue.add("double", {"v": 21})
        result = await job.wait_until_finished(events, timeout=10.0)
        assert result == 42
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_job_wait_until_finished_returns_none_when_store_results_off(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)
    events.on("completed", lambda *_a: None)
    await events.wait_until_ready()

    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        job = await queue.add("void", {})
        result = await job.wait_until_finished(events, timeout=10.0)
        assert result is None
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_job_wait_until_finished_raises_on_failed(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)
    events.on("failed", lambda *_a: None)
    await events.wait_until_ready()

    async def handler(job: Job) -> None:
        raise RuntimeError("handler said no")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )
    queue = Queue(queue_name, redis_url=redis_url)
    run_task = await _run_worker(worker)
    try:
        job = await queue.add("rejects", {})
        with pytest.raises(RuntimeError, match="handler said no"):
            await job.wait_until_finished(events, timeout=10.0)
    finally:
        await events.close()
        await worker.close()
        await asyncio.wait([run_task], timeout=5.0)
        await queue.close()


@pytest.mark.asyncio
async def test_job_wait_until_finished_times_out(
    redis_url: str, queue_name: str
) -> None:
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)
    events.on("completed", lambda *_a: None)
    await events.wait_until_ready()

    queue = Queue(queue_name, redis_url=redis_url)
    try:
        # No worker; the job sits waiting forever.
        job = await queue.add("orphan", {})
        with pytest.raises(WaitUntilFinishedTimeoutError):
            await job.wait_until_finished(events, timeout=0.3)
    finally:
        await events.close()
        await queue.close()
