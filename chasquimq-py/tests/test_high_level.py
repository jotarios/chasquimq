"""Integration tests for the high-level Queue / Worker / QueueEvents shim."""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import timedelta

import pytest

from chasquimq import (
    BackoffSpec,
    Job,
    Queue,
    QueueEvents,
    RepeatPattern,
    UnrecoverableError,
    Worker,
)
from chasquimq._encoding import decode_payload
from conftest import (
    delayed_key_for,
    dlq_key_for,
    repeat_key_for,
    stream_key_for,
)


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_queue_add_writes_msgpack_payload_to_stream(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add("send-email", {"to": "ada@example.com"})
    assert job.id
    assert job.name == "send-email"
    assert job.data == {"to": "ada@example.com"}

    entries = await redis_client.xrange(stream_key_for(queue_name), count=1)
    assert len(entries) == 1
    _xid, fields = entries[0]
    encoded = fields[b"d"]
    decoded = decode_payload(encoded)
    # The engine envelope is `[id, payload, created_at_ms, attempt]`. The
    # high-level shim msgpack-encodes the user data only.
    assert isinstance(decoded, list)
    payload = decode_payload(decoded[1])
    assert payload == {"to": "ada@example.com"}

    await queue.close()


@pytest.mark.asyncio
async def test_queue_add_round_trips_through_worker(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    received: list[Job] = []
    done = asyncio.Event()

    async def handler(job: Job) -> None:
        received.append(job)
        done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        await queue.add("hello-task", {"value": 42, "tag": "x"})
        await asyncio.wait_for(done.wait(), timeout=10.0)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()

    assert len(received) == 1
    received_job = received[0]
    assert received_job.id
    assert received_job.name == ""
    assert received_job.data == {"value": 42, "tag": "x"}
    assert received_job.attempt == 0


@pytest.mark.asyncio
async def test_queue_add_bulk_fast_path_when_simple(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    jobs = await queue.add_bulk(
        [
            {"name": "n", "data": {"i": i}}
            for i in range(5)
        ]
    )
    assert len(jobs) == 5
    assert all(j.id for j in jobs)
    assert (
        await redis_client.xlen(stream_key_for(queue_name))
    ) == 5
    await queue.close()


@pytest.mark.asyncio
async def test_queue_add_bulk_falls_back_when_per_job_options(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    jobs = await queue.add_bulk(
        [
            {"name": "x", "data": {"i": 0}, "attempts": 7},
            {"name": "y", "data": {"i": 1}},
        ]
    )
    assert len(jobs) == 2
    assert (
        await redis_client.xlen(stream_key_for(queue_name))
    ) == 2
    await queue.close()


@pytest.mark.asyncio
async def test_queue_add_with_delay_lands_in_delayed_zset(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add("delayed-task", {"k": 1}, delay=60_000)
    assert job.id
    assert (
        await redis_client.zcard(delayed_key_for(queue_name))
    ) == 1
    assert (
        await redis_client.xlen(stream_key_for(queue_name))
    ) == 0
    await queue.close()


@pytest.mark.asyncio
async def test_queue_add_with_attempts_and_backoff_round_trips(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    backoff = BackoffSpec.exponential(
        100, multiplier=2.5, max_ms=10_000, jitter_ms=25
    )
    await queue.add("retry-task", {"k": 1}, attempts=4, backoff=backoff)

    entries = await redis_client.xrange(stream_key_for(queue_name), count=1)
    assert len(entries) == 1
    _xid, fields = entries[0]
    decoded = decode_payload(fields[b"d"])
    assert isinstance(decoded, list) and len(decoded) == 5
    retry = decoded[4]
    assert retry == [4, ["exponential", 100, 10_000, 2.5, 25]]
    await queue.close()


@pytest.mark.asyncio
async def test_queue_cancel_delayed_round_trip(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    await queue.add(
        "x", {"k": 1}, delay=60_000, job_id="cancel-target"
    )
    assert (
        await redis_client.zcard(delayed_key_for(queue_name))
    ) == 1

    removed = await queue.cancel_delayed("cancel-target")
    assert removed is True
    again = await queue.cancel_delayed("cancel-target")
    assert again is False
    await queue.close()


@pytest.mark.asyncio
async def test_queue_repeatable_upsert_list_remove(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "send-digest",
        {"who": "all"},
        repeat=RepeatPattern.every(60_000),
    )
    assert job.id

    listed = await queue.get_repeatable_jobs(10)
    assert len(listed) == 1
    meta = listed[0]
    assert meta.key == job.id
    assert meta.job_name == "send-digest"
    assert meta.pattern.kind == "every"
    assert meta.pattern.interval_ms == 60_000
    assert meta.next_fire_ms > 0

    removed = await queue.remove_repeatable_by_key(job.id)
    assert removed is True
    again = await queue.remove_repeatable_by_key(job.id)
    assert again is False
    assert (
        await redis_client.zcard(repeat_key_for(queue_name))
    ) == 0
    await queue.close()


@pytest.mark.asyncio
async def test_worker_handler_unrecoverable_routes_to_dlq(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)

    invocations: list[int] = []

    async def handler(job: Job) -> None:
        invocations.append(job.attempt)
        raise UnrecoverableError("poison pill")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=99,
        read_block_ms=200,
        delayed_enabled=True,
        run_scheduler=False,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        await queue.add("poison", {"x": 1})

        async def in_dlq() -> bool:
            length = await redis_client.xlen(dlq_key_for(queue_name))
            return int(length) >= 1

        deadline = asyncio.get_event_loop().time() + 15.0
        while True:
            if await in_dlq():
                break
            if asyncio.get_event_loop().time() > deadline:
                raise AssertionError("DLQ did not receive entry")
            await asyncio.sleep(0.1)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()

    dlq_entries = await redis_client.xrange(
        dlq_key_for(queue_name), count=10
    )
    assert len(dlq_entries) == 1
    assert len(invocations) == 1, (
        f"unrecoverable must not retry; saw {invocations}"
    )
    _, fields = dlq_entries[0]
    assert fields.get(b"reason") == b"unrecoverable"


@pytest.mark.asyncio
async def test_queue_events_stream_observes_completed_event(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=500)

    async def handler(job: Job) -> None:
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
        events_enabled=True,
    )

    run_task = asyncio.create_task(worker.run())
    seen: list[str] = []

    async def collect() -> None:
        async for ev in events:
            seen.append(ev.name)
            if "completed" in seen:
                return

    collect_task = asyncio.create_task(collect())
    try:
        # Give the events subscriber a beat to start its first XREAD-BLOCK
        # so it doesn't miss the engine emit on a fast loopback Redis.
        await asyncio.sleep(0.2)
        await queue.add("ev-task", {"x": 1})
        await asyncio.wait_for(collect_task, timeout=15.0)
    finally:
        await events.close()
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await queue.close()

    assert "completed" in seen, f"events seen: {seen}"


@pytest.mark.asyncio
async def test_worker_runs_embedded_scheduler_for_repeatable(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)

    fires: list[Job] = []
    seen_two = asyncio.Event()

    async def handler(job: Job) -> None:
        fires.append(job)
        if len(fires) >= 2:
            seen_two.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=3,
        read_block_ms=100,
        delayed_enabled=True,
        run_scheduler=True,
        scheduler_tick_ms=50,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        await queue.add(
            "tick", {"hi": True}, repeat=RepeatPattern.every(75)
        )
        await asyncio.wait_for(seen_two.wait(), timeout=15.0)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        # Remove the spec so the scheduler doesn't keep firing across tests.
        await queue.remove_repeatable_by_key("tick::every:75")
        await queue.close()

    assert len(fires) >= 2
    for f in fires:
        assert f.data == {"hi": True}


@pytest.mark.asyncio
async def test_unsupported_dict_repeat_kind_raises(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError):
        await queue.add(
            "x", {"k": 1}, repeat={"kind": "weird"}
        )
    await queue.close()


@pytest.mark.asyncio
async def test_invalid_backoff_type_raises(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(TypeError):
        await queue.add(
            "x", {"k": 1}, backoff="not-a-thing"  # type: ignore[arg-type]
        )
    await queue.close()


@pytest.mark.asyncio
async def test_negative_delay_raises(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError, match="non-negative"):
        await queue.add("x", {"k": 1}, delay=-1)
    with pytest.raises(ValueError, match="non-negative"):
        await queue.add("x", {"k": 1}, delay=-0.5)
    with pytest.raises(ValueError, match="non-negative"):
        await queue.add(
            "x", {"k": 1}, delay=timedelta(milliseconds=-100)
        )
    await queue.close()


@pytest.mark.asyncio
async def test_non_finite_float_delay_raises(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError, match="finite non-negative"):
        await queue.add("x", {"k": 1}, delay=float("inf"))
    with pytest.raises(ValueError, match="finite non-negative"):
        await queue.add("x", {"k": 1}, delay=float("-inf"))
    with pytest.raises(ValueError, match="finite non-negative"):
        await queue.add("x", {"k": 1}, delay=float("nan"))
    await queue.close()


@pytest.mark.asyncio
async def test_worker_close_is_idempotent_and_safe_during_run(
    redis_url: str, queue_name: str
) -> None:
    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )

    run_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)

    results = await asyncio.gather(
        worker.close(),
        worker.close(),
        worker.close(),
        return_exceptions=True,
    )
    assert all(not isinstance(r, BaseException) for r in results)

    await asyncio.wait_for(run_task, timeout=5.0)
    assert worker.is_running is False


def test_worker_default_concurrency_is_100() -> None:
    import inspect

    sig = inspect.signature(Worker.__init__)
    assert sig.parameters["concurrency"].default == 100


@pytest.mark.asyncio
async def test_worker_scheduler_error_is_logged(
    redis_url: str,
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(job: Job) -> None:
        return None

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
    )

    class _BoomScheduler:
        async def run(self) -> None:
            raise RuntimeError("redis unreachable on startup")

        def shutdown(self) -> None:
            pass

    worker._scheduler = _BoomScheduler()  # type: ignore[assignment]

    caplog.set_level(logging.WARNING, logger="chasquimq.worker")

    run_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)
    await worker.close()
    await asyncio.wait_for(run_task, timeout=5.0)

    matching = [
        r
        for r in caplog.records
        if "scheduler stopped with error" in r.getMessage()
        and "redis unreachable on startup" in r.getMessage()
    ]
    assert matching, (
        f"expected scheduler-error warning; saw: {[r.getMessage() for r in caplog.records]}"
    )
