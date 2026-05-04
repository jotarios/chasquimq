"""Integration tests for `NativeConsumer` against a live Redis 8.6.

Each test:
1. Pushes a job onto the queue's stream by hand-encoding the engine's `Job<T>`
   msgpack envelope (the producer binding lands in slice A2 — to keep this
   test self-contained, we encode here).
2. Spins up a `NativeConsumer.run(handler)` task on the asyncio loop.
3. Asserts on Redis state (XLEN, DLQ XLEN, attempt count) after the handler
   runs.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import List

import msgpack
import pytest
import redis.asyncio as aioredis

from chasquimq import NativeConsumer, NativeJob, UnrecoverableError

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")


def _stream_key(queue: str) -> str:
    return f"{{chasqui:{queue}}}:stream"


def _delayed_key(queue: str) -> str:
    return f"{{chasqui:{queue}}}:delayed"


def _dlq_key(queue: str) -> str:
    return f"{{chasqui:{queue}}}:dlq"


def _events_key(queue: str) -> str:
    return f"{{chasqui:{queue}}}:events"


def _encode_job(job_id: str, payload: bytes, attempt: int = 0) -> bytes:
    """Hand-encode a `Job<RawBytes>` msgpack envelope. Mirrors the engine's
    `rmp-serde` array form: `[id, payload, created_at_ms, attempt]`. Skips the
    optional 5th `retry` field (back-compat with pre-slice-8 wire shape)."""
    created_at_ms = int(time.time() * 1000)
    return msgpack.packb([job_id, payload, created_at_ms, attempt], use_bin_type=True)


def _decode_job(blob: bytes) -> dict:
    arr = msgpack.unpackb(blob, raw=False)
    out = {
        "id": arr[0],
        "payload": arr[1],
        "created_at_ms": arr[2],
        "attempt": arr[3],
    }
    if len(arr) > 4:
        out["retry"] = arr[4]
    return out


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(REDIS_URL, decode_responses=False)
    yield client
    await client.aclose()


@pytest.fixture
def queue_name() -> str:
    return f"py-consumer-test-{uuid.uuid4().hex[:8]}"


async def _flush_queue(redis_client: aioredis.Redis, queue: str) -> None:
    for k in (
        _stream_key(queue),
        _delayed_key(queue),
        _dlq_key(queue),
        _events_key(queue),
    ):
        await redis_client.delete(k)


async def _xadd_job(
    redis_client: aioredis.Redis, queue: str, payload: bytes, *, attempt: int = 0
) -> str:
    job_id = uuid.uuid4().hex
    blob = _encode_job(job_id, payload, attempt=attempt)
    await redis_client.xadd(_stream_key(queue), {b"d": blob})
    return job_id


async def _wait_for(predicate, *, timeout: float = 10.0, interval: float = 0.05) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("timed out waiting for predicate")
        await asyncio.sleep(interval)


async def _xlen(redis_client: aioredis.Redis, key: str) -> int:
    return int(await redis_client.xlen(key))


@pytest.mark.asyncio
async def test_handler_success_acks_job(redis_client, queue_name):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        max_attempts=3,
        # Smaller block so shutdown is prompt.
        read_block_ms=200,
        delayed_enabled=False,
    )

    received: List[NativeJob] = []
    done = asyncio.Event()

    async def handler(job: NativeJob) -> None:
        received.append(job)
        done.set()

    await _xadd_job(redis_client, queue_name, b"hello")

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        await asyncio.wait_for(done.wait(), timeout=10.0)

        # Wait for the ack to flush so the pending entries list (PEL) drains.
        async def acked() -> bool:
            pending = await redis_client.xpending(_stream_key(queue_name), "default")
            return int(pending["pending"]) == 0

        await _wait_for(acked, timeout=10.0)
    finally:
        consumer.shutdown()
        await asyncio.wait_for(run_task, timeout=10.0)

    assert len(received) == 1
    assert received[0].payload == b"hello"
    assert received[0].attempt == 0


@pytest.mark.asyncio
async def test_handler_returns_quickly_does_not_double_ack(redis_client, queue_name):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
    )

    call_count = 0
    done = asyncio.Event()

    async def handler(job: NativeJob) -> None:
        nonlocal call_count
        call_count += 1
        done.set()

    await _xadd_job(redis_client, queue_name, b"once")

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        await asyncio.wait_for(done.wait(), timeout=10.0)

        async def acked() -> bool:
            pending = await redis_client.xpending(_stream_key(queue_name), "default")
            return int(pending["pending"]) == 0

        await _wait_for(acked, timeout=10.0)
        # Idle a beat; if we were going to double-deliver, we would by now.
        await asyncio.sleep(1.0)
    finally:
        consumer.shutdown()
        await asyncio.wait_for(run_task, timeout=10.0)

    assert call_count == 1


@pytest.mark.asyncio
async def test_handler_raises_generic_exception_retries(redis_client, queue_name):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        max_attempts=4,
        read_block_ms=200,
        delayed_enabled=True,
    )

    attempts: List[int] = []
    seen_two = asyncio.Event()

    async def handler(job: NativeJob) -> None:
        attempts.append(job.attempt)
        if len(attempts) >= 2:
            seen_two.set()
        raise RuntimeError("boom")

    await _xadd_job(redis_client, queue_name, b"retryme")

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        # The first attempt errors → the job is rescheduled into the delayed
        # ZSET with a backoff. The embedded promoter (delayed_enabled=True)
        # promotes it back into the stream once the backoff elapses, and the
        # second invocation sees `attempt == 1`.
        await asyncio.wait_for(seen_two.wait(), timeout=15.0)
    finally:
        consumer.shutdown()
        await asyncio.wait_for(run_task, timeout=15.0)

    assert attempts[0] == 0, f"first run should be attempt 0, got {attempts}"
    assert attempts[1] >= 1, f"second run attempt must increase, got {attempts}"


@pytest.mark.asyncio
async def test_handler_raises_unrecoverable_routes_to_dlq(redis_client, queue_name):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        # Even with a generous retry budget, UnrecoverableError must skip it.
        max_attempts=99,
        read_block_ms=200,
        delayed_enabled=True,
    )

    invocations: List[int] = []

    async def handler(job: NativeJob) -> None:
        invocations.append(job.attempt)
        raise UnrecoverableError("poison pill")

    await _xadd_job(redis_client, queue_name, b"unrecoverable")

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        async def in_dlq() -> bool:
            return await _xlen(redis_client, _dlq_key(queue_name)) >= 1

        await _wait_for(in_dlq, timeout=15.0)
    finally:
        consumer.shutdown()
        await asyncio.wait_for(run_task, timeout=15.0)

    dlq_len = await _xlen(redis_client, _dlq_key(queue_name))
    assert dlq_len == 1, f"DLQ should hold 1 entry, got {dlq_len}"
    # Exactly one handler invocation: the unrecoverable signal short-circuits
    # the retry budget.
    assert len(invocations) == 1, f"unrecoverable must not retry; saw {invocations}"

    # Confirm DLQ entry's reason is `Unrecoverable`.
    entries = await redis_client.xrange(_dlq_key(queue_name), count=1)
    assert entries
    _, fields = entries[0]
    reason = fields.get(b"reason")
    assert reason == b"unrecoverable", f"unexpected DLQ reason: {reason!r}"


@pytest.mark.asyncio
async def test_handler_raises_unrecoverable_subclass_routes_to_dlq(
    redis_client, queue_name
):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        max_attempts=99,
        read_block_ms=200,
        delayed_enabled=True,
    )

    class PoisonPill(UnrecoverableError):
        pass

    invocations: List[int] = []

    async def handler(job: NativeJob) -> None:
        invocations.append(job.attempt)
        raise PoisonPill("subclass poison pill")

    await _xadd_job(redis_client, queue_name, b"unrecoverable-subclass")

    run_task = asyncio.ensure_future(consumer.run(handler))
    try:
        async def in_dlq() -> bool:
            return await _xlen(redis_client, _dlq_key(queue_name)) >= 1

        await _wait_for(in_dlq, timeout=15.0)
    finally:
        consumer.shutdown()
        await asyncio.wait_for(run_task, timeout=15.0)

    dlq_len = await _xlen(redis_client, _dlq_key(queue_name))
    assert dlq_len == 1, f"DLQ should hold 1 entry, got {dlq_len}"
    assert len(invocations) == 1, (
        f"unrecoverable subclass must not retry; saw {invocations}"
    )

    entries = await redis_client.xrange(_dlq_key(queue_name), count=1)
    assert entries
    _, fields = entries[0]
    reason = fields.get(b"reason")
    assert reason == b"unrecoverable", f"unexpected DLQ reason: {reason!r}"


@pytest.mark.asyncio
async def test_shutdown_from_another_task_ends_run_promptly(redis_client, queue_name):
    await _flush_queue(redis_client, queue_name)
    consumer = NativeConsumer(
        REDIS_URL,
        queue_name,
        concurrency=1,
        max_attempts=3,
        read_block_ms=200,
        delayed_enabled=False,
    )

    async def handler(job: NativeJob) -> None:
        return None

    run_task = asyncio.ensure_future(consumer.run(handler))

    async def trip_shutdown() -> None:
        # Give the consumer a moment to enter its read loop.
        await asyncio.sleep(0.5)
        consumer.shutdown()

    shutdown_task = asyncio.create_task(trip_shutdown())

    # If shutdown works, the run future resolves within a couple seconds —
    # deadline well below the hang behavior.
    await asyncio.wait_for(run_task, timeout=10.0)
    await shutdown_task
