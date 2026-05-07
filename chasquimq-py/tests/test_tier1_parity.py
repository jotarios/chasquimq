"""Tier 1 cross-shim DX parity tests for the Python shim.

Mirrors ``chasquimq-node/__test__/tier1-parity.test.ts``. Covers:

- Single ``failed`` event on ``UnrecoverableError`` (Fix B).
- ``failed_reason`` carries just the user message, no FFI prefix (Fix C).
- Numeric event fields are typed as ``int`` (parity with Node).
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from chasquimq import (
    Job,
    Queue,
    QueueEvents,
    UnrecoverableError,
    Worker,
)


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_single_failed_event_on_unrecoverable(
    redis_url: str, queue_name: str
) -> None:
    """The engine emits exactly one ``failed`` event per handler
    invocation. The shim's :class:`QueueEvents` must NOT synthesize a
    second ``failed`` from the downstream ``dlq`` event.
    """
    queue = Queue(queue_name, redis_url=redis_url)
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)

    async def handler(_job: Job) -> None:
        raise UnrecoverableError("poison")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=5,  # generous — must short-circuit
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
        events_enabled=True,
    )

    run_task = asyncio.create_task(worker.run())
    failed_count = 0
    dlq_count = 0
    failed_job_ids: list[str | None] = []

    async def collect() -> None:
        nonlocal failed_count, dlq_count
        async for ev in events:
            if ev.name == "failed":
                failed_count += 1
                failed_job_ids.append(ev.job_id)
            if ev.name == "dlq":
                dlq_count += 1
            # Drain past at least one of each to confirm no second `failed`.
            if failed_count >= 2 or dlq_count >= 2:
                return

    collect_task = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.2)
        added = await queue.add("poison", {"k": 1})
        # Wait for either the contract violation (failed_count >= 2) or the
        # natural drain (failed=1 + dlq=1) — whichever fires first.
        for _ in range(150):
            if dlq_count >= 1 and failed_count >= 1:
                break
            await asyncio.sleep(0.05)
        # One full extra block window to give a hypothetical second `failed`
        # time to arrive.
        await asyncio.sleep(0.5)
    finally:
        await events.close()
        # collect_task may still be sitting in the iterator; cancel it.
        collect_task.cancel()
        try:
            await collect_task
        except (asyncio.CancelledError, Exception):
            pass
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass
        await queue.close()

    assert failed_count == 1, f"expected exactly one failed event, got {failed_count}"
    assert added.id in failed_job_ids, f"failed event missing job_id: {failed_job_ids}"


@pytest.mark.asyncio
async def test_failed_reason_has_no_ffi_prefix(
    redis_url: str, queue_name: str
) -> None:
    """``failed.reason`` must be just the user's exception message — no
    ``"Python handler raised: ..."`` FFI prefix, no engine ``"handler: "``
    prefix, no ``"OSError(...)"`` repr noise.
    """
    queue = Queue(queue_name, redis_url=redis_url)
    events = QueueEvents(queue_name, redis_url=redis_url, block_ms=300)

    async def handler(_job: Job) -> None:
        raise RuntimeError("smtp timeout")

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        max_attempts=1,  # one attempt → route straight to DLQ on first fail
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
        events_enabled=True,
    )

    run_task = asyncio.create_task(worker.run())
    seen_reason: list[str] = []

    async def collect() -> None:
        async for ev in events:
            if ev.name == "failed":
                reason = ev.fields.get("reason", "")
                seen_reason.append(reason if isinstance(reason, str) else str(reason))
                return

    collect_task = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.2)
        await queue.add("failing", {"k": 1})
        await asyncio.wait_for(collect_task, timeout=10.0)
    finally:
        await events.close()
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass
        await queue.close()

    assert seen_reason, "expected at least one failed event"
    # The user message must come through cleanly.
    assert seen_reason[0] == "smtp timeout", (
        f"expected just the message, got {seen_reason[0]!r}"
    )
    # Defensive: the prefixes that used to leak through must NOT be present.
    for forbidden in ("handler:", "Python handler raised:", "RuntimeError("):
        assert forbidden not in seen_reason[0], (
            f"forbidden prefix {forbidden!r} found in {seen_reason[0]!r}"
        )


@pytest.mark.asyncio
async def test_queue_events_typed_numeric_fields(
    redis_url: str, queue_name: str, redis_client
) -> None:
    """``QueueEvent.fields`` decodes documented numeric fields
    (``attempt`` / ``backoff_ms`` / ``delay_ms`` / ``duration_us`` /
    ``ts``) as ``int``, mirroring the Node shim's ``parseIntSafe`` use.
    Unknown / non-numeric values pass through as ``str``.
    """
    stream_key = f"{{chasqui:{queue_name}}}:events"
    events = QueueEvents(
        queue_name, redis_url=redis_url, block_ms=500, last_event_id="0"
    )

    received: list = []

    async def collect() -> None:
        async for ev in events:
            received.append(ev)
            if len(received) >= 1:
                return

    collect_task = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.1)
        await redis_client.xadd(
            stream_key,
            {
                "e": "active",
                "id": "01J-fake-id",
                "ts": "1700000000000",
                "n": "send-email",
                "attempt": "3",
                "backoff_ms": "750",
                "duration_us": "1234",
                "extra": "stay-as-string",
            },
        )
        await asyncio.wait_for(collect_task, timeout=5.0)
    finally:
        await events.close()

    assert received, "expected one event"
    ev = received[0]
    assert ev.name == "active"
    assert ev.job_name == "send-email"
    # Numeric fields → int.
    assert ev.fields.get("attempt") == 3
    assert ev.fields.get("backoff_ms") == 750
    assert ev.fields.get("duration_us") == 1234
    assert ev.fields.get("ts") == 1700000000000
    # Unknown field stays as str.
    assert ev.fields.get("extra") == "stay-as-string"


@pytest.mark.asyncio
async def test_queue_async_context_manager_closes(
    redis_url: str, queue_name: str
) -> None:
    """Python parity check for Node's ``Symbol.asyncDispose``: ``async
    with Queue(...)`` flips ``is_closed`` after the block exits.
    """
    async with Queue(queue_name, redis_url=redis_url) as queue:
        await queue.add("x", {"k": 1})
        assert queue.is_closed is False
    assert queue.is_closed is True


@pytest.mark.asyncio
async def test_worker_async_context_manager_closes(
    redis_url: str, queue_name: str
) -> None:
    async def handler(_job: Job) -> None:
        return None

    async with Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=1,
        delayed_enabled=False,
        run_scheduler=False,
    ) as worker:
        assert worker.is_closed is False
    assert worker.is_closed is True
