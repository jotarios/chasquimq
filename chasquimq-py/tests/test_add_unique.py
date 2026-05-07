"""Tests for Queue.add(job_id=...) idempotency + Queue.add_unique sugar."""

from __future__ import annotations

import os
import time

import pytest

from chasquimq import Queue
from conftest import delayed_key_for, stream_key_for


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")

pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_add_with_job_id_on_delayed_path_is_idempotent(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job_id = f"dup-{int(time.time() * 1000)}"
    a = await queue.add("job", {"k": "a"}, job_id=job_id, delay_ms=60_000)
    b = await queue.add("job", {"k": "b"}, job_id=job_id, delay_ms=60_000)
    assert a.id == job_id
    assert b.id == job_id
    # Only the first call's payload landed in the delayed ZSET — Lua
    # SET NX EX dedup marker gated the second ZADD.
    zcard = await redis_client.zcard(delayed_key_for(queue_name))
    assert zcard == 1
    await queue.close()


@pytest.mark.asyncio
async def test_add_with_job_id_on_immediate_path_is_idempotent_within_producer(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job_id = f"dup-imm-{int(time.time() * 1000)}"
    a = await queue.add("job", {"k": "a"}, job_id=job_id)
    b = await queue.add("job", {"k": "b"}, job_id=job_id)
    assert a.id == job_id
    assert b.id == job_id
    # Single stream entry — Redis 8.6 `XADD IDMP <producer_id> <job_id>`
    # gates the second write at the wire layer. Note: IDMP scope is the
    # producer id (one per Queue), so two distinct Queue instances with
    # different producer IDs would both succeed on the immediate path.
    # For cross-process idempotency, use a delay > 0 (true SET-NX-EX gate).
    xlen = await redis_client.xlen(stream_key_for(queue_name))
    assert xlen == 1
    await queue.close()


@pytest.mark.asyncio
async def test_add_unique_requires_job_id(redis_url: str, queue_name: str) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError):
        await queue.add_unique("job", {"k": "x"}, job_id="")
    with pytest.raises(TypeError):
        # Missing the kw-only `job_id` is a TypeError from Python itself.
        await queue.add_unique("job", {"k": "x"})  # type: ignore[call-arg]
    await queue.close()


@pytest.mark.asyncio
async def test_add_unique_rejects_whitespace_job_id(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError):
        await queue.add_unique("job", {"k": "x"}, job_id="   ")
    with pytest.raises(ValueError):
        await queue.add_unique("job", {"k": "x"}, job_id="\t\n")
    await queue.close()


@pytest.mark.asyncio
async def test_add_rejects_empty_or_whitespace_job_id(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError):
        await queue.add("job", {"k": "x"}, job_id="")
    with pytest.raises(ValueError):
        await queue.add("job", {"k": "x"}, job_id="   ")
    with pytest.raises(ValueError):
        await queue.add("job", {"k": "x"}, job_id="\t")
    await queue.close()


@pytest.mark.asyncio
async def test_add_unique_with_delay_is_strictly_idempotent(
    redis_url: str, queue_name: str, redis_client
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job_id = f"unique-{int(time.time() * 1000)}"
    a = await queue.add_unique("job", {"k": "a"}, job_id=job_id, delay_ms=60_000)
    b = await queue.add_unique("job", {"k": "b"}, job_id=job_id, delay_ms=60_000)
    assert a.id == job_id
    assert b.id == job_id
    zcard = await redis_client.zcard(delayed_key_for(queue_name))
    assert zcard == 1
    await queue.close()
