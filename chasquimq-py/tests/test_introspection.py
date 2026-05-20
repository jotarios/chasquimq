"""Tests for Queue introspection — getJob / getJobState / getJobCounts
/ getJobs and the sticky consumer_group constructor option.

Mirrors `chasquimq-node/__test__/introspection.test.ts` 1:1 plus the
explicit NOGROUP case the Node side also exercises.
"""

from __future__ import annotations

import os

import pytest

from chasquimq import Queue


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")

pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_get_job_counts_on_empty_queue_is_all_zeros(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        counts = await queue.get_job_counts()
        assert counts["waiting"] == 0
        assert counts["active"] == 0
        assert counts["delayed"] == 0
        assert counts["failed"] == 0
        assert counts["paused"] == 0
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_state_unknown_for_missing_id(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        state = await queue.get_job_state("does-not-exist")
        assert state == "unknown"
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_returns_none_for_missing_id(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.get_job("does-not-exist")
        assert job is None
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_state_waiting_for_unread_entry(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add("hello", {"msg": "hi"})
        state = await queue.get_job_state(job.id)
        assert state == "waiting"
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_retrieves_waiting_job_with_decoded_payload(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add("hello", {"msg": "round-trip"})
        fetched = await queue.get_job(job.id)
        assert fetched is not None
        assert fetched.id == job.id
        assert fetched.name == "hello"
        assert fetched.data == {"msg": "round-trip"}
        assert fetched.attempt == 0
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_state_delayed_for_future_scheduled_job(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add(
            "delayed",
            {"msg": "later"},
            delay_ms=3_600_000,
            job_id="stable-delay-id",
        )
        assert job.id == "stable-delay-id"
        state = await queue.get_job_state(job.id)
        assert state == "delayed"
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_counts_reflects_waiting_and_delayed(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(4):
            await queue.add("w", {"msg": f"w{i}"})
        for i in range(2):
            await queue.add("d", {"msg": f"d{i}"}, delay_ms=3_600_000)
        counts = await queue.get_job_counts()
        assert counts["waiting"] == 4
        assert counts["delayed"] == 2
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_counts_filters_by_requested_types(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        await queue.add("w", {"msg": "one"})
        filtered = await queue.get_job_counts("waiting")
        assert sorted(filtered.keys()) == ["waiting"]
        assert filtered["waiting"] == 1
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_jobs_paginates_waiting_state(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(7):
            await queue.add("w", {"msg": f"n{i}"})
        page = await queue.get_jobs("waiting")
        assert len(page) == 7
        assert page[0].data == {"msg": "n0"}
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_jobs_supports_offset_limit(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(5):
            await queue.add("w", {"msg": f"n{i}"})
        sliced = await queue.get_jobs("waiting", offset=1, limit=3)
        assert len(sliced) == 3
        assert sliced[0].data == {"msg": "n1"}
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_pause_resume_reflected_via_get_job_counts(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        await queue.pause()
        assert await queue.is_paused() is True
        counts = await queue.get_job_counts()
        assert counts["paused"] == 1
        await queue.resume()
        assert await queue.is_paused() is False
        counts2 = await queue.get_job_counts()
        assert counts2["paused"] == 0
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_job_preserves_dispatch_name_from_n_field(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        job = await queue.add("send-email", {"msg": "subject"})
        fetched = await queue.get_job(job.id)
        assert fetched is not None
        assert fetched.name == "send-email"
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_get_jobs_unknown_state_raises_value_error(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        with pytest.raises(ValueError):
            await queue.get_jobs("not-a-real-state")
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_consumer_group_option_does_not_error_on_nogroup(
    redis_url: str, queue_name: str
) -> None:
    # Custom consumer group, no consumer ever started — XPENDING under
    # that group returns NOGROUP. The introspector must swallow that
    # and report waiting count instead of erroring out.
    queue = Queue(
        queue_name,
        redis_url=redis_url,
        consumer_group="never-actually-used",
    )
    try:
        await queue.add("w", {"msg": "orphan"})
        counts = await queue.get_job_counts()
        assert counts["waiting"] == 1
        assert counts["active"] == 0
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_count_returns_waiting_plus_active_plus_delayed(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(3):
            await queue.add("w", {"msg": f"w{i}"})
        await queue.add("d", {"msg": "d"}, delay_ms=3_600_000)
        total = await queue.count()
        assert total == 4
    finally:
        await queue.close()


@pytest.mark.asyncio
async def test_individual_count_helpers(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(2):
            await queue.add("w", {"msg": f"w{i}"})
        await queue.add("d", {"msg": "d"}, delay_ms=3_600_000)
        assert await queue.get_waiting_count() == 2
        assert await queue.get_delayed_count() == 1
        assert await queue.get_active_count() == 0
        assert await queue.get_failed_count() == 0
        assert await queue.get_completed_count() == 0
    finally:
        await queue.close()
