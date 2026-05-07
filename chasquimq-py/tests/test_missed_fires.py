"""High-level shim coverage for `MissedFiresPolicy` round-trip.

These tests confirm the policy is encoded into the stored spec and
returned verbatim by ``Queue.get_repeatable_jobs()``. The engine's
catch-up behavior itself is unit-tested in
``chasquimq/src/repeat.rs``; here we only verify the FFI / shim hop
preserves the policy.
"""

from __future__ import annotations

import os

import pytest

from chasquimq import MissedFiresPolicy, Queue, RepeatPattern


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_default_missed_fires_absent_from_listing(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "mf-default", {"i": 0}, repeat=RepeatPattern.every(60_000)
    )
    listed = await queue.get_repeatable_jobs(10)
    meta = next((m for m in listed if m.key == job.id), None)
    assert meta is not None
    # `Skip` is the engine default and the stored spec omits it; the
    # shim surfaces this as `None` so callers can branch on truthiness.
    assert meta.missed_fires is None
    await queue.remove_repeatable_by_key(job.id)
    await queue.close()


@pytest.mark.asyncio
async def test_fire_once_round_trips_through_listing(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "mf-fire-once",
        {"i": 0},
        repeat=RepeatPattern.cron("0 * * * *", tz="UTC"),
        missed_fires=MissedFiresPolicy.fire_once(),
    )
    listed = await queue.get_repeatable_jobs(10)
    meta = next((m for m in listed if m.key == job.id), None)
    assert meta is not None
    assert meta.missed_fires == MissedFiresPolicy.fire_once()
    await queue.remove_repeatable_by_key(job.id)
    await queue.close()


@pytest.mark.asyncio
async def test_fire_all_carries_max_catchup_through_listing(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "mf-fire-all",
        {"i": 0},
        repeat=RepeatPattern.every(60_000),
        missed_fires=MissedFiresPolicy.fire_all(max_catchup=23),
    )
    listed = await queue.get_repeatable_jobs(10)
    meta = next((m for m in listed if m.key == job.id), None)
    assert meta is not None
    assert meta.missed_fires == MissedFiresPolicy.fire_all(max_catchup=23)
    await queue.remove_repeatable_by_key(job.id)
    await queue.close()


@pytest.mark.asyncio
async def test_explicit_skip_matches_engine_default(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "mf-explicit-skip",
        {"i": 0},
        repeat=RepeatPattern.every(60_000),
        missed_fires=MissedFiresPolicy.skip(),
    )
    listed = await queue.get_repeatable_jobs(10)
    meta = next((m for m in listed if m.key == job.id), None)
    assert meta is not None
    # Explicit Skip serializes to the same omission as the default —
    # the shim normalizes it to `None` on the way out.
    assert meta.missed_fires is None
    await queue.remove_repeatable_by_key(job.id)
    await queue.close()


@pytest.mark.asyncio
async def test_dict_form_accepted_for_advanced_users(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    job = await queue.add(
        "mf-dict",
        {"i": 0},
        repeat=RepeatPattern.every(60_000),
        missed_fires={"kind": "fire-all", "max_catchup": 5},
    )
    listed = await queue.get_repeatable_jobs(10)
    meta = next((m for m in listed if m.key == job.id), None)
    assert meta is not None
    assert meta.missed_fires == MissedFiresPolicy.fire_all(max_catchup=5)
    await queue.remove_repeatable_by_key(job.id)
    await queue.close()


@pytest.mark.asyncio
async def test_missed_fires_without_repeat_raises(
    redis_url: str, queue_name: str
) -> None:
    queue = Queue(queue_name, redis_url=redis_url)
    with pytest.raises(ValueError, match="missed_fires is only meaningful"):
        await queue.add(
            "no-repeat",
            {"i": 0},
            missed_fires=MissedFiresPolicy.fire_once(),
        )
    await queue.close()


def test_missed_fires_policy_negative_max_catchup_rejected() -> None:
    with pytest.raises(ValueError, match=r"max_catchup must be a positive integer"):
        MissedFiresPolicy.fire_all(max_catchup=-1)


def test_missed_fires_policy_zero_max_catchup_rejected() -> None:
    # `max_catchup = 0` would be wire-distinct from but semantically equivalent
    # to Skip — the engine's catch-up loop short-circuits when count >=
    # max_catchup so nothing fires. Almost certainly a caller mistake.
    with pytest.raises(ValueError, match=r"max_catchup must be a positive integer"):
        MissedFiresPolicy.fire_all(max_catchup=0)


def test_missed_fires_from_dict_unknown_kind_raises() -> None:
    # Defense in depth: a wire payload with an unrecognized `kind` must
    # surface as an error rather than silently producing `None` (which
    # would mask data corruption / cross-version drift).
    from chasquimq.queue import _missed_fires_from_dict

    with pytest.raises(ValueError, match=r"unknown missed_fires kind"):
        _missed_fires_from_dict({"kind": "weird"})
