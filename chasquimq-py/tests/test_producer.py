import time

import msgpack
import pytest

from chasquimq import NativeProducer

from conftest import (
    delayed_key_for,
    dlq_key_for,
    repeat_key_for,
    stream_key_for,
)


pytestmark = pytest.mark.usefixtures("cleanup_keys")


@pytest.mark.asyncio
async def test_add_returns_id_and_writes_to_stream(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({"hello": "world"})
    job_id = await producer.add(payload)
    assert isinstance(job_id, str) and job_id

    length = await redis_client.xlen(stream_key_for(queue_name))
    assert length == 1


@pytest.mark.asyncio
async def test_add_in_zadds_to_delayed_set(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({"x": 1})
    job_id = await producer.add_in(60_000, payload)
    assert isinstance(job_id, str) and job_id

    zcard = await redis_client.zcard(delayed_key_for(queue_name))
    assert zcard == 1


@pytest.mark.asyncio
async def test_add_with_options_explicit_id_is_idempotent(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({"k": "v"})
    explicit_id = "stable-id-1"

    first = await producer.add_in_with_options(
        30_000, payload, {"id": explicit_id}
    )
    second = await producer.add_in_with_options(
        30_000, payload, {"id": explicit_id}
    )
    assert first == explicit_id
    assert second == explicit_id

    zcard = await redis_client.zcard(delayed_key_for(queue_name))
    assert zcard == 1


@pytest.mark.asyncio
async def test_retry_override_round_trips_in_payload(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    inner = msgpack.packb({"task": "send-email"})
    job_id = await producer.add_with_options(
        inner,
        {
            "retry": {
                "max_attempts": 5,
                "backoff": {
                    "kind": "exponential",
                    "delay_ms": 250,
                    "max_delay_ms": 30_000,
                    "multiplier": 2.0,
                    "jitter_ms": 50,
                },
            }
        },
    )
    assert job_id

    entries = await redis_client.xrange(
        stream_key_for(queue_name), min="-", max="+", count=1
    )
    assert len(entries) == 1
    _xid, fields = entries[0]
    payload_bytes = fields[b"d"]
    decoded = msgpack.unpackb(payload_bytes, raw=False)
    assert isinstance(decoded, list) and len(decoded) == 5
    decoded_id, decoded_payload, _created, attempt, retry = decoded
    assert decoded_id == job_id
    assert decoded_payload == inner
    assert attempt == 0
    assert retry == [5, ["exponential", 250, 30_000, 2.0, 50]]


@pytest.mark.asyncio
async def test_cancel_delayed_round_trip(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({"k": 1})
    job_id = await producer.add_in_with_options(
        45_000, payload, {"id": "cancel-me"}
    )
    assert job_id == "cancel-me"
    assert await redis_client.zcard(delayed_key_for(queue_name)) == 1

    removed = await producer.cancel_delayed("cancel-me")
    assert removed is True
    assert await redis_client.zcard(delayed_key_for(queue_name)) == 0

    again = await producer.cancel_delayed("cancel-me")
    assert again is False


@pytest.mark.asyncio
async def test_add_bulk_writes_n_entries(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payloads = [msgpack.packb({"i": i}) for i in range(7)]
    ids = await producer.add_bulk(payloads)
    assert len(ids) == 7
    assert all(isinstance(i, str) and i for i in ids)
    length = await redis_client.xlen(stream_key_for(queue_name))
    assert length == 7


@pytest.mark.asyncio
async def test_peek_dlq_and_replay_dlq_round_trip(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    user_payload = msgpack.packb({"task": "broken"})
    job_id = "dlq-job-1"
    created_ms = int(time.time() * 1000)
    encoded_job = msgpack.packb([job_id, user_payload, created_ms, 3])

    dlq_key = dlq_key_for(queue_name)
    await redis_client.xadd(
        dlq_key,
        {
            "d": encoded_job,
            "source_id": job_id,
            "reason": "panic",
            "detail": "synthetic",
        },
    )

    entries = await producer.peek_dlq(10)
    assert len(entries) == 1
    e = entries[0]
    assert e["source_id"] == job_id
    assert e["reason"] == "panic"
    assert e["detail"] == "synthetic"
    assert e["payload"] == encoded_job

    replayed = await producer.replay_dlq(10)
    assert replayed == 1
    main_len = await redis_client.xlen(stream_key_for(queue_name))
    assert main_len == 1


@pytest.mark.asyncio
async def test_upsert_list_remove_repeatable(
    redis_url: str, queue_name: str, redis_client
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({"job": "send-digest"})
    spec = {
        "job_name": "send-digest",
        "pattern": {"kind": "every", "interval_ms": 60_000},
        "payload": payload,
    }
    key = await producer.upsert_repeatable(spec)
    assert key

    listed = await producer.list_repeatable(10)
    assert len(listed) == 1
    meta = listed[0]
    assert meta["key"] == key
    assert meta["job_name"] == "send-digest"
    assert meta["pattern"]["kind"] == "every"
    assert meta["pattern"]["interval_ms"] == 60_000
    assert meta["next_fire_ms"] > 0

    removed = await producer.remove_repeatable_by_key(key)
    assert removed is True
    again = await producer.remove_repeatable_by_key(key)
    assert again is False
    repeat_len = await redis_client.zcard(repeat_key_for(queue_name))
    assert repeat_len == 0


@pytest.mark.asyncio
async def test_unknown_backoff_kind_is_rejected(
    redis_url: str, queue_name: str
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({})
    with pytest.raises(ValueError) as ei:
        await producer.add_with_options(
            payload,
            {
                "retry": {
                    "max_attempts": 3,
                    "backoff": {"kind": "bogus", "delay_ms": 100},
                }
            },
        )
    assert "bogus" in str(ei.value)


@pytest.mark.asyncio
async def test_unknown_pattern_kind_is_rejected(
    redis_url: str, queue_name: str
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({})
    with pytest.raises(ValueError) as ei:
        await producer.upsert_repeatable(
            {
                "job_name": "x",
                "pattern": {"kind": "weird"},
                "payload": payload,
            }
        )
    assert "weird" in str(ei.value)


@pytest.mark.asyncio
async def test_zero_interval_every_is_rejected(
    redis_url: str, queue_name: str
) -> None:
    producer = NativeProducer(redis_url, queue_name)
    payload = msgpack.packb({})
    with pytest.raises(ValueError):
        await producer.upsert_repeatable(
            {
                "job_name": "x",
                "pattern": {"kind": "every", "interval_ms": 0},
                "payload": payload,
            }
        )
