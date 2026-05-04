import os
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as aioredis


REDIS_URL = os.environ.get("CHASQUIMQ_TEST_REDIS_URL", "redis://127.0.0.1:6379")


def _stream_key(queue_name: str) -> str:
    return f"{{chasqui:{queue_name}}}:stream"


def _delayed_key(queue_name: str) -> str:
    return f"{{chasqui:{queue_name}}}:delayed"


def _dlq_key(queue_name: str) -> str:
    return f"{{chasqui:{queue_name}}}:dlq"


def _repeat_key(queue_name: str) -> str:
    return f"{{chasqui:{queue_name}}}:repeat"


@pytest.fixture
def queue_name() -> str:
    return f"py-test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def redis_url() -> str:
    return REDIS_URL


@pytest_asyncio.fixture
async def redis_client(redis_url: str):
    client = aioredis.from_url(redis_url)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def cleanup_keys(redis_client, queue_name: str):
    yield
    pattern = f"{{chasqui:{queue_name}}}*"
    async for key in redis_client.scan_iter(match=pattern):
        await redis_client.delete(key)


def stream_key_for(queue_name: str) -> str:
    return _stream_key(queue_name)


def delayed_key_for(queue_name: str) -> str:
    return _delayed_key(queue_name)


def dlq_key_for(queue_name: str) -> str:
    return _dlq_key(queue_name)


def repeat_key_for(queue_name: str) -> str:
    return _repeat_key(queue_name)
