"""Rate-limiter coverage for the Python shim.

Pins the flat ``rate_limit_*`` kwargs on :class:`Worker` (which land on
engine ``ConsumerConfig.rate_limit`` via the native ``Consumer``):

  1. A configured limiter caps observed completion throughput — a burst of
     jobs drains no faster than roughly ``max`` per ``duration`` window
     (with a generous slack for the token-bucket cold-start burst so the
     assertion is not timing-flaky).
  2. ``rate_limit_group_key`` is reserved and rejected with a clear error.
  3. ``rate_limit_duration_ms`` without ``rate_limit_max`` is rejected.
  4. A zero / non-positive ``max`` or ``duration`` is rejected.

The engine-level correctness (shared bucket across workers, ~0 CPU while
throttled, shutdown-while-throttled) lives in the Rust integration tests
(``chasquimq/tests/rate_limit.rs``) and the cross-shim workflow; this file
pins the Python FFI wiring.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from chasquimq import Queue, Worker
from chasquimq import _native


pytestmark = pytest.mark.asyncio


async def test_rate_limit_caps_throughput(
    redis_url: str, queue_name: str, cleanup_keys
) -> None:
    """A limiter of ``max`` jobs per ``duration`` window must bound the
    observed completion rate. We use a coarse limit (10 jobs / 1000ms) and
    enqueue a burst well past one window's worth, then assert that after a
    fixed observation window only a bounded number completed.

    Slack: the token bucket starts FULL, so the first window admits an
    initial burst of up to ``max`` before settling to the steady-state
    ``max``/``duration``. Over ~1.5 windows we therefore expect at most
    ~``2 * max`` plus jitter; the burst of 60 jobs cannot all drain in
    that time. We assert generously (< burst) so the test proves the cap
    exists without racing exact token accounting.
    """
    limit_max = 10
    duration_ms = 1000
    burst = 60

    completed = 0

    async def handler(_job) -> None:
        nonlocal completed
        completed += 1

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=8,
        read_block_ms=100,
        delayed_enabled=False,
        run_scheduler=False,
        stalled_detector_enabled=False,
        rate_limit_max=limit_max,
        rate_limit_duration_ms=duration_ms,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        queue = Queue(queue_name, redis_url=redis_url)
        try:
            for n in range(burst):
                await queue.add("rl", {"n": n})

            # Observe for ~1.5 windows. With max=10/1000ms and a full-start
            # burst, the ceiling is roughly 2*max = 20 plus a little jitter.
            # 60 jobs cannot all drain in 1.5s under this limiter; assert a
            # generous upper bound (< burst) so timing wobble never flakes.
            await asyncio.sleep(1.5)
            snapshot = completed
        finally:
            await queue.close()
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except (asyncio.TimeoutError, BaseException):
            pass

    assert snapshot >= 1, "limiter must still admit some jobs"
    assert snapshot < burst, (
        f"limiter must throttle: saw {snapshot}/{burst} completed within "
        f"~1.5 windows (expected the cap to hold most of the burst back)"
    )
    # Tight-but-generous ceiling: 2*max (cold-start burst + one window of
    # refill) plus generous slack for scheduler jitter / a partial third
    # window landing inside the 1.5s observation.
    assert snapshot <= 2 * limit_max + 15, (
        f"limiter admitted too many: saw {snapshot} within ~1.5 windows for "
        f"max={limit_max}/{duration_ms}ms"
    )


async def test_rate_limit_group_key_rejected(
    redis_url: str, queue_name: str
) -> None:
    """``rate_limit_group_key`` is reserved in this version and must be
    rejected with a clear, self-describing error from the native layer."""
    async def handler(_job) -> None:
        return None

    with pytest.raises(Exception) as exc_info:
        Worker(
            queue_name,
            handler,
            redis_url=redis_url,
            rate_limit_max=10,
            rate_limit_duration_ms=1000,
            rate_limit_group_key="tenant",
        )
    msg = str(exc_info.value)
    assert "group_key" in msg and "not supported" in msg, (
        f"error must explain the reserved field: {msg}"
    )


async def test_rate_limit_duration_without_max_rejected(
    redis_url: str, queue_name: str
) -> None:
    """``rate_limit_duration_ms`` set without ``rate_limit_max`` is a
    misconfiguration and must be rejected."""
    async def handler(_job) -> None:
        return None

    with pytest.raises(Exception) as exc_info:
        Worker(
            queue_name,
            handler,
            redis_url=redis_url,
            rate_limit_duration_ms=1000,
        )
    assert "rate_limit_max" in str(exc_info.value)


async def test_rate_limit_zero_max_rejected(
    redis_url: str, queue_name: str
) -> None:
    """A non-positive ``rate_limit_max`` must be rejected."""
    async def handler(_job) -> None:
        return None

    with pytest.raises(Exception) as exc_info:
        Worker(
            queue_name,
            handler,
            redis_url=redis_url,
            rate_limit_max=0,
            rate_limit_duration_ms=1000,
        )
    assert "positive" in str(exc_info.value)


async def test_native_consumer_accepts_rate_limit_kwargs(
    redis_url: str, queue_name: str
) -> None:
    """The native ``Consumer`` constructor accepts the three
    ``rate_limit_*`` kwargs directly (the shim just forwards them)."""
    c = _native.Consumer(
        redis_url,
        queue_name,
        rate_limit_max=100,
        rate_limit_duration_ms=1000,
    )
    # Constructing a valid limiter must not raise; the consumer is inert
    # until run(). Prove the reserved group_key path rejects at the native
    # layer too.
    assert c is not None
    with pytest.raises(Exception):
        _native.Consumer(
            redis_url,
            queue_name,
            rate_limit_max=100,
            rate_limit_duration_ms=1000,
            rate_limit_group_key="tenant",
        )
