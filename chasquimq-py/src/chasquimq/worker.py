"""High-level :class:`Worker` — runs an asyncio handler against a queue.

Wraps :class:`chasquimq._native.NativeConsumer` with MessagePack
decoding and a clean shutdown surface. Auto-spawns an embedded
:class:`chasquimq._native.NativeScheduler` so repeatable / cron specs
upserted via :meth:`Queue.add(..., repeat=...)` actually fire on this
worker process. Multiple workers cooperate via the engine's existing
``SET NX EX`` leader election on ``{chasqui:<queue>}:scheduler:lock`` —
only one worker fires at a time.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from . import _native
from ._encoding import decode_payload
from .job import Job


Handler = Callable[[Job], Awaitable[Any]]


class Worker:
    """High-level async worker for a single ChasquiMQ queue.

    Construction does not start the engine loop — call :meth:`run`. To
    stop, call :meth:`close`; the engine drains its in-flight handlers
    up to its configured shutdown deadline and then resolves.
    """

    def __init__(
        self,
        queue_name: str,
        handler: Handler,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        concurrency: int = 1,
        max_attempts: int = 25,
        group: str = "default",
        consumer_id: Optional[str] = None,
        read_block_ms: Optional[int] = None,
        read_count: Optional[int] = None,
        claim_min_idle_ms: Optional[int] = None,
        max_payload_bytes: Optional[int] = None,
        dlq_max_stream_len: Optional[int] = None,
        events_enabled: bool = True,
        delayed_enabled: bool = True,
        run_scheduler: bool = True,
        scheduler_tick_ms: Optional[int] = None,
    ) -> None:
        self._queue_name = queue_name
        self._handler = handler
        self._redis_url = redis_url
        self._run_scheduler = run_scheduler

        consumer_kwargs: dict[str, Any] = {
            "concurrency": concurrency,
            "max_attempts": max_attempts,
            "group": group,
            "events_enabled": events_enabled,
            "delayed_enabled": delayed_enabled,
        }
        if consumer_id is not None:
            consumer_kwargs["consumer_id"] = consumer_id
        if read_block_ms is not None:
            consumer_kwargs["read_block_ms"] = read_block_ms
        if read_count is not None:
            consumer_kwargs["read_count"] = read_count
        if claim_min_idle_ms is not None:
            consumer_kwargs["claim_min_idle_ms"] = claim_min_idle_ms
        if max_payload_bytes is not None:
            consumer_kwargs["max_payload_bytes"] = max_payload_bytes
        if dlq_max_stream_len is not None:
            consumer_kwargs["dlq_max_stream_len"] = dlq_max_stream_len
        self._consumer = _native.NativeConsumer(
            redis_url, queue_name, **consumer_kwargs
        )

        self._scheduler: Optional[_native.NativeScheduler] = None
        if run_scheduler:
            sched_kwargs: dict[str, Any] = {}
            if scheduler_tick_ms is not None:
                sched_kwargs["tick_interval_ms"] = scheduler_tick_ms
            self._scheduler = _native.NativeScheduler(
                redis_url, queue_name, **sched_kwargs
            )

        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._scheduler_task: Optional[asyncio.Task[None]] = None
        self._running = False

    @property
    def name(self) -> str:
        return self._queue_name

    async def run(self) -> None:
        """Start the engine loop and resolve once it drains.

        Idempotent — calling :meth:`run` more than once awaits the
        in-flight loop instead of starting a second one.
        """
        if self._running:
            assert self._consumer_task is not None
            await self._consumer_task
            return

        self._running = True

        async def native_handler(native_job: Any) -> None:
            data = decode_payload(bytes(native_job.payload))
            job = Job(
                id=native_job.id,
                name="",
                data=data,
                attempt=native_job.attempt,
                created_at_ms=native_job.created_at_ms,
            )
            await self._handler(job)

        self._consumer_task = asyncio.ensure_future(
            self._consumer.run(native_handler)
        )
        if self._scheduler is not None:
            self._scheduler_task = asyncio.ensure_future(self._scheduler.run())

        try:
            await self._consumer_task
        finally:
            if self._scheduler_task is not None:
                self._scheduler.shutdown()  # type: ignore[union-attr]
                try:
                    await self._scheduler_task
                except Exception:
                    pass
            self._running = False

    async def close(self) -> None:
        """Signal shutdown and await the engine drain."""
        self._consumer.shutdown()
        if self._scheduler is not None:
            self._scheduler.shutdown()
        if self._consumer_task is not None:
            try:
                await self._consumer_task
            except Exception:
                pass
        if self._scheduler_task is not None:
            try:
                await self._scheduler_task
            except Exception:
                pass
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running
