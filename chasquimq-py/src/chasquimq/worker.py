"""High-level :class:`Worker` — runs an asyncio handler against a queue.

Wraps :class:`chasquimq._native.Consumer` with MessagePack decoding and
a clean shutdown surface. The native :class:`Consumer` itself
auto-embeds a scheduler task so repeatable / cron specs upserted via
:meth:`Queue.add(..., repeat=...)` actually fire on this worker
process. Multiple workers cooperate via the engine's existing
``SET NX EX`` leader election on ``{chasqui:<queue>}:scheduler:lock`` —
only one worker fires at a time.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional

from . import _native
from ._encoding import decode_payload, encode_payload
from .job import Job


Handler = Callable[[Job], Awaitable[Any]]


class Worker:
    """High-level async worker for a single ChasquiMQ queue.

    Construction does not start the engine loop — call :meth:`run`. To
    stop, call :meth:`close`; the engine drains its in-flight handlers
    up to its configured shutdown deadline and then resolves.

    ``concurrency`` defaults to ``100`` to match the Node shim and the
    headline throughput target. Pass ``concurrency=1`` explicitly when
    serial processing is required (e.g. handlers that mutate shared
    state without their own synchronization).
    """

    def __init__(
        self,
        queue_name: str,
        handler: Handler,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        tls: bool = False,
        concurrency: int = 100,
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
        store_results: bool = False,
        result_ttl_ms: Optional[int] = None,
    ) -> None:
        self._queue_name = queue_name
        self._handler = handler
        self._redis_url = (
            redis_url.replace("redis://", "rediss://", 1)
            if tls and redis_url.startswith("redis://")
            else redis_url
        )
        self._run_scheduler = run_scheduler

        consumer_kwargs: dict[str, Any] = {
            "concurrency": concurrency,
            "max_attempts": max_attempts,
            "group": group,
            "events_enabled": events_enabled,
            "delayed_enabled": delayed_enabled,
            "run_scheduler": run_scheduler,
            "store_results": store_results,
        }
        if result_ttl_ms is not None:
            consumer_kwargs["result_ttl_ms"] = result_ttl_ms
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
        if scheduler_tick_ms is not None:
            consumer_kwargs["scheduler_tick_ms"] = scheduler_tick_ms
        self._consumer = _native.Consumer(
            self._redis_url, queue_name, **consumer_kwargs
        )

        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._closed = False

    @property
    def name(self) -> str:
        return self._queue_name

    async def run(self) -> None:
        """Start the engine loop and resolve once it drains.

        Idempotent — calling :meth:`run` more than once awaits the
        in-flight loop instead of starting a second one. Returns when
        the consumer task completes (handler exit, exception, or
        :meth:`close`).
        """
        if self._running:
            assert self._consumer_task is not None
            await self._consumer_task
            return

        self._running = True

        async def native_handler(native_job: Any) -> Optional[bytes]:
            data = decode_payload(bytes(native_job.payload))
            job = Job(
                id=native_job.id,
                name=native_job.name,
                data=data,
                attempt=native_job.attempt,
                created_at_ms=native_job.created_at_ms,
            )
            result = await self._handler(job)
            if result is None:
                return None
            return encode_payload(result)

        self._consumer_task = asyncio.ensure_future(
            self._consumer.run(native_handler)
        )

        try:
            await self._consumer_task
        finally:
            self._running = False

    async def close(self) -> None:
        """Signal shutdown. Safe to call from any coroutine, any number
        of times.

        Trips the consumer's shutdown token; the in-flight :meth:`run`
        returns promptly. ``close`` does not await the engine task
        itself — that avoids the double-await race when a caller
        invokes ``close`` while ``run`` is still in flight.
        """
        if self._closed:
            return
        self._closed = True
        self._consumer.shutdown()

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "Worker":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
