"""Lightweight :class:`Job` value type passed to user handlers.

This is the high-level shim's projection of the engine's ``Job<T>``
envelope. The dataclass is intentionally minimal: the engine streams
jobs via ``XREADGROUP`` / ``XACK`` and does not persist progress, return
values, or per-job state metadata, so there is no mutable
round-trippable state to expose. ``name`` carries the producer-supplied
dispatch name through the engine's stream-level ``n`` field; jobs
produced by pre-name-on-wire producers (or scheduled via the delayed /
repeatable paths that re-encode without ``n``) deliver as ``name=''``.

The optional :attr:`_queue` backreference is set by :class:`Queue` when
it returns a freshly enqueued job, and is consumed by
:meth:`wait_for_result` to issue ``Queue.get_job_result`` polls.
``Worker``-side jobs (constructed inside the consumer's native handler
to deliver to the user's processor) leave it ``None`` — calling
:meth:`wait_for_result` on those raises a clear ``RuntimeError``.
"""

from __future__ import annotations

import asyncio
import time
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .queue import Queue


@dataclass
class Job:
    id: str
    name: str
    data: Any
    attempt: int
    created_at_ms: int

    # Backreference set by `Queue.add` / `Queue.add_bulk` / etc. so
    # `wait_for_result` can call `queue.get_job_result(self.id)`.
    # Excluded from repr / equality to keep the dataclass shape stable
    # for user code that prints / compares Jobs (the queue handle
    # contains a connection pool, both noisy and identity-based).
    _queue: Optional["Queue"] = field(
        default=None, repr=False, compare=False, hash=False
    )

    @property
    def attempts_made(self) -> int:
        """Alias for :attr:`attempt` — matches BullMQ naming for compatibility."""
        return self.attempt

    async def wait_for_result(
        self,
        *,
        timeout: float = 30.0,
        poll_interval: float = 0.1,
    ) -> Any | None:
        """Poll until the engine's stored result for this job is
        readable, or until ``timeout`` elapses, or until the surrounding
        :class:`asyncio.Task` is cancelled.

        Returns the msgpack-decoded handler return value on success.
        Raises :class:`asyncio.TimeoutError` (alias of
        :class:`TimeoutError` on Python 3.11+) on deadline.

        **The void-handler trap.** If the producing worker resolved its
        handler with ``None``, *or* ran without ``store_results=True``,
        no result key is ever written. The polling loop has no way to
        distinguish that case from "the job hasn't completed yet", so
        this method will time out. Mirror the worker's ``store_results``
        config on the consumer side before relying on
        :meth:`wait_for_result`.

        Note:
            After polling for >1s with no result key, this method emits
            a one-shot :class:`RuntimeWarning` to surface the most
            common cause: the worker was started without
            ``store_results=True``, so no result will ever appear.
            Either set ``Worker(store_results=True)`` on the consumer
            side, or switch to :class:`QueueEvents` subscription
            instead of polling.

        **Polling cost.** Default ``poll_interval=0.1`` is fine for one
        or two concurrent waiters; ``N`` simultaneous
        :meth:`wait_for_result` calls fan out to ``N`` ``GET`` round
        trips per interval. For high-fanout workloads (>10 concurrent
        waiters) subscribe to :class:`QueueEvents` instead — the engine
        emits ``completed`` events natively and you avoid the polling
        tax against Redis.

        **TTL race.** A short ``Worker(result_ttl_ms=...)`` plus a long
        ``timeout`` here can race: the result expires mid-wait and this
        method times out even though the handler succeeded. As a rule
        of thumb keep ``result_ttl_ms >= timeout * 2000``.
        """
        if self._queue is None:
            raise RuntimeError(
                "Job.wait_for_result requires a Queue reference; call "
                "from a Job returned by Queue.add / Queue.add_bulk, "
                "not from a Worker handler"
            )
        if timeout <= 0:
            raise ValueError(
                f"timeout must be a positive number of seconds, got {timeout!r}"
            )
        if poll_interval <= 0:
            raise ValueError(
                f"poll_interval must be a positive number of seconds, got {poll_interval!r}"
            )

        started = time.monotonic()
        warned = False

        async def _loop() -> Any | None:
            nonlocal warned
            while True:
                value = await self._queue.get_job_result(self.id)  # type: ignore[union-attr]
                if value is not None:
                    return value
                if not warned and (time.monotonic() - started) > 1.0:
                    warnings.warn(
                        "wait_for_result has been polling for >1s with no "
                        "result key. If the worker was started without "
                        "store_results=True, no result will ever appear. "
                        "Either: (a) ensure Worker(store_results=True) on "
                        "the consumer side, or (b) switch to QueueEvents "
                        "subscription instead of polling.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                    warned = True
                await asyncio.sleep(poll_interval)

        return await asyncio.wait_for(_loop(), timeout=timeout)
