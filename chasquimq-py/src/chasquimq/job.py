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

from .errors import WaitUntilFinishedTimeoutError

if TYPE_CHECKING:
    from .queue import Queue
    from .queue_events import QueueEvents


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

    async def wait_until_finished(
        self,
        queue_events: "QueueEvents",
        *,
        timeout: Optional[float] = None,
    ) -> Any | None:
        """Event-driven completion wait.

        Subscribes to the engine's cross-process events stream and
        resolves / raises when the ``completed`` or ``failed`` event
        for **this** job fires. Mirrors the Node shim's
        :meth:`Job.waitUntilFinished`.

        Unlike :meth:`wait_for_result`, this method is event-driven (no
        polling, no Redis ``GET`` per interval) and does not require
        ``Worker(store_results=True)`` to *detect* completion. It does,
        however, require an attached :class:`QueueEvents` subscriber so
        the events-stream traffic actually reaches this process.

        Return / raise semantics:

        * On ``completed``, returns the handler's return value when
          ``store_results=True`` was set on the worker. The value is
          fetched via ``Queue.get_job_result(self.id)`` after the event
          fires. If ``store_results`` was not enabled (or the handler
          returned ``None``), returns ``None``. The events stream
          itself never carries the return value.
        * On ``failed``, raises :class:`RuntimeError` carrying the
          engine-reported ``failedReason`` (the same string surfaced
          on the :class:`Worker`'s ``failed`` event).
        * On ``timeout`` elapse, raises
          :class:`WaitUntilFinishedTimeoutError`.

        **Race window.** If the job completed (or failed) *before*
        this call wires up its listeners, the events-stream event has
        already been dispatched and this method has nothing to
        subscribe to. The ``timeout`` will fire normally. For producers
        that want to await a job that may already have finished, pair
        this with a :meth:`Queue.get_job_state` check, or use
        :meth:`wait_for_result` (which can read a persisted result key
        written before the wait started).
        """
        # Surface a queue/queue_events mismatch up-front rather than
        # letting the wait silently time out (the events stream is
        # per-queue, so a ``QueueEvents`` for a different queue will
        # never fire the per-id channel). Only checked when we have the
        # queue backref; jobs constructed without one (the worker-side
        # path, which has no queue handle) skip the guard.
        if self._queue is not None and queue_events.name != self._queue.name:
            raise ValueError(
                f"Job.wait_until_finished: queue_events is for "
                f"{queue_events.name!r} but this job is on "
                f"{self._queue.name!r} — pass a QueueEvents subscribed "
                f"to the right queue"
            )
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        completed_channel = f"completed:{self.id}"
        failed_channel = f"failed:{self.id}"

        def _on_completed(payload: dict, _event_id: str) -> None:
            if fut.done():
                return
            # Best-effort result fetch. The engine emits ``completed``
            # *before* the per-entry result write lands (events emit
            # is off the ack hot path; result write is on it), so poll
            # briefly. If ``store_results=False`` was set on the
            # worker, every poll returns ``None`` and we resolve with
            # ``None`` — same shape as handlers that return ``None``.
            async def _resolve() -> None:
                if fut.done():
                    return
                value: Any = None
                if self._queue is not None:
                    for _ in range(10):
                        try:
                            v = await self._queue.get_job_result(self.id)
                        except Exception:
                            break
                        if v is not None:
                            value = v
                            break
                        await asyncio.sleep(0.05)
                if not fut.done():
                    fut.set_result(value)

            asyncio.ensure_future(_resolve())

        def _on_failed(payload: dict, _event_id: str) -> None:
            if fut.done():
                return
            reason = payload.get("failedReason") or payload.get("reason") or "job failed"
            fut.set_exception(RuntimeError(reason))

        queue_events.on(completed_channel, _on_completed)
        queue_events.on(failed_channel, _on_failed)

        try:
            if timeout is None:
                return await fut
            try:
                return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            except asyncio.TimeoutError as exc:
                raise WaitUntilFinishedTimeoutError(
                    f"Job.wait_until_finished: no terminal event for "
                    f"{self.id} after {timeout}s"
                ) from exc
        finally:
            queue_events.off(completed_channel, _on_completed)
            queue_events.off(failed_channel, _on_failed)
