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
import inspect
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Optional, Tuple, Union

from . import _native
from ._encoding import decode_payload, encode_payload
from ._url import apply_tls
from .job import Job
from .queue_events import QueueEvents


_log = logging.getLogger("chasquimq.worker")


Handler = Callable[[Job], Awaitable[Any]]


WorkerListener = Callable[..., Union[Any, Awaitable[Any]]]
"""A callback registered via :meth:`Worker.on`. May be sync or
``async def``; async callbacks are scheduled on the running loop."""

CredentialProvider = Callable[
    [Optional[str]], Awaitable[Tuple[Optional[str], Optional[str]]]
]
"""See :data:`chasquimq.queue.CredentialProvider`. Re-aliased here so
typed worker code does not have to import from ``chasquimq.queue``."""


class Worker:
    """High-level async worker for a single ChasquiMQ queue.

    Construction does not start the engine loop — call :meth:`run`. To
    stop, call :meth:`close`; the engine drains its in-flight handlers
    up to its configured shutdown deadline and then resolves.

    ``concurrency`` defaults to ``100`` to match the Node shim and the
    headline throughput target. Pass ``concurrency=1`` explicitly when
    serial processing is required (e.g. handlers that mutate shared
    state without their own synchronization).

    ## Events

    Event names mirror the Node shim's ``Worker`` listener interface,
    so existing application code reads naturally across languages.
    Subscribe with :meth:`on`; callbacks may be plain functions or
    ``async def`` coroutines.

    * ``ready``     — ``()``. Fired once when :meth:`run` starts the
      engine loop.
    * ``active``    — ``(job: Job)``. Fired before each handler
      invocation.
    * ``completed`` — ``(job: Job, result)``. Fired after the handler
      returns. The engine acks the job.
    * ``failed``    — ``(job: Job, err: BaseException)``. Fired after
      the handler raises. The exception is re-raised so the engine
      routes the job to retry-or-DLQ.
    * ``error``     — ``(err: BaseException)``. Fired on engine-side
      errors surfaced from the native loop or the drained subscriber.
    * ``closing``   — ``()``. Fired at the start of :meth:`close`.
    * ``closed``    — ``()``. Fired once shutdown completes.
    * ``drained``   — ``()``. Fired when the engine observes a
      full→empty transition on the main stream. Lazily subscribes to
      the cross-process events stream on the first ``on('drained',
      ...)`` call; the subscriber is torn down on :meth:`close`.
      **Cross-process scope:** every worker on this queue receives
      ``drained``, not just this one.
    * ``paused``    — ``()``. Fired when :meth:`pause` is called.
    * ``resumed``   — ``()``. Fired when :meth:`resume` is called.
    * ``progress``  — ``(job: Job, progress: int)``. Fired every time a
      processor calls ``await job.update_progress(n)``. The engine
      writes the persisted progress key first, then emits an
      ``e=progress`` event onto the events stream; the worker
      subscribes to its own :class:`QueueEvents` (lazily spawned the
      first time a ``progress`` listener attaches) and re-emits onto
      this EE so callers see ``(job, n)`` in the same process that ran
      the handler. Disable the events fan-out (and therefore this
      event) by passing ``Worker(events_progress_enabled=False)``.

    Listener names accepted for parity but currently no-op (engine
    doesn't emit the underlying transition yet): ``stalled``. These
    can be wired up without raising; promoted to active events when
    the corresponding engine work lands.
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
        reconnect_max_attempts: Optional[int] = None,
        credential_provider: Optional[CredentialProvider] = None,
        log_max_stream_len: Optional[int] = None,
        log_max_line_bytes: Optional[int] = None,
        events_progress_enabled: Optional[bool] = None,
    ) -> None:
        self._queue_name = queue_name
        self._handler = handler
        self._redis_url = apply_tls(redis_url, tls)
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
        # ``0`` (the engine default) = retry forever. A positive value
        # bounds fred's reconnect loop so a permanently rejecting
        # ``credential_provider`` gives up instead of looping forever.
        if reconnect_max_attempts is not None:
            consumer_kwargs["reconnect_max_attempts"] = reconnect_max_attempts
        if log_max_stream_len is not None:
            consumer_kwargs["log_max_stream_len"] = log_max_stream_len
        if log_max_line_bytes is not None:
            consumer_kwargs["log_max_line_bytes"] = log_max_line_bytes
        if events_progress_enabled is not None:
            consumer_kwargs["events_progress_enabled"] = events_progress_enabled

        if credential_provider is not None:
            # The native consumer captures the running asyncio loop at
            # construction time so fred's reconnect-driven AUTH dispatch
            # can later hand the awaited coroutine back to it. That
            # capture must happen inside a running loop — defer
            # construction to ``run()`` (which is async by definition)
            # rather than ``__init__`` (which historically does not
            # require an active loop).
            self._consumer: Optional[_native.Consumer] = None
            self._deferred_kwargs: Optional[dict[str, Any]] = consumer_kwargs
            self._deferred_kwargs["credential_provider"] = credential_provider
        else:
            self._consumer = _native.Consumer(
                self._redis_url, queue_name, **consumer_kwargs
            )
            self._deferred_kwargs = None

        self._consumer_task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._closed = False
        # Pause intent recorded before the (deferred) native consumer
        # exists; applied once it is constructed in ``run()``.
        self._pending_paused = False
        # Listener API. Mirrors :class:`QueueEvents`'s shape so the
        # mental model is one. Sync and async callbacks both work; an
        # exception from a sync callback is logged and swallowed so a
        # buggy listener cannot crash the worker.
        self._listeners: dict[str, list[WorkerListener]] = defaultdict(list)
        # Lazy embedded :class:`QueueEvents` subscriber for the
        # cross-process ``drained`` + ``progress`` events. ``None`` until
        # a listener for either attaches; torn down in :meth:`close`.
        # Workers that never subscribe pay no extra Redis connections.
        self._internal_events: Optional[QueueEvents] = None
        self._internal_redis_url = redis_url
        self._internal_tls = tls
        # In-flight :class:`Job` instances by id, populated for the
        # duration of each handler invocation so the ``progress``
        # forwarder can surface the same :class:`Job` reference the
        # handler is holding (so ``worker.on('progress', (job, n) ->
        # ...)`` and the handler observe identical state). Entries are
        # removed in the handler's ``finally`` so the map stays bounded
        # to current concurrency.
        self._inflight: dict[str, Job] = {}

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

        # If a credential_provider was passed, the native Consumer was
        # deferred to here so it can capture the now-running asyncio
        # loop. Construct it once on first ``run`` and keep the handle —
        # the binding's ``shutdown()`` is then valid for ``close()``.
        if self._consumer is None:
            assert self._deferred_kwargs is not None
            self._consumer = _native.Consumer(
                self._redis_url, self._queue_name, **self._deferred_kwargs
            )
            # Carry forward a pause requested before the deferred
            # consumer existed so it parks before its first read.
            if self._pending_paused:
                self._consumer.pause()

        self._running = True
        self._emit("ready")

        # If a ``drained`` or ``progress`` listener attached before
        # ``run()`` was called, hold the engine start until the
        # subscriber's first ``XREAD BLOCK`` is in flight. Best-effort:
        # cancellation / error on the subscriber side resolves the
        # wait so the worker startup is never wedged.
        if self._internal_events is not None:
            try:
                await self._internal_events.wait_until_ready()
            except Exception:
                pass

        async def native_handler(native_job: Any) -> Optional[bytes]:
            data = decode_payload(bytes(native_job.payload))
            job = Job(
                id=native_job.id,
                name=native_job.name,
                data=data,
                attempt=native_job.attempt,
                created_at_ms=native_job.created_at_ms,
                _handle=native_job,
            )
            self._inflight[native_job.id] = job
            # ``active`` fires before the handler runs so subscribers
            # building a "currently running" view see jobs even for
            # long-running handlers. Mirrors the Node shim.
            self._emit("active", job)
            try:
                try:
                    result = await self._handler(job)
                except asyncio.CancelledError:
                    # Cancellation (from shutdown / shield bypass) is a
                    # control-flow signal, not a handler failure. The
                    # engine treats the cancelled handler as in-progress
                    # at shutdown; no ``failed`` event fires (a cancelled
                    # handler is not a handler failure).
                    raise
                except BaseException as exc:
                    # ``failed`` fires before re-raising so subscribers see
                    # the exception that triggered the routing decision
                    # (retry vs. DLQ-unrecoverable). The native binding
                    # detects ``UnrecoverableError`` via MRO; this emit
                    # path stays agnostic.
                    self._emit("failed", job, exc)
                    raise
                self._emit("completed", job, result)
                if result is None:
                    return None
                return encode_payload(result)
            finally:
                # Drop the inflight entry so the map stays bounded to
                # current concurrency; a late ``progress`` event arriving
                # after the handler resolved finds no live Job and is
                # silently dropped (matches the Node shim).
                self._inflight.pop(native_job.id, None)

        self._consumer_task = asyncio.ensure_future(
            self._consumer.run(native_handler)
        )

        try:
            await self._consumer_task
        except asyncio.CancelledError:
            # External cancellation (test teardown, shutdown via
            # ``close``). Not an engine error — propagate without
            # firing the ``error`` channel so subscribers don't see
            # spurious shutdown noise.
            raise
        except BaseException as exc:
            # Engine-side errors surfaced from the native loop. Mirrors
            # the Node shim's ``error`` channel.
            self._emit("error", exc)
            raise
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
        self._emit("closing")
        # ``close()`` may be called before ``run()`` (e.g. an aborted
        # async-context-manager path). When a credential_provider
        # deferred construction, the native consumer may not exist yet —
        # there is nothing to drain, so just flag closed.
        if self._consumer is not None:
            self._consumer.shutdown()
        # Tear down the lazy internal subscriber if one was started.
        # Best-effort: swallow errors so a transient Redis blip on
        # close doesn't mask the worker's own shutdown path.
        if self._internal_events is not None:
            try:
                await self._internal_events.close()
            except Exception:
                pass
            self._internal_events = None
        self._emit("closed")

    def pause(self) -> None:
        """Pause this worker's reader at the next batch boundary.

        Jobs already being processed run to completion; no new jobs are
        dispatched until :meth:`resume`. Process-local (does not write
        the cross-process Redis flag — use :meth:`Queue.pause` for
        queue-wide durable pause). Idempotent. Safe to call before
        :meth:`run`: a pause requested while the native consumer is
        still deferred (``credential_provider`` path) is applied as soon
        as the consumer is constructed, so it parks before its first
        read.
        """
        if self._consumer is not None:
            self._consumer.pause()
        else:
            self._pending_paused = True
        # Emit AFTER trip so a listener firing :meth:`pause`
        # synchronously observes consistent state:
        # :meth:`is_paused` returns ``True`` by the time ``paused``
        # fires. Process-local; mirrors the Node shim.
        self._emit("paused")

    def resume(self) -> None:
        """Resume a paused worker. The reader wakes immediately (no
        poll-interval latency for the in-process path). Idempotent.
        """
        self._pending_paused = False
        if self._consumer is not None:
            self._consumer.resume()
        self._emit("resumed")

    def is_paused(self) -> bool:
        """Whether this worker is paused via :meth:`pause`. Does not
        reflect a cross-process :meth:`Queue.pause`.
        """
        if self._consumer is not None:
            return self._consumer.is_paused()
        return self._pending_paused

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_closed(self) -> bool:
        return self._closed

    # --- Listener API surface -----------------------------------------------

    def on(self, event_name: str, callback: WorkerListener) -> None:
        """Register a callback for a worker event.

        See the class docstring for the supported event names. Callbacks
        may be plain functions or ``async def`` coroutines. Listeners
        for ``'drained'`` or ``'progress'`` lazily spawn an internal
        cross-process events-stream subscriber the first time one
        attaches; the subscriber is torn down in :meth:`close`.
        """
        self._listeners[event_name].append(callback)
        if (
            event_name in ("drained", "progress")
            and self._internal_events is None
            and not self._closed
        ):
            self._spawn_internal_subscriber()

    def off(self, event_name: str, callback: WorkerListener) -> None:
        """Remove a previously-registered callback.

        Removes the first matching callback only; passing the same
        callback twice removes both copies via two ``off`` calls. No
        error if the callback is not registered.
        """
        listeners = self._listeners.get(event_name)
        if not listeners:
            return
        try:
            listeners.remove(callback)
        except ValueError:
            pass
        if not listeners:
            self._listeners.pop(event_name, None)

    remove_listener = off
    """Alias matching the Node ``EventEmitter`` naming."""

    def once(self, event_name: str, callback: WorkerListener) -> None:
        """Register a callback that fires exactly once and then removes
        itself.
        """
        async def _wrapper(*args: Any, **kwargs: Any) -> None:
            self.off(event_name, _wrapper)
            res = callback(*args, **kwargs)
            if inspect.isawaitable(res):
                await res

        self.on(event_name, _wrapper)

    def listener_count(self, event_name: Optional[str] = None) -> int:
        if event_name is not None:
            return len(self._listeners.get(event_name, []))
        return sum(len(v) for v in self._listeners.values())

    def _emit(self, event_name: str, *args: Any) -> None:
        listeners = self._listeners.get(event_name)
        if not listeners:
            return
        # Snapshot before invoking so a ``once`` wrapper's ``off``
        # call during dispatch doesn't mutate the live list.
        for cb in list(listeners):
            try:
                result = cb(*args)
            except Exception as exc:
                _log.warning(
                    "Worker listener for %r raised; swallowing: %s",
                    event_name,
                    exc,
                    exc_info=True,
                )
                continue
            if inspect.isawaitable(result):
                task = asyncio.ensure_future(result)
                task.add_done_callback(_log_listener_task_exception)

    def _spawn_internal_subscriber(self) -> None:
        """Lazily start a :class:`QueueEvents` subscriber that forwards
        the engine's cross-process ``drained`` and ``progress`` events
        onto this worker's listener registry. Idempotent.

        Progress event semantics: the engine emits one ``e=progress``
        entry per ``Job.update_progress`` call. This forwarder looks the
        live :class:`Job` up by id in :attr:`_inflight` (populated for
        the duration of the handler's run) so subscribers receive the
        same :class:`Job` reference the handler is holding — identical
        to BullMQ's ``(job, progress)`` shape. Progress events for jobs
        whose handlers have already resolved are dropped silently;
        they would race the cleanup of the inflight map and arrive
        with no live :class:`Job` to dispatch on.

        ``block_ms=1000`` keeps :meth:`close` snappy — the QueueEvents
        default 5s would mean every worker shutdown drags for up to
        5s before ``closed`` fires; 1s + small grace ≈ 1s teardown.
        """
        events = QueueEvents(
            self._queue_name,
            redis_url=self._internal_redis_url,
            tls=self._internal_tls,
            block_ms=1000,
        )

        def _on_drained(_event_id: str) -> None:
            self._emit("drained")

        def _on_progress(payload: dict, _event_id: str) -> None:
            job_id = payload.get("jobId")
            progress = payload.get("progress")
            if job_id is None or progress is None:
                return
            job = self._inflight.get(job_id)
            if job is None:
                return
            try:
                progress_int = int(progress)
            except (TypeError, ValueError):
                return
            # Mirror the persisted progress onto the local Job so
            # listeners and the handler observe consistent state. The
            # handler itself already set this via ``update_progress``;
            # this branch covers listeners that fire before the
            # handler awaits.
            job.progress = progress_int
            self._emit("progress", job, progress_int)

        events.on("drained", _on_drained)
        events.on("progress", _on_progress)
        # Note: :class:`QueueEvents` does not currently expose an
        # explicit ``error`` channel the way Node's ``EventEmitter``
        # does; transient subscriber errors are logged inside the
        # ``_listener_loop``. If a future slice adds one, wire it onto
        # this Worker's ``error`` emitter the same way the Node shim
        # does.
        self._internal_events = events

    async def __aenter__(self) -> "Worker":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


def _log_listener_task_exception(task: "asyncio.Task[Any]") -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _log.warning(
            "Worker async listener task failed: %s", exc, exc_info=exc
        )
