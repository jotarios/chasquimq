"""Cross-process events stream subscriber.

The engine emits transition events (``waiting`` / ``active`` /
``completed`` / ``failed`` / ``retry-scheduled`` / ``delayed`` / ``dlq``
/ ``drained``) onto ``{chasqui:<queue>}:events`` as plain Redis Stream
entries. :class:`QueueEvents` is the asyncio-friendly subscriber: it
``XREAD`` -blocks on that stream and yields :class:`QueueEvent` values.

Two surfaces are supported:

* **Async iterator** — ``async for ev in events: ...`` — the original
  Pythonic surface; one consumer, mutually exclusive with the listener
  API.
* **Listener API** — ``events.on("completed", cb)`` —
  ``EventEmitter``-style surface. Callbacks may be plain functions or
  ``async def`` coroutines; async callbacks are scheduled on the
  current loop. Per-id channels ``"completed:<jobId>"`` /
  ``"failed:<jobId>"`` / ``"active:<jobId>"`` let
  :meth:`Job.wait_until_finished` target a single job without paying
  the broadcast dispatch cost.

The first ``.on(...)`` call lazily spawns an internal subscriber task;
:meth:`close` cancels it.

Implementation note: this uses ``redis-py`` directly rather than the
native binding because the events stream is a generic Redis Stream
(human-readable ASCII fields, not msgpack), so a thin async-redis
client gives us the simplest cross-process subscriber. Mirrors the
Node shim's choice to use ``ioredis`` for the same reason.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union

import redis.asyncio as aioredis

from ._url import apply_tls


_log = logging.getLogger("chasquimq.queue_events")

# A listener callback. May be sync (``Callable[..., Any]``) or async
# (``Callable[..., Awaitable[Any]]``). The dispatcher invokes both
# shapes correctly — sync callbacks fire inline (caller's exception
# only logs; never raises into the subscriber loop), async callbacks
# are scheduled on the running loop via ``asyncio.create_task``.
Listener = Callable[..., Union[Any, Awaitable[Any]]]


@dataclass(frozen=True)
class QueueEvent:
    """One event emitted by the engine.

    ``name`` is the engine event identifier (e.g. ``"completed"``).
    ``job_id`` is ``None`` for queue-scoped events (``"drained"``).
    ``job_name`` is the dispatch name from the engine ``n`` field
    (slice 5 of name-on-the-wire) — ``""`` when the engine omitted ``n``
    on the entry (the producer added the job without a name, or the
    event is queue-scoped). Surfaces job kind without msgpack-decoding
    payload.
    ``fields`` carries the remaining decoded fields verbatim. Numeric
    fields documented by the engine schema (``attempt`` / ``backoff_ms``
    / ``delay_ms`` / ``duration_us`` / ``ts``) are decoded into ``int``
    at parse time so subscribers don't have to remember which fields
    need an explicit cast — this mirrors the Node shim's
    ``parseIntSafe`` in ``queue-events.ts``. A malformed entry whose
    value can't be parsed as ``int`` silently falls back to the raw
    string so the iterator never crashes on an unexpected event shape.
    Other fields stay as ``str`` (or raw ``bytes`` for unknown keys).
    """

    name: str
    job_id: Optional[str]
    job_name: str
    fields: dict[str, Any]


class QueueEvents:
    """Subscribe to a queue's events stream.

    Two surfaces, pick one (they share the same Redis connection but
    only one consumer of XREAD at a time):

    Async iterator::

        events = QueueEvents("emails")
        async for ev in events:
            print(ev.name, ev.job_id, ev.fields)

    Listener API (``EventEmitter``-style)::

        events = QueueEvents("emails")
        events.on("completed", lambda payload, event_id: print(payload))
        events.on(f"completed:{job_id}", on_one_completed)  # targeted
        # ... do work ...
        await events.close()

    The subscriber starts from ``$`` (only events emitted after the
    iterator is opened) by default — pass ``last_event_id="0"`` to
    replay history. Iteration / dispatch ends when :meth:`close` is
    called.
    """

    def __init__(
        self,
        queue_name: str,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        tls: bool = False,
        last_event_id: str = "$",
        block_ms: int = 5_000,
        count: int = 100,
    ) -> None:
        self._queue_name = queue_name
        self._stream_key = f"{{chasqui:{queue_name}}}:events"
        self._client = aioredis.from_url(apply_tls(redis_url, tls), decode_responses=False)
        self._last_id = last_event_id
        self._block_ms = block_ms
        self._count = count
        self._closed = False
        # Listener API state. Two-tier map: event-name → list of
        # callbacks; per-id channels live in the same dict under the
        # qualified key (``"completed:<jobId>"``). Plain dict is fine —
        # all mutation is on the asyncio loop thread.
        self._listeners: dict[str, list[Listener]] = defaultdict(list)
        # Backing task for the listener-API subscriber loop. Spawned
        # lazily on the first ``on(...)`` call; ``None`` when no
        # listeners are attached so workers that only use the iterator
        # API pay no extra task.
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._listener_ready: Optional[asyncio.Future[None]] = None

    @property
    def name(self) -> str:
        return self._queue_name

    # --- Async iterator surface ---------------------------------------------

    def __aiter__(self) -> AsyncIterator[QueueEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[QueueEvent]:
        while not self._closed:
            try:
                res = await self._client.xread(
                    {self._stream_key: self._last_id},
                    count=self._count,
                    block=self._block_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._closed:
                    return
                # Transient: short backoff before retrying. XREAD will
                # resume from the same id on the next iteration so no data
                # is lost.
                await asyncio.sleep(0.2)
                continue

            if not res:
                continue

            for _stream_key_bytes, entries in res:
                for entry_id_bytes, fields_bytes in entries:
                    entry_id = _to_str(entry_id_bytes)
                    self._last_id = entry_id
                    yield _parse_event(fields_bytes)
                    if self._closed:
                        return

    # --- Listener API surface -----------------------------------------------

    def on(self, event_name: str, callback: Listener) -> None:
        """Register a callback for a named event.

        ``event_name`` accepts both broadcast event names (``"waiting"``
        / ``"active"`` / ``"completed"`` / ``"failed"`` /
        ``"retry-scheduled"`` / ``"delayed"`` / ``"dlq"`` / ``"drained"``
        / ``"retries-exhausted"``) and per-id qualified names
        (``"completed:<jobId>"`` / ``"failed:<jobId>"`` /
        ``"active:<jobId>"``) for :meth:`Job.wait_until_finished` and
        other targeted subscribers.

        Callbacks may be plain functions or ``async def`` coroutines;
        async callbacks are scheduled on the running loop. A raised
        exception from a sync callback is logged and swallowed — the
        subscriber loop survives. The callback is invoked with one
        argument for queue-scoped events (``drained``) and two
        arguments for per-job events (``payload, event_id``); mirrors
        the Node shim's ``EventEmitter`` arity.

        The first ``on(...)`` call lazily starts the subscriber task —
        prefer attaching listeners before producing work, since events
        emitted before the first ``XREAD BLOCK`` lands are lost (same
        race window as the Node shim).
        """
        self._listeners[event_name].append(callback)
        if self._listener_task is None and not self._closed:
            self._spawn_listener_task()

    def off(self, event_name: str, callback: Listener) -> None:
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

    def once(self, event_name: str, callback: Listener) -> None:
        """Register a callback that fires exactly once and then removes
        itself.
        """
        # Bind the callback as a closure so the wrapper is its own
        # identity for ``off`` lookup.
        async def _wrapper(*args: Any, **kwargs: Any) -> None:
            self.off(event_name, _wrapper)
            res = callback(*args, **kwargs)
            if inspect.isawaitable(res):
                await res

        self.on(event_name, _wrapper)

    def listener_count(self, event_name: Optional[str] = None) -> int:
        """Total number of registered callbacks, optionally for a
        single event name. Used in tests to assert wiring; users
        normally won't need this.
        """
        if event_name is not None:
            return len(self._listeners.get(event_name, []))
        return sum(len(v) for v in self._listeners.values())

    def _spawn_listener_task(self) -> None:
        # Capture a ready future so callers (notably :class:`Worker`'s
        # internal drained subscriber) can await the first XREAD
        # before producing work. The future resolves once we've issued
        # the first BLOCK call so the subscription window is open.
        # Uses ``get_running_loop`` (not ``get_event_loop``) so a
        # sync call from outside a coroutine surfaces as a clear
        # ``RuntimeError`` rather than silently binding to a stale or
        # implicit loop — matches the existing shim's "must be inside
        # an async context" posture.
        loop = asyncio.get_running_loop()
        self._listener_ready = loop.create_future()
        self._listener_task = asyncio.ensure_future(self._listener_loop())

    async def wait_until_ready(self) -> None:
        """Block until the listener subscriber has issued its first
        ``XREAD BLOCK`` (i.e. the subscription window is open). No-op
        when no listeners are attached.
        """
        if self._listener_ready is not None:
            await self._listener_ready

    async def _listener_loop(self) -> None:
        last_id = self._last_id
        first = True
        while not self._closed:
            try:
                xread_task = asyncio.ensure_future(
                    self._client.xread(
                        {self._stream_key: last_id},
                        count=self._count,
                        block=self._block_ms,
                    )
                )
                if first:
                    # Yield once so the XREAD command flushes onto the
                    # socket (redis-py uses an async writer that needs
                    # one loop turn to drain its buffer), then release
                    # the ready gate. After this point the subscription
                    # window is open from Redis's perspective.
                    await asyncio.sleep(0)
                    if (
                        self._listener_ready is not None
                        and not self._listener_ready.done()
                    ):
                        self._listener_ready.set_result(None)
                    first = False
                res = await xread_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._closed:
                    return
                _log.warning("QueueEvents listener loop error: %s", exc)
                await asyncio.sleep(0.2)
                continue

            if not res:
                continue

            for _stream_key_bytes, entries in res:
                for entry_id_bytes, fields_bytes in entries:
                    entry_id = _to_str(entry_id_bytes)
                    last_id = entry_id
                    self._last_id = entry_id
                    ev = _parse_event(fields_bytes)
                    self._dispatch(ev, entry_id)
                    if self._closed:
                        return

    def _dispatch(self, ev: QueueEvent, event_id: str) -> None:
        # Broadcast handlers first.
        broadcast = list(self._listeners.get(ev.name, ()))
        # Per-id targeted handlers for events that carry a job id.
        targeted: list[Listener] = []
        if ev.job_id is not None and ev.name in _PER_ID_EVENTS:
            targeted = list(self._listeners.get(f"{ev.name}:{ev.job_id}", ()))
        if not broadcast and not targeted:
            return

        # Payload shape mirrors the Node shim: a single dict argument
        # for per-job events, ``(event_id,)`` for queue-scoped events
        # (``drained``).
        if ev.job_id is None:
            args: tuple[Any, ...] = (event_id,)
        else:
            payload = {
                "jobId": ev.job_id,
                "name": ev.job_name,
                **ev.fields,
            }
            # Map the engine's ``reason`` field to ``failedReason`` as
            # well, so subscribers using the camelCase event payload key
            # see it under both names.
            if ev.name == "failed" and "reason" in ev.fields:
                payload["failedReason"] = ev.fields["reason"]
            args = (payload, event_id)

        for cb in broadcast + targeted:
            self._invoke(cb, args)

    def _invoke(self, cb: Listener, args: tuple[Any, ...]) -> None:
        try:
            result = cb(*args)
        except Exception as exc:
            # Sync callbacks raising must not kill the subscriber loop.
            _log.warning(
                "QueueEvents listener raised; swallowing: %s", exc, exc_info=True
            )
            return
        if inspect.isawaitable(result):
            # Async callbacks are scheduled on the running loop. We
            # attach a done-callback so an unhandled rejection
            # surfaces in the log rather than as a silent
            # ``Task exception was never retrieved`` warning.
            task = asyncio.ensure_future(result)
            task.add_done_callback(_log_task_exception)

    async def close(self) -> None:
        """Stop iteration / dispatch and release the Redis connection."""
        self._closed = True
        if self._listener_task is not None:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except (asyncio.CancelledError, Exception):
                pass
            self._listener_task = None
        if self._listener_ready is not None and not self._listener_ready.done():
            self._listener_ready.set_exception(RuntimeError("QueueEvents closed"))
        try:
            await self._client.aclose()
        except Exception:
            pass


def _log_task_exception(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _log.warning("QueueEvents async listener task failed: %s", exc, exc_info=exc)


def _to_str(v: Any) -> str:
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("utf-8", errors="replace")
    return str(v)


# Numeric event fields the engine emits as decimal strings. Coerce these
# into ``int`` at parse time so subscribers don't have to remember which
# fields need ``int(...)``. Mirrors the Node shim's ``parseIntSafe`` use
# in ``queue-events.ts`` — a non-numeric value silently falls back to the
# raw string so a malformed entry never crashes the iterator.
_NUMERIC_EVENT_FIELDS: frozenset[str] = frozenset(
    {"attempt", "backoff_ms", "delay_ms", "duration_us", "ts"}
)

# Events for which we emit a per-id targeted channel
# (``"<event>:<jobId>"``) in addition to the broadcast channel.
# Mirrors the Node shim's `active:`, `completed:`, `failed:` per-id
# emit. The targeted channels exist specifically so
# :meth:`Job.wait_until_finished` doesn't have to filter every
# broadcast event by jobId.
_PER_ID_EVENTS: frozenset[str] = frozenset({"active", "completed", "failed"})


def _maybe_int(s: str) -> Any:
    try:
        return int(s)
    except (TypeError, ValueError):
        return s


def _parse_event(fields: dict) -> QueueEvent:
    decoded: dict[str, Any] = {}
    for k, v in fields.items():
        ks = _to_str(k)
        vs = _to_str(v) if isinstance(v, (bytes, bytearray)) else v
        if ks in _NUMERIC_EVENT_FIELDS and isinstance(vs, str):
            vs = _maybe_int(vs)
        decoded[ks] = vs

    name = decoded.pop("e", "")
    job_id_raw = decoded.pop("id", "")
    job_id: Optional[str] = job_id_raw if job_id_raw else None
    # Slice 5: pull `n` out of the field bag and surface as a top-level
    # `job_name`. The engine omits `n` when the producer set no name, so
    # missing → empty string (not None) — keeps the type stable.
    job_name = decoded.pop("n", "")
    return QueueEvent(name=name, job_id=job_id, job_name=job_name, fields=decoded)
