"""Cross-process events stream subscriber.

The engine emits transition events (``waiting`` / ``active`` /
``completed`` / ``failed`` / ``retry-scheduled`` / ``delayed`` / ``dlq``
/ ``drained``) onto ``{chasqui:<queue>}:events`` as plain Redis Stream
entries. :class:`QueueEvents` is the asyncio-friendly subscriber: it
``XREAD`` -blocks on that stream and yields :class:`QueueEvent` values.

Implementation note: this uses ``redis-py`` directly rather than the
native binding because the events stream is a generic Redis Stream
(human-readable ASCII fields, not msgpack), so a thin async-redis
client gives us the simplest cross-process subscriber. Mirrors the
Node shim's choice to use ``ioredis`` for the same reason.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

import redis.asyncio as aioredis


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
    """Subscribe to a queue's events stream as an async iterator.

    Usage::

        events = QueueEvents("emails")
        async for ev in events:
            print(ev.name, ev.job_id, ev.fields)

    The subscriber starts from ``$`` (only events emitted after the
    iterator is opened) by default — pass ``last_event_id="0"`` to
    replay history. Iteration ends when :meth:`close` is called.
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
        url = (
            redis_url.replace("redis://", "rediss://", 1)
            if tls and redis_url.startswith("redis://")
            else redis_url
        )
        self._client = aioredis.from_url(url, decode_responses=False)
        self._last_id = last_event_id
        self._block_ms = block_ms
        self._count = count
        self._closed = False

    @property
    def name(self) -> str:
        return self._queue_name

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

    async def close(self) -> None:
        """Stop iteration and release the Redis connection."""
        self._closed = True
        try:
            await self._client.aclose()
        except Exception:
            pass


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
