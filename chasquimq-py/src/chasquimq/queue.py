"""High-level :class:`Queue` — the asyncio-friendly producer entry point.

Wraps :class:`chasquimq._native.Producer` with MessagePack encoding and
Pythonic option translation. The wire format mirrors the Node shim
exactly — payloads are msgpack-encoded user data only (no ``(name,
data)`` tuple), so a Python producer and a Node worker (or vice versa)
drain the same Redis stream without translation.
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Union

from . import _native
from ._encoding import decode_payload, encode_payload
from .errors import NotSupportedError
from .job import Job
from .repeat import BackoffSpec, MissedFiresPolicy, RepeatableMeta, RepeatPattern


DelayMsLike = Union[int, float, datetime]
"""Anything :meth:`Queue.add` accepts as ``delay_ms``.

* ``int`` — milliseconds before the job becomes processable (BullMQ-compat).
* ``float`` — milliseconds (fractional accepted; truncated to int).
* :class:`datetime.datetime` — absolute fire time. Naive datetimes are
  treated as UTC.

For ergonomic relative durations expressed in seconds, pass a
:class:`datetime.timedelta` via the parallel ``delay`` keyword instead.
"""

DelayLike = DelayMsLike
"""Backwards-compatible alias. Prefer :data:`DelayMsLike`."""

BackoffLike = Union[int, BackoffSpec, dict]
"""Anything :meth:`Queue.add` accepts as ``backoff``.

* ``int`` — fixed delay in milliseconds.
* :class:`BackoffSpec` — typed builder.
* ``dict`` — raw native shape (advanced; bypasses validation).
"""

RepeatLike = Union[RepeatPattern, dict]

MissedFiresLike = Union[MissedFiresPolicy, dict]
"""Anything :meth:`Queue.upsert_repeatable_job` accepts as ``missed_fires``.

* :class:`MissedFiresPolicy` — typed builder (recommended).
* ``dict`` — raw native shape (``{"kind": "skip" | "fire-once" | "fire-all", "max_catchup"?: int}``).
"""


class Queue:
    """High-level producer for a single ChasquiMQ queue.

    Construct one per queue. The native producer pool is created lazily
    on the first :meth:`add` / :meth:`add_bulk` / etc. call. Safe to
    share across asyncio tasks and across threads.
    """

    def __init__(
        self,
        name: str,
        *,
        redis_url: str = "redis://127.0.0.1:6379",
        max_stream_len: Optional[int] = None,
        max_delay_secs: Optional[int] = None,
    ) -> None:
        self._name = name
        self._redis_url = redis_url
        self._max_stream_len = max_stream_len
        self._max_delay_secs = max_delay_secs
        self._producer: Optional[_native.Producer] = None
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_closed(self) -> bool:
        return self._closed

    def _get_producer(self) -> _native.Producer:
        if self._producer is None:
            kwargs: dict[str, Any] = {}
            if self._max_stream_len is not None:
                kwargs["max_stream_len"] = self._max_stream_len
            if self._max_delay_secs is not None:
                kwargs["max_delay_secs"] = self._max_delay_secs
            self._producer = _native.Producer(
                self._redis_url, self._name, **kwargs
            )
        return self._producer

    async def add(
        self,
        name: str,
        data: Any,
        *,
        delay_ms: Optional[DelayMsLike] = None,
        delay: Optional[timedelta] = None,
        attempts: Optional[int] = None,
        backoff: Optional[BackoffLike] = None,
        job_id: Optional[str] = None,
        repeat: Optional[RepeatLike] = None,
        missed_fires: Optional[MissedFiresLike] = None,
    ) -> Job:
        """Enqueue a single job.

        Returns a :class:`Job` whose ``id`` is the engine-minted ULID
        (or the resolved spec key for repeatable upserts). The
        ``data`` round-trips verbatim to the worker handler.

        ``delay_ms`` schedules the job for future delivery in
        **milliseconds** (BullMQ-compatible) — ``int``, ``float``, or a
        :class:`datetime.datetime` for an absolute fire time. For
        seconds-friendly Python ergonomics pass a
        :class:`datetime.timedelta` via the parallel ``delay`` kwarg.
        Supplying both raises :class:`ValueError`.
        """
        if repeat is not None:
            return await self.upsert_repeatable_job(
                name,
                data,
                repeat=_coerce_repeat(repeat),
                attempts=attempts,
                backoff=backoff,
                job_id=job_id,
                missed_fires=missed_fires,
            )
        if missed_fires is not None:
            raise ValueError(
                "missed_fires is only meaningful with `repeat`; "
                "pass repeat=RepeatPattern.cron(...) or .every(...) too"
            )

        if job_id is not None and (
            not isinstance(job_id, str) or not job_id.strip()
        ):
            raise ValueError(
                "Queue.add: job_id must be a non-empty, non-whitespace string"
            )

        effective_delay = _resolve_delay_kwargs(delay_ms, delay)
        delay_ms_int = _coerce_delay_ms(effective_delay)
        absolute_ms = _coerce_absolute_ms(effective_delay)
        opts = _build_add_options(job_id, attempts, backoff, name=name)
        payload = encode_payload(data)
        producer = self._get_producer()

        if absolute_ms is not None:
            if opts is not None:
                job_id_ret = await producer.add_at_with_options(
                    absolute_ms, payload, opts
                )
            else:
                job_id_ret = await producer.add_at(absolute_ms, payload)
        elif delay_ms_int is not None and delay_ms_int > 0:
            if opts is not None:
                job_id_ret = await producer.add_in_with_options(
                    delay_ms_int, payload, opts
                )
            else:
                job_id_ret = await producer.add_in(delay_ms_int, payload)
        elif opts is not None:
            job_id_ret = await producer.add_with_options(payload, opts)
        elif name:
            job_id_ret = await producer.add_with_options(
                payload, {"name": name}
            )
        else:
            job_id_ret = await producer.add(payload)

        return Job(
            id=job_id_ret,
            name=name,
            data=data,
            attempt=0,
            created_at_ms=_now_ms(),
            _queue=self,
        )

    async def add_unique(
        self,
        name: str,
        data: Any,
        *,
        job_id: str,
        delay_ms: Optional[DelayMsLike] = None,
        delay: Optional[timedelta] = None,
        attempts: Optional[int] = None,
        backoff: Optional[BackoffLike] = None,
        repeat: Optional[RepeatLike] = None,
    ) -> Job:
        """Idempotent variant of :meth:`add`. Requires ``job_id``.

        Raises :class:`ValueError` when ``job_id`` is missing or empty.
        Otherwise behaves exactly like ``add(name, data, job_id=...)``.

        Idempotency guarantees differ by path:

        * **Delayed** (``delay`` set, > 0) — strict and cross-process.
          Re-enqueueing the same ``job_id`` while the dedup marker is still
          alive is a no-op at Redis (Lua ``SET NX EX`` on
          ``{{chasqui:<queue>}}:dlid:<job_id>`` gates the ``ZADD``). The
          marker TTL outlives the fire time by 1h so a producer-retry can't
          race a successful promotion. Two different :class:`Queue`
          instances calling ``add_unique`` with the same id will only
          schedule once.
        * **Immediate** (no ``delay``) — strict within a single
          :class:`Queue` instance, not across instances. Redis 8.6
          ``XADD IDMP <producer_id> <job_id>`` dedups at the wire layer,
          but the IDMP scope is the producer id (one per :class:`Queue`).
          For cross-process idempotency on the immediate path, give all
          callers the same ``job_id`` *and* use a ``delay`` so the
          delayed-path SET-NX-EX guard kicks in.

        Immediate-path dedup is also bounded by the stream's
        ``IDMP-MAXSIZE`` LRU; high-cardinality ``job_id`` workloads may
        silently lose dedup for the oldest entries.

        A ``Producer`` mints a new UUID on each construction (process
        restart, ``Producer(...)``); immediate-path dedup is therefore
        not preserved across producer instances even with the same
        ``job_id``. For cross-process / cross-restart strict dedup, use
        ``delay > 0`` (delayed-path uses cross-process Lua dedup on the
        ``:dlid:<job_id>`` key).
        """
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError(
                "Queue.add_unique: job_id must be a non-empty, non-whitespace string"
            )
        return await self.add(
            name,
            data,
            delay_ms=delay_ms,
            delay=delay,
            attempts=attempts,
            backoff=backoff,
            job_id=job_id,
            repeat=repeat,
        )

    async def add_bulk(self, jobs: Sequence[dict]) -> list[Job]:
        """Enqueue many jobs.

        Each entry is a dict with keys ``name``, ``data`` and optional
        ``delay_ms`` (or :class:`datetime.timedelta` ``delay``) /
        ``attempts`` / ``backoff`` / ``job_id``. When all entries lack
        per-job overrides the call routes through the underlying
        ``add_bulk_named`` (per-entry names) for pipelining; otherwise
        the bulk degrades to a per-entry :meth:`add` loop.
        """
        if not jobs:
            return []

        all_simple = all(
            j.get("delay_ms") is None
            and j.get("delay") is None
            and j.get("job_id") is None
            and j.get("attempts") is None
            and j.get("backoff") is None
            and j.get("repeat") is None
            for j in jobs
        )
        if all_simple:
            producer = self._get_producer()
            named: list[tuple[str, bytes]] = [
                (j.get("name") or "", encode_payload(j["data"])) for j in jobs
            ]
            ids = await producer.add_bulk_named(named)
            now = _now_ms()
            return [
                Job(
                    id=ids[i],
                    name=j["name"],
                    data=j["data"],
                    attempt=0,
                    created_at_ms=now,
                    _queue=self,
                )
                for i, j in enumerate(jobs)
            ]

        out: list[Job] = []
        for j in jobs:
            out.append(
                await self.add(
                    j["name"],
                    j["data"],
                    delay_ms=j.get("delay_ms"),
                    delay=j.get("delay"),
                    attempts=j.get("attempts"),
                    backoff=j.get("backoff"),
                    job_id=j.get("job_id"),
                    repeat=j.get("repeat"),
                )
            )
        return out

    async def cancel_delayed(self, job_id: str) -> bool:
        """Atomically remove a delayed job by its stable id.

        Returns ``True`` if the job was still in the delayed ZSET and
        is now removed, ``False`` if it was already promoted into the
        stream (or never existed).
        """
        producer = self._get_producer()
        return await producer.cancel_delayed(job_id)

    async def peek_dlq(self, limit: int = 20) -> list[dict]:
        """Return up to ``limit`` DLQ entries, oldest first."""
        producer = self._get_producer()
        return await producer.peek_dlq(limit)

    async def replay_dlq(self, limit: int = 100) -> int:
        """Move up to ``limit`` DLQ entries back into the main stream.

        Returns the number of entries actually replayed.
        """
        producer = self._get_producer()
        return await producer.replay_dlq(limit)

    async def upsert_repeatable_job(
        self,
        name: str,
        data: Any,
        *,
        repeat: RepeatPattern,
        limit: Optional[int] = None,
        start_after_ms: Optional[int] = None,
        end_before_ms: Optional[int] = None,
        attempts: Optional[int] = None,
        backoff: Optional[BackoffLike] = None,
        job_id: Optional[str] = None,
        missed_fires: Optional[MissedFiresLike] = None,
    ) -> Job:
        """Upsert a repeatable / cron spec. Returns a :class:`Job`
        whose ``id`` is the resolved spec key — pair with
        :meth:`remove_repeatable_by_key` to delete."""
        # `attempts` / `backoff` / `job_id` accepted for API symmetry with
        # :meth:`add`; the engine's repeatable path does not yet thread
        # per-fire retry overrides, so they are ignored at the wire layer.
        del attempts, backoff, job_id

        spec: dict[str, Any] = {
            "key": "",
            "job_name": name,
            "pattern": repeat.to_dict(),
            "payload": encode_payload(data),
        }
        if limit is not None:
            spec["limit"] = limit
        if start_after_ms is not None:
            spec["start_after_ms"] = start_after_ms
        if end_before_ms is not None:
            spec["end_before_ms"] = end_before_ms
        if missed_fires is not None:
            spec["missed_fires"] = _missed_fires_to_dict(missed_fires)

        producer = self._get_producer()
        resolved_key = await producer.upsert_repeatable(spec)
        return Job(
            id=resolved_key,
            name=name,
            data=data,
            attempt=0,
            created_at_ms=_now_ms(),
            _queue=self,
        )

    async def get_repeatable_jobs(self, limit: int = 100) -> list[RepeatableMeta]:
        """List repeatable specs, ordered by ``next_fire_ms`` ascending."""
        producer = self._get_producer()
        metas = await producer.list_repeatable(limit)
        return [_meta_from_dict(m) for m in metas]

    async def get_job_result(self, job_id: str) -> Any:
        """Read the stored result for ``job_id``.

        Returns the msgpack-decoded value the handler returned, or
        ``None`` for three indistinguishable cases: the job has not yet
        completed, the result key already expired (``result_ttl_ms`` on
        the worker side), or no result was ever written (the handler
        returned ``None``, the worker ran with ``store_results=False``,
        or the job was DLQ'd).
        """
        producer = self._get_producer()
        raw = await producer.get_result(job_id)
        if raw is None:
            return None
        return decode_payload(bytes(raw))

    async def get_job_result_bulk(self, job_ids: Sequence[str]) -> list[Any]:
        """Pipelined bulk variant of :meth:`get_job_result`.

        Returns a list aligned by index with ``job_ids``; entries are
        the msgpack-decoded value or ``None`` for the same three cases
        documented on :meth:`get_job_result`.
        """
        if not job_ids:
            return []
        producer = self._get_producer()
        raws = await producer.get_result_bulk(list(job_ids))
        return [None if r is None else decode_payload(bytes(r)) for r in raws]

    async def remove_repeatable_by_key(self, key: str) -> bool:
        """Remove a repeatable spec by its resolved key.

        Returns ``True`` if a spec was removed, ``False`` if no spec
        with that key existed.
        """
        producer = self._get_producer()
        return await producer.remove_repeatable_by_key(key)

    async def close(self) -> None:
        """Drop the cached native producer.

        The native pool tears itself down when all references are
        released; calling :meth:`close` simply discards the handle so
        a future call lazily reconnects with the same options. Safe to
        call multiple times; the :attr:`is_closed` flag flips to
        ``True`` on the first call.
        """
        self._producer = None
        self._closed = True

    async def __aenter__(self) -> "Queue":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _coerce_repeat(r: RepeatLike) -> RepeatPattern:
    if isinstance(r, RepeatPattern):
        return r
    if isinstance(r, dict):
        kind = r.get("kind")
        if kind == "cron":
            return RepeatPattern.cron(r["expression"], tz=r.get("tz"))
        if kind == "every":
            return RepeatPattern.every(int(r["interval_ms"]))
        raise ValueError(f"repeat dict must include kind='cron' or 'every'; got {kind!r}")
    raise TypeError(
        f"repeat must be a RepeatPattern or dict; got {type(r).__name__}"
    )


def _resolve_delay_kwargs(
    delay_ms: Optional[DelayMsLike],
    delay: Optional[timedelta],
) -> Optional[DelayMsLike]:
    """Reconcile the two delay kwargs into a single value for downstream
    coercion. ``delay_ms`` is the canonical milliseconds-flavored kwarg;
    ``delay`` is the parallel :class:`datetime.timedelta` path. Passing
    both is a programmer error and raises ``ValueError``.
    """
    if delay_ms is not None and delay is not None:
        raise ValueError(
            "pass either delay_ms (int/float/datetime) or delay "
            "(timedelta), not both"
        )
    if delay is not None:
        if not isinstance(delay, timedelta):
            raise TypeError(
                f"delay must be a datetime.timedelta; got {type(delay).__name__}"
            )
        return delay
    return delay_ms


def _coerce_delay_ms(
    delay: Optional[Union[DelayMsLike, timedelta]],
) -> Optional[int]:
    if delay is None:
        return None
    if isinstance(delay, datetime):
        return None
    if isinstance(delay, timedelta):
        secs = delay.total_seconds()
        if secs < 0:
            raise ValueError(
                f"delay must be non-negative, got {delay!r}"
            )
        return int(secs * 1000)
    if isinstance(delay, bool):
        raise TypeError(
            "delay_ms must be int/float/datetime; got bool"
        )
    if isinstance(delay, int):
        if delay < 0:
            raise ValueError(f"delay_ms must be non-negative, got {delay}")
        return delay
    if isinstance(delay, float):
        if not math.isfinite(delay) or delay < 0:
            raise ValueError(
                f"delay_ms must be a finite non-negative number, got {delay!r}"
            )
        return int(delay)
    raise TypeError(
        f"delay_ms must be int (ms) / float (ms) / datetime; "
        f"got {type(delay).__name__}"
    )


def _coerce_absolute_ms(
    delay: Optional[Union[DelayMsLike, timedelta]],
) -> Optional[int]:
    if not isinstance(delay, datetime):
        return None
    # Naive datetime → assume UTC. Same convention as `datetime.timestamp()`
    # under Python 3, except we make it explicit so users on systems with
    # an unusual local tz get a deterministic result.
    if delay.tzinfo is None:
        delay = delay.replace(tzinfo=timezone.utc)
    return int(delay.timestamp() * 1000)


def _build_add_options(
    job_id: Optional[str],
    attempts: Optional[int],
    backoff: Optional[BackoffLike],
    name: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    has_name = bool(name)
    if (
        job_id is None
        and attempts is None
        and backoff is None
        and not has_name
    ):
        return None
    out: dict[str, Any] = {}
    if job_id is not None:
        out["id"] = job_id
    if has_name:
        out["name"] = name
    retry = _build_retry_override(attempts, backoff)
    if retry is not None:
        out["retry"] = retry
    return out


def _build_retry_override(
    attempts: Optional[int], backoff: Optional[BackoffLike]
) -> Optional[dict[str, Any]]:
    if attempts is None and backoff is None:
        return None
    out: dict[str, Any] = {}
    if attempts is not None:
        out["max_attempts"] = attempts
    if backoff is not None:
        out["backoff"] = _backoff_to_dict(backoff)
    return out


def _backoff_to_dict(b: BackoffLike) -> dict[str, Any]:
    if isinstance(b, BackoffSpec):
        return b.to_dict()
    if isinstance(b, dict):
        return b
    if isinstance(b, bool):
        raise TypeError("backoff must be int / BackoffSpec / dict; got bool")
    if isinstance(b, int):
        return {"kind": "fixed", "delay_ms": b}
    raise TypeError(
        f"backoff must be int (ms) / BackoffSpec / dict; got {type(b).__name__}"
    )


def _missed_fires_to_dict(p: MissedFiresLike) -> dict[str, Any]:
    if isinstance(p, MissedFiresPolicy):
        return p.to_dict()
    if isinstance(p, dict):
        return p
    raise TypeError(
        f"missed_fires must be a MissedFiresPolicy or dict; got {type(p).__name__}"
    )


def _missed_fires_from_dict(d: Optional[dict]) -> Optional[MissedFiresPolicy]:
    if not d:
        return None
    kind = d.get("kind")
    if kind == "skip":
        return MissedFiresPolicy.skip()
    if kind == "fire-once":
        return MissedFiresPolicy.fire_once()
    if kind == "fire-all":
        return MissedFiresPolicy.fire_all(int(d.get("max_catchup", 0)))
    raise ValueError(f"unknown missed_fires kind: {kind!r}")


def _meta_from_dict(m: dict[str, Any]) -> RepeatableMeta:
    p = m["pattern"]
    if p["kind"] == "cron":
        pattern = RepeatPattern.cron(p["expression"], tz=p.get("tz"))
    elif p["kind"] == "every":
        pattern = RepeatPattern.every(int(p["interval_ms"]))
    else:
        raise NotSupportedError(f"unknown pattern kind on the wire: {p['kind']!r}")
    return RepeatableMeta(
        key=m["key"],
        job_name=m["job_name"],
        pattern=pattern,
        next_fire_ms=int(m["next_fire_ms"]),
        limit=m.get("limit"),
        start_after_ms=m.get("start_after_ms"),
        end_before_ms=m.get("end_before_ms"),
        missed_fires=_missed_fires_from_dict(m.get("missed_fires")),
    )
