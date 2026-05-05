"""High-level :class:`Queue` — the asyncio-friendly producer entry point.

Wraps :class:`chasquimq._native.NativeProducer` with MessagePack
encoding and Pythonic option translation. The wire format mirrors the
Node shim exactly — payloads are msgpack-encoded user data only (no
``(name, data)`` tuple), so a Python producer and a Node worker (or
vice versa) drain the same Redis stream without translation.
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Union

from . import _native
from ._encoding import encode_payload
from .errors import NotSupportedError
from .job import Job
from .repeat import BackoffSpec, RepeatableMeta, RepeatPattern


DelayLike = Union[int, float, datetime, timedelta]
"""Anything :meth:`Queue.add` accepts as ``delay``.

* ``int`` — milliseconds before the job becomes processable (BullMQ-compat).
* ``float`` — seconds (Pythonic; multiplied by 1000 internally).
* :class:`datetime.timedelta` — relative duration.
* :class:`datetime.datetime` — absolute fire time. Naive datetimes are
  treated as UTC.
"""

BackoffLike = Union[int, BackoffSpec, dict]
"""Anything :meth:`Queue.add` accepts as ``backoff``.

* ``int`` — fixed delay in milliseconds.
* :class:`BackoffSpec` — typed builder.
* ``dict`` — raw native shape (advanced; bypasses validation).
"""

RepeatLike = Union[RepeatPattern, dict]


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
        self._producer: Optional[_native.NativeProducer] = None

    @property
    def name(self) -> str:
        return self._name

    def _get_producer(self) -> _native.NativeProducer:
        if self._producer is None:
            kwargs: dict[str, Any] = {}
            if self._max_stream_len is not None:
                kwargs["max_stream_len"] = self._max_stream_len
            if self._max_delay_secs is not None:
                kwargs["max_delay_secs"] = self._max_delay_secs
            self._producer = _native.NativeProducer(
                self._redis_url, self._name, **kwargs
            )
        return self._producer

    async def add(
        self,
        name: str,
        data: Any,
        *,
        delay: Optional[DelayLike] = None,
        attempts: Optional[int] = None,
        backoff: Optional[BackoffLike] = None,
        job_id: Optional[str] = None,
        repeat: Optional[RepeatLike] = None,
    ) -> Job:
        """Enqueue a single job.

        Returns a :class:`Job` whose ``id`` is the engine-minted ULID
        (or the resolved spec key for repeatable upserts). The
        ``data`` round-trips verbatim to the worker handler.
        """
        if repeat is not None:
            return await self.upsert_repeatable_job(
                name,
                data,
                repeat=_coerce_repeat(repeat),
                attempts=attempts,
                backoff=backoff,
                job_id=job_id,
            )

        delay_ms = _coerce_delay_ms(delay)
        absolute_ms = _coerce_absolute_ms(delay)
        is_delayed = absolute_ms is not None or (delay_ms is not None and delay_ms > 0)
        # Engine guard rejects non-empty `AddOptions::name` on `add_in_with_options`
        # / `add_at_with_options` until the slice that wires `n` through the
        # delayed path lands; pass the name only on the immediate XADD.
        opts = _build_add_options(
            job_id, attempts, backoff, name=None if is_delayed else name
        )
        payload = encode_payload(data)
        producer = self._get_producer()

        if absolute_ms is not None:
            if opts is not None:
                job_id_ret = await producer.add_at_with_options(
                    absolute_ms, payload, opts
                )
            else:
                job_id_ret = await producer.add_at(absolute_ms, payload)
        elif delay_ms is not None and delay_ms > 0:
            if opts is not None:
                job_id_ret = await producer.add_in_with_options(
                    delay_ms, payload, opts
                )
            else:
                job_id_ret = await producer.add_in(delay_ms, payload)
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
        )

    async def add_bulk(self, jobs: Sequence[dict]) -> list[Job]:
        """Enqueue many jobs.

        Each entry is a dict with keys ``name``, ``data`` and optional
        ``delay`` / ``attempts`` / ``backoff`` / ``job_id``. When all
        entries lack per-job overrides the call routes through
        :meth:`NativeProducer.add_bulk_named` (per-entry names) for
        pipelining; otherwise the bulk degrades to a per-entry
        :meth:`add` loop.
        """
        if not jobs:
            return []

        all_simple = all(
            j.get("delay") is None
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
                )
                for i, j in enumerate(jobs)
            ]

        out: list[Job] = []
        for j in jobs:
            out.append(
                await self.add(
                    j["name"],
                    j["data"],
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

        producer = self._get_producer()
        resolved_key = await producer.upsert_repeatable(spec)
        return Job(
            id=resolved_key,
            name=name,
            data=data,
            attempt=0,
            created_at_ms=_now_ms(),
        )

    async def get_repeatable_jobs(self, limit: int = 100) -> list[RepeatableMeta]:
        """List repeatable specs, ordered by ``next_fire_ms`` ascending."""
        producer = self._get_producer()
        metas = await producer.list_repeatable(limit)
        return [_meta_from_dict(m) for m in metas]

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
        a future call lazily reconnects with the same options.
        """
        self._producer = None


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


def _coerce_delay_ms(delay: Optional[DelayLike]) -> Optional[int]:
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
        raise TypeError("delay must be int/float/datetime/timedelta; got bool")
    if isinstance(delay, int):
        if delay < 0:
            raise ValueError(f"delay must be non-negative, got {delay}")
        return delay
    if isinstance(delay, float):
        if not math.isfinite(delay) or delay < 0:
            raise ValueError(
                f"delay must be a finite non-negative number, got {delay!r}"
            )
        return int(delay * 1000)
    raise TypeError(
        f"delay must be int (ms) / float (s) / datetime / timedelta; "
        f"got {type(delay).__name__}"
    )


def _coerce_absolute_ms(delay: Optional[DelayLike]) -> Optional[int]:
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
    )
