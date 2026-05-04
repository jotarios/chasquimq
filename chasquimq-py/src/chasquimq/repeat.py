"""Repeatable / cron job specs and backoff strategies.

These dataclasses are pure value types — they do not touch Redis. They
serialize to dicts that the native producer accepts (:meth:`to_dict`)
and round-trip back through :class:`RepeatableMeta` for listing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class RepeatPattern:
    """Recurring-job schedule descriptor.

    Build instances via the :meth:`cron` or :meth:`every` constructors.
    Direct field access is supported for inspection but the recommended
    surface is the named factories so the ``kind`` discriminant is set
    consistently.
    """

    kind: str
    expression: Optional[str] = None
    tz: Optional[str] = None
    interval_ms: Optional[int] = None

    @staticmethod
    def cron(expression: str, *, tz: Optional[str] = None) -> "RepeatPattern":
        """Return a cron-driven pattern.

        ``expression`` accepts both 5-field (``m h dom mon dow``) and
        6-field (with leading seconds) syntax. ``tz`` may be ``"UTC"`` /
        ``"Z"``, a fixed offset (``"+05:30"``), or any IANA name
        (``"America/New_York"``); IANA names are DST-aware.
        """
        return RepeatPattern(kind="cron", expression=expression, tz=tz)

    @staticmethod
    def every(interval_ms: int) -> "RepeatPattern":
        """Return a fixed-interval pattern.

        First fire lands one ``interval_ms`` after upsert (no immediate
        fire). ``interval_ms`` must be positive — the engine rejects
        zero and the native binding raises :class:`ValueError` up-front.
        """
        return RepeatPattern(kind="every", interval_ms=interval_ms)

    def to_dict(self) -> dict[str, Any]:
        if self.kind == "cron":
            d: dict[str, Any] = {"kind": "cron", "expression": self.expression}
            if self.tz is not None:
                d["tz"] = self.tz
            return d
        if self.kind == "every":
            return {"kind": "every", "interval_ms": self.interval_ms}
        raise ValueError(f"unknown RepeatPattern kind {self.kind!r}")


@dataclass(frozen=True)
class BackoffSpec:
    """Per-job retry backoff override.

    The engine's retry path consults the per-job override before the
    queue-wide default; pair this with ``attempts`` on
    :meth:`Queue.add` to scope retry behavior to a single job.
    """

    kind: str
    delay_ms: int
    max_delay_ms: Optional[int] = None
    multiplier: Optional[float] = None
    jitter_ms: Optional[int] = None

    @staticmethod
    def fixed(delay_ms: int, jitter_ms: Optional[int] = None) -> "BackoffSpec":
        """Return a fixed-delay backoff."""
        return BackoffSpec(kind="fixed", delay_ms=delay_ms, jitter_ms=jitter_ms)

    @staticmethod
    def exponential(
        initial_ms: int,
        *,
        multiplier: float = 2.0,
        max_ms: Optional[int] = None,
        jitter_ms: Optional[int] = None,
    ) -> "BackoffSpec":
        """Return an exponential backoff."""
        return BackoffSpec(
            kind="exponential",
            delay_ms=initial_ms,
            max_delay_ms=max_ms,
            multiplier=multiplier,
            jitter_ms=jitter_ms,
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind, "delay_ms": self.delay_ms}
        if self.max_delay_ms is not None:
            d["max_delay_ms"] = self.max_delay_ms
        if self.multiplier is not None:
            d["multiplier"] = self.multiplier
        if self.jitter_ms is not None:
            d["jitter_ms"] = self.jitter_ms
        return d


@dataclass(frozen=True)
class RepeatableMeta:
    """Wire-compatible projection of a repeatable spec.

    Returned by :meth:`Queue.get_repeatable_jobs`. Carries no payload —
    only the schedule and identity, so listing thousands of specs stays
    cheap. Pair ``key`` with :meth:`Queue.remove_repeatable_by_key` to
    delete.
    """

    key: str
    job_name: str
    pattern: RepeatPattern
    next_fire_ms: int
    limit: Optional[int] = None
    start_after_ms: Optional[int] = None
    end_before_ms: Optional[int] = None
