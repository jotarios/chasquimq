"""Lightweight :class:`Job` value type passed to user handlers.

This is the high-level shim's projection of the engine's ``Job<T>``
envelope. Frozen on purpose: the engine streams jobs via
``XREADGROUP`` / ``XACK`` and does not persist progress, return values,
or per-job state metadata, so there is no mutable round-trippable
state to expose. ``name`` is currently always ``''`` for jobs delivered
to a worker — the engine does not yet carry the producer-supplied name
on the wire (mirrors the Node shim's behavior).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Job:
    id: str
    name: str
    data: Any
    attempt: int
    created_at_ms: int
