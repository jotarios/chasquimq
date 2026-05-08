"""ChasquiMQ — the fastest open-source message broker for Redis.

The ergonomic high-level path is asyncio-first: import :class:`Queue`
to enqueue jobs, :class:`Worker` to process them, and :class:`Job` for
the frozen dataclass returned by :meth:`Queue.add` and passed to your
handler. :class:`QueueEvents` subscribes to lifecycle transitions.

The engine-handle public surface is for power users who want raw FFI
access without reaching into a private module: :class:`Producer`,
:class:`Consumer`, and :class:`Scheduler` are re-exported from the
package root (they also live at :mod:`chasquimq._native`).

There is exactly one user-facing :class:`Job` — the high-level
dataclass. The internal PyO3 wire-format pyclass is intentionally
underscore-prefixed (:class:`chasquimq._native._Job`) and not part of
any public surface.

Errors: :class:`UnrecoverableError` (raise from a handler to bypass
retries and route directly to DLQ) and :class:`NotSupportedError`.

Builders: :class:`BackoffSpec`, :class:`MissedFiresPolicy`,
:class:`RepeatPattern`, :class:`RepeatableMeta`, plus the
:data:`Handler` type alias.

Plus :func:`version` for introspection.
"""

import logging as _logging

_logging.getLogger("chasquimq").addHandler(_logging.NullHandler())

from ._native import Consumer, Producer, Scheduler, version
from .errors import NotSupportedError, UnrecoverableError
from .job import Job
from .queue import Queue
from .queue_events import QueueEvent, QueueEvents
from .repeat import BackoffSpec, MissedFiresPolicy, RepeatableMeta, RepeatPattern
from .worker import Handler, Worker


__all__ = [
    "BackoffSpec",
    "Consumer",
    "Handler",
    "Job",
    "MissedFiresPolicy",
    "NotSupportedError",
    "Producer",
    "Queue",
    "QueueEvent",
    "QueueEvents",
    "RepeatPattern",
    "RepeatableMeta",
    "Scheduler",
    "UnrecoverableError",
    "Worker",
    "version",
]
