"""ChasquiMQ — the fastest open-source message broker for Redis.

The public surface is asyncio-first and intentionally small: import
:class:`Queue` to enqueue jobs, :class:`Worker` to process them,
:class:`Job` for the frozen dataclass returned by :meth:`Queue.add` and
passed to your handler, and :class:`QueueEvents` to subscribe to
lifecycle transitions.

Errors: :class:`UnrecoverableError` (raise from a handler to bypass
retries and route directly to DLQ) and :class:`NotSupportedError`.

Builders: :class:`BackoffSpec`, :class:`MissedFiresPolicy`,
:class:`RepeatPattern`, :class:`RepeatableMeta`, plus the
:data:`Handler` type alias.

Plus :func:`version` for introspection.

The low-level engine handles (``Producer``, ``Consumer``,
``Scheduler``) live in :mod:`chasquimq._native` for power users who
need raw FFI access.
"""

import logging as _logging

_logging.getLogger("chasquimq").addHandler(_logging.NullHandler())

from ._native import version
from .errors import NotSupportedError, UnrecoverableError
from .job import Job
from .queue import Queue
from .queue_events import QueueEvent, QueueEvents
from .repeat import BackoffSpec, MissedFiresPolicy, RepeatableMeta, RepeatPattern
from .worker import Handler, Worker


__all__ = [
    "BackoffSpec",
    "Handler",
    "Job",
    "MissedFiresPolicy",
    "NotSupportedError",
    "Queue",
    "QueueEvent",
    "QueueEvents",
    "RepeatPattern",
    "RepeatableMeta",
    "UnrecoverableError",
    "Worker",
    "version",
]
