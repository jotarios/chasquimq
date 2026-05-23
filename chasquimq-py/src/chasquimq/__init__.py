"""ChasquiMQ — the fastest open-source message broker for Redis.

High-level asyncio surface: :class:`Queue`, :class:`Worker`,
:class:`Job`, :class:`QueueEvents`. Power-user engine handles re-exported
from the package root: :class:`Producer`, :class:`Consumer`,
:class:`Scheduler`. There is exactly one user-facing :class:`Job` (the
high-level dataclass); the internal wire-format pyclass at
:class:`chasquimq._native._Job` is intentionally underscore-prefixed
and not part of any public surface.
"""

import logging as _logging

_logging.getLogger("chasquimq").addHandler(_logging.NullHandler())

from ._native import Consumer, Producer, Scheduler, version
from .errors import (
    NotSupportedError,
    UnrecoverableError,
    WaitUntilFinishedTimeoutError,
)
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
    "WaitUntilFinishedTimeoutError",
    "Worker",
    "version",
]
