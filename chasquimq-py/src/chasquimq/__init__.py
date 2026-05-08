"""ChasquiMQ — the fastest open-source message broker for Redis.

The public surface is asyncio-first: import :class:`Queue` to enqueue
jobs, :class:`Worker` to process them, and :class:`QueueEvents` to
subscribe to lifecycle transitions. There is a single user-facing
:class:`Job` — the high-level frozen dataclass returned by
:meth:`Queue.add` and passed to your :class:`Worker` handler. Power
users can also reach the native engine handles directly —
:class:`Producer`, :class:`Consumer`, and :class:`Scheduler` are
re-exported here.
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
