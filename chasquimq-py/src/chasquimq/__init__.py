"""ChasquiMQ — the fastest open-source message broker for Redis.

The public surface is asyncio-first: import :class:`Queue` to enqueue
jobs, :class:`Worker` to process them, and :class:`QueueEvents` to
subscribe to lifecycle transitions. Power users can also reach the
native engine handles directly — :class:`Producer`, :class:`Consumer`,
and :class:`Scheduler` are re-exported here from the underlying
``chasquimq._native`` PyO3 extension. The native ``Job`` value type
collides with the high-level :class:`Job` dataclass; the high-level
projection wins the unqualified name on this module. If you need the
native one, import it explicitly via
``from chasquimq._native import Job as NativeJob``.
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
