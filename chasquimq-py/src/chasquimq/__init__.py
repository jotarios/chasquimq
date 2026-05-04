"""ChasquiMQ — the fastest open-source message broker for Redis.

This package's public surface is asyncio-first: import :class:`Queue`
to enqueue jobs, :class:`Worker` to process them, and
:class:`QueueEvents` to subscribe to lifecycle transitions. The native
PyO3 layer (``chasquimq._native``) is reachable for power users who
want the raw engine handles.
"""

from ._native import (
    NativeConsumer,
    NativeJob,
    NativeProducer,
    NativeScheduler,
    version,
)
from .errors import NotSupportedError, UnrecoverableError
from .job import Job
from .queue import Queue
from .queue_events import QueueEvent, QueueEvents
from .repeat import BackoffSpec, RepeatableMeta, RepeatPattern
from .worker import Handler, Worker


__all__ = [
    "BackoffSpec",
    "Handler",
    "Job",
    "NativeConsumer",
    "NativeJob",
    "NativeProducer",
    "NativeScheduler",
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
