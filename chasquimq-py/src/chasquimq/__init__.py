"""ChasquiMQ — the fastest open-source message broker for Redis.

This package re-exports the native PyO3 extension. Higher-level constructs
(``Queue`` / ``Worker`` / ``Job`` / ``QueueEvents``) ship in subsequent slices.
"""

from ._native import NativeConsumer, NativeJob, NativeProducer, version


class UnrecoverableError(RuntimeError):
    """Raise from a handler to skip retries and route the job straight to
    the DLQ with ``DlqReason::Unrecoverable``.

    The native consumer detects this class by name (``__class__.__name__ ==
    "UnrecoverableError"``) so user code can subclass it without losing the
    short-circuit behavior, as long as the subclass keeps the same name.
    """


__all__ = [
    "NativeConsumer",
    "NativeJob",
    "NativeProducer",
    "UnrecoverableError",
    "version",
]
