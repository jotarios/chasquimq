"""Error classes for the high-level chasquimq shim."""

from __future__ import annotations


class NotSupportedError(RuntimeError):
    """Raised when a caller asks for a feature that is intentionally not
    implemented in v1 (function-reference enqueue, parent/child flows,
    in-stream removal, pause/resume, etc.).

    Detect with ``except NotSupportedError`` rather than string-matching
    error messages.
    """


class UnrecoverableError(RuntimeError):
    """Raise from a handler to skip retries and route the job straight to
    the DLQ with ``DlqReason::Unrecoverable``.

    The native consumer detects this class via an MRO-aware
    ``issubclass`` check against this exact class object, so user code
    can freely subclass ``UnrecoverableError`` (e.g. ``class
    PoisonPill(UnrecoverableError): ...``) and still get the
    short-circuit behavior.
    """


class WaitUntilFinishedTimeoutError(TimeoutError):
    """Raised by :meth:`Job.wait_until_finished` when neither a
    ``completed`` nor a ``failed`` event for the watched job arrives
    within the supplied ``timeout`` seconds.

    Distinct from a failed job: a failed job raises a regular
    :class:`RuntimeError` carrying the engine-reported ``failedReason``.
    This error fires only when the events stream itself goes silent
    (the worker died, the network blipped, or the ``timeout`` was too
    short for the handler). Subclasses :class:`TimeoutError` so callers
    that catch the broad timeout case still match.
    """
