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
