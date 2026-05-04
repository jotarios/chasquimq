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

    The native consumer detects this class by name (``__class__.__name__
    == "UnrecoverableError"``) so user code can subclass it without losing
    the short-circuit behavior, as long as the subclass keeps the same
    name.
    """
