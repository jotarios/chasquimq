"""ChasquiMQ — the fastest open-source message broker for Redis.

This package re-exports the native PyO3 extension. Higher-level constructs
(``Queue`` / ``Worker`` / ``Job`` / ``QueueEvents``) ship in subsequent slices.
"""

from ._native import NativeProducer, version

__all__ = ["NativeProducer", "version"]
