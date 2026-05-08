import chasquimq
from chasquimq import Consumer, Job, Producer, Queue, Scheduler, Worker


REDIS_URL = "redis://127.0.0.1:6379"


def test_version_is_non_empty_string() -> None:
    value = chasquimq.version()
    assert isinstance(value, str)
    assert value


def test_minimal_public_surface() -> None:
    """The top-level package exposes the engine handles and the
    high-level surface, but the wire-format ``_Job`` pyclass stays
    underscore-prefixed in ``chasquimq._native``.
    """
    from chasquimq import _native

    assert "Producer" in chasquimq.__all__
    assert "Consumer" in chasquimq.__all__
    assert "Scheduler" in chasquimq.__all__
    assert _native._Job.__name__ == "_Job"


def test_native_classes_reexported_from_top_level() -> None:
    """``Producer`` / ``Consumer`` / ``Scheduler`` at the package root
    are the same objects as their ``chasquimq._native`` counterparts —
    the top-level names are re-exports, not re-implementations.
    """
    from chasquimq import _native

    assert Producer is _native.Producer
    assert Consumer is _native.Consumer
    assert Scheduler is _native.Scheduler


def test_high_level_job_wins_unqualified_name() -> None:
    """``chasquimq.Job`` is the high-level dataclass, not the wire-format type.

    The internal ``_native._Job`` pyclass remains a separate object — pinning
    that invariant here keeps the public-surface guarantee defensible.
    """
    from chasquimq import _native

    assert Job is not _native._Job
    instance = Job(id="abc", name="t", data=None, attempt=1, created_at_ms=0)
    assert instance.id == "abc"


def test_native_producer_constructs_from_native_module() -> None:
    """Producer constructs cleanly against a live Redis (``__new__`` connects)."""
    producer = Producer(REDIS_URL, "py-smoke-flat")
    assert producer.stream_key() == "{chasqui:py-smoke-flat}:stream"
    assert producer.delayed_key() == "{chasqui:py-smoke-flat}:delayed"
    assert producer.dlq_key() == "{chasqui:py-smoke-flat}:dlq"
    assert isinstance(producer.producer_id(), str) and producer.producer_id()


def test_native_consumer_constructs_from_native_module() -> None:
    consumer = Consumer(REDIS_URL, "py-smoke-flat", concurrency=4)
    # Calling shutdown() before run() is a no-op; just proves the binding wired up.
    consumer.shutdown()


def test_native_scheduler_constructs_from_native_module() -> None:
    scheduler = Scheduler(REDIS_URL, "py-smoke-flat")
    scheduler.shutdown()


def test_high_level_classes_still_available() -> None:
    """Round-trip sanity: high-level Queue/Worker still construct."""

    async def _handler(job: Job) -> None:  # pragma: no cover - never invoked
        pass

    queue = Queue("py-smoke-flat", redis_url=REDIS_URL)
    worker = Worker("py-smoke-flat", _handler, redis_url=REDIS_URL)
    assert queue.name == "py-smoke-flat"
    assert worker.name == "py-smoke-flat"


def test_job_attempts_made_aliases_attempt() -> None:
    """``Job.attempts_made`` is a read-only BullMQ-compatible alias for
    the canonical 0-indexed ``attempt`` field."""
    job = Job(id="x", name="t", data=None, attempt=3, created_at_ms=0)
    assert job.attempts_made == 3
    assert job.attempts_made == job.attempt
    # Read-only — assigning to a property without a setter raises.
    import pytest

    with pytest.raises(AttributeError):
        job.attempts_made = 7  # type: ignore[misc]
