import chasquimq
from chasquimq import (
    Consumer,
    Job,
    Producer,
    Queue,
    Scheduler,
    Worker,
)


REDIS_URL = "redis://127.0.0.1:6379"


def test_version_is_non_empty_string() -> None:
    value = chasquimq.version()
    assert isinstance(value, str)
    assert value


def test_native_classes_reexported_from_top_level() -> None:
    """The flat import shape: native classes available on `chasquimq`."""
    from chasquimq import _native

    assert Producer is _native.Producer
    assert Consumer is _native.Consumer
    assert Scheduler is _native.Scheduler


def test_high_level_job_wins_unqualified_name() -> None:
    """Native ``Job`` does NOT shadow the high-level ``Job`` dataclass."""
    from chasquimq import _native

    assert Job is not _native.Job
    # ``chasquimq.Job`` is the high-level frozen dataclass.
    instance = Job(id="abc", name="t", data=None, attempt=1, created_at_ms=0)
    assert instance.id == "abc"


def test_native_producer_constructs_from_top_level() -> None:
    """Producer constructs cleanly against a live Redis (``__new__`` connects)."""
    producer = Producer(REDIS_URL, "py-smoke-flat")
    assert producer.stream_key() == "{chasqui:py-smoke-flat}:stream"
    assert producer.delayed_key() == "{chasqui:py-smoke-flat}:delayed"
    assert producer.dlq_key() == "{chasqui:py-smoke-flat}:dlq"
    assert isinstance(producer.producer_id(), str) and producer.producer_id()


def test_native_consumer_constructs_from_top_level() -> None:
    consumer = Consumer(REDIS_URL, "py-smoke-flat", concurrency=4)
    # Calling shutdown() before run() is a no-op; just proves the binding wired up.
    consumer.shutdown()


def test_native_scheduler_constructs_from_top_level() -> None:
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
