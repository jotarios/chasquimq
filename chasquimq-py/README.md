# chasquimq (Python)

Python bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; Python `asyncio` handlers process them.

> **Status:** 1.0. abi3 wheels for Python 3.9+ on Linux (x86_64 + aarch64), macOS (x86_64 + aarch64), Windows (x86_64).

## Install

```bash
pip install chasquimq
```

## Quickstart

```python
import asyncio
from chasquimq import Queue, Worker, Job, BackoffSpec, UnrecoverableError


async def send_email(job: Job) -> dict:
    to = job.data["to"]
    print(f"sending to {to} (attempt {job.attempts_made + 1})")
    if "@unrecoverable" in to:
        raise UnrecoverableError(f"hard bounce: {to}")
    return {"sent_at": time.time(), "to": to}


async def main() -> None:
    async with Queue("emails") as queue, \
               Worker("emails", send_email, store_results=True) as worker:

        # Plain enqueue.
        await queue.add("welcome", {"to": "ada@example.com"})

        # Stable jobId — second call with the same id is a no-op (idempotent).
        await queue.add_unique(
            "welcome", {"to": "alice@example.com"},
            job_id="welcome:alice",
        )

        # Per-job retry with exponential backoff.
        await queue.add(
            "welcome", {"to": "grace@flaky.example"},
            attempts=3,
            backoff=BackoffSpec.exponential(100, multiplier=2.0, max_ms=10_000),
        )

        # Delayed enqueue (in milliseconds; for `timedelta` use delay= instead).
        await queue.add("welcome", {"to": "ka@later.example"}, delay_ms=2_000)

        # Block on a single job's result, with timeout.
        job = await queue.add("welcome", {"to": "ada@example.com"})
        result = await job.wait_for_result(timeout=30.0)
        print(result)

        # Drain the worker.
        await worker.run()


asyncio.run(main())
```

## What's in the box

| Surface | What it does |
|---|---|
| `Queue` | Producer + queue inspection. `add` / `add_bulk` / `add_unique` / `get_job_result` / `peek_dlq` / `replay_dlq` / `cancel_delayed` / `get_repeatable_jobs` / `remove_repeatable_by_key`. Async context manager. |
| `Worker` | Consumer pool. asyncio-first dispatch, opt-in result storage (`store_results=True`), graceful shutdown. Async context manager. |
| `Job` | Frozen dataclass returned by `Queue.add`. Has `id`, `name`, `data`, `attempts_made`, `wait_for_result(timeout=)`. |
| `QueueEvents` | Asyncio iterator over the engine events stream. Cross-process pub/sub for `completed` / `failed` / `dlq` / `retry-scheduled` / `delayed`. |
| `BackoffSpec` | Builders: `.fixed(delay_ms)` / `.exponential(initial_ms, multiplier, max_ms, jitter_ms)`. |
| `RepeatPattern` | Builders: `.cron(expr, tz=)` / `.every(interval_ms)`. DST-aware via IANA tz names. |
| `MissedFiresPolicy` | `.skip()` / `.fire_once()` / `.fire_all(max_catchup)` for cron catch-up after scheduler downtime. |
| `UnrecoverableError` | Raise from your handler to bypass retries and route the job directly to DLQ. |

### TLS / `rediss://`

For TLS-fronted Redis (ElastiCache encryption-in-transit, MemoryDB), pass a `rediss://` URL:

```python
async with Queue("emails", redis_url="rediss://my-cluster.cache.amazonaws.com:6379") as queue:
    ...
```

Trust roots come from the system TLS stack (Secure Transport on macOS, OpenSSL on Linux, Schannel on Windows), so AWS Trust CA-signed endpoints work out of the box. For private CAs, install them in the system store.

## Power-user surface

The native engine handles ship from the same top-level package:

```python
from chasquimq import Producer, Consumer, Scheduler
```

There is one user-facing `Job` — the high-level dataclass returned by `Queue.add` and passed to your `Worker` handler. The native binding's wire-format pyclass is internal-only (`chasquimq._native._Job`) and not re-exported (mirrors the Node shim).

## Build from source

```bash
cd chasquimq-py
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop          # editable install
pytest tests/            # smoke + integration tests (requires Redis 8.6+)
maturin build --release  # wheels under target/wheels/
```

## See also

- [Main repo README](https://github.com/jotarios/chasquimq#readme) — pitch, headline numbers, feature comparison
- [Engine internals](https://github.com/jotarios/chasquimq/blob/main/docs/engine.md) — retry semantics, delayed jobs, result backends, observability
- [Phase 4 design doc](https://github.com/jotarios/chasquimq/blob/main/docs/phase4-pyo3-design.md) — the PyO3 binding architecture

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
