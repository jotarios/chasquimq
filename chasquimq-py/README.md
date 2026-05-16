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

For TLS-fronted Redis (ElastiCache encryption-in-transit, or any non-cluster Redis with TLS), set `tls=True` on `Queue` / `Worker` / `QueueEvents`, or pass a `rediss://` URL directly:

```python
async with Queue("emails", redis_url="redis://my-cluster.cache.amazonaws.com:6379", tls=True) as queue:
    ...
# or:
async with Queue("emails", redis_url="rediss://my-cluster.cache.amazonaws.com:6379") as queue:
    ...
```

Trust roots come from the platform store via `rustls-native-certs`: keychain on macOS, the OS CA bundle on Linux (probed by `openssl-probe`), system store on Windows — so AWS Trust CA-signed endpoints work out of the box. For private CAs, point `SSL_CERT_FILE` at a PEM bundle before launching Python; that env var takes precedence over the platform store.

### Rotating IAM tokens / `credential_provider`

For Redis deployments that use short-lived auth tokens — most notably **AWS ElastiCache IAM auth**, where tokens expire roughly every 15 minutes — pass an async `credential_provider` callback. The engine calls it before every `AUTH` / `HELLO` command (initial connect and every reconnect), so a long-lived `Queue` / `Worker` stays authenticated through token rotation without rebuilding.

```python
from typing import Optional, Tuple

import aioboto3  # or your preferred async AWS SDK

from chasquimq import Queue, Worker


async def elasticache_credentials(
    host: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Called by the engine before every AUTH/HELLO.

    ``host`` is the target server as ``"hostname:port"`` (or ``None`` when
    fred has no specific endpoint to report — e.g. cluster bootstrap).
    Returns ``(username, password)``; either side may be ``None``.
    """
    session = aioboto3.Session()
    async with session.client("elasticache") as ec:
        token = await ec.generate_iam_auth_token(...)
    return ("my-iam-user", token)


async with Queue(
    "emails",
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
) as queue, Worker(
    "emails",
    send_email,
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
) as worker:
    ...
```

Notes:

- **Construction is deferred when a `credential_provider` is supplied.** The callback dispatches back to the asyncio loop that constructed the `Queue` / `Worker`, so the engine waits until the first awaited method (`queue.add`, `worker.run`, ...) to open the pool — that's the moment a running loop is guaranteed.
- **Auth errors trigger reconnect.** The engine's default `reconnect_on_auth_error = true` means a token-fetch failure is retried on the next AUTH, with exponential backoff. Raise from your callback (or return stale credentials) and the next reconnect picks up a fresh token. By default a permanently broken provider retry-loops inside fred forever; bound it with [`reconnect_max_attempts`](#bounding-reconnect-attempts) so it gives up after N attempts instead.
- **Same callback for both `Queue` and `Worker`.** Pass the same async function to each — the native producer and consumer each capture their own asyncio-loop reference internally.

### Bounding reconnect attempts

By default the engine reconnects forever (`reconnect_max_attempts=0`). That's the right behaviour for a transient network blip, but a permanently rejecting `credential_provider` — a revoked IAM user, an expired role — will loop on reconnect indefinitely instead of surfacing the failure. Cap it with the `reconnect_max_attempts` keyword:

```python
async with Queue(
    "emails",
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
    # Give up after 10 failed reconnects instead of looping forever.
    reconnect_max_attempts=10,
) as queue, Worker(
    "emails",
    send_email,
    redis_url="rediss://my-cluster.cache.amazonaws.com:6379",
    credential_provider=elasticache_credentials,
    reconnect_max_attempts=10,
) as worker:
    ...
```

Available on both `Queue` (producer pool) and `Worker` (consumer). `0` or `None` (the default) keeps the unbounded behaviour. Pair a positive cap with alerting on reconnect churn so a bounded failure is loud, not silent.

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
