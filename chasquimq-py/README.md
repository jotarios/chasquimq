# chasquimq (Python)

Python bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; Python `asyncio` handlers process them.

> **Status:** Phase 4, slice A4 (high-level shim). Public API is pre-1.0 and may change.

## Layout

- `src-rs/` — PyO3 Rust bindings, compiled as the `chasquimq._native` extension module.
- `src/chasquimq/` — Python shim (the `src layout`); re-exports the native module behind `Queue` / `Worker` / `Job` / `QueueEvents`.
- `tests/` — pytest harness.

## Build

```bash
cd chasquimq-py
maturin develop          # editable install into the active venv
pytest tests/            # smoke + integration tests (needs Redis 8.6+)
maturin build --release  # wheels under target/wheels/
```

## Quickstart

```python
import asyncio

from chasquimq import Queue, Worker, Job, RepeatPattern


async def send_email(job: Job) -> None:
    print(f"sending {job.data}")


async def main() -> None:
    queue = Queue("emails")

    # Enqueue a one-shot job.
    await queue.add("send-email", {"to": "ada@example.com"})

    # Enqueue a recurring job (fires every 60s on this worker process).
    await queue.add(
        "daily-digest",
        {"who": "all"},
        repeat=RepeatPattern.every(60_000),
    )

    # `concurrency` defaults to 100; tune it to your handler's I/O profile.
    worker = Worker("emails", send_email)
    try:
        await worker.run()
    finally:
        await worker.close()
        await queue.close()


asyncio.run(main())
```

## See also

- [Main repo README](https://github.com/jotarios/chasquimq#readme)
- [Phase 4 design doc](../docs/phase4-pyo3-design.md)

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
