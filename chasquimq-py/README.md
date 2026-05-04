# chasquimq (Python)

Python bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; Python `asyncio` handlers process them.

> **Status:** Phase 4, slice A1 (scaffold). Only `chasquimq.version()` is exposed today; `Queue` / `Worker` / `Job` / `QueueEvents` ship in subsequent slices. Public API is pre-1.0 and may change.

## Layout

- `src-rs/` — PyO3 Rust bindings, compiled as the `chasquimq._native` extension module.
- `src/chasquimq/` — Python shim (the `src layout`); re-exports the native module.
- `tests/` — pytest harness.

## Build

```bash
cd chasquimq-py
maturin develop          # editable install into the active venv
pytest tests/            # smoke tests
maturin build --release  # wheels under target/wheels/
```

## See also

- [Main repo README](https://github.com/jotarios/chasquimq#readme)
- [Phase 4 design doc](../docs/phase4-pyo3-design.md)

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
