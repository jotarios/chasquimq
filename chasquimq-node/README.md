# chasquimq (Node.js)

Node.js bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; JavaScript handlers process them.

> **Status:** Phase 3 complete. The high-level shim ships `Queue` / `Worker` / `Job` / `QueueEvents` (with per-job retries, repeatable / cron jobs, DLQ peek/replay, delayed cancel, and `UnrecoverableError` short-circuit to DLQ); the unwrapped engine bindings live under `chasquimq/native`. Public API is pre-1.0 and may change.

## Install

```bash
npm install chasquimq
```

The install pulls a prebuilt native binary for your platform from the matching `chasquimq-<platform>-<arch>` package (e.g. `chasquimq-darwin-arm64`). Supported targets:

- `darwin-arm64`, `darwin-x64`
- `linux-x64-gnu`, `linux-arm64-gnu`
- `win32-x64-msvc`

## Two API layers

```ts
// High-level Queue/Worker shim (TypeScript)
import { Queue, Worker } from "chasquimq";

// Direct NAPI bindings to the Rust engine
import { Producer, Consumer, Promoter } from "chasquimq/native";
```

See the [main repo README](https://github.com/jotarios/chasquimq#readme) for the full pitch, performance numbers, and design docs.

## License

MIT — see [LICENSE](https://github.com/jotarios/chasquimq/blob/main/LICENSE) at the workspace root.
