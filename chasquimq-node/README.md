# chasquimq (Node.js)

Node.js bindings for [ChasquiMQ](https://github.com/jotarios/chasquimq) — the fastest open-source message broker for Redis. The Rust engine pulls jobs; JavaScript handlers process them.

> **Status:** Phase 3 scaffolding. Native bindings, the high-level shim, and tests are forthcoming. Today this package builds, exposes `engineVersion()`, and nothing else.

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
