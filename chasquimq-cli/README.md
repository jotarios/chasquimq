# chasquimq-cli

`chasqui` — a command-line dashboard for [ChasquiMQ](https://github.com/jotarios/chasquimq).

Phase 4 / Track B of the project. See [`docs/phase4-pyo3-design.md`](../docs/phase4-pyo3-design.md) §8 and §11
for the full command surface and roadmap.

## Status

This crate is in active scaffolding. Today it ships **`chasqui inspect`** — a one-shot
snapshot of a queue's stream depth, pending count, DLQ depth, delayed-set depth, oldest
delayed lag, and repeatable count. Subsequent slices add `dlq peek` / `dlq replay`,
`repeatable list` / `repeatable remove`, `watch`, and `events`.

## Install

From the workspace root:

```bash
cargo install --path chasquimq-cli
```

## Usage

```
chasqui inspect <queue> [--redis-url redis://127.0.0.1:6379] [--group default]
```

`--group` defaults to `default`, which matches the engine's
[`ConsumerConfig::group`](../chasquimq/src/config.rs) default. Pass the value you set
in your worker config if it's different.

### Example

```
$ chasqui inspect smoketest-queue
┌─────────────────────────┬────────────────┐
│ queue: smoketest-queue  ┆ group: default │
╞═════════════════════════╪════════════════╡
│ stream depth            ┆ 3              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ pending                 ┆ 2              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ DLQ depth               ┆ 1              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ delayed depth           ┆ 1              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ oldest delayed lag (ms) ┆ 0              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ repeatable count        ┆ 1              │
└─────────────────────────┴────────────────┘
```

When the consumer group does not exist yet, the `pending` cell renders as
`0 (group not created)` instead of erroring out.

## Sources

| Field | Redis call |
| :--- | :--- |
| stream depth | `XLEN {chasqui:<queue>}:stream` |
| pending | `XPENDING {chasqui:<queue>}:stream <group>` (summary form) |
| DLQ depth | `XLEN {chasqui:<queue>}:dlq` |
| delayed depth | `ZCARD {chasqui:<queue>}:delayed` |
| oldest delayed lag (ms) | `ZRANGE {chasqui:<queue>}:delayed 0 0 WITHSCORES` then `now_ms - score`, clamped at 0 |
| repeatable count | `ZCARD {chasqui:<queue>}:repeat` |

All six commands are issued in a single fred `Pipeline`, so a snapshot is one Redis
round trip.
