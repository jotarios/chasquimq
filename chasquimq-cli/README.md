# chasquimq-cli

`chasqui` — a command-line dashboard for [ChasquiMQ](https://github.com/jotarios/chasquimq).

Phase 4 / Track B of the project. See [`docs/phase4-pyo3-design.md`](../docs/phase4-pyo3-design.md) §8 and §11
for the full command surface and roadmap.

## Status

Slice B3 complete. Today the CLI ships:

- `chasqui inspect` — one-shot snapshot of stream depth, pending, DLQ depth, delayed depth, oldest delayed lag, and repeatable count.
- `chasqui dlq peek` — render the next N DLQ entries plus a `reason` histogram.
- `chasqui dlq replay` — atomically replay up to N DLQ entries back into the main stream (resets the attempt counter).
- `chasqui repeatable list` — render repeatable specs ordered by next fire time.
- `chasqui repeatable remove` — remove a repeatable spec by key.
- `chasqui watch` — auto-refreshing inspect table with deltas for stream + DLQ depth.
- `chasqui events` — tail the per-queue events stream with ISO-8601 timestamps.

## Install

### Prebuilt binary (Linux, macOS, Windows)

Each `chasquimq-cli-v*` git tag publishes a [GitHub Release](https://github.com/jotarios/chasquimq/releases) with prebuilt `chasqui` binaries for x86_64 and aarch64 Linux, x86_64 and aarch64 macOS, and x86_64 Windows. The release also ships shell and PowerShell installers that pick the right tarball for the host:

```bash
# macOS / Linux — picks the host's tarball, extracts to $CARGO_HOME/bin
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/jotarios/chasquimq/releases/latest/download/chasquimq-cli-installer.sh | sh

# Windows PowerShell
powershell -c "irm https://github.com/jotarios/chasquimq/releases/latest/download/chasquimq-cli-installer.ps1 | iex"
```

For a specific version, replace `latest` with the tag (e.g., `download/chasquimq-cli-v0.1.0/`).

The release pipeline is driven by [`cargo dist`](https://opensource.axo.dev/cargo-dist/) — see the [`CLI Release` workflow](../.github/workflows/release.yml) and the [Releasing chasquimq-cli](../CONTRIBUTING.md#releasing-chasquimq-cli) section of `CONTRIBUTING.md` for the release flow.

### From source

From the workspace root:

```bash
cargo install --path chasquimq-cli
```

## Usage

### `inspect`

```
chasqui inspect <queue> [--redis-url redis://127.0.0.1:6379] [--group default]
```

`--group` defaults to `default`, which matches the engine's
[`ConsumerConfig::group`](../chasquimq/src/config.rs) default. Pass the value you set
in your worker config if it's different.

### `dlq peek`

```
chasqui dlq peek <queue> [--limit N=20] [--redis-url URL]
```

Renders one row per DLQ entry — `dlq_id`, `source_id`, `reason`, `detail` (truncated
to 60 chars), payload size in bytes — followed by a histogram of `reason → count`
across the entries shown. Payload bytes are intentionally not decoded — they're
opaque msgpack and may carry user-defined types.

```
$ chasqui dlq peek smoketest-queue
┌───────────────────────────┬────────────┬───────────────────┬────────────────────┬───────────────┐
│ dlq peek: smoketest-queue ┆ source_id  ┆ reason            ┆ detail             ┆ payload bytes │
╞═══════════════════════════╪════════════╪═══════════════════╪════════════════════╪═══════════════╡
│ 1777863108519-0           ┆ test-job-1 ┆ retries_exhausted ┆ smoke test fixture ┆ 36            │
└───────────────────────────┴────────────┴───────────────────┴────────────────────┴───────────────┘

┌───────────────────┬───────┐
│ reason            ┆ count │
╞═══════════════════╪═══════╡
│ retries_exhausted ┆ 1     │
└───────────────────┴───────┘
```

### `dlq replay`

```
chasqui dlq replay <queue> [--limit N=100] [--redis-url URL] [--yes]
```

Atomically `XADD`s up to `--limit` DLQ entries back to the main stream and `XDEL`s
them from the DLQ. Each replayed entry's `attempt` counter is reset to 0 so the
job gets a fresh retry budget. Per-job `JobRetryOverride` settings are preserved
verbatim — replay does not silently revert to queue-wide retry config.

Without `--yes`, prints a confirmation prompt to stderr and reads a single line
from stdin. Anything other than `y` / `Y` / `yes` aborts with exit code 1.

```
$ chasqui dlq replay smoketest-queue --limit 1 --yes
replayed 1 of 1 entries
```

### `repeatable list`

```
chasqui repeatable list <queue> [--limit N=100] [--redis-url URL]
```

Lists repeatable specs ordered by next fire time (ascending). The payload bytes
are intentionally not fetched — listing thousands of specs returns one bulk
string per spec only.

```
$ chasqui repeatable list emails
┌──────────────────────────┬─────────────┬───────────────┬────┬────────────────┬───────┐
│ repeatable: emails       ┆ job_name    ┆ pattern       ┆ tz ┆ next fire (ms) ┆ limit │
╞══════════════════════════╪═════════════╪═══════════════╪════╪════════════════╪═══════╡
│ fixture-job::every:60000 ┆ fixture-job ┆ every 60000ms ┆ -  ┆ 1777863248084  ┆ -     │
└──────────────────────────┴─────────────┴───────────────┴────┴────────────────┴───────┘
```

For `Cron` patterns, `tz` shows the configured timezone (`UTC` when unset). For
`Every` patterns, `tz` is `-` and the pattern column carries `every Nms`.

### `repeatable remove`

```
chasqui repeatable remove <queue> <key> [--redis-url URL]
```

Atomically `ZREM`s the spec from the repeat ZSET and `DEL`s its spec hash in a
single Lua round trip — no half-removed state is observable to a concurrent
scheduler tick. Prints `removed` on success or `not found` if no spec with that
key existed.

### `watch`

```
chasqui watch <queue> [--interval-ms 1000] [--redis-url URL] [--group default]
```

Same fields as `inspect`, refreshed every `--interval-ms` until Ctrl+C. Adds a
`Δ` column for stream depth and DLQ depth, computed against the previous tick
(`+N` for positive deltas, plain `-N` for negative, `0` for steady-state). The
first tick has no previous snapshot so the delta column is omitted from that
render. The terminal is cleared and redrawn each tick — no alternate screen, so
the final state stays on-screen after exit.

Each tick is a single fred `Pipeline` round trip — same six commands as
`inspect`, batched.

```
$ chasqui watch smoketest-queue --interval-ms 500
chasqui watch smoketest-queue  refresh=500ms  2026-05-04T13:21:27.950Z  (Ctrl+C to exit)
┌─────────────────────────┬───────────────────────┬────┐
│ queue: smoketest-queue  ┆ group: default        ┆ Δ  │
╞═════════════════════════╪═══════════════════════╪════╡
│ stream depth            ┆ 1                     ┆ +1 │
│ pending                 ┆ 0 (group not created) ┆ -  │
│ DLQ depth               ┆ 0                     ┆ 0  │
│ delayed depth           ┆ 0                     ┆ -  │
│ oldest delayed lag (ms) ┆ -                     ┆ -  │
│ repeatable count        ┆ 0                     ┆ -  │
└─────────────────────────┴───────────────────────┴────┘
```

### `events`

```
chasqui events <queue> [--from <id>] [--redis-url URL]
```

Tails `{chasqui:<queue>}:events` over `XREAD BLOCK` and prints one line per event:

```
<ts_iso8601>  <stream_id>  <event>  id=<job_id> [k=v ...]
```

`--from` defaults to `$` (only new events). Pass `0` to replay history from
the start of the stream (or any specific stream id to resume). Extra fields are
sorted by key for deterministic output and values >80 chars are truncated with
`...`. The `e`, `id`, and `ts` fields are pulled out into the leading columns;
all other event-specific fields (`attempt`, `reason`, `duration_us`,
`backoff_ms`, `delay_ms`) are rendered as sorted `k=v` pairs.

```
$ chasqui events smoketest-queue
2026-05-04T13:21:39.000Z 1777900899221-0 completed id=testjob attempt=1 duration_us=1234
2026-05-04T13:21:55.000Z 1777900915849-0 drained id=-
2026-05-04T13:21:55.000Z 1777900915942-0 retry-scheduled id=job-1 attempt=2 backoff_ms=500
2026-05-04T13:21:55.000Z 1777900916019-0 dlq id=job-2 attempt=5 reason=retries_exhausted
```

Schema reference: [`chasquimq/src/events.rs`](../chasquimq/src/events.rs) — the
engine writes `e=<event>`, `id=<job_id>`, `ts=<unix_ms>` plus per-event fields.

## Sources

| Field | Redis call |
| :--- | :--- |
| stream depth | `XLEN {chasqui:<queue>}:stream` |
| pending | `XPENDING {chasqui:<queue>}:stream <group>` (summary form) |
| DLQ depth | `XLEN {chasqui:<queue>}:dlq` |
| delayed depth | `ZCARD {chasqui:<queue>}:delayed` |
| oldest delayed lag (ms) | `ZRANGE {chasqui:<queue>}:delayed 0 0 WITHSCORES` then `now_ms - score`, clamped at 0 |
| repeatable count | `ZCARD {chasqui:<queue>}:repeat` |

`inspect` issues all six in a single fred `Pipeline`, so a snapshot is one Redis
round trip. `dlq peek` / `dlq replay` / `repeatable list` / `repeatable remove`
all reuse the engine `Producer` directly — no parallel script paths.

When the consumer group does not exist yet, `inspect`'s `pending` cell renders
as `0 (group not created)` instead of erroring out.
