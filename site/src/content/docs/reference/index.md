---
title: Reference
description: API reference for the ChasquiMQ engine, Node and Python shims, the chasqui CLI, error codes, and the on-wire format.
sidebar:
  order: 1
---

Reference is information-oriented. Look up a class, method, option type,
error code, or CLI flag. For tutorials, see [/start](/start/getting-started/).
For how-tos, see [/guides](/guides/). For mental model, see
[/concepts](/concepts/).

Everything here is exhaustive by design. Pages are dense; that is the
job. The voice and density follow the
[Rust standard library reference](https://doc.rust-lang.org/std/) — direct,
factual, no marketing.

## In this section

- **[Node.js API](/reference/node-api/)** — `Queue`, `Worker`, `Job`,
  `QueueEvents`, every option type and every method signature.
- **[Python API](/reference/python-api/)** — `Queue`, `Worker`, `Job`,
  `QueueEvents`, `QueueEvent`, `RepeatPattern`, `BackoffSpec`,
  `MissedFiresPolicy`, `RepeatableMeta`. Mirrors the Node surface.
- **[Rust API](/reference/rust-api/)** — engine internals: `Producer`,
  `Consumer`, `Promoter`, `Scheduler`, every config struct, every
  observability event, the error enum.
- **[CLI](/reference/cli/)** — `chasqui inspect`, `chasqui dlq`,
  `chasqui repeatable`, `chasqui watch`, `chasqui events`. One section
  per subcommand.
- **[Options index](/reference/options/)** — the cross-language
  cheat-sheet you reach for when tuning concurrency, retry budgets,
  payload size, or DLQ depth. Three columns: option, where it lives,
  what it controls.
- **[Error codes](/reference/error-codes/)** — every `CMQ-*` code the
  engine and shims emit, with **When**, **Why**, **Fix**, and
  **See also** for each. Stable across releases.
- **[Wire format](/reference/wire-format/)** — the on-Redis byte
  layout: MessagePack envelope, stream-entry fields, idempotent-add
  header, ack semantics. For the engineer at a `redis-cli` prompt.

## Conventions

Code blocks are language-tagged so syntax highlighting fires. File
paths in prose are code-formatted (`chasquimq/src/producer/mod.rs`).
Per-option defaults are bolded inside the description, not in a
separate column. Multi-language method pairs use synced tabs so the
Node and Python shapes sit side by side.
