---
title: ChasquiMQ
description: The fastest open-source message broker for Redis.
template: doc
---

# ChasquiMQ

> **Placeholder landing page.** A dedicated landing-page agent will replace
> this with the proper hero, headline numbers, and quickstart cards.

ChasquiMQ is a Redis-backed message broker and background job queue, written
in Rust on `tokio`, with first-class bindings for Node.js and Python.

The pitch: **the fastest open-source message broker for Redis.** Goal is
3–5× the throughput and ≥50% less worker CPU compared to Node.js queues
on the same Redis instance.

## Where to go next

- **[Getting started](/start/getting-started/)** — install and run a "hello world" job.
- **Guides** — task-oriented how-tos (delayed jobs, retries, DLQ, repeatable jobs).
- **Reference** — API surfaces for the Rust engine, Node shim, Python shim, and `chasqui` CLI.
- **Concepts** — why Streams, why MessagePack, why batched `XACK`.
- **Benchmarks** — measured numbers and how to reproduce them.

## Status

ChasquiMQ shipped its 1.0 in May 2026. The engine, delayed jobs, retries,
Node bindings, and Python bindings are all production-ready. See the
[GitHub repo](https://github.com/jotarios/chasquimq) for releases and changelog.
