---
title: Concepts
description: How ChasquiMQ is built and why.
sidebar:
  order: 0
---

> **Placeholder.** A content agent will fill in the architecture deep-dives:
> Streams vs. lists, MessagePack on the wire, batched `XACK`, the delayed-job
> sorted set, and the post-1.0 use of Redis 8.x features (`XADD IDMP`,
> `XACKDEL`, `XDELEX`).

## What goes here

The **concepts** section explains the *why* behind ChasquiMQ's design.
Architecture, trade-offs, and the load-bearing constraints that make the
3–5× throughput claim defensible.
