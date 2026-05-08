---
title: Getting started
description: Install ChasquiMQ and run your first job.
sidebar:
  order: 1
---

> **Stub.** This page is intentionally minimal — a content agent will expand
> it with a full installation matrix (Rust crate, Node, Python, Docker) and
> a worked "hello world" example.

## Quick install

Pick the language you'll be calling ChasquiMQ from:

```bash
# Node.js
npm install @chasquimq/node

# Python (3.9+)
pip install chasquimq

# Rust
cargo add chasquimq
```

You also need a running Redis 8.6+ instance. The fastest way locally:

```bash
docker run -d --name chasquimq-redis -p 6379:6379 redis:8.6
```

## Hello world (Node)

```js
import { Queue, Worker } from "@chasquimq/node";

const queue = new Queue("emails", { connection: { url: "redis://localhost:6379" } });

await queue.add("send", { to: "ada@example.com" });

new Worker("emails", async (job) => {
  console.log("sending email to", job.data.to);
}, { connection: { url: "redis://localhost:6379" } });
```

## Next steps

- Browse the [guides](/guides/) for delayed jobs, retries, and DLQ tooling.
- See the [reference](/reference/) for the full API.
- Read [concepts](/concepts/) to understand the Streams-based design.
