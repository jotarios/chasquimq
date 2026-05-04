"""Cross-shim wire-format test fixture: Python producer.

Pushes COUNT jobs (i = 0..COUNT-1) onto QUEUE with payloads of shape
{"i": <int>, "tag": <str>} so a worker on either side of the FFI can
verify that each delivered payload round-trips bit-for-bit through
the engine without a translation layer.

Env vars:
  QUEUE       — required, queue name (engine derives stream key as `{chasqui:<QUEUE>}:stream`).
  COUNT       — required, number of jobs.
  MODE        — `immediate` (default) | `delayed`. `delayed` exercises the ZSET
                 wire format via a 100ms delay on every job.
  TAG, JOB_NAME, REDIS_URL — optional.
"""

from __future__ import annotations

import asyncio
import os
import sys

from chasquimq import Queue


# Match the Node fixture: 100ms delay per job. Encoded as int milliseconds
# so the Python shim's `delay: int` branch (treats int as ms) lines up.
DELAYED_MS = 100


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    count = int(os.environ["COUNT"])
    job_name = os.environ.get("JOB_NAME", "cross-shim")
    tag = os.environ.get("TAG", "py")
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    mode = os.environ.get("MODE", "immediate").lower()

    if mode not in ("immediate", "delayed"):
        print(f"[py-producer] ERROR: unknown MODE={mode!r}", file=sys.stderr)
        return 1

    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(count):
            if mode == "delayed":
                await queue.add(job_name, {"i": i, "tag": tag}, delay=DELAYED_MS)
            else:
                await queue.add(job_name, {"i": i, "tag": tag})
    finally:
        await queue.close()

    print(
        f"[py-producer] enqueued {count} jobs to {queue_name!r} "
        f"with tag={tag!r} mode={mode!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
