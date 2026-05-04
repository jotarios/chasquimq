"""Cross-shim wire-format test fixture: Python producer.

Pushes COUNT jobs (i = 0..COUNT-1) onto QUEUE with payloads of shape
{"i": <int>, "tag": <str>} so a worker on either side of the FFI can
verify that each delivered payload round-trips bit-for-bit through
the engine without a translation layer.
"""

from __future__ import annotations

import asyncio
import os
import sys

from chasquimq import Queue


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    count = int(os.environ["COUNT"])
    job_name = os.environ.get("JOB_NAME", "cross-shim")
    tag = os.environ.get("TAG", "py")
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(count):
            await queue.add(job_name, {"i": i, "tag": tag})
    finally:
        await queue.close()

    print(f"[py-producer] enqueued {count} jobs to {queue_name!r} with tag={tag!r}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
