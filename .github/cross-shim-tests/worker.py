"""Cross-shim wire-format test fixture: Python worker.

Consumes COUNT jobs from QUEUE. Each payload must be a dict with
integer ``i`` in [0, COUNT) and string ``tag`` matching EXPECT_TAG.
Exits 0 on full distinct-id coverage within TIMEOUT_SECS, else 1.
"""

from __future__ import annotations

import asyncio
import os
import sys

from chasquimq import Job, Queue, Worker


async def main() -> int:
    queue_name = os.environ["QUEUE"]
    count = int(os.environ["COUNT"])
    expect_tag = os.environ.get("EXPECT_TAG", "py")
    timeout_secs = float(os.environ.get("TIMEOUT_SECS", "30"))
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

    seen: set[int] = set()
    done = asyncio.Event()
    errors: list[str] = []

    async def handler(job: Job) -> None:
        data = job.data
        if not isinstance(data, dict):
            errors.append(f"payload not a dict: {data!r}")
            done.set()
            return
        i = data.get("i")
        tag = data.get("tag")
        if not isinstance(i, int) or i < 0 or i >= count:
            errors.append(f"i out of range: {i!r}")
            done.set()
            return
        if tag != expect_tag:
            errors.append(f"tag mismatch: got {tag!r}, want {expect_tag!r}")
            done.set()
            return
        seen.add(i)
        if len(seen) >= count:
            done.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=redis_url,
        concurrency=8,
        max_attempts=1,
        read_block_ms=200,
        delayed_enabled=False,
        run_scheduler=False,
    )

    run_task = asyncio.create_task(worker.run())
    try:
        try:
            await asyncio.wait_for(done.wait(), timeout=timeout_secs)
        except asyncio.TimeoutError:
            print(
                f"[py-worker] TIMEOUT after {timeout_secs}s — saw {len(seen)}/{count}",
                file=sys.stderr,
            )
            return 1
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=5.0)
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0)

    if errors:
        for e in errors:
            print(f"[py-worker] ERROR: {e}", file=sys.stderr)
        return 1
    if len(seen) != count:
        print(
            f"[py-worker] coverage gap: saw {len(seen)}/{count} (missing: "
            f"{sorted(set(range(count)) - seen)[:10]}...)",
            file=sys.stderr,
        )
        return 1

    print(f"[py-worker] OK — drained {count} distinct jobs with tag={expect_tag!r}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
