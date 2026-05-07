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
  JOB_NAME    — optional. When non-empty, jobs are enqueued with this name
                 (paired with EXPECT_JOB_NAME on the worker side to assert
                 name round-trips through the wire format).
  JOB_IDS_FILE — optional. When set, the resolved engine-minted job IDs are
                 written one-per-line to this path so a downstream verifier
                 (verify_results.{py,ts}) can read them back and assert the
                 result-backend round-trip after the worker drains.
  TAG, REDIS_URL — optional.
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
    job_name = os.environ.get("JOB_NAME", "")
    tag = os.environ.get("TAG", "py")
    redis_url = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
    mode = os.environ.get("MODE", "immediate").lower()
    job_ids_file = os.environ.get("JOB_IDS_FILE", "")

    if mode not in ("immediate", "delayed"):
        print(f"[py-producer] ERROR: unknown MODE={mode!r}", file=sys.stderr)
        return 1

    job_ids: list[str] = []
    queue = Queue(queue_name, redis_url=redis_url)
    try:
        for i in range(count):
            if mode == "delayed":
                job = await queue.add(
                    job_name, {"i": i, "tag": tag}, delay=DELAYED_MS
                )
            else:
                job = await queue.add(job_name, {"i": i, "tag": tag})
            job_ids.append(job.id)
    finally:
        await queue.close()

    if job_ids_file:
        with open(job_ids_file, "w", encoding="utf-8") as fh:
            for jid in job_ids:
                fh.write(f"{jid}\n")
        print(f"[py-producer] wrote {len(job_ids)} ids to {job_ids_file!r}")

    print(
        f"[py-producer] enqueued {count} jobs to {queue_name!r} "
        f"with tag={tag!r} mode={mode!r} name={job_name!r}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
