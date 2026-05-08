"""Python-handler-in-loop benchmark.

Measures the throughput of a ChasquiMQ ``Worker`` when its handler is a
no-op Python coroutine. Exercises the full FFI dispatch path:

    engine reader (tokio) -> TSFN-equivalent (pyo3-async-runtimes
    `into_future_with_locals`) -> asyncio loop -> handler coroutine ->
    asyncio resolution -> tokio task -> XACKDEL pipeline.

The producer side runs first and to completion: jobs are pre-loaded into
the stream via the native ``Producer.add_bulk`` (the same Rust producer
the bench harness uses, just driven from Python). Then a single
``Worker`` drains the stream while the bench measures handler-side
throughput. Producer noise stays out of the consumer window.

Outputs a single-line markdown row + a trailing summary block. Run with::

    python benchmarks/scripts/python_handler_bench.py \\
        --redis-url redis://127.0.0.1:6379 \\
        --jobs 100000 --concurrency 100 --warmup 10000

Environment notes:

* Requires the ``chasquimq`` wheel built against this checkout
  (``cd chasquimq-py && maturin develop --release``).
* Requires Redis 8.6+ on the configured URL.
* Single-host caveats apply (bench process + Redis share cores).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import resource
import statistics
import sys
import time
import uuid

import msgpack

from chasquimq import Worker
from chasquimq._native import Producer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ChasquiMQ Python-handler-in-loop bench")
    p.add_argument(
        "--redis-url",
        default=os.environ.get("CHASQUIMQ_BENCH_REDIS_URL", "redis://127.0.0.1:6379"),
    )
    p.add_argument(
        "--jobs",
        type=int,
        default=100_000,
        help="Bench window size after warmup (default: 100k)",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=10_000,
        help="Jobs to drain before starting the timer (default: 10k)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=100,
        help="Worker concurrency (default: 100, mirrors worker-concurrent)",
    )
    p.add_argument(
        "--payload-bytes",
        type=int,
        default=100,
        help="Approximate msgpack payload size, mirrors bullmq-bench 10x10",
    )
    p.add_argument(
        "--queue-prefix",
        default="py-handler-bench",
    )
    p.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Independent repeats; reported as mean + stddev",
    )
    return p.parse_args()


def make_payload(approx_bytes: int) -> bytes:
    s = "x" * max(1, approx_bytes - 8)
    return msgpack.packb({"i": 0, "s": s}, use_bin_type=True)


async def preload(redis_url: str, queue_name: str, total: int, payload: bytes) -> None:
    """Drive the queue full via the native (Rust) producer.

    Uses ``add_bulk`` with 100-job batches — same shape as the
    chasquimq-bench preload path. Producer pool size 8 to keep the
    XADD pipeline saturated.
    """
    producer = Producer(
        redis_url,
        queue_name,
        pool_size=8,
        max_stream_len=2_000_000,
    )
    batch = 100
    payloads = [payload] * batch
    sent = 0
    while sent < total:
        n = min(batch, total - sent)
        if n != batch:
            await producer.add_bulk(payloads[:n])
        else:
            await producer.add_bulk(payloads)
        sent += n


async def flush_queue(redis_url: str, queue_name: str) -> None:
    import redis.asyncio as aioredis

    client = aioredis.from_url(redis_url)
    try:
        async for k in client.scan_iter(match=f"{{chasqui:{queue_name}}}*"):
            await client.delete(k)
    finally:
        await client.aclose()


def rusage_diff(start: resource.struct_rusage, end: resource.struct_rusage) -> tuple[float, float]:
    user = end.ru_utime - start.ru_utime
    sys_ = end.ru_stime - start.ru_stime
    return user, sys_


async def run_one(args: argparse.Namespace, queue_name: str) -> dict:
    payload = make_payload(args.payload_bytes)
    total = args.warmup + args.jobs

    await flush_queue(args.redis_url, queue_name)
    await preload(args.redis_url, queue_name, total, payload)

    seen = 0
    started_ts = 0.0
    started_rusage: resource.struct_rusage | None = None
    elapsed = 0.0
    rusage_user = 0.0
    rusage_sys = 0.0
    finished = asyncio.Event()
    intervals_us: list[int] = []
    sample_every = max(1, args.jobs // 4096)
    last_ts = 0.0

    # Nonlocal mutations below are GIL-safe only because no awaits in handler;
    # adding an await requires a lock or asyncio.Queue.
    async def handler(_job) -> None:
        nonlocal seen, started_ts, started_rusage, elapsed, rusage_user, rusage_sys, last_ts
        seen += 1
        now = time.perf_counter()
        # Timer starts after `args.warmup` warmup jobs have completed, so the
        # measured window is exactly `args.jobs` jobs (seen `warmup+1` ..
        # `warmup+jobs`), not `args.jobs + 1`.
        if seen == args.warmup + 1:
            started_ts = now
            last_ts = now
            started_rusage = resource.getrusage(resource.RUSAGE_SELF)
        elif seen > args.warmup + 1:
            # Inter-handler dispatch interval — proxy for handler-side
            # latency, not affected by pre-load dwell time. With
            # concurrency > 1 this measures the *aggregate* dispatch
            # rate (interleaved across worker tasks).
            if (seen - args.warmup) % sample_every == 0:
                gap_us = max(0, int((now - last_ts) * 1_000_000))
                intervals_us.append(gap_us)
                last_ts = now
        if seen == total:
            assert started_rusage is not None
            elapsed = time.perf_counter() - started_ts
            end_rusage = resource.getrusage(resource.RUSAGE_SELF)
            rusage_user, rusage_sys = rusage_diff(started_rusage, end_rusage)
            finished.set()

    worker = Worker(
        queue_name,
        handler,
        redis_url=args.redis_url,
        concurrency=args.concurrency,
        max_attempts=1,
        read_block_ms=100,
        run_scheduler=False,
    )
    run_task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(finished.wait(), timeout=120)
    finally:
        await worker.close()
        try:
            await asyncio.wait_for(run_task, timeout=10)
        except asyncio.TimeoutError:
            pass

    jobs_per_sec = args.jobs / elapsed
    cpu_total = rusage_user + rusage_sys
    return {
        "queue": queue_name,
        "jobs": args.jobs,
        "elapsed_s": elapsed,
        "jobs_per_sec": jobs_per_sec,
        "cpu_user_s": rusage_user,
        "cpu_sys_s": rusage_sys,
        "cpu_total_pct": 100.0 * cpu_total / elapsed if elapsed > 0 else 0.0,
        "jobs_per_cpu_sec": (args.jobs / cpu_total) if cpu_total > 0 else 0.0,
        "intervals_us": intervals_us,
    }


def percentile(xs: list[int], p: float) -> int:
    if not xs:
        return 0
    s = sorted(xs)
    idx = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[idx]


async def main() -> int:
    args = parse_args()
    runs: list[dict] = []
    for r in range(args.repeats):
        queue_name = f"{args.queue_prefix}-{uuid.uuid4().hex[:8]}-{r}"
        print(f"[run {r + 1}/{args.repeats}] queue={queue_name}", file=sys.stderr)
        result = await run_one(args, queue_name)
        runs.append(result)
        print(
            f"[run {r + 1}/{args.repeats}] {result['jobs_per_sec']:>10,.0f} jobs/s | "
            f"elapsed {result['elapsed_s']:.3f}s | "
            f"CPU {result['cpu_total_pct']:.1f}%",
            file=sys.stderr,
        )

    rates = [r["jobs_per_sec"] for r in runs]
    cpu_pcts = [r["cpu_total_pct"] for r in runs]
    p50s = [percentile(r["intervals_us"], 50) for r in runs if r["intervals_us"]]
    p99s = [percentile(r["intervals_us"], 99) for r in runs if r["intervals_us"]]

    print()
    print("## ChasquiMQ Python-handler-in-loop")
    print()
    print(
        f"redis_url={args.redis_url} jobs={args.jobs} warmup={args.warmup} "
        f"concurrency={args.concurrency} payload_bytes={args.payload_bytes} repeats={args.repeats}"
    )
    print()
    print("| Run | jobs/s | elapsed (s) | CPU % | p50 dispatch gap (us) | p99 dispatch gap (us) |")
    print("|----:|-------:|------------:|------:|----------------------:|----------------------:|")
    for i, r in enumerate(runs):
        p50 = percentile(r["intervals_us"], 50) if r["intervals_us"] else 0
        p99 = percentile(r["intervals_us"], 99) if r["intervals_us"] else 0
        print(
            f"| {i + 1} | {r['jobs_per_sec']:,.0f} | {r['elapsed_s']:.3f} | "
            f"{r['cpu_total_pct']:.1f} | {p50:,} | {p99:,} |"
        )
    print()
    print(
        f"**Mean:** {statistics.fmean(rates):,.0f} jobs/s "
        f"(stddev {statistics.pstdev(rates):,.0f}); "
        f"CPU {statistics.fmean(cpu_pcts):.1f}% "
        f"(stddev {statistics.pstdev(cpu_pcts):.1f}%)"
    )
    if p50s:
        print(
            f"**Dispatch gap:** p50 mean {statistics.fmean(p50s):,.0f}us, "
            f"p99 mean {statistics.fmean(p99s):,.0f}us "
            f"(handler-to-handler interval, sampled every {max(1, args.jobs // 4096)} jobs)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
