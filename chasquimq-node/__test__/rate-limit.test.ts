/**
 * Cross-FFI tests for the per-queue rate limiter on the Node shim.
 *
 * The engine-level correctness (shared bucket across workers, ~0 CPU while
 * throttled, shutdown-while-throttled, fail-closed reply parsing) lives in
 * the Rust integration tests (`chasquimq/tests/rate_limit.rs`) and the
 * cross-shim workflow. This file pins the FFI-surface invariants:
 *   1. A configured `limiter` caps observed completion throughput.
 *   2. `limiter.groupKey` is reserved and throws from the constructor.
 *   3. A `limiter` missing/zero `max` or `duration` throws.
 *   4. The native `Consumer` rejects the reserved `groupKey` too.
 */
import { afterEach, describe, expect, it } from "vitest";
import { Queue } from "../dist/queue.js";
import { Worker } from "../dist/worker.js";
import { Consumer } from "../dist/index.js";

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const HAS_REDIS = Boolean(process.env.REDIS_URL);
const d = HAS_REDIS ? describe : describe.skip;

function uniqueQueue(tag: string): string {
  return `rl-${tag}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
}

function parseConn(url: string) {
  const u = new URL(url);
  return {
    host: u.hostname || "127.0.0.1",
    port: u.port ? Number(u.port) : 6379,
    password: u.password || undefined,
    username: u.username || undefined,
    db:
      u.pathname && u.pathname !== "/"
        ? Number(u.pathname.slice(1))
        : undefined,
  };
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length) {
    await cleanups.pop()!().catch(() => {});
  }
});

// ---------------------------------------------------------------------------
// Constructor validation (no Redis required, but gated for parity with the
// throughput test's `d` block so the whole file skips together off-CI).
// ---------------------------------------------------------------------------

d("Worker limiter constructor validation", () => {
  it("throws when limiter.groupKey is set (reserved this version)", () => {
    expect(
      () =>
        new Worker(uniqueQueue("gk"), async () => {}, {
          connection: parseConn(REDIS_URL),
          autorun: false,
          limiter: { max: 10, duration: 1000, groupKey: "tenant" },
        }),
    ).toThrow(/groupKey is not supported/i);
  });

  it("throws when limiter.max is missing or non-positive", () => {
    expect(
      () =>
        new Worker(uniqueQueue("nomax"), async () => {}, {
          connection: parseConn(REDIS_URL),
          autorun: false,
          // @ts-expect-error deliberately omit max
          limiter: { duration: 1000 },
        }),
    ).toThrow(/positive .*max.* and .*duration/i);

    expect(
      () =>
        new Worker(uniqueQueue("zeromax"), async () => {}, {
          connection: parseConn(REDIS_URL),
          autorun: false,
          limiter: { max: 0, duration: 1000 },
        }),
    ).toThrow(/positive/i);
  });

  it("throws when limiter.duration is missing or non-positive", () => {
    expect(
      () =>
        new Worker(uniqueQueue("nodur"), async () => {}, {
          connection: parseConn(REDIS_URL),
          autorun: false,
          // @ts-expect-error deliberately omit duration
          limiter: { max: 10 },
        }),
    ).toThrow(/positive/i);

    expect(
      () =>
        new Worker(uniqueQueue("zerodur"), async () => {}, {
          connection: parseConn(REDIS_URL),
          autorun: false,
          limiter: { max: 10, duration: 0 },
        }),
    ).toThrow(/positive/i);
  });

  it("native Consumer rejects the reserved groupKey", () => {
    expect(
      () =>
        new Consumer(REDIS_URL, {
          queueName: uniqueQueue("nc-gk"),
          limiter: { max: 10, duration: 1000, groupKey: "tenant" },
        }),
    ).toThrow(/groupKey is not supported/i);
  });

  it("native Consumer accepts a valid limiter", () => {
    const c = new Consumer(REDIS_URL, {
      queueName: uniqueQueue("nc-ok"),
      limiter: { max: 100, duration: 1000 },
    });
    expect(c).toBeDefined();
    expect(c.isPaused()).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Behavioural: a limiter caps observed throughput.
// ---------------------------------------------------------------------------

d("Worker limiter caps throughput", () => {
  it("throttles a burst to roughly max jobs per window", async () => {
    const queueName = uniqueQueue("cap");
    const queue = new Queue(queueName, { connection: parseConn(REDIS_URL) });
    cleanups.push(() => queue.close());

    const limitMax = 10;
    const durationMs = 1000;
    const burst = 60;

    let completed = 0;
    const worker = new Worker<{ n: number }, void>(
      queueName,
      async () => {
        completed++;
      },
      {
        connection: parseConn(REDIS_URL),
        concurrency: 8,
        autorun: false,
        drainDelay: 100,
        delayedEnabled: false,
        runScheduler: false,
        stalledDetectorEnabled: false,
        limiter: { max: limitMax, duration: durationMs },
      },
    );
    cleanups.push(() => worker.close());
    void worker.run();

    for (let n = 0; n < burst; n++) await queue.add("rl", { n });

    // Observe ~1.5 windows. The bucket starts FULL, so the first window
    // admits an initial burst of up to `max` before settling to the
    // steady-state max/duration. Ceiling ≈ 2*max plus jitter; 60 jobs
    // cannot all drain in 1.5s under this limiter. Assert generously
    // (< burst) so timing wobble never flakes.
    await new Promise((r) => setTimeout(r, 1500));
    const snapshot = completed;

    expect(snapshot).toBeGreaterThanOrEqual(1);
    expect(snapshot).toBeLessThan(burst);
    // 2*max (cold-start burst + one refill window) + generous slack for
    // scheduler jitter / a partial third window inside the 1.5s.
    expect(snapshot).toBeLessThanOrEqual(2 * limitMax + 15);
  }, 30_000);
});
