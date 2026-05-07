import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Queue, Worker, WaitForResultTimeoutError } from "../dist/index.js";

const REDIS_URL = process.env.REDIS_URL;
const skipIfNoRedis = REDIS_URL ? describe : describe.skip;

skipIfNoRedis("Job.waitForResult", () => {
  let queueName: string;
  let queue: Queue<{ value: number }, { ok: number }>;
  let worker: Worker<{ value: number }, { ok: number } | undefined> | undefined;

  beforeEach(() => {
    queueName = `qmq-test-wfr-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    queue = new Queue(queueName, { connection: parseConn(REDIS_URL!) });
  });

  afterEach(async () => {
    if (worker) {
      await worker.close().catch(() => {});
      worker = undefined;
    }
    await queue.close().catch(() => {});
  });

  it("happy path: resolves with the handler return value", async () => {
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      async (job) => ({ ok: job.data.value * 2 }),
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        storeResults: true,
      },
    );
    void worker.run();

    const job = await queue.add("compute", { value: 21 });
    const result = await job.waitForResult({ timeoutMs: 5_000 });
    expect(result).toEqual({ ok: 42 });
  });

  it("timeout: throws WaitForResultTimeoutError when no result lands in time", async () => {
    // No worker — the result key will never be written.
    const job = await queue.add("orphan", { value: 1 });
    let err: unknown;
    try {
      await job.waitForResult({ timeoutMs: 250, intervalMs: 50 });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WaitForResultTimeoutError);
    expect((err as Error).name).toBe("WaitForResultTimeoutError");
  });

  it("AbortSignal: pre-aborted signal throws synchronously on entry", async () => {
    const job = await queue.add("preabort", { value: 1 });
    const ac = new AbortController();
    ac.abort(new Error("cancelled before call"));
    let err: unknown;
    try {
      await job.waitForResult({ timeoutMs: 30_000, signal: ac.signal });
    } catch (e) {
      err = e;
    }
    expect(err).toBeDefined();
    expect((err as Error).message).toBe("cancelled before call");
  });

  it("AbortSignal: aborts mid-poll and rejects with the abort reason", async () => {
    // Slow / non-existent worker — the loop spins on get_result polling.
    const job = await queue.add("mid-abort", { value: 1 });
    const ac = new AbortController();
    setTimeout(() => ac.abort(new Error("mid-poll cancel")), 100);

    let err: unknown;
    try {
      await job.waitForResult({
        timeoutMs: 30_000,
        intervalMs: 250,
        signal: ac.signal,
      });
    } catch (e) {
      err = e;
    }
    expect(err).toBeDefined();
    expect((err as Error).message).toBe("mid-poll cancel");
  });

  it("void handler with storeResults=true: times out (documented behavior)", async () => {
    worker = new Worker<{ value: number }, { ok: number } | undefined>(
      queueName,
      // eslint-disable-next-line @typescript-eslint/require-await
      async () => undefined,
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        storeResults: true,
      },
    );
    void worker.run();

    const job = await queue.add("void", { value: 1 });
    let err: unknown;
    try {
      await job.waitForResult({ timeoutMs: 500, intervalMs: 50 });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WaitForResultTimeoutError);
  });

  it("storeResults=false: times out (documented behavior)", async () => {
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      async (job) => ({ ok: job.data.value }),
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 1,
        autorun: false,
        // storeResults defaults to false
      },
    );
    void worker.run();

    const job = await queue.add("no-store", { value: 1 });
    let err: unknown;
    try {
      await job.waitForResult({ timeoutMs: 500, intervalMs: 50 });
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(WaitForResultTimeoutError);
  });

  it("jobs from addBulk also have a working queue ref", async () => {
    worker = new Worker<{ value: number }, { ok: number }>(
      queueName,
      async (job) => ({ ok: job.data.value + 100 }),
      {
        connection: parseConn(REDIS_URL!),
        concurrency: 4,
        autorun: false,
        storeResults: true,
      },
    );
    void worker.run();

    const jobs = await queue.addBulk([
      { name: "b", data: { value: 1 } },
      { name: "b", data: { value: 2 } },
    ]);
    const results = await Promise.all(
      jobs.map((j) => j.waitForResult({ timeoutMs: 5_000 })),
    );
    expect(results).toEqual([{ ok: 101 }, { ok: 102 }]);
  });
});

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
