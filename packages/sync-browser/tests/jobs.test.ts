import { test, expect, beforeEach } from "bun:test";
import { job, type JobTraceEvent } from "../src/job";
import { queue } from "../src/queue";

let testCounter = 0;
const uid = (name: string): string => `${name}-${Date.now()}-${++testCounter}`;

const waitFor = async (pred: () => boolean, timeoutMs = 5_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

beforeEach(() => {
  testCounter = 0;
});

// ==========================
// Happy path
// ==========================

test("submit + process + after (success path)", async () => {
  let afterCalled = false;
  let seenData: number | undefined;

  const worker = job<void, number>({
    id: uid("happy"),
    process: async () => 42,
    after: async ({ ctx }) => {
      afterCalled = true;
      seenData = ctx.data;
    },
  });

  await worker.submit({ key: "chat:1" });
  await waitFor(() => afterCalled);
  expect(seenData).toBe(42);

  worker.stop();
});

test("ctx.key / ctx.jobId flow through", async () => {
  let afterKey: string | null = null;
  let afterJobId: string | null = null;

  const worker = job({
    id: uid("ctx-keys"),
    process: async () => {},
    after: async ({ ctx }) => {
      afterKey = ctx.key;
      afterJobId = ctx.jobId;
    },
  });

  const id = await worker.submit({ key: "chat:xyz" });
  await waitFor(() => afterKey !== null);
  expect(afterKey).toBe("chat:xyz");
  expect(afterJobId).toBe(id);

  worker.stop();
});

test("ctx.duration is non-negative in after", async () => {
  let afterDuration: number | null = null;

  const worker = job({
    id: uid("duration"),
    process: async () => {
      await Bun.sleep(30);
    },
    after: async ({ ctx }) => {
      afterDuration = ctx.duration;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => afterDuration !== null);
  expect(afterDuration!).toBeGreaterThanOrEqual(25);

  worker.stop();
});

// ==========================
// Typed input
// ==========================

test("typed input flows through submit → process → after", async () => {
  let seen: { userId: string } | undefined;
  let afterSeen: { userId: string } | undefined;

  const worker = job<{ userId: string }, { processed: string }>({
    id: uid("typed-input"),
    process: async ({ ctx }) => {
      seen = ctx.input;
      return { processed: ctx.input.userId };
    },
    after: async ({ ctx }) => {
      afterSeen = ctx.input;
    },
  });

  await worker.submit({ key: "welcome:42", input: { userId: "42" } });
  await waitFor(() => afterSeen !== undefined);
  expect(seen?.userId).toBe("42");
  expect(afterSeen?.userId).toBe("42");

  worker.stop();
});

test("no input: ctx.input is undefined", async () => {
  let inputVal: unknown = "sentinel";

  const worker = job({
    id: uid("no-input"),
    process: async ({ ctx }) => {
      inputVal = ctx.input;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => inputVal !== "sentinel");
  expect(inputVal).toBeUndefined();

  worker.stop();
});

// ==========================
// ctx.data / ctx.error
// ==========================

test("process throws → after sees ctx.error, no ctx.data", async () => {
  let afterError: Error | undefined;
  let afterData: unknown;

  const worker = job({
    id: uid("error"),
    process: async () => {
      throw new Error("kaboom");
    },
    after: async ({ ctx }) => {
      afterError = ctx.error;
      afterData = ctx.data;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => afterError !== undefined);
  expect(afterError?.message).toBe("kaboom");
  expect(afterData).toBeUndefined();

  worker.stop();
});

test("process returns non-JSON value (Date) — passes in-memory to after", async () => {
  let afterData: Date | undefined;
  const sent = new Date("2024-01-15T10:00:00Z");

  const worker = job<void, Date>({
    id: uid("date-result"),
    process: async () => sent,
    after: async ({ ctx }) => {
      afterData = ctx.data;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => afterData !== undefined);
  expect(afterData).toBeInstanceOf(Date);
  expect(afterData).toBe(sent);

  worker.stop();
});

// ==========================
// Idempotency
// ==========================

test("concurrent submit with same key returns same jobId and runs once", async () => {
  let runs = 0;

  const worker = job({
    id: uid("same-key-concurrent"),
    process: async () => {
      runs += 1;
      await Bun.sleep(80);
    },
  });

  const [id1, id2, id3] = await Promise.all([
    worker.submit({ key: "chat:1" }),
    worker.submit({ key: "chat:1" }),
    worker.submit({ key: "chat:1" }),
  ]);
  expect(id1).toBe(id2);
  expect(id2).toBe(id3);

  await Bun.sleep(300);
  expect(runs).toBe(1);

  worker.stop();
});

test("key released after terminal success; resubmit gets new jobId", async () => {
  let runs = 0;

  const worker = job({
    id: uid("key-release-success"),
    process: async () => {
      runs += 1;
    },
  });

  const id1 = await worker.submit({ key: "chat:x" });
  await waitFor(() => runs === 1);
  await waitFor(() => worker.metric().dispatches === 1);

  const id2 = await worker.submit({ key: "chat:x" });
  expect(id2).not.toBe(id1);
  await waitFor(() => runs === 2);

  worker.stop();
});

test("terminal failure (no reschedule) releases key", async () => {
  let afterRuns = 0;

  const worker = job({
    id: uid("key-release-fail"),
    process: async () => {
      throw new Error("boom");
    },
    after: async () => {
      afterRuns += 1;
    },
  });

  const id1 = await worker.submit({ key: "chat:x" });
  await waitFor(() => afterRuns === 1);
  await waitFor(() => worker.metric().failures === 1);

  const id2 = await worker.submit({ key: "chat:x" });
  expect(id2).not.toBe(id1);
  await waitFor(() => afterRuns === 2);

  worker.stop();
});

test("pending reschedule holds the key", async () => {
  let attempts = 0;

  const worker = job({
    id: uid("reschedule-holds-key"),
    process: async () => {
      attempts += 1;
      if (attempts === 1) throw new Error("first attempt");
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 3) ctx.reschedule({ delayMs: 100 });
    },
  });

  const id1 = await worker.submit({ key: "chat:x" });

  await Bun.sleep(30);
  const id2 = await worker.submit({ key: "chat:x" });
  expect(id2).toBe(id1);

  await waitFor(() => attempts === 2);
  expect(attempts).toBe(2);

  worker.stop();
});

// ==========================
// ctx.reschedule + failureCount
// ==========================

test("ctx.reschedule drives redelivery; failureCount increments", async () => {
  const seenFailureCounts: number[] = [];
  let succeededAt = -1;

  const worker = job({
    id: uid("reschedule-count"),
    process: async ({ ctx }) => {
      seenFailureCounts.push(ctx.failureCount);
      if (ctx.failureCount < 3) throw new Error(`fail ${ctx.failureCount}`);
      succeededAt = ctx.failureCount;
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 5) ctx.reschedule({ delayMs: 30 });
    },
  });

  await worker.submit({ key: "r:1" });
  await waitFor(() => succeededAt >= 0, 10_000);
  expect(seenFailureCounts).toEqual([0, 1, 2, 3]);

  worker.stop();
});

test("no after defined → default is terminal", async () => {
  let attempts = 0;

  const worker = job({
    id: uid("default-terminal"),
    process: async () => {
      attempts += 1;
      throw new Error("nope");
    },
  });

  await worker.submit({ key: "x" });
  await Bun.sleep(400);
  expect(attempts).toBe(1);

  worker.stop();
});

test("ctx.reschedule on SUCCESS re-queues (polling pattern)", async () => {
  let runs = 0;

  const worker = job<void, { hasMore: boolean }>({
    id: uid("success-reschedule"),
    process: async () => {
      runs += 1;
      return { hasMore: runs < 3 };
    },
    after: async ({ ctx }) => {
      if (ctx.data?.hasMore) ctx.reschedule({ delayMs: 30 });
    },
  });

  await worker.submit({ key: "poller" });
  await waitFor(() => runs === 3, 5_000);
  await Bun.sleep(200);
  expect(runs).toBe(3);

  worker.stop();
});

// ==========================
// metrics()
// ==========================

test("metric() counts dispatches / failures / reschedules", async () => {
  const worker = job({
    id: uid("metrics"),
    process: async ({ ctx }) => {
      if (ctx.key === "good") return;
      if (ctx.key === "retry-then-good") {
        if (ctx.failureCount < 1) throw new Error("retry me");
        return;
      }
      throw new Error("bad");
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 1) ctx.reschedule({ delayMs: 10 });
    },
  });

  await worker.submit({ key: "good" });
  await worker.submit({ key: "retry-then-good" });
  await worker.submit({ key: "bad" });

  await waitFor(() => {
    const m = worker.metric();
    return m.dispatches + m.failures >= 2;
  }, 5_000);

  const m = worker.metric();
  expect(m.dispatches).toBeGreaterThanOrEqual(2);
  expect(m.failures).toBeGreaterThanOrEqual(1);
  expect(m.reschedules).toBeGreaterThanOrEqual(1);

  m.dispatches = 9999;
  expect(worker.metric().dispatches).not.toBe(9999);

  worker.stop();
});

// ==========================
// delayMs
// ==========================

test("delayMs delays first execution", async () => {
  let ranAt: number | null = null;
  const submittedAt = Date.now();

  const worker = job({
    id: uid("delay"),
    process: async () => {
      ranAt = Date.now();
    },
  });

  await worker.submit({ key: "x", delayMs: 800 });
  await waitFor(() => ranAt !== null, 5_000);
  expect(ranAt! - submittedAt).toBeGreaterThanOrEqual(700);

  worker.stop();
});

// ==========================
// heartbeat
// ==========================

test("ctx.heartbeat extends lease for long runs", async () => {
  let done = false;

  const worker = job({
    id: uid("heartbeat"),
    defaults: { leaseMs: 300 },
    process: async ({ ctx }) => {
      for (let i = 0; i < 5; i++) {
        await Bun.sleep(100);
        await ctx.heartbeat();
      }
      done = true;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => done, 5_000);
  expect(done).toBe(true);

  worker.stop();
});

test("stop aborts the active callback without completing its delivery", async () => {
  let signal: AbortSignal | undefined;
  let afterCalled = false;
  const events: string[] = [];

  const worker = job({
    id: uid("stop-signal"),
    defaults: { leaseMs: 100 },
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      signal = ctx.signal;
      while (!ctx.signal.aborted) await Bun.sleep(10);
    },
    after: async () => {
      afterCalled = true;
    },
  });

  await worker.submit({ key: "cancel-me" });
  await waitFor(() => signal !== undefined);
  worker.stop();
  await waitFor(() => signal?.aborted === true);
  await Bun.sleep(50);

  expect(afterCalled).toBe(false);
  expect(worker.metric().dispatches).toBe(0);
  expect(events).toEqual(["submitted", "started"]);
});

test("submit after stop restarts only after the aborted callback exits", async () => {
  const processed: string[] = [];
  let firstStarted = false;

  const worker = job({
    id: uid("stop-restart"),
    process: async ({ ctx }) => {
      if (ctx.key === "first") {
        firstStarted = true;
        while (!ctx.signal.aborted) await Bun.sleep(10);
        await Bun.sleep(25);
        return;
      }
      processed.push(ctx.key);
    },
  });

  await worker.submit({ key: "first" });
  await waitFor(() => firstStarted);
  worker.stop();
  await worker.submit({ key: "second" });

  await waitFor(() => processed.includes("second"));
  expect(processed).toEqual(["second"]);
  worker.stop();
});

test("stop during started trace does not invoke the process callback", async () => {
  let startedTrace = false;
  let processCalled = false;
  let releaseTrace: (() => void) | undefined;
  const traceGate = new Promise<void>((resolve) => {
    releaseTrace = resolve;
  });

  const worker = job({
    id: uid("stop-before-process"),
    trace: async (event) => {
      if (event.type !== "started") return;
      startedTrace = true;
      await traceGate;
    },
    process: async () => {
      processCalled = true;
    },
  });

  try {
    await worker.submit({ key: "x" });
    await waitFor(() => startedTrace);
    worker.stop();
    releaseTrace?.();
    await Bun.sleep(50);

    expect(processCalled).toBe(false);
    expect(worker.metric().dispatches).toBe(0);
  } finally {
    releaseTrace?.();
    worker.stop();
  }
});

test("a failed heartbeat aborts the attempt without false completion", async () => {
  const id = uid("lease-loss");
  let started = false;
  let observedAbort = false;
  let afterCalled = false;
  const events: string[] = [];

  const worker = job({
    id,
    defaults: { leaseMs: 50 },
    trace: (event) => {
      events.push(event.type);
    },
    process: async ({ ctx }) => {
      started = true;
      await Bun.sleep(100);
      await ctx.heartbeat();
      observedAbort = ctx.signal.aborted;
    },
    after: async () => {
      afterCalled = true;
    },
  });

  await worker.submit({ key: "lease-loss" });
  await waitFor(() => started);
  await Bun.sleep(70);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" });
  const stolen = await competitor.recv({ wait: false });
  expect(stolen).not.toBeNull();

  await waitFor(() => observedAbort);
  worker.stop();
  expect(await stolen?.ack()).toBe(true);

  expect(afterCalled).toBe(false);
  expect(worker.metric().dispatches).toBe(0);
  expect(events).toEqual(["submitted", "started"]);
});

test("a rejected reschedule does not increment the reschedule metric", async () => {
  const id = uid("lost-reschedule");
  let started = false;
  let afterCalled = false;

  const worker = job({
    id,
    defaults: { leaseMs: 50 },
    process: async () => {
      started = true;
      await Bun.sleep(120);
    },
    after: async ({ ctx }) => {
      afterCalled = true;
      ctx.reschedule();
    },
  });

  await worker.submit({ key: "lost-reschedule" });
  await waitFor(() => started);
  await Bun.sleep(70);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" });
  const stolen = await competitor.recv({ wait: false });
  expect(stolen).not.toBeNull();

  await waitFor(() => afterCalled);
  await Bun.sleep(20);
  expect(worker.metric().reschedules).toBe(0);

  worker.stop();
  expect(await stolen?.ack()).toBe(true);
});

// ==========================
// after error swallow
// ==========================

test("errors thrown inside after do not blow up the worker", async () => {
  const processedKeys: string[] = [];
  let secondProcessed = false;

  const worker = job<void, string>({
    id: uid("after-throws"),
    process: async ({ ctx }) => {
      processedKeys.push(ctx.key);
      return ctx.key;
    },
    after: async ({ ctx }) => {
      if (ctx.data === "a") throw new Error("after error — should be swallowed");
      if (ctx.data === "b") secondProcessed = true;
    },
  });

  await worker.submit({ key: "a" });
  await waitFor(() => processedKeys.includes("a"));

  await worker.submit({ key: "b" });
  await waitFor(() => secondProcessed, 3_000);
  expect(processedKeys.sort()).toEqual(["a", "b"]);

  worker.stop();
});

// ==========================
// trace
// ==========================

test("trace records job lifecycle and only emits submitted for new jobs", async () => {
  const events: JobTraceEvent<{ userId: string }, { ok: boolean }>[] = [];

  const worker = job<{ userId: string }, { ok: boolean }>({
    id: uid("trace-lifecycle"),
    trace: (event) => {
      events.push(event);
    },
    process: async () => ({ ok: true }),
  });

  const first = await worker.submit({
    key: "user:1",
    input: { userId: "u1" },
    meta: { source: "test" },
  });
  const duplicate = await worker.submit({
    key: "user:1",
    input: { userId: "u1" },
  });

  expect(duplicate).toBe(first);
  await waitFor(() => events.some((event) => event.type === "finished"));

  expect(events.map((event) => event.type)).toEqual(["submitted", "started", "succeeded", "finished"]);

  const submitted = events[0] as Extract<JobTraceEvent<{ userId: string }, { ok: boolean }>, { type: "submitted" }>;
  expect(submitted.jobId).toBe(first);
  expect(submitted.key).toBe("user:1");
  expect(submitted.input?.userId).toBe("u1");
  expect(submitted.meta?.source).toBe("test");

  const started = events[1] as Extract<JobTraceEvent<{ userId: string }, { ok: boolean }>, { type: "started" }>;
  expect(started.attempt).toBe(1);

  const succeeded = events[2] as Extract<JobTraceEvent<{ userId: string }, { ok: boolean }>, { type: "succeeded" }>;
  expect(succeeded.data.ok).toBe(true);
  expect(succeeded.durationMs).toBeGreaterThanOrEqual(0);

  const finished = events[3] as Extract<JobTraceEvent<{ userId: string }, { ok: boolean }>, { type: "finished" }>;
  expect(finished.status).toBe("succeeded");

  worker.stop();
});

test("trace records failed attempts, reschedules, and terminal finish", async () => {
  const events: JobTraceEvent<void, string>[] = [];

  const worker = job<void, string>({
    id: uid("trace-reschedule"),
    trace: (event) => {
      events.push(event);
    },
    process: async ({ ctx }) => {
      if (ctx.failureCount === 0) throw new Error("try again");
      return "ok";
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount === 0) ctx.reschedule({ delayMs: 20 });
    },
  });

  await worker.submit({ key: "retry" });
  await waitFor(() => events.some((event) => event.type === "finished"), 5_000);

  expect(events.map((event) => event.type)).toEqual([
    "submitted",
    "started",
    "failed",
    "rescheduled",
    "started",
    "succeeded",
    "finished",
  ]);

  const failed = events[2] as Extract<JobTraceEvent<void, string>, { type: "failed" }>;
  expect(failed.error.message).toBe("try again");

  const rescheduled = events[3] as Extract<JobTraceEvent<void, string>, { type: "rescheduled" }>;
  expect(rescheduled.attempt).toBe(1);
  expect(rescheduled.delayMs).toBe(20);

  const secondStarted = events[4] as Extract<JobTraceEvent<void, string>, { type: "started" }>;
  expect(secondStarted.attempt).toBe(2);

  worker.stop();
});

test("trace errors are swallowed and do not affect job execution", async () => {
  const originalWarn = console.warn;
  let warnings = 0;
  console.warn = () => {
    warnings += 1;
  };

  let ran = false;
  const worker = job({
    id: uid("trace-throws"),
    trace: () => {
      throw new Error("trace failed");
    },
    process: async () => {
      ran = true;
    },
  });

  try {
    await worker.submit({ key: "x" });
    await waitFor(() => ran);
    await waitFor(() => worker.metric().dispatches === 1);
    expect(warnings).toBeGreaterThan(0);
  } finally {
    console.warn = originalWarn;
    worker.stop();
  }
});

test("metrics are per handle and a later lease default is not overridden", async () => {
  const id = `shared-metrics-${Date.now()}`;
  let ran = 0;

  const worker = job<{ n: number }>({
    id,
    process: async () => {
      ran += 1;
    },
  });
  // A second handle for observability, exactly as a dashboard would create.
  const observer = job<{ n: number }>({ id, defaults: { leaseMs: 500 } });

  await worker.submit({ key: "k1", input: { n: 1 } });
  while (ran === 0) await Bun.sleep(10);
  await Bun.sleep(150);

  // The server keeps metrics closure-local, so the observer reports zeros.
  expect(worker.metric().dispatches).toBeGreaterThanOrEqual(1);
  expect(observer.metric().dispatches).toBe(0);

  worker.stop();
  observer.stop();
});
