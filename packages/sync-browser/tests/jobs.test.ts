import { test, expect, beforeEach } from "bun:test";
import { job } from "../src/job";

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
