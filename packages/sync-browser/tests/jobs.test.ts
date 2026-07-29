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
  let afterSignalAborted: boolean | undefined;
  let seenData: number | undefined;

  const worker = job<void, number>({
    id: uid("happy"),
    process: async () => 42,
    after: async ({ ctx }) => {
      afterCalled = true;
      afterSignalAborted = ctx.signal.aborted;
      seenData = ctx.data;
    },
  });

  await worker.submit({ key: "chat:1" });
  await waitFor(() => afterCalled);
  expect(seenData).toBe(42);
  expect(afterSignalAborted).toBe(false);

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

test("colon-rich job prefix and id tuples do not share worker state", async () => {
  const base = uid("identity-collision");
  let firstRuns = 0;
  let secondRuns = 0;
  const first = job({
    id: "b",
    prefix: `${base}:a`,
    process: async () => {
      firstRuns += 1;
    },
  });
  const second = job({
    id: "a:b",
    prefix: base,
    process: async () => {
      secondRuns += 1;
    },
  });

  await first.submit({ key: "same-key" });
  await second.submit({ key: "same-key" });
  await waitFor(() => firstRuns === 1 && secondRuns === 1);

  expect(firstRuns).toBe(1);
  expect(secondRuns).toBe(1);
  first.stop();
  second.stop();
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

test("concurrent submit waits for a pending enqueue and recovers from serialization failure", async () => {
  type Input = { value: string; self?: Input };
  const processed: string[] = [];
  const worker = job<Input>({
    id: uid("pending-enqueue-failure"),
    process: async ({ ctx }) => {
      processed.push(ctx.input.value);
    },
  });
  const invalid: Input = { value: "invalid" };
  invalid.self = invalid;

  try {
    const failed = worker.submit({ key: "same", input: invalid });
    const recovered = worker.submit({ key: "same", input: { value: "valid" } });

    await expect(failed).rejects.toThrow();
    expect(await recovered).toBe("2");
    await waitFor(() => processed.length === 1);
    expect(processed).toEqual(["valid"]);
  } finally {
    worker.stop();
  }
});

test("an already waiting same-id worker does not discard a pending enqueue", async () => {
  const id = uid("pending-worker-race");
  const processed: string[] = [];
  const waitingWorker = job<{ value: string }>({
    id,
    process: async ({ ctx }) => {
      processed.push(ctx.input.value);
    },
  });
  const submittingWorker = job<{ value: string }>({
    id,
    process: async ({ ctx }) => {
      processed.push(ctx.input.value);
    },
  });

  try {
    await waitingWorker.submit({ key: "seed", input: { value: "seed" } });
    await waitFor(() => processed.includes("seed"));

    const jobId = await submittingWorker.submit({ key: "next", input: { value: "next" } });
    await waitFor(() => processed.includes("next"));
    expect(jobId).toBe("2");
    expect(processed.filter((value) => value === "next")).toHaveLength(1);
  } finally {
    waitingWorker.stop();
    submittingWorker.stop();
  }
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

test("job timings reject non-finite and unsupported delays before allocating an id", async () => {
  expect(() =>
    job({
      id: uid("invalid-default"),
      defaults: { leaseMs: Number.NaN },
      process: async () => {},
    }),
  ).toThrow("defaults.leaseMs must be a safe integer");

  const worker = job({ id: uid("invalid-submit"), process: async () => {} });
  await expect(worker.submit({ key: "nan", delayMs: Number.NaN })).rejects.toThrow(
    "submit.delayMs must be a safe integer",
  );
  await expect(worker.submit({ key: "infinite", at: Number.POSITIVE_INFINITY })).rejects.toThrow(
    "submit.at must be a safe integer",
  );
  await expect(worker.submit({ key: "too-far", delayMs: 30 * 24 * 60 * 60 * 1_000 })).rejects.toThrow(
    "submit.delayMs must be a safe integer",
  );

  expect(await worker.submit({ key: "valid" })).toBe("1");
  worker.stop();

  const callbackErrors: string[] = [];
  const callbackWorker = job({
    id: uid("invalid-callback-timings"),
    process: async ({ ctx }) => {
      try {
        await ctx.heartbeat({ leaseMs: Number.NaN });
      } catch (error) {
        callbackErrors.push((error as Error).message);
      }
    },
    after: ({ ctx }) => {
      try {
        ctx.reschedule({ delayMs: Number.POSITIVE_INFINITY });
      } catch (error) {
        callbackErrors.push((error as Error).message);
      }
    },
  });
  await callbackWorker.submit({ key: "callback" });
  await waitFor(() => callbackWorker.metric().dispatches === 1);
  expect(callbackErrors).toEqual([
    expect.stringContaining("heartbeat.leaseMs must be a safe integer"),
    expect.stringContaining("reschedule.delayMs must be a safe integer"),
  ]);
  callbackWorker.stop();
});

test("job reschedule supports delays beyond the queue's old seven-day default", async () => {
  const delayMs = 8 * 24 * 60 * 60 * 1_000;
  const worker = job({
    id: uid("long-reschedule"),
    process: async () => {},
    after: ({ ctx }) => {
      if (ctx.failureCount === 0) ctx.reschedule({ delayMs });
    },
  });

  await worker.submit({ key: "long" });
  await waitFor(() => worker.metric().reschedules === 1);
  worker.stop();
});

// ==========================
// heartbeat
// ==========================

const raceHeartbeat = async (
  id: string,
  body: (ctx: { heartbeat: () => Promise<void> }) => Promise<void>,
): Promise<{ stolen: boolean }> => {
  let running = false;
  let finished = false;

  const worker = job({
    id,
    defaults: { leaseMs: 300 },
    process: async ({ ctx }) => {
      running = true;
      await body(ctx);
      finished = true;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => running);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" }).reader();
  let stolen = false;
  while (!finished && !stolen) {
    const message = await competitor.recv({ wait: false });
    if (message) {
      stolen = true;
      await message.ack();
    } else {
      await Bun.sleep(50);
    }
  }
  worker.stop();
  return { stolen };
};

test("ctx.heartbeat extends lease for long runs", async () => {
  const { stolen } = await raceHeartbeat(uid("heartbeat"), async (ctx) => {
    for (let i = 0; i < 8; i++) {
      await Bun.sleep(100);
      await ctx.heartbeat();
    }
  });

  expect(stolen).toBe(false);
});

test("without a heartbeat a long browser run loses its lease", async () => {
  const { stolen } = await raceHeartbeat(uid("no-heartbeat"), async () => {
    await Bun.sleep(1_500);
  });

  expect(stolen).toBe(true);
});

test("a per-submit lease overrides a longer handle default on first delivery", async () => {
  const id = uid("submit-lease");
  let running = false;
  const worker = job({
    id,
    defaults: { leaseMs: 5_000 },
    process: async () => {
      running = true;
      await Bun.sleep(800);
    },
  });

  await worker.submit({ key: "short", leaseMs: 100 });
  await waitFor(() => running);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" }).reader();
  let stolen = false;
  const deadline = Date.now() + 3_000;
  while (!stolen && Date.now() < deadline) {
    const message = await competitor.recv({ wait: false });
    if (message) {
      stolen = true;
      await message.ack();
      break;
    }
    await Bun.sleep(25);
  }

  expect(stolen).toBe(true);
  worker.stop();
});

test("a slow started trace cannot hand expired browser work to process", async () => {
  const id = uid("slow-start-trace");
  let traceStarted = false;
  let processRan = false;
  const worker = job({
    id,
    defaults: { leaseMs: 100 },
    trace: async (event) => {
      if (event.type !== "started") return;
      traceStarted = true;
      await Bun.sleep(400);
    },
    process: async () => {
      processRan = true;
    },
  });

  await worker.submit({ key: "trace" });
  await waitFor(() => traceStarted);

  const competitor = queue({ id: `${id}:work`, prefix: "sync:job:queue" }).reader();
  let stolen = false;
  const deadline = Date.now() + 3_000;
  while (!stolen && Date.now() < deadline) {
    const message = await competitor.recv({ wait: false });
    if (message) {
      stolen = true;
      await message.ack();
      break;
    }
    await Bun.sleep(25);
  }

  await Bun.sleep(350);
  expect(stolen).toBe(true);
  expect(processRan).toBe(false);
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

test("lease loss after process skips after and transport completion", async () => {
  const id = uid("lost-reschedule");
  let started = false;
  let processFinished = false;
  let afterCalled = false;
  let signal: AbortSignal | undefined;

  const worker = job({
    id,
    defaults: { leaseMs: 50 },
    process: async ({ ctx }) => {
      started = true;
      signal = ctx.signal;
      await Bun.sleep(120);
      processFinished = true;
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

  await waitFor(() => processFinished);
  await waitFor(() => signal?.aborted === true);
  await Bun.sleep(20);
  expect(afterCalled).toBe(false);
  expect(worker.metric().reschedules).toBe(0);

  worker.stop();
  expect(await stolen?.ack()).toBe(true);
});

test("initial delay keeps the idempotency claim beyond keyTtlMs", async () => {
  let started = false;
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  const worker = job({
    id: uid("delay-claim-refresh"),
    process: async () => {
      started = true;
      await gate;
    },
  });

  const first = await worker.submit({ key: "same", keyTtlMs: 1_000, delayMs: 1_500 });
  await Bun.sleep(1_100);
  expect(started).toBe(false);
  expect(await worker.submit({ key: "same", keyTtlMs: 1_000 })).toBe(first);

  await waitFor(() => started, 3_000);
  release?.();
  await waitFor(() => worker.metric().dispatches === 1);
  worker.stop();
});

test("heartbeat renews the idempotency claim", async () => {
  let heartbeatDone = false;
  let release: (() => void) | undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  const worker = job({
    id: uid("heartbeat-claim-refresh"),
    process: async ({ ctx }) => {
      await Bun.sleep(700);
      await ctx.heartbeat();
      heartbeatDone = true;
      await gate;
    },
  });

  const first = await worker.submit({ key: "same", keyTtlMs: 1_000 });
  await waitFor(() => heartbeatDone);
  await Bun.sleep(450);
  expect(await worker.submit({ key: "same", keyTtlMs: 1_000 })).toBe(first);

  release?.();
  await waitFor(() => worker.metric().dispatches === 1);
  worker.stop();
});

test("active claim outlives a short key TTL until its delivery lease ends", async () => {
  const attempts: number[] = [];
  const worker = job({
    id: uid("active-claim-lease"),
    defaults: { leaseMs: 3_000, keyTtlMs: 1_000 },
    process: async ({ ctx }) => {
      attempts.push(ctx.failureCount);
      if (ctx.failureCount === 0) {
        await Bun.sleep(1_100);
        throw new Error("retry");
      }
    },
    after: ({ ctx }) => {
      if (ctx.error) ctx.reschedule();
    },
  });

  try {
    await worker.submit({ key: "slow" });
    await waitFor(() => attempts.length === 2, 8_000);
    await waitFor(() => worker.metric().dispatches === 1);
    expect(attempts).toEqual([0, 1]);
    expect(worker.metric().reschedules).toBe(1);
    expect(worker.metric().dispatches).toBe(1);
  } finally {
    worker.stop();
  }
});

test("reschedule holds the idempotency claim across a delay longer than its TTL", async () => {
  let firstAttemptFinished = false;

  const worker = job({
    id: uid("reschedule-claim-refresh"),
    process: async ({ ctx }) => {
      if (ctx.failureCount === 0) firstAttemptFinished = true;
    },
    after: async ({ ctx }) => {
      if (ctx.failureCount === 0) ctx.reschedule({ delayMs: 1_600 });
    },
  });

  const first = await worker.submit({ key: "same", keyTtlMs: 1_000 });
  await waitFor(() => firstAttemptFinished && worker.metric().reschedules === 1);
  await Bun.sleep(1_200);
  expect(await worker.submit({ key: "same", keyTtlMs: 1_000 })).toBe(first);
  worker.stop();
});

test("claim loss fences a stale browser attempt before after and completion", async () => {
  const runs: string[] = [];
  let firstStarted = false;
  let afterCalls = 0;
  let releaseFirst: (() => void) | undefined;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const worker = job({
    id: uid("claim-loss-fence"),
    defaults: { leaseMs: 5_000 },
    process: async ({ ctx }) => {
      runs.push(ctx.jobId);
      if (ctx.jobId === "1") {
        firstStarted = true;
        await firstGate;
      }
    },
    after: async () => {
      afterCalls += 1;
    },
  });
  const originalNow = Date.now;

  try {
    await worker.submit({ key: "same", keyTtlMs: 1_000 });
    await waitFor(() => firstStarted);

    const shiftedNow = originalNow() + 6_000;
    Date.now = () => shiftedNow;
    const second = await worker.submit({ key: "same", keyTtlMs: 1_000 });
    expect(second).toBe("2");
    Date.now = originalNow;

    releaseFirst?.();
    await waitFor(() => worker.metric().dispatches === 1);
    expect(runs).toEqual(["1", "2"]);
    expect(afterCalls).toBe(1);
  } finally {
    Date.now = originalNow;
    releaseFirst?.();
    worker.stop();
  }
});

test("heartbeat after stop does not extend browser ownership", async () => {
  let firstStarted = false;
  let heartbeatDone = false;
  const processed: string[] = [];
  let releaseFirst: (() => void) | undefined;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const worker = job({
    id: uid("stopped-heartbeat"),
    defaults: { leaseMs: 1_000, keyTtlMs: 1_000 },
    process: async ({ ctx }) => {
      processed.push(ctx.jobId);
      if (ctx.jobId !== "1") return;
      firstStarted = true;
      await firstGate;
      await ctx.heartbeat({ leaseMs: 5_000 });
      heartbeatDone = true;
    },
  });
  const originalNow = Date.now;

  try {
    const first = await worker.submit({ key: "same" });
    await waitFor(() => firstStarted);
    worker.stop();
    releaseFirst?.();
    await waitFor(() => heartbeatDone);

    const shiftedNow = originalNow() + 2_000;
    Date.now = () => shiftedNow;
    const second = await worker.submit({ key: "same" });
    Date.now = originalNow;

    expect(second).not.toBe(first);
    await waitFor(() => processed.includes(second));
  } finally {
    Date.now = originalNow;
    releaseFirst?.();
    worker.stop();
  }
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
