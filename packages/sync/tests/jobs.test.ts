import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { job, queue, type JobMetrics, type JobTraceEvent } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:job:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

const waitFor = async (pred: () => boolean, timeoutMs = 5_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

const internalJobQueueReader = (id: string) =>
  queue({
    id: `${id}:work`,
    prefix: "sync:job:queue",
  }).reader();

// ==========================
// Happy path
// ==========================

test("submit + process + after (success path)", async () => {
  let afterCalled = false;
  let afterSignalAborted: boolean | undefined;
  let seenData: number | undefined;
  let seenError: Error | undefined;

  const worker = job<void, number>({
    id: uid("happy"),
    process: async () => 42,
    after: async ({ ctx }) => {
      afterCalled = true;
      afterSignalAborted = ctx.signal.aborted;
      seenData = ctx.data;
      seenError = ctx.error;
    },
  });

  await worker.submit({ key: "chat:1" });
  await waitFor(() => afterCalled);
  expect(seenData).toBe(42);
  expect(seenError).toBeUndefined();
  expect(afterSignalAborted).toBe(false);

  worker.stop();
});

test("job id and submission key delimiter combinations do not share claims", async () => {
  let firstRuns = 0;
  let secondRuns = 0;
  const first = job({
    id: "a:idempotency:b",
    process: async () => {
      firstRuns += 1;
    },
  });
  const second = job({
    id: "a",
    process: async () => {
      secondRuns += 1;
    },
  });

  try {
    await Promise.all([
      first.submit({ key: "c" }),
      second.submit({ key: "b:idempotency:c" }),
    ]);
    await waitFor(() => firstRuns === 1 && secondRuns === 1);
  } finally {
    first.stop();
    second.stop();
  }
});

test("jobs fail closed when a legacy claim already exists", async () => {
  const id = "legacy-job";
  const key = "scope-key";
  await redis.set(`sync:job:${id}:idempotency:${key}`, "old-worker-claim");
  const worker = job({ id, process: async () => {} });

  try {
    await expect(worker.submit({ key })).rejects.toThrow(/job claim migration required/);
  } finally {
    worker.stop();
  }
});

test("ordinary job claims use the full identity tuple", async () => {
  const id = uid("legacy-claim");
  let started = false;
  let release = (): void => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const worker = job({
    id,
    process: async () => {
      started = true;
      await gate;
    },
  });

  try {
    const jobId = await worker.submit({ key: "simple" });
    await waitFor(() => started);
    const claimKey =
      `sync:job:claim:v2:${encodeURIComponent(JSON.stringify(["sync:job", id, "simple"]))}`;
    expect(await redis.get(claimKey)).toContain(jobId);
    expect(await redis.get(`sync:job:${id}:idempotency:simple`)).toBeNull();
  } finally {
    release();
    worker.stop();
  }
});

test("ctx.key and ctx.jobId flow through process and after", async () => {
  let processKey: string | null = null;
  let afterKey: string | null = null;
  let afterJobId: string | null = null;

  const worker = job({
    id: uid("ctx-keys"),
    process: async ({ ctx }) => {
      processKey = ctx.key;
    },
    after: async ({ ctx }) => {
      afterKey = ctx.key;
      afterJobId = ctx.jobId;
    },
  });

  const id = await worker.submit({ key: "chat:xyz" });
  await waitFor(() => afterKey !== null);
  expect(processKey).toBe("chat:xyz");
  expect(afterKey).toBe("chat:xyz");
  expect(afterJobId).toBe(id);

  worker.stop();
});

test("ctx.duration reflects elapsed ms in process and after", async () => {
  let processDuration: number | null = null;
  let afterDuration: number | null = null;

  const worker = job({
    id: uid("duration"),
    process: async ({ ctx }) => {
      await Bun.sleep(30);
      processDuration = ctx.duration;
    },
    after: async ({ ctx }) => {
      afterDuration = ctx.duration;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => afterDuration !== null);
  expect(processDuration!).toBeGreaterThanOrEqual(25);
  expect(afterDuration!).toBeGreaterThanOrEqual(processDuration!);

  worker.stop();
});

// ==========================
// Typed Input
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

test("no input: submit omits input, ctx.input is undefined", async () => {
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

test("process throws → after sees ctx.error and no ctx.data", async () => {
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
  expect(afterData!.toISOString()).toBe(sent.toISOString());
  // Same instance — proof that no JSON round-trip happened
  expect(afterData).toBe(sent);

  worker.stop();
});

// ==========================
// Idempotency key lifecycle
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

test("key released after successful terminal; resubmit with same key gets new jobId", async () => {
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
  let attempts = 0;
  let afterRuns = 0;

  const worker = job({
    id: uid("key-release-fail"),
    process: async () => {
      attempts += 1;
      throw new Error("boom");
    },
    after: async () => {
      afterRuns += 1;
      // no ctx.reschedule() → terminal
    },
  });

  const id1 = await worker.submit({ key: "chat:x" });
  await waitFor(() => afterRuns === 1);
  await waitFor(() => worker.metric().failures === 1);
  expect(attempts).toBe(1);

  const id2 = await worker.submit({ key: "chat:x" });
  expect(id2).not.toBe(id1);
  await waitFor(() => afterRuns === 2);
  expect(attempts).toBe(2);

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

test("ctx.reschedule nacks with delay; failureCount increments", async () => {
  const failureCounts: number[] = [];
  let succeeded = false;

  const worker = job({
    id: uid("reschedule-count"),
    process: async ({ ctx }) => {
      if (ctx.failureCount < 2) throw new Error(`attempt ${ctx.failureCount + 1} fails`);
      succeeded = true;
    },
    after: async ({ ctx }) => {
      if (ctx.error) {
        failureCounts.push(ctx.failureCount);
        if (ctx.failureCount < 5) ctx.reschedule({ delayMs: 50 });
      }
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => succeeded, 10_000);
  expect(failureCounts).toEqual([0, 1]);

  worker.stop();
});

test("after with no reschedule call → terminal, no more attempts", async () => {
  let attempts = 0;
  let afterCalls = 0;

  const worker = job({
    id: uid("no-reschedule"),
    process: async () => {
      attempts += 1;
      throw new Error("nope");
    },
    after: async () => {
      afterCalls += 1;
    },
  });

  await worker.submit({ key: "x" });
  await waitFor(() => afterCalls === 1);
  await Bun.sleep(300);

  expect(attempts).toBe(1);
  expect(afterCalls).toBe(1);

  worker.stop();
});

test("no after defined → default is terminal (no reschedule)", async () => {
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

test("metric() reflects dispatches / failures / reschedules", async () => {
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
    return m.dispatches >= 2 && m.failures >= 1 && m.reschedules >= 1;
  }, 5_000);

  const m = worker.metric();
  // At least 2 succeeded ("good" + "retry-then-good" after retry),
  // at least 1 failed ("bad"), at least 1 rescheduled ("retry-then-good")
  expect(m.dispatches).toBeGreaterThanOrEqual(2);
  expect(m.failures).toBeGreaterThanOrEqual(1);
  expect(m.reschedules).toBeGreaterThanOrEqual(1);

  // metric() returns a copy
  m.dispatches = 9999;
  expect(worker.metric().dispatches).not.toBe(9999);

  worker.stop();
});

test("ctx.metric is a live reference inside after", async () => {
  const references: JobMetrics[] = [];

  const worker = job({
    id: uid("ctx-metric"),
    process: async () => {},
    after: async ({ ctx }) => {
      references.push(ctx.metric);
    },
  });

  try {
    await worker.submit({ key: "a" });
    await waitFor(() => references.length === 1 && worker.metric().dispatches === 1);
    expect(references[0]?.dispatches).toBe(1);

    await worker.submit({ key: "b" });
    await waitFor(() => references.length === 2 && worker.metric().dispatches === 2);
    expect(references[0]?.dispatches).toBe(2);
    expect(references[1]).toBe(references[0]);
  } finally {
    worker.stop();
  }
});

// ==========================
// delayMs / at
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

test("initial delay keeps the idempotency key claimed beyond keyTtlMs", async () => {
  let started = false;

  const worker = job({
    id: uid("delay-holds-key"),
    process: async () => {
      started = true;
    },
  });

  const first = await worker.submit({ key: "same", keyTtlMs: 1_000, delayMs: 1_500 });
  await Bun.sleep(1_100);
  expect(started).toBe(false);
  expect(await worker.submit({ key: "same", keyTtlMs: 1_000 })).toBe(first);

  await waitFor(() => worker.metric().dispatches === 1);
  worker.stop();
});

test("job timings reject non-finite and unsupported delays before claiming a key", async () => {
  expect(() =>
    job({
      id: uid("invalid-default"),
      defaults: { leaseMs: Number.NaN },
      process: async () => {},
    }),
  ).toThrow("defaults.leaseMs must be a safe integer");

  const id = uid("invalid-submit");
  const worker = job({ id, process: async () => {} });

  await expect(worker.submit({ key: "nan", delayMs: Number.NaN })).rejects.toThrow(
    "submit.delayMs must be a safe integer",
  );
  await expect(worker.submit({ key: "infinite", at: Number.POSITIVE_INFINITY })).rejects.toThrow(
    "submit.at must be a safe integer",
  );
  await expect(worker.submit({ key: "too-far", delayMs: 30 * 24 * 60 * 60 * 1_000 })).rejects.toThrow(
    "submit.delayMs must be a safe integer",
  );

  expect(await redis.send("GET", [`sync:job:${id}:seq`])).toBeNull();
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
// ctx.heartbeat / signal
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
  await waitFor(() => running, 10_000);

  // A competing consumer on the internal work queue. Its non-waiting recv
  // forces maintenance, so an expired lease is actually reaped — without one,
  // a single busy worker means nothing ever reaps and the lease lapsing has no
  // observable effect at all.
  const competitor = internalJobQueueReader(id);
  let stolen = false;
  while (!finished && !stolen) {
    const taken = await competitor.recv({ wait: false });
    if (taken) {
      stolen = true;
      await taken.ack();
    } else {
      await Bun.sleep(50);
    }
  }

  worker.stop();
  return { stolen };
};

test("ctx.heartbeat extends the lease so a long run is not redelivered", async () => {
  // `done` used to be set by the process body itself, so the old assertion held
  // whether the heartbeat succeeded, failed, or was never called.
  const { stolen } = await raceHeartbeat(uid("heartbeat"), async (ctx) => {
    for (let i = 0; i < 8; i++) {
      await Bun.sleep(100);
      await ctx.heartbeat();
    }
  });

  expect(stolen).toBe(false);
}, 30_000);

test("without a heartbeat a long run does lose its lease", async () => {
  // The negative control that gives the test above its meaning.
  const { stolen } = await raceHeartbeat(uid("no-heartbeat"), async () => {
    await Bun.sleep(1_500);
  });

  expect(stolen).toBe(true);
}, 30_000);

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

  const competitor = internalJobQueueReader(id);
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

test("a slow started trace cannot hand expired work to process", async () => {
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

  const competitor = internalJobQueueReader(id);
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

// ==========================
// stop()
// ==========================

test("stop halts the receive loop until the next submit restarts it", async () => {
  const processed: string[] = [];

  const worker = job({
    id: uid("stop"),
    process: async ({ ctx }) => {
      processed.push(ctx.key);
    },
  });

  // The old test called stop() before any worker existed — a no-op — then let
  // submit() restart one, and asserted the job *ran*, under a name promising
  // the loop halts. Neither half of the documented behaviour was tested.
  await worker.submit({ key: "first" });
  await waitFor(() => worker.metric().dispatches === 1, 10_000);

  worker.stop();
  await Bun.sleep(400);
  expect(processed).toEqual(["first"]);

  // A later submit restarts the loop, as documented.
  await worker.submit({ key: "second" });
  await waitFor(() => processed.length === 2, 10_000);
  expect(processed).toEqual(["first", "second"]);

  worker.stop();
}, 30_000);

// ==========================
// after error-swallow
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
