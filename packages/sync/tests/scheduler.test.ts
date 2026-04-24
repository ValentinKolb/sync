import { beforeEach, afterEach, expect, test } from "bun:test";
import { redis } from "bun";
import { scheduler, type Scheduler } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const waitFor = async (pred: () => boolean | Promise<boolean>, timeoutMs = 5_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:scheduler:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

let activeSchedulers: Scheduler[] = [];
afterEach(async () => {
  await Promise.all(activeSchedulers.map((s) => s.stop()));
  activeSchedulers = [];
});

const makeScheduler = (id: string, overrides?: Partial<Parameters<typeof scheduler>[0]>): Scheduler => {
  const s = scheduler({
    id,
    leader: { leaseMs: 1_000, heartbeatMs: 100 },
    dispatch: { tickMs: 100 },
    ...overrides,
  });
  activeSchedulers.push(s);
  return s;
};

// ==========================
// create / get / list / delete
// ==========================

test("create registers a schedule; get and list return it", async () => {
  const s = makeScheduler(uid("basic"));
  await s.create({
    id: "daily",
    cron: "0 3 * * *",
    tz: "Europe/Berlin",
    process: async () => {},
  });

  const info = await s.get({ id: "daily" });
  expect(info).not.toBeNull();
  expect(info?.id).toBe("daily");
  expect(info?.cron).toBe("0 3 * * *");
  expect(info?.tz).toBe("Europe/Berlin");
  expect(info?.runNumber).toBe(0);
  expect(info?.failureCount).toBe(0);
  expect(info?.nextRunAt).toBeGreaterThan(Date.now());

  const all = await s.list();
  expect(all).toHaveLength(1);
  expect(all[0]?.id).toBe("daily");
});

test("create is idempotent by id; second call updates", async () => {
  const s = makeScheduler(uid("idempotent"));

  const first = await s.create({
    id: "x",
    cron: "0 * * * *",
    tz: "UTC",
    process: async () => {},
  });
  expect(first.created).toBe(true);
  expect(first.updated).toBe(false);

  const second = await s.create({
    id: "x",
    cron: "0 * * * *",
    tz: "UTC",
    process: async () => {},
  });
  expect(second.created).toBe(false);
  expect(second.updated).toBe(true);

  const all = await s.list();
  expect(all).toHaveLength(1);
});

test("create with invalid cron throws", async () => {
  const s = makeScheduler(uid("invalid-cron"));
  await expect(
    s.create({
      id: "x",
      cron: "not a cron",
      process: async () => {},
    }),
  ).rejects.toThrow();
});

test("create with invalid tz throws", async () => {
  const s = makeScheduler(uid("invalid-tz"));
  await expect(
    s.create({
      id: "x",
      cron: "0 * * * *",
      tz: "Mars/Olympus_Mons",
      process: async () => {},
    }),
  ).rejects.toThrow();
});

test("delete removes schedule; get returns null; list excludes it", async () => {
  const s = makeScheduler(uid("delete"));
  await s.create({ id: "x", cron: "0 * * * *", process: async () => {} });
  await s.delete({ id: "x" });

  const info = await s.get({ id: "x" });
  expect(info).toBeNull();

  const all = await s.list();
  expect(all).toHaveLength(0);
});

test("delete on non-existent schedule is a no-op", async () => {
  const s = makeScheduler(uid("delete-nonexistent"));
  await s.delete({ id: "ghost" });
});

// ==========================
// runNow
// ==========================

test("runNow invokes process and increments runNumber", async () => {
  const s = makeScheduler(uid("runnow"));
  let runs = 0;
  let seenRunNumber = -1;

  await s.create({
    id: "manual",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      runs += 1;
      seenRunNumber = ctx.runNumber;
    },
  });

  await s.runNow({ id: "manual" });
  expect(runs).toBe(1);
  expect(seenRunNumber).toBe(1);

  await s.runNow({ id: "manual" });
  expect(runs).toBe(2);
  expect(seenRunNumber).toBe(2);
});

test("runNow does not advance nextRunAt (regular cron continues)", async () => {
  const s = makeScheduler(uid("runnow-preserves-next"));
  await s.create({
    id: "m",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {},
  });

  const before = (await s.get({ id: "m" }))!.nextRunAt;
  await s.runNow({ id: "m" });
  const after = (await s.get({ id: "m" }))!.nextRunAt;

  expect(after).toBe(before);
});

test("runNow rejects for unknown schedule", async () => {
  const s = makeScheduler(uid("runnow-missing"));
  await expect(s.runNow({ id: "ghost" })).rejects.toThrow("not found");
});

// ==========================
// dispatch loop & cron
// ==========================

test("scheduler dispatches due schedules via tick loop", async () => {
  const s = makeScheduler(uid("dispatch"));
  let runs = 0;

  await s.create({
    id: "tick",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
    },
  });

  const keyPrefix = `sync:scheduler:${s.id}`;
  const raw = await redis.get(`${keyPrefix}:schedule:tick`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:tick`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "tick"]);

  s.start();
  await waitFor(() => runs >= 1, 5_000);
  expect(runs).toBeGreaterThanOrEqual(1);
});

test("nextRunAt advances after successful dispatch", async () => {
  const s = makeScheduler(uid("advance"));
  let runs = 0;

  await s.create({
    id: "a",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
    },
  });

  const keyPrefix = `sync:scheduler:${s.id}`;
  const raw = await redis.get(`${keyPrefix}:schedule:a`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:a`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "a"]);

  s.start();
  await waitFor(() => runs >= 1, 5_000);
  await Bun.sleep(200);

  const info = await s.get({ id: "a" });
  expect(info?.nextRunAt).toBeGreaterThan(Date.now());
});

// ==========================
// ctx.reschedule
// ==========================

test("ctx.reschedule in after overrides cron advance", async () => {
  const s = makeScheduler(uid("reschedule"));
  let runs = 0;

  await s.create({
    id: "r",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
    },
    after: async ({ ctx }) => {
      if (ctx.runNumber === 1) ctx.reschedule({ delayMs: 100 });
    },
  });

  s.start();
  await s.runNow({ id: "r" });
  expect(runs).toBe(1);

  const info1 = await s.get({ id: "r" });
  expect(info1!.nextRunAt).toBeLessThan(Date.now() + 300);

  await waitFor(() => runs >= 2, 3_000);
  expect(runs).toBe(2);
});

test("ctx.reschedule on success triggers immediate re-run (polling pattern)", async () => {
  const s = makeScheduler(uid("polling"));
  let runs = 0;

  await s.create({
    id: "p",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
      return { hasMore: runs < 3 };
    },
    after: async ({ ctx }) => {
      if (ctx.data?.hasMore) ctx.reschedule({ delayMs: 30 });
    },
  });

  s.start();
  await s.runNow({ id: "p" });
  await waitFor(() => runs >= 3, 5_000);
  await Bun.sleep(200);
  expect(runs).toBe(3);
});

// ==========================
// failureCount
// ==========================

test("failureCount increments on failure and resets on success", async () => {
  const s = makeScheduler(uid("failurecount"));
  const seen: number[] = [];
  let succeedNext = false;

  await s.create({
    id: "f",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      seen.push(ctx.failureCount);
      if (!succeedNext) throw new Error("fail");
    },
    after: async ({ ctx }) => {
      if (ctx.error) ctx.reschedule({ delayMs: 30 });
    },
  });

  s.start();
  await s.runNow({ id: "f" });
  await waitFor(() => seen.length >= 2, 5_000);

  expect(seen[0]).toBe(0);
  expect(seen[1]).toBe(1);

  succeedNext = true;
  await waitFor(() => seen.length >= 3, 3_000);
  await Bun.sleep(200);

  const info = await s.get({ id: "f" });
  expect(info?.failureCount).toBe(0);
});

test("ctx.runNumber is 1-indexed and monotonic", async () => {
  const s = makeScheduler(uid("runnumber"));
  const seen: number[] = [];

  await s.create({
    id: "n",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      seen.push(ctx.runNumber);
    },
  });

  await s.runNow({ id: "n" });
  await s.runNow({ id: "n" });
  await s.runNow({ id: "n" });

  expect(seen).toEqual([1, 2, 3]);

  const info = await s.get({ id: "n" });
  expect(info?.runNumber).toBe(3);
});

test("runNumber preserved across re-registration with same cron", async () => {
  const s = makeScheduler(uid("rn-preserved"));

  await s.create({ id: "p", cron: "0 3 * * *", tz: "UTC", process: async () => {} });
  await s.runNow({ id: "p" });
  await s.runNow({ id: "p" });

  await s.create({ id: "p", cron: "0 3 * * *", tz: "UTC", process: async () => {} });

  const info = await s.get({ id: "p" });
  expect(info?.runNumber).toBe(2);
});

test("runNumber preserved but failureCount reset when cron changes", async () => {
  const s = makeScheduler(uid("rn-preserved-cron-change"));

  await s.create({ id: "p", cron: "0 3 * * *", tz: "UTC", process: async () => {} });
  await s.runNow({ id: "p" });
  await s.runNow({ id: "p" });

  await s.create({ id: "p", cron: "0 4 * * *", tz: "UTC", process: async () => {} });

  const info = await s.get({ id: "p" });
  expect(info?.runNumber).toBe(2);
  expect(info?.failureCount).toBe(0);
});

// ==========================
// metric()
// ==========================

test("metric() reflects dispatches, failures, reschedules", async () => {
  const s = makeScheduler(uid("metrics"));

  await s.create({
    id: "ok",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {},
  });
  await s.create({
    id: "bad",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      throw new Error("bad");
    },
  });
  await s.create({
    id: "retrying",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      if (ctx.failureCount < 1) throw new Error("retry me");
    },
    after: async ({ ctx }) => {
      if (ctx.error && ctx.failureCount < 1) ctx.reschedule({ delayMs: 10 });
    },
  });

  s.start();
  await s.runNow({ id: "ok" });
  await s.runNow({ id: "bad" });
  await s.runNow({ id: "retrying" });

  await Bun.sleep(300);

  const m = s.metric();
  expect(m.dispatches).toBeGreaterThanOrEqual(2);
  expect(m.failures).toBeGreaterThanOrEqual(1);
  expect(m.reschedules).toBeGreaterThanOrEqual(1);

  m.dispatches = 9999;
  expect(s.metric().dispatches).not.toBe(9999);
});

test("ctx.metric is live reference inside after", async () => {
  const s = makeScheduler(uid("ctx-metric"));
  let seen: number = -1;

  await s.create({
    id: "m",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {},
    after: async ({ ctx }) => {
      seen = ctx.metric.dispatches;
    },
  });

  await s.runNow({ id: "m" });
  expect(seen).toBe(0);

  await Bun.sleep(50);
  expect(s.metric().dispatches).toBe(1);
});

// ==========================
// leader election
// ==========================

test("metric.isLeader flips to true after start", async () => {
  const s = makeScheduler(uid("leader"));
  expect(s.metric().isLeader).toBe(false);

  s.start();
  await waitFor(() => s.metric().isLeader, 3_000);
  expect(s.metric().isLeader).toBe(true);
  expect(s.metric().leaderChanges).toBeGreaterThanOrEqual(1);

  await s.stop();
  expect(s.metric().isLeader).toBe(false);
});

test("only one of two schedulers with same id is leader at a time", async () => {
  const schedId = uid("leader-election");
  const s1 = makeScheduler(schedId);
  const s2 = makeScheduler(schedId);

  s1.start();
  s2.start();

  await waitFor(() => s1.metric().isLeader || s2.metric().isLeader, 3_000);

  const s1Leader = s1.metric().isLeader;
  const s2Leader = s2.metric().isLeader;
  expect(s1Leader !== s2Leader).toBe(true);
});

// ==========================
// after error swallow
// ==========================

test("errors thrown in after do not crash the scheduler", async () => {
  const s = makeScheduler(uid("after-throws"));
  let processed = 0;

  await s.create({
    id: "e",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      processed += 1;
    },
    after: async () => {
      throw new Error("after threw");
    },
  });

  await s.runNow({ id: "e" });
  await s.runNow({ id: "e" });

  expect(processed).toBe(2);

  const info = await s.get({ id: "e" });
  expect(info?.runNumber).toBe(2);
});

// ==========================
// ctx.trigger
// ==========================

test("ctx.trigger is 'manual' when invoked via runNow", async () => {
  const s = makeScheduler(uid("trigger-manual"));
  let processTrigger: string | null = null;
  let afterTrigger: string | null = null;

  await s.create({
    id: "t",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      processTrigger = ctx.trigger;
    },
    after: async ({ ctx }) => {
      afterTrigger = ctx.trigger;
    },
  });

  await s.runNow({ id: "t" });
  expect(processTrigger).toBe("manual");
  expect(afterTrigger).toBe("manual");
});

test("ctx.trigger is 'cron' when dispatched via tick loop", async () => {
  const s = makeScheduler(uid("trigger-cron"));
  let processTrigger: string | null = null;
  let afterTrigger: string | null = null;

  await s.create({
    id: "t",
    cron: "* * * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      processTrigger = ctx.trigger;
    },
    after: async ({ ctx }) => {
      afterTrigger = ctx.trigger;
    },
  });

  // Force due
  const keyPrefix = `sync:scheduler:${s.id}`;
  const raw = await redis.get(`${keyPrefix}:schedule:t`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:t`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "t"]);

  s.start();
  await waitFor(() => processTrigger !== null, 5_000);
  expect(processTrigger).toBe("cron");
  expect(afterTrigger).toBe("cron");
});
