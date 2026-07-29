import { beforeEach, afterEach, expect, test } from "bun:test";
import { redis } from "bun";
import {
  SchedulerControlNotFoundError,
  SchedulerControlUnavailableError,
  scheduler,
  schedulerControl,
  type Scheduler,
  type SchedulerTraceEvent,
} from "../index";

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

// ==========================
// trace
// ==========================

test("trace records schedule creation and manual run lifecycle", async () => {
  const s = makeScheduler(uid("trace-lifecycle"));
  const events: SchedulerTraceEvent<{ ok: boolean }>[] = [];

  await s.create<{ ok: boolean }>({
    id: "t",
    cron: "0 3 * * *",
    tz: "UTC",
    meta: { source: "test" },
    trace: (event) => {
      events.push(event);
    },
    process: async () => ({ ok: true }),
  });

  expect(events.map((event) => event.type)).toEqual(["scheduled"]);
  const scheduled = events[0] as Extract<SchedulerTraceEvent<{ ok: boolean }>, { type: "scheduled" }>;
  expect(scheduled.scheduleId).toBe("t");
  expect(scheduled.cron).toBe("0 3 * * *");
  expect(scheduled.tz).toBe("UTC");
  expect(scheduled.nextRunAt).toBeGreaterThan(Date.now());
  expect(scheduled.meta?.source).toBe("test");

  await s.runNow({ id: "t" });

  expect(events.map((event) => event.type)).toEqual(["scheduled", "started", "succeeded"]);
  const started = events[1] as Extract<SchedulerTraceEvent<{ ok: boolean }>, { type: "started" }>;
  expect(started.runNumber).toBe(1);
  expect(started.trigger).toBe("manual");
  expect(started.slotTs).toBe(scheduled.nextRunAt);

  const succeeded = events[2] as Extract<SchedulerTraceEvent<{ ok: boolean }>, { type: "succeeded" }>;
  expect(succeeded.runNumber).toBe(1);
  expect(succeeded.data.ok).toBe(true);
  expect(succeeded.durationMs).toBeGreaterThanOrEqual(0);
});

test("trace scheduled reports preserved nextRunAt on unchanged update", async () => {
  const s = makeScheduler(uid("trace-preserved-next"));
  const events: SchedulerTraceEvent<void>[] = [];

  const cfg = {
    id: "t",
    cron: "0 3 * * *",
    tz: "UTC",
    trace: (event: SchedulerTraceEvent<void>) => {
      events.push(event);
    },
    process: async () => {},
  };

  await s.create(cfg);
  const firstNextRunAt = (events[0] as Extract<SchedulerTraceEvent<void>, { type: "scheduled" }>).nextRunAt;

  await s.runNow({ id: "t" });
  await s.create(cfg);

  const current = await s.get({ id: "t" });
  const secondScheduled = events.filter((event) => event.type === "scheduled")[1] as Extract<
    SchedulerTraceEvent<void>,
    { type: "scheduled" }
  >;
  expect(secondScheduled.nextRunAt).toBe(firstNextRunAt);
  expect(secondScheduled.nextRunAt).toBe(current?.nextRunAt);
});

test("trace records scheduler failures and reschedules", async () => {
  const s = makeScheduler(uid("trace-reschedule"));
  const events: SchedulerTraceEvent<string>[] = [];

  await s.create<string>({
    id: "r",
    cron: "0 3 * * *",
    tz: "UTC",
    trace: (event) => {
      events.push(event);
    },
    process: async () => {
      throw new Error("try again");
    },
    after: async ({ ctx }) => {
      if (ctx.error) ctx.reschedule({ delayMs: 25 });
    },
  });

  await s.runNow({ id: "r" });

  expect(events.map((event) => event.type)).toEqual(["scheduled", "started", "failed", "rescheduled"]);
  const failed = events[2] as Extract<SchedulerTraceEvent<string>, { type: "failed" }>;
  expect(failed.error.message).toBe("try again");

  const rescheduled = events[3] as Extract<SchedulerTraceEvent<string>, { type: "rescheduled" }>;
  expect(rescheduled.runNumber).toBe(1);
  expect(rescheduled.delayMs).toBe(25);
});

test("trace errors are swallowed and do not affect scheduler execution", async () => {
  const originalWarn = console.warn;
  let warnings = 0;
  console.warn = () => {
    warnings += 1;
  };

  const s = makeScheduler(uid("trace-throws"));
  let ran = false;

  try {
    await s.create({
      id: "t",
      cron: "0 3 * * *",
      tz: "UTC",
      trace: () => {
        throw new Error("trace failed");
      },
      process: async () => {
        ran = true;
      },
    });

    await s.runNow({ id: "t" });
    expect(ran).toBe(true);
    expect((await s.get({ id: "t" }))?.runNumber).toBe(1);
    expect(warnings).toBeGreaterThan(0);
  } finally {
    console.warn = originalWarn;
  }
});

// ==========================
// schedulerControl
// ==========================

test("schedulerControl lists schedules with meta and availability", async () => {
  const s = makeScheduler(uid("control-list"));

  await s.create({
    id: "sync-users",
    cron: "0 3 * * *",
    tz: "UTC",
    meta: { label: "Sync users" },
    process: async () => {},
  });

  const control = schedulerControl();
  let listed = await control.list();
  let info = listed.find((entry) => entry.schedulerId === s.id && entry.scheduleId === "sync-users");
  expect(info?.cron).toBe("0 3 * * *");
  expect(info?.tz).toBe("UTC");
  expect(info?.state).toBe("unavailable");
  expect(info?.meta?.label).toBe("Sync users");

  s.start();
  await waitFor(async () => {
    listed = await control.list();
    info = listed.find((entry) => entry.schedulerId === s.id && entry.scheduleId === "sync-users");
    return info?.state === "available";
  });
});

test("schedulerControl runNow is accepted by a live scheduler and does not advance cron", async () => {
  const s = makeScheduler(uid("control-run"));
  const events: SchedulerTraceEvent<void>[] = [];
  let runs = 0;
  let trigger: "cron" | "manual" | null = null;

  await s.create({
    id: "reindex",
    cron: "0 3 * * *",
    tz: "UTC",
    trace: (event) => {
      events.push(event);
    },
    process: async ({ ctx }) => {
      runs += 1;
      trigger = ctx.trigger;
    },
  });

  const before = (await s.get({ id: "reindex" }))!.nextRunAt;
  s.start();
  const control = schedulerControl();
  await waitFor(async () => {
    const listed = await control.list();
    return listed.some((entry) => entry.schedulerId === s.id && entry.scheduleId === "reindex" && entry.state === "available");
  });

  await control.runNow({ schedulerId: s.id, scheduleId: "reindex", timeoutMs: 5_000 });

  await waitFor(() => runs === 1);
  expect(trigger).toBe("manual");
  expect((await s.get({ id: "reindex" }))!.nextRunAt).toBe(before);
  expect(events.some((event) => event.type === "started" && event.trigger === "manual")).toBe(true);
});

test("schedulerControl runNow reports unavailable when no live handler exists", async () => {
  const s = makeScheduler(uid("control-unavailable"));
  await s.create({ id: "cleanup", cron: "0 3 * * *", tz: "UTC", process: async () => {} });

  await expect(
    schedulerControl().runNow({ schedulerId: s.id, scheduleId: "cleanup", timeoutMs: 100 }),
  ).rejects.toBeInstanceOf(SchedulerControlUnavailableError);
});

test("schedulerControl runNow reports not found for missing schedules", async () => {
  await expect(
    schedulerControl().runNow({ schedulerId: uid("missing-scheduler"), scheduleId: "missing", timeoutMs: 100 }),
  ).rejects.toBeInstanceOf(SchedulerControlNotFoundError);
});

test("schedulerControl keeps handler availability alive during a long callback", async () => {
  const prefix = `test:sched:${uid("control-heartbeat-prefix")}`;
  const schedId = uid("control-heartbeat");
  const sched = makeScheduler(schedId, { prefix, dispatch: { tickMs: 50 } });
  let release = (): void => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let started = false;

  await sched.create({
    id: "slow",
    cron: "0 3 * * *",
    process: async () => {
      started = true;
      await gate;
    },
  });
  sched.start();
  const control = schedulerControl({ prefix });
  await waitFor(async () => {
    const listed = await control.list();
    return listed.some((entry) => entry.schedulerId === schedId && entry.scheduleId === "slow" && entry.state === "available");
  });

  try {
    await control.runNow({ schedulerId: schedId, scheduleId: "slow", timeoutMs: 2_000 });
    await waitFor(() => started);

    const members = await redis.send("SMEMBERS", [`${prefix}:${schedId}:control:slow:handlers`]);
    expect(Array.isArray(members)).toBe(true);
    expect((members as unknown[]).length).toBe(1);
    await redis.send("PEXPIRE", [String((members as unknown[])[0]), "100"]);

    await Bun.sleep(2_100);
    const listed = await control.list();
    expect(
      listed.find((entry) => entry.schedulerId === schedId && entry.scheduleId === "slow")?.state,
    ).toBe("available");
  } finally {
    release();
  }
});

test("schedulerControl executes one target for a repeated requestId", async () => {
  const prefix = `test:sched:${uid("control-request-prefix")}`;
  const schedId = uid("control-request");
  const sched = makeScheduler(schedId, { prefix, dispatch: { tickMs: 50 } });
  let runs = 0;

  await sched.create({
    id: "one",
    cron: "0 3 * * *",
    process: async () => {
      runs += 1;
      await Bun.sleep(100);
    },
  });
  await sched.create({ id: "two", cron: "0 3 * * *", process: async () => {} });
  sched.start();

  const control = schedulerControl({ prefix });
  await waitFor(async () => {
    const listed = await control.list();
    return listed.filter((entry) => entry.schedulerId === schedId && entry.state === "available").length === 2;
  });

  const requestId = uid("request");
  const request = { schedulerId: schedId, scheduleId: "one", requestId, timeoutMs: 2_000 };
  await Promise.all([control.runNow(request), control.runNow(request)]);
  await waitFor(() => runs === 1);
  await Bun.sleep(200);
  expect(runs).toBe(1);

  await expect(
    control.runNow({ schedulerId: schedId, scheduleId: "two", requestId, timeoutMs: 2_000 }),
  ).rejects.toThrow("already bound to another schedule");
});

test("schedule meta round-trips byte-equivalent JSON", async () => {
  const sched = scheduler({ id: uid("meta-fidelity"), prefix: "test:sched" });
  const meta = {
    tags: [] as string[],
    emptyObj: {},
    unicode: "日本😀",
    big: 9007199254740991,
    nested: [[], {}] as unknown[],
  };

  await sched.create({ id: "s1", cron: "0 3 * * *", meta, process: async () => {} });
  expect((await sched.get({ id: "s1" }))?.meta).toEqual(meta);

  // An update re-runs the record through the upsert script a second time.
  await sched.create({ id: "s1", cron: "0 4 * * *", meta, process: async () => {} });
  const after = await sched.get({ id: "s1" });
  expect(after?.meta).toEqual(meta);
  expect(Array.isArray((after?.meta as { tags: unknown }).tags)).toBe(true);

  const listed = (await sched.list()).find((s) => s.id === "s1");
  expect(listed?.meta).toEqual(meta);
});

test("schedules written in the 5.8.0 record format keep their meta", async () => {
  const id = uid("meta-legacy");
  const sched = scheduler({ id, prefix: "test:sched" });

  await redis.send("SET", [
    `test:sched:${id}:schedule:legacy`,
    JSON.stringify({
      id: "legacy",
      cron: "0 3 * * *",
      tz: "UTC",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: Date.now() + 60_000,
      runNumber: 4,
      failureCount: 0,
      meta: { source: "5.8.0" },
    }),
  ]);
  await redis.send("SADD", [`test:sched:${id}:index`, "legacy"]);

  const info = await sched.get({ id: "legacy" });
  expect(info?.meta).toEqual({ source: "5.8.0" });
  expect(info?.runNumber).toBe(4);
});

test("a repeatedly failing control dispatch is reported, not replayed forever", async () => {
  const schedId = uid("control-storm");
  const sched = scheduler({ id: schedId, prefix: "test:sched", dispatch: { tickMs: 50 } });
  activeSchedulers.push(sched);

  let runs = 0;
  await sched.create({
    id: "boom",
    cron: "0 3 * * *", // never naturally due
    process: async () => {
      runs += 1;
    },
  });
  sched.start();

  // Corrupt the due ZSET so the terminal write inside dispatchOne fails on every
  // attempt. The schedule record itself stays readable, so the control loop
  // accepts the request and then fails while persisting.
  const dueKey = `test:sched:${schedId}:due`;
  await redis.send("DEL", [dueKey]);
  await redis.send("SET", [dueKey, "corrupt"]);

  const control = schedulerControl({ prefix: "test:sched" });
  await control.runNow({ schedulerId: schedId, scheduleId: "boom", timeoutMs: 3_000 }).catch(() => {});

  await Bun.sleep(2_500);
  const settled = runs;
  await Bun.sleep(2_000);

  // The control queue allows effectively unlimited deliveries, so an
  // unconditional nack replayed the user's callback about every 250ms for the
  // full five-minute message-age window.
  expect(settled).toBeGreaterThanOrEqual(1);
  expect(settled).toBeLessThanOrEqual(3);
  expect(runs).toBe(settled);

  await redis.send("DEL", [dueKey]);
}, 30_000);
