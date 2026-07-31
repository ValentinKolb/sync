import { afterEach, expect, test } from "bun:test";
import { redis } from "bun";
import {
  SchedulerControlNotFoundError,
  SchedulerControlUnavailableError,
  scheduler,
  schedulerControl,
  type Scheduler,
  type SchedulerMetrics,
  type SchedulerTraceEvent,
} from "../index";
import {
  legacySchedulerScheduleKey,
  markSchedulerControlAccepted,
  markSchedulerControlPending,
  markSchedulerControlUnavailable,
  refreshSchedulerControlRequestBinding,
  schedulerDueKey,
  schedulerScheduleTombstoneKey,
  schedulerScheduleKey,
  schedulerV2ScheduleKey,
  schedulerControlQueue,
  type SchedulerControlRequest,
} from "../src/scheduler-control";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
const encodedIdentity = (value: string): string => value.replaceAll("%", "%25").replaceAll(":", "%3A");

const waitFor = async (pred: () => boolean | Promise<boolean>, timeoutMs = 5_000, pollMs = 20): Promise<void> => {
  const start = Date.now();
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(pollMs);
  }
};

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
  const recordKey = schedulerScheduleKey("sync:scheduler", s.id, "tick");
  const raw = await redis.get(recordKey);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(recordKey, JSON.stringify(parsed));
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
  const recordKey = schedulerScheduleKey("sync:scheduler", s.id, "a");
  const raw = await redis.get(recordKey);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(recordKey, JSON.stringify(parsed));
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

test("non-finite reschedule delays cannot corrupt durable state", async () => {
  const s = makeScheduler(uid("reschedule-invalid"));
  await s.create({
    id: "poll",
    cron: "0 3 * * *",
    process: async () => {},
    after: async ({ ctx }) => {
      ctx.reschedule({ delayMs: Number.NaN });
    },
  });
  const before = await s.get({ id: "poll" });

  await s.runNow({ id: "poll" });

  const after = await s.get({ id: "poll" });
  expect(after?.runNumber).toBe(1);
  expect(after?.nextRunAt).toBe(before?.nextRunAt);
  expect(Number.isFinite(after?.nextRunAt)).toBe(true);
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

  await waitFor(() => s.metric().dispatches >= 2, 2_500);

  const m = s.metric();
  expect(m.dispatches).toBeGreaterThanOrEqual(2);
  expect(m.failures).toBeGreaterThanOrEqual(1);
  expect(m.reschedules).toBeGreaterThanOrEqual(1);

  m.dispatches = 9999;
  expect(s.metric().dispatches).not.toBe(9999);
});

test("ctx.metric is live reference inside after", async () => {
  const s = makeScheduler(uid("ctx-metric"));
  const references: SchedulerMetrics[] = [];

  await s.create({
    id: "m",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {},
    after: async ({ ctx }) => {
      references.push(ctx.metric);
    },
  });

  await s.runNow({ id: "m" });
  expect(references).toHaveLength(1);
  expect(references[0]?.dispatches).toBe(1);

  await s.runNow({ id: "m" });
  expect(references).toHaveLength(2);
  expect(references[0]?.dispatches).toBe(2);
  expect(references[1]).toBe(references[0]);
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
  const recordKey = schedulerScheduleKey("sync:scheduler", s.id, "t");
  const raw = await redis.get(recordKey);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(recordKey, JSON.stringify(parsed));
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

test("schedulerControl waits for the cross-handle dispatch lock before acceptance", async () => {
  const prefix = `test:sched:${uid("control-lock-prefix")}`;
  const schedulerId = uid("control-lock");
  const first = makeScheduler(schedulerId, {
    prefix,
    leader: { leaseMs: 500, heartbeatMs: 50 },
    dispatch: { tickMs: 50 },
  });
  const second = makeScheduler(schedulerId, {
    prefix,
    leader: { leaseMs: 500, heartbeatMs: 50 },
    dispatch: { tickMs: 50 },
  });
  let starts = 0;
  let releaseFirst = (): void => {};
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const process = async (): Promise<void> => {
    starts += 1;
    if (starts === 1) await firstGate;
  };

  await first.create({ id: "reindex", cron: "0 3 * * *", process });
  await second.create({ id: "reindex", cron: "0 3 * * *", process });

  const direct = first.runNow({ id: "reindex" });
  await waitFor(() => starts === 1);
  second.start();
  const control = schedulerControl({ prefix });
  await waitFor(async () => {
    const listed = await control.list();
    return listed.some(
      (entry) => entry.schedulerId === schedulerId && entry.scheduleId === "reindex" && entry.state === "available",
    );
  });

  let accepted = false;
  const remote = control
    .runNow({ schedulerId, scheduleId: "reindex", timeoutMs: 5_000 })
    .then(() => {
      accepted = true;
    });
  await Bun.sleep(250);
  expect(accepted).toBe(false);
  expect(starts).toBe(1);

  releaseFirst();
  await Promise.all([direct, remote]);
  await waitFor(() => starts === 2);
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

    const members = await redis.send("SMEMBERS", [
      `${prefix}:${encodedIdentity(schedId)}:control:${encodedIdentity("slow")}:handlers`,
    ]);
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

test("scheduler control queues keep colon-rich target identities distinct", async () => {
  const prefix = `test:sched:${uid("control-identity")}`;
  const first = schedulerControlQueue(prefix, "a:b", "c");
  const second = schedulerControlQueue(prefix, "a", "b:c");

  await first.send({
    data: { requestId: "same", schedulerId: "a:b", scheduleId: "c", requestedAt: Date.now() },
    idempotencyKey: "same",
  });
  await second.send({
    data: { requestId: "same", schedulerId: "a", scheduleId: "b:c", requestedAt: Date.now() },
    idempotencyKey: "same",
  });

  const firstMessage = await first.recv({ wait: false });
  const secondMessage = await second.recv({ wait: false });
  expect(firstMessage?.data.schedulerId).toBe("a:b");
  expect(secondMessage?.data.schedulerId).toBe("a");
  await firstMessage?.ack();
  await secondMessage?.ack();
});

test("scheduler control queues use the collision-free namespace", async () => {
  const prefix = `test:sched:${uid("control-v2")}`;
  const schedulerId = "worker";
  const scheduleId = "daily";
  const queueId = `${encodedIdentity(schedulerId)}:${encodedIdentity(scheduleId)}:manual`;
  const base =
    `sync:queue:namespace:v2:${encodeURIComponent(JSON.stringify([`${prefix}:control`, "default", queueId]))}`;
  await redis.set(`${prefix}:control:default:${queueId}:seq`, "99");
  await redis.send("LPUSH", [`${prefix}:control:default:${queueId}:ready`, "legacy-request"]);
  await redis.set(`${base}:seq`, "41");
  const controlQueue = schedulerControlQueue(prefix, schedulerId, scheduleId);

  const sent = await controlQueue.send({
    data: { requestId: "request", schedulerId, scheduleId, requestedAt: Date.now() },
  });
  expect(sent.messageId).toBe("42");
  expect((await controlQueue.recv({ wait: false }))?.data.scheduleId).toBe(scheduleId);
  expect(await redis.send("LLEN", [`${prefix}:control:default:${queueId}:ready`])).toBe(1);
});

test("colon-rich scheduler and schedule ids keep durable records distinct", async () => {
  const prefix = `test:sched:${uid("record-identity")}`;
  const first = makeScheduler("a:schedule:b", { prefix });
  const second = makeScheduler("a", { prefix });

  await first.create({ id: "c", cron: "0 1 * * *", meta: { owner: "first" }, process: async () => {} });
  await second.create({
    id: "b:schedule:c",
    cron: "0 2 * * *",
    meta: { owner: "second" },
    process: async () => {},
  });

  expect((await first.get({ id: "c" }))?.meta).toEqual({ owner: "first" });
  expect((await second.get({ id: "b:schedule:c" }))?.meta).toEqual({ owner: "second" });

  const listed = await schedulerControl({ prefix }).list();
  expect(listed.find((entry) => entry.schedulerId === "a:schedule:b")?.scheduleId).toBe("c");
  expect(listed.find((entry) => entry.schedulerId === "a")?.scheduleId).toBe("b:schedule:c");
});

test("colon-rich schedules keep distinct state and serialize through the legacy upgrade fence", async () => {
  const prefix = `test:sched:${uid("all-key-identities")}`;
  const first = makeScheduler("a:dispatch:b", { prefix });
  const second = makeScheduler("a", { prefix });
  const started: string[] = [];
  let release = (): void => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  await first.create({
    id: "c",
    cron: "0 1 * * *",
    process: async () => {
      started.push("first");
      if (started.length === 1) await gate;
    },
  });
  await second.create({
    id: "b:dispatch:c",
    cron: "0 2 * * *",
    process: async () => {
      started.push("second");
      if (started.length === 1) await gate;
    },
  });

  expect(schedulerScheduleKey(prefix, "a", "b:due")).not.toBe(schedulerDueKey(prefix, "a:schedule:b"));

  const runs = Promise.all([
    first.runNow({ id: "c" }),
    second.runNow({ id: "b:dispatch:c" }),
  ]);
  try {
    await waitFor(() => started.length === 1);
    await Bun.sleep(100);
    expect(started).toHaveLength(1);
  } finally {
    release();
  }
  await runs;
  expect(started.sort()).toEqual(["first", "second"]);
});

test("a colon-rich schedule still coordinates with a legacy dispatch lock", async () => {
  const prefix = `test:sched:${uid("legacy-dispatch-lock")}`;
  const schedulerId = "rolling:worker";
  const scheduleId = "daily:sync";
  const sched = makeScheduler(schedulerId, { prefix, leader: { leaseMs: 500 } });
  let started = false;
  await sched.create({
    id: scheduleId,
    cron: "0 1 * * *",
    process: async () => {
      started = true;
    },
  });

  const legacyLockKey = `${prefix}:dispatch:${schedulerId}:dispatch:${scheduleId}`;
  await redis.send("SET", [legacyLockKey, "old-worker", "PX", "5000"]);
  const running = sched.runNow({ id: scheduleId });

  try {
    await Bun.sleep(100);
    expect(started).toBe(false);
  } finally {
    await redis.send("DEL", [legacyLockKey]);
  }
  await running;
  expect(started).toBe(true);
});

test("a new colliding target cannot overwrite an unmigrated legacy schedule", async () => {
  const prefix = `test:sched:${uid("mixed-record-identity")}`;
  const legacyKey = `${prefix}:a:schedule:b:schedule:c`;
  await redis.set(
    legacyKey,
    JSON.stringify({
      id: "c",
      cron: "0 1 * * *",
      tz: "UTC",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: Date.now() + 60_000,
      runNumber: 9,
      failureCount: 0,
      metaJson: JSON.stringify({ owner: "legacy" }),
    }),
  );

  const current = makeScheduler("a", { prefix });
  await current.create({
    id: "b:schedule:c",
    cron: "0 2 * * *",
    meta: { owner: "current" },
    process: async () => {},
  });
  expect((await current.get({ id: "b:schedule:c" }))?.meta).toEqual({ owner: "current" });
  expect(JSON.parse((await redis.get(legacyKey)) as string).id).toBe("c");

  await redis.send("SADD", [`${prefix}:index`, "a:schedule:b"]);
  const legacy = makeScheduler("a:schedule:b", { prefix });
  expect((await legacy.get({ id: "c" }))?.runNumber).toBe(9);
  expect((await legacy.get({ id: "c" }))?.meta).toEqual({ owner: "legacy" });
});

test("colliding scheduler identities with different schedules do not share due state", async () => {
  const root = `test:sched:${uid("namespace-owner")}`;
  const first = makeScheduler("b", { prefix: `${root}:a` });
  const second = makeScheduler("a:b", { prefix: root });

  await Promise.all([
    first.create({ id: "first", cron: "0 1 * * *", process: async () => {} }),
    second.create({ id: "second", cron: "0 2 * * *", process: async () => {} }),
  ]);

  expect((await first.list()).map((schedule) => schedule.id)).toEqual(["first"]);
  expect((await second.list()).map((schedule) => schedule.id)).toEqual(["second"]);
});

test("a colliding scheduler delete cannot remove the legacy owner's schedule", async () => {
  const root = `test:sched:${uid("namespace-delete")}`;
  const owner = makeScheduler("b", { prefix: `${root}:a` });
  const other = makeScheduler("a:b", { prefix: root });
  await owner.create({ id: "shared", cron: "0 1 * * *", process: async () => {} });
  await other.create({ id: "other", cron: "0 2 * * *", process: async () => {} });

  await other.delete({ id: "shared" });

  expect((await owner.get({ id: "shared" }))?.id).toBe("shared");
  expect((await other.get({ id: "other" }))?.id).toBe("other");

  await other.delete({ id: "other" });
  expect(await other.list()).toEqual([]);
  expect((await owner.get({ id: "shared" }))?.id).toBe("shared");
});

test("ambiguous legacy scheduler ownership fails closed across prefixes", async () => {
  const root = `test:sched:${uid("legacy-owner")}`;
  const firstPrefix = `${root}:a`;
  const firstSchedulerId = "b";
  const secondPrefix = root;
  const secondSchedulerId = "a:b";
  const scheduleId = "daily";
  const legacyKey = legacySchedulerScheduleKey(firstPrefix, firstSchedulerId, scheduleId);
  await redis.set(legacyKey, JSON.stringify({
    id: scheduleId,
    cron: "0 1 * * *",
    tz: "UTC",
    createdAt: Date.now(),
    updatedAt: Date.now(),
    nextRunAt: Date.now() + 60_000,
    runNumber: 0,
    failureCount: 0,
  }));
  await redis.send("SADD", [`${firstPrefix}:index`, firstSchedulerId]);
  await redis.send("SADD", [`${secondPrefix}:index`, secondSchedulerId]);

  const first = makeScheduler(firstSchedulerId, { prefix: firstPrefix });
  const second = makeScheduler(secondSchedulerId, { prefix: secondPrefix });
  await expect(first.get({ id: scheduleId })).rejects.toThrow(/scheduler namespace migration required/);
  await expect(second.get({ id: scheduleId })).rejects.toThrow(/scheduler namespace migration required/);
});

test("a late conflicting legacy registration fences an existing namespace owner", async () => {
  const root = `test:sched:${uid("late-legacy-owner")}`;
  const prefix = `${root}:a`;
  const schedulerId = "b";
  const sched = makeScheduler(schedulerId, { prefix });
  await sched.create({ id: "daily", cron: "0 1 * * *", process: async () => {} });

  await redis.send("SADD", [`${root}:index`, "a:b"]);

  await expect(sched.get({ id: "daily" })).rejects.toThrow(/scheduler namespace migration required/);
  await expect(sched.delete({ id: "daily" })).rejects.toThrow(/scheduler namespace migration required/);
  expect(await redis.get(schedulerScheduleKey(prefix, schedulerId, "daily"))).not.toBeNull();
});

test("a current-only colliding registration is not treated as a late legacy owner", async () => {
  const root = `test:sched:${uid("current-only-owner")}`;
  const legacyOwner = makeScheduler("b", { prefix: `${root}:a` });
  const currentOnly = makeScheduler("a:b", { prefix: root });

  expect(await legacyOwner.get({ id: "missing" })).toBeNull();
  await currentOnly.create({
    id: "current",
    cron: "0 2 * * *",
    process: async () => {},
  });

  expect(await legacyOwner.get({ id: "missing" })).toBeNull();
  await legacyOwner.create({
    id: "legacy",
    cron: "0 1 * * *",
    process: async () => {},
  });

  expect((await legacyOwner.list()).map((schedule) => schedule.id)).toEqual(["legacy"]);
  expect((await currentOnly.list()).map((schedule) => schedule.id)).toEqual(["current"]);
});

test("revisionless workers cannot roll back revisioned config but can advance compatible runtime", async () => {
  const prefix = `test:sched:${uid("rolling-records")}`;
  const schedulerId = "rolling";
  const scheduleId = "sync";
  const sched = makeScheduler(schedulerId, { prefix });
  await sched.create({
    id: scheduleId,
    cron: "0 2 * * *",
    tz: "Europe/Berlin",
    meta: { generation: "current" },
    process: async () => {},
  });

  const currentKey = schedulerScheduleKey(prefix, schedulerId, scheduleId);
  const v2Key = schedulerV2ScheduleKey(prefix, schedulerId, scheduleId);
  const legacyKey = legacySchedulerScheduleKey(prefix, schedulerId, scheduleId);
  const initial = JSON.parse((await redis.get(currentKey)) as string);

  const staleConfigWrite = {
    id: scheduleId,
    cron: "0 1 * * *",
    tz: "UTC",
    meta: { generation: "legacy" },
    createdAt: initial.createdAt - 1_000,
    updatedAt: initial.updatedAt + 10_000,
    nextRunAt: initial.nextRunAt + 10_000,
    runNumber: 40,
    failureCount: 3,
    lastError: "stale failure",
  };
  await redis.set(legacyKey, JSON.stringify(staleConfigWrite));

  await sched.create({
    id: scheduleId,
    cron: "0 2 * * *",
    tz: "Europe/Berlin",
    meta: { generation: "current" },
    process: async () => {},
  });
  const afterUpdate = await sched.get({ id: scheduleId });
  expect(afterUpdate?.cron).toBe("0 2 * * *");
  expect(afterUpdate?.tz).toBe("Europe/Berlin");
  expect(afterUpdate?.meta).toEqual({ generation: "current" });
  expect(afterUpdate?.runNumber).toBe(0);

  const revisioned = JSON.parse((await redis.get(currentKey)) as string);
  expect(typeof revisioned.revision).toBe("string");
  expect(revisioned.revision.length).toBeGreaterThan(0);

  const compatibleRuntimeWrite = {
    id: scheduleId,
    cron: revisioned.cron,
    tz: revisioned.tz,
    meta: { generation: "legacy" },
    createdAt: revisioned.createdAt - 1_000,
    updatedAt: revisioned.updatedAt + 20_000,
    nextRunAt: revisioned.nextRunAt + 20_000,
    runNumber: 5,
    failureCount: 2,
    lastError: "retrying",
  };
  await redis.set(legacyKey, JSON.stringify(compatibleRuntimeWrite));

  const merged = await sched.get({ id: scheduleId });
  expect(merged?.cron).toBe("0 2 * * *");
  expect(merged?.tz).toBe("Europe/Berlin");
  expect(merged?.meta).toEqual({ generation: "current" });
  expect(merged?.runNumber).toBe(5);
  expect(merged?.nextRunAt).toBe(compatibleRuntimeWrite.nextRunAt);
  expect(merged?.failureCount).toBe(2);
  expect(merged?.lastError).toBe("retrying");

  const controlInfo = (await schedulerControl({ prefix }).list()).find(
    (entry) => entry.schedulerId === schedulerId && entry.scheduleId === scheduleId,
  );
  expect(controlInfo?.meta).toEqual({ generation: "current" });
  expect(controlInfo?.runNumber).toBe(5);

  await sched.runNow({ id: scheduleId });
  for (const key of [currentKey, v2Key, legacyKey]) {
    const stored = JSON.parse((await redis.get(key)) as string);
    expect(stored.revision).toBe(revisioned.revision);
    expect(stored.cron).toBe("0 2 * * *");
    expect(stored.tz).toBe("Europe/Berlin");
    expect(JSON.parse(stored.metaJson)).toEqual({ generation: "current" });
    expect(stored.runNumber).toBe(6);
  }
});

test("a revisionless post-delete write cannot resurrect a deleted schedule", async () => {
  const prefix = `test:sched:${uid("delete-migration")}`;
  const schedulerId = "migration";
  const scheduleId = "daily";
  const sched = makeScheduler(schedulerId, { prefix });
  const legacyKey = legacySchedulerScheduleKey(prefix, schedulerId, scheduleId);
  const v2Key = schedulerV2ScheduleKey(prefix, schedulerId, scheduleId);
  const currentKey = schedulerScheduleKey(prefix, schedulerId, scheduleId);
  await sched.create({ id: scheduleId, cron: "0 3 * * *", process: async () => {} });
  await sched.delete({ id: scheduleId });

  const tombstoneKey = schedulerScheduleTombstoneKey(prefix, schedulerId, scheduleId);
  const tombstone = JSON.parse((await redis.get(tombstoneKey)) as string);
  const postDeleteLegacyWrite = JSON.stringify({
    id: scheduleId,
    cron: "0 3 * * *",
    tz: "UTC",
    meta: { owner: "legacy" },
    createdAt: tombstone.deletedAt - 1_000,
    updatedAt: tombstone.deletedAt + 10_000,
    nextRunAt: tombstone.deletedAt + 60_000,
    runNumber: 7,
    failureCount: 0,
  });
  await redis.set(legacyKey, postDeleteLegacyWrite);
  await redis.set(v2Key, postDeleteLegacyWrite);

  expect(await sched.get({ id: scheduleId })).toBeNull();
  expect(await redis.get(currentKey)).toBeNull();
  expect(await redis.get(v2Key)).toBeNull();
  expect(await redis.get(legacyKey)).toBeNull();
  expect(await redis.get(tombstoneKey)).not.toBeNull();
  expect(
    (await schedulerControl({ prefix }).list()).some(
      (entry) => entry.schedulerId === schedulerId && entry.scheduleId === scheduleId,
    ),
  ).toBe(false);
});

test("a revisionless old worker cannot affect a recreated schedule", async () => {
  const prefix = `test:sched:${uid("recreate-fence")}`;
  const schedulerId = "migration";
  const scheduleId = "daily";
  const sched = makeScheduler(schedulerId, { prefix });
  const legacyKey = legacySchedulerScheduleKey(prefix, schedulerId, scheduleId);

  await sched.create({
    id: scheduleId,
    cron: "0 3 * * *",
    meta: { generation: 1 },
    process: async () => {},
  });
  await sched.delete({ id: scheduleId });
  await sched.create({
    id: scheduleId,
    cron: "0 4 * * *",
    meta: { generation: 2 },
    process: async () => {},
  });

  const tombstone = JSON.parse(
    (await redis.get(schedulerScheduleTombstoneKey(prefix, schedulerId, scheduleId))) as string,
  );
  await redis.set(legacyKey, JSON.stringify({
    id: scheduleId,
    cron: "0 4 * * *",
    tz: "UTC",
    createdAt: tombstone.deletedAt - 1_000,
    updatedAt: tombstone.deletedAt + 10_000,
    nextRunAt: tombstone.deletedAt + 60_000,
    runNumber: 99,
    failureCount: 8,
  }));

  expect(await sched.get({ id: scheduleId })).toMatchObject({
    cron: "0 4 * * *",
    runNumber: 0,
    failureCount: 0,
    meta: { generation: 2 },
  });
  expect(await redis.get(legacyKey)).toContain('"revision"');
});

test("a recreated revision survives a tombstone from the same millisecond", async () => {
  const prefix = `test:sched:${uid("same-ms-recreate")}`;
  const schedulerId = "worker";
  const scheduleId = "daily";
  const sched = makeScheduler(schedulerId, { prefix });
  await sched.create({ id: scheduleId, cron: "0 3 * * *", process: async () => {} });
  await sched.delete({ id: scheduleId });
  await sched.create({ id: scheduleId, cron: "0 4 * * *", process: async () => {} });

  const record = JSON.parse(
    (await redis.get(schedulerScheduleKey(prefix, schedulerId, scheduleId))) as string,
  );
  const tombstoneKey = schedulerScheduleTombstoneKey(prefix, schedulerId, scheduleId);
  const tombstone = JSON.parse((await redis.get(tombstoneKey)) as string);
  tombstone.deletedAt = record.updatedAt;
  await redis.set(tombstoneKey, JSON.stringify(tombstone));

  expect(await sched.get({ id: scheduleId })).toMatchObject({ cron: "0 4 * * *", runNumber: 0 });
});

test("scheduler tombstones deduplicate mirrored revisions", async () => {
  const prefix = `test:sched:${uid("tombstone-revisions")}`;
  const schedulerId = "worker";
  const scheduleId = "daily";
  const sched = makeScheduler(schedulerId, { prefix });
  await sched.create({ id: scheduleId, cron: "0 3 * * *", process: async () => {} });
  await sched.delete({ id: scheduleId });

  const tombstone = JSON.parse(
    (await redis.get(schedulerScheduleTombstoneKey(prefix, schedulerId, scheduleId))) as string,
  );
  expect(tombstone.revisions).toHaveLength(1);
});

test("a failed first upsert cannot leave a schedule without its global index", async () => {
  const prefix = `test:sched:${uid("atomic-index")}`;
  const schedulerId = "worker";
  const scheduleId = "daily";
  const sched = makeScheduler(schedulerId, { prefix });
  await redis.set(`${prefix}:index`, "wrong-type");

  await expect(
    sched.create({ id: scheduleId, cron: "0 3 * * *", process: async () => {} }),
  ).rejects.toThrow(/global index key has wrong type/);
  expect(await redis.get(schedulerScheduleKey(prefix, schedulerId, scheduleId))).toBeNull();
  expect(await redis.get(legacySchedulerScheduleKey(prefix, schedulerId, scheduleId))).toBeNull();

  await redis.del(`${prefix}:index`);
  await expect(
    sched.create({ id: scheduleId, cron: "0 3 * * *", process: async () => {} }),
  ).resolves.toEqual({ created: true, updated: false });
  expect(await redis.send("SISMEMBER", [`${prefix}:index`, schedulerId])).toBe(1);
});

test("scheduler control request identities include the full prefix", async () => {
  const root = `test:sched:${uid("request-prefix")}`;
  const first = makeScheduler("first", { prefix: `${root}:control:request:child`, dispatch: { tickMs: 20 } });
  const second = makeScheduler("second", { prefix: root, dispatch: { tickMs: 20 } });
  let firstRuns = 0;
  let secondRuns = 0;

  await first.create({
    id: "run",
    cron: "0 3 * * *",
    process: async () => {
      firstRuns += 1;
    },
  });
  await second.create({
    id: "run",
    cron: "0 3 * * *",
    process: async () => {
      secondRuns += 1;
    },
  });
  first.start();
  second.start();

  await Promise.all([
    schedulerControl({ prefix: `${root}:control:request:child` }).runNow({
      schedulerId: "first",
      scheduleId: "run",
      requestId: "request",
    }),
    schedulerControl({ prefix: root }).runNow({
      schedulerId: "second",
      scheduleId: "run",
      requestId: "child:control:request:request",
    }),
  ]);
  await waitFor(() => firstRuns === 1 && secondRuns === 1);
});

test("scheduler rejects a control request whose payload targets another schedule", async () => {
  const prefix = `test:sched:${uid("control-mismatch")}`;
  const schedulerId = uid("control-local");
  const sched = makeScheduler(schedulerId, { prefix, dispatch: { tickMs: 20 } });
  let runs = 0;
  await sched.create({
    id: "local",
    cron: "0 3 * * *",
    process: async () => {
      runs += 1;
    },
  });
  sched.start();

  const request: SchedulerControlRequest = {
    requestId: uid("mismatch"),
    schedulerId: "other",
    scheduleId: "remote",
    requestedAt: Date.now(),
  };
  await redis.set(
    `${prefix}:control:request:${encodedIdentity(request.requestId)}`,
    JSON.stringify([request.schedulerId, request.scheduleId]),
  );
  await schedulerControlQueue(prefix, schedulerId, "local").send({
    data: request,
    idempotencyKey: request.requestId,
  });

  const responseKey = `${prefix}:control:response:${encodedIdentity(request.requestId)}`;
  await waitFor(async () => (await redis.get(responseKey)) !== null);
  expect(JSON.parse((await redis.get(responseKey)) as string).status).toBe("unavailable");
  expect(runs).toBe(0);
});

test("a stale control request cannot refresh or accept a rebound requestId", async () => {
  const prefix = `test:sched:${uid("control-rebound")}`;
  const requestId = uid("request");
  const stale: SchedulerControlRequest = {
    requestId,
    schedulerId: "old-scheduler",
    scheduleId: "old-schedule",
    requestedAt: Date.now(),
  };
  const current: SchedulerControlRequest = {
    requestId,
    schedulerId: "new-scheduler",
    scheduleId: "new-schedule",
    requestedAt: Date.now(),
  };
  const bindingKey = `${prefix}:control:request:${encodedIdentity(requestId)}`;
  const responseKey = `${prefix}:control:response:${encodedIdentity(requestId)}`;
  await redis.set(bindingKey, JSON.stringify([current.schedulerId, current.scheduleId]));

  expect(await refreshSchedulerControlRequestBinding(prefix, stale)).toBe(false);
  expect(await markSchedulerControlAccepted(prefix, stale)).toBe(false);
  expect(await redis.get(responseKey)).toBeNull();

  expect(await refreshSchedulerControlRequestBinding(prefix, current)).toBe(true);
  expect(await markSchedulerControlPending(prefix, current)).toBe("pending");
  await redis.send("PEXPIRE", [responseKey, "20"]);
  expect(await refreshSchedulerControlRequestBinding(prefix, current, 1_000)).toBe(true);
  await Bun.sleep(40);
  expect(JSON.parse((await redis.get(responseKey)) as string).status).toBe("pending");

  expect(await markSchedulerControlAccepted(prefix, current)).toBe(true);
  expect(JSON.parse((await redis.get(responseKey)) as string).status).toBe("accepted");

  await redis.send("PEXPIRE", [responseKey, "20"]);
  expect(await refreshSchedulerControlRequestBinding(prefix, current, 1_000)).toBe(true);
  await Bun.sleep(40);
  expect(JSON.parse((await redis.get(responseKey)) as string).status).toBe("accepted");
});

test("scheduler timing options reject non-finite values before use", async () => {
  expect(() => scheduler({ id: uid("invalid-lease"), leader: { leaseMs: Number.NaN } })).toThrow(
    "leader.leaseMs",
  );
  expect(() => scheduler({ id: uid("invalid-heartbeat"), leader: { heartbeatMs: Number.POSITIVE_INFINITY } }))
    .toThrow("leader.heartbeatMs");
  expect(() => scheduler({ id: uid("invalid-tick"), dispatch: { tickMs: Number.NaN } })).toThrow(
    "dispatch.tickMs",
  );
  expect(() => schedulerControl({ timeoutMs: Number.NaN })).toThrow("timeoutMs");

  const prefix = `test:sched:${uid("invalid-control-timeout")}`;
  await expect(
    schedulerControl({ prefix }).runNow({
      schedulerId: "missing",
      scheduleId: "missing",
      timeoutMs: Number.POSITIVE_INFINITY,
    }),
  ).rejects.toThrow("timeoutMs");
  expect(await redis.send("KEYS", [`${prefix}:*`])).toEqual([]);
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
  await redis.send("SADD", ["test:sched:index", id]);

  const info = await sched.get({ id: "legacy" });
  expect(info?.meta).toEqual({ source: "5.8.0" });
  expect(info?.runNumber).toBe(4);
});

test("a legacy record for a colon-rich scheduler id migrates without losing state", async () => {
  const id = `legacy:${uid("scheduler")}`;
  const sched = scheduler({ id, prefix: "test:sched" });
  const legacyKey = `test:sched:${id}:schedule:legacy`;
  await redis.set(
    legacyKey,
    JSON.stringify({
      id: "legacy",
      cron: "0 3 * * *",
      tz: "UTC",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: Date.now() + 60_000,
      runNumber: 7,
      failureCount: 2,
    }),
  );
  await redis.send("SADD", [`test:sched:${id}:index`, "legacy"]);
  await redis.send("SADD", ["test:sched:index", id]);

  expect((await sched.get({ id: "legacy" }))?.runNumber).toBe(7);
  await sched.create({ id: "legacy", cron: "0 3 * * *", process: async () => {} });
  expect((await sched.get({ id: "legacy" }))?.failureCount).toBe(2);

  await sched.delete({ id: "legacy" });
  expect(await redis.get(legacyKey)).toBeNull();
  expect(await sched.get({ id: "legacy" })).toBeNull();
});

test("an accepted control response cannot be downgraded to unavailable", async () => {
  const prefix = `test:sched:${uid("accepted-terminal")}`;
  const schedulerId = uid("accepted-terminal");
  const sched = makeScheduler(schedulerId, { prefix, dispatch: { tickMs: 20 } });
  await sched.create({ id: "run", cron: "0 3 * * *", process: async () => {} });
  sched.start();

  const requestId = uid("accepted-request");
  const request = { requestId, schedulerId, scheduleId: "run", requestedAt: Date.now() };
  const control = schedulerControl({ prefix });
  await control.runNow({ schedulerId, scheduleId: "run", requestId, timeoutMs: 2_000 });

  expect(await markSchedulerControlUnavailable(prefix, request, "late failure")).toBe(false);
  await expect(
    control.runNow({ schedulerId, scheduleId: "run", requestId, timeoutMs: 2_000 }),
  ).resolves.toBeUndefined();
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

  const originalSend = redis.send.bind(redis);
  redis.send = (async (command, args) => {
    if (command === "EVAL" && String(args[0]).includes("existing.runNumber = patch.runNumber")) {
      throw new Error("injected terminal write failure");
    }
    return await originalSend(command, args);
  }) as typeof redis.send;

  try {
    const control = schedulerControl({ prefix: "test:sched" });
    await control.runNow({ schedulerId: schedId, scheduleId: "boom", timeoutMs: 3_000 }).catch(() => {});

    await Bun.sleep(2_500);
    const settled = runs;
    await Bun.sleep(2_000);

    // The control queue allows effectively unlimited deliveries, so an
    // unconditional nack replayed the user's callback about every 250ms for the
    // full five-minute message-age window.
    expect(settled).toBeGreaterThanOrEqual(2);
    expect(settled).toBeLessThanOrEqual(3);
    expect(runs).toBe(settled);
  } finally {
    redis.send = originalSend as typeof redis.send;
  }
}, 30_000);
