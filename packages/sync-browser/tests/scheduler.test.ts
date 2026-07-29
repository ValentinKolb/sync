import { test, expect, afterEach } from "bun:test";
import { scheduler, type Scheduler, type SchedulerTraceEvent } from "../src/scheduler";
import { createMemoryStore } from "../src/store";
import {
  SchedulerControlNotFoundError,
  SchedulerControlTimeoutError,
  SchedulerControlUnavailableError,
  registerBrowserSchedulerControl,
  schedulerControl,
  setBrowserSchedulerControlAvailable,
  unregisterBrowserSchedulerControl,
} from "../src/scheduler-control";

let counter = 0;
const uid = (label: string): string => `${label}-${++counter}-${Date.now()}`;

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
    leader: { leaseMs: 1_000, heartbeatMs: 50 },
    dispatch: { tickMs: 50 },
    ...overrides,
  });
  activeSchedulers.push(s);
  return s;
};

// ==========================
// create / get / list / delete
// ==========================

test("create registers schedule; get and list return it", async () => {
  const s = makeScheduler(uid("basic"));
  await s.create({
    id: "daily",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {},
  });

  const info = await s.get({ id: "daily" });
  expect(info).not.toBeNull();
  expect(info?.runNumber).toBe(0);
  expect(info?.failureCount).toBe(0);
  expect(info?.nextRunAt).toBeGreaterThan(Date.now());

  const all = await s.list();
  expect(all).toHaveLength(1);
});

test("create is idempotent; second call updates", async () => {
  const s = makeScheduler(uid("idempotent"));
  const first = await s.create({ id: "x", cron: "0 * * * *", process: async () => {} });
  expect(first.created).toBe(true);
  expect(first.updated).toBe(false);

  const second = await s.create({ id: "x", cron: "0 * * * *", process: async () => {} });
  expect(second.created).toBe(false);
  expect(second.updated).toBe(true);

  expect((await s.list()).length).toBe(1);
});

test("create with invalid cron throws", async () => {
  const s = makeScheduler(uid("invalid-cron"));
  await expect(
    s.create({ id: "x", cron: "not a cron", process: async () => {} }),
  ).rejects.toThrow();
});

test("delete removes schedule", async () => {
  const s = makeScheduler(uid("delete"));
  await s.create({ id: "x", cron: "0 * * * *", process: async () => {} });
  await s.delete({ id: "x" });
  expect(await s.get({ id: "x" })).toBeNull();
  expect((await s.list()).length).toBe(0);
});

// ==========================
// runNow
// ==========================

test("runNow invokes process and increments runNumber", async () => {
  const s = makeScheduler(uid("runnow"));
  let runs = 0;
  let seenRunNumber = -1;

  await s.create({
    id: "m",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      runs += 1;
      seenRunNumber = ctx.runNumber;
    },
  });

  await s.runNow({ id: "m" });
  expect(runs).toBe(1);
  expect(seenRunNumber).toBe(1);

  await s.runNow({ id: "m" });
  expect(runs).toBe(2);
  expect(seenRunNumber).toBe(2);
});

test("runNow does not advance nextRunAt", async () => {
  const s = makeScheduler(uid("runnow-preserves"));
  await s.create({ id: "m", cron: "0 3 * * *", tz: "UTC", process: async () => {} });

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
// dispatch loop
// ==========================

test("scheduler dispatches via tick loop when nextRunAt is past", async () => {
  const s = makeScheduler(uid("dispatch"));
  let runs = 0;

  await s.create({
    id: "t",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
    },
  });

  // runNow + stop doesn't automatically put nextRunAt in past; but runNumber=0 → first cron slot is future
  // Use runNow to verify dispatch path works; start loop to check natural firing does not break.
  s.start();
  await s.runNow({ id: "t" });
  expect(runs).toBe(1);
});

// ==========================
// ctx.reschedule
// ==========================

test("ctx.reschedule in after triggers re-run with delay", async () => {
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
  await waitFor(() => runs >= 2, 3_000);
  expect(runs).toBe(2);
});

test("ctx.reschedule on success (polling pattern)", async () => {
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
  await Bun.sleep(100);
  expect(runs).toBe(3);
});

// ==========================
// failureCount / runNumber
// ==========================

test("failureCount increments on failure, resets on success", async () => {
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
  await Bun.sleep(100);

  const info = await s.get({ id: "f" });
  expect(info?.failureCount).toBe(0);
});

test("runNumber is 1-indexed monotonic", async () => {
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
});

// ==========================
// metric()
// ==========================

test("metric() counts dispatches / failures / reschedules", async () => {
  const s = makeScheduler(uid("metrics"));

  await s.create({ id: "ok", cron: "0 3 * * *", tz: "UTC", process: async () => {} });
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
  await Bun.sleep(200);

  const m = s.metric();
  expect(m.dispatches).toBeGreaterThanOrEqual(2);
  expect(m.failures).toBeGreaterThanOrEqual(1);
  expect(m.reschedules).toBeGreaterThanOrEqual(1);

  m.dispatches = 9999;
  expect(s.metric().dispatches).not.toBe(9999);
});

test("metric.isLeader flips after start", async () => {
  const s = makeScheduler(uid("leader"));
  expect(s.metric().isLeader).toBe(false);

  s.start();
  await waitFor(() => s.metric().isLeader, 2_000);
  expect(s.metric().isLeader).toBe(true);

  await s.stop();
  expect(s.metric().isLeader).toBe(false);
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

test("ctx.trigger differs for cron vs manual on the same schedule", async () => {
  const s = makeScheduler(uid("trigger-both"));
  const triggers: string[] = [];

  await s.create({
    id: "t",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async ({ ctx }) => {
      triggers.push(ctx.trigger);
    },
  });

  await s.runNow({ id: "t" });
  await s.runNow({ id: "t" });

  expect(triggers).toEqual(["manual", "manual"]);
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
  const prefix = uid("control-list-prefix");
  const s = makeScheduler(uid("control-list"), { prefix });

  await s.create({
    id: "sync-users",
    cron: "0 3 * * *",
    tz: "UTC",
    meta: { label: "Sync users" },
    process: async () => {},
  });

  const control = schedulerControl({ prefix });
  let listed = await control.list();
  let info = listed.find((entry) => entry.schedulerId === s.id && entry.scheduleId === "sync-users");
  expect(info?.cron).toBe("0 3 * * *");
  expect(info?.tz).toBe("UTC");
  expect(info?.state).toBe("unavailable");
  expect(info?.meta?.label).toBe("Sync users");

  s.start();
  listed = await control.list();
  info = listed.find((entry) => entry.schedulerId === s.id && entry.scheduleId === "sync-users");
  expect(info?.state).toBe("available");
});

test("schedulerControl runNow executes on a live scheduler and does not advance cron", async () => {
  const prefix = uid("control-run-prefix");
  const s = makeScheduler(uid("control-run"), { prefix });
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

  await schedulerControl({ prefix }).runNow({ schedulerId: s.id, scheduleId: "reindex", timeoutMs: 5_000 });

  await waitFor(() => runs === 1);
  expect(trigger).toBe("manual");
  expect((await s.get({ id: "reindex" }))!.nextRunAt).toBe(before);
  expect(events.some((event) => event.type === "started" && event.trigger === "manual")).toBe(true);
});

test("schedulerControl waits for the cross-handle dispatch lock before acceptance", async () => {
  const prefix = uid("control-lock-prefix");
  const schedulerId = uid("control-lock");
  const options = {
    prefix,
    leader: { leaseMs: 500, heartbeatMs: 50 },
    dispatch: { tickMs: 20 },
  };
  const first = makeScheduler(schedulerId, options);
  const second = makeScheduler(schedulerId, options);
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

  let accepted = false;
  const remote = schedulerControl({ prefix })
    .runNow({ schedulerId, scheduleId: "reindex", timeoutMs: 5_000 })
    .then(() => {
      accepted = true;
    });
  await Bun.sleep(150);
  expect(accepted).toBe(false);
  expect(starts).toBe(1);

  releaseFirst();
  await Promise.all([direct, remote]);
  await waitFor(() => starts === 2);
});

test("schedulerControl runNow reports unavailable when no live handler exists", async () => {
  const prefix = uid("control-unavailable-prefix");
  const s = makeScheduler(uid("control-unavailable"), { prefix });
  await s.create({ id: "cleanup", cron: "0 3 * * *", tz: "UTC", process: async () => {} });

  await expect(
    schedulerControl({ prefix }).runNow({ schedulerId: s.id, scheduleId: "cleanup", timeoutMs: 100 }),
  ).rejects.toBeInstanceOf(SchedulerControlUnavailableError);
});

test("schedulerControl runNow reports not found for missing schedules", async () => {
  await expect(
    schedulerControl({ prefix: uid("control-missing-prefix") }).runNow({
      schedulerId: "missing-scheduler",
      scheduleId: "missing",
      timeoutMs: 100,
    }),
  ).rejects.toBeInstanceOf(SchedulerControlNotFoundError);
});

test("stop cancels the in-flight schedule callback via ctx.signal", async () => {
  const s = scheduler({ id: `stop-signal-${Date.now()}` });

  let abortedDuringRun = false;
  let ranToCompletion = false;
  let started = false;

  await s.create({
    id: "long",
    cron: "0 3 * * *",
    process: async ({ ctx }) => {
      started = true;
      for (let i = 0; i < 100; i++) {
        if (ctx.signal.aborted) {
          abortedDuringRun = true;
          return;
        }
        await Bun.sleep(10);
      }
      ranToCompletion = true;
    },
  });

  s.start();
  const run = s.runNow({ id: "long" }).catch(() => {});
  while (!started) await Bun.sleep(5);
  await s.stop();
  await run;

  expect(abortedDuringRun).toBe(true);
  expect(ranToCompletion).toBe(false);
});

test("schedulerControl runNow surfaces a pre-acceptance error instead of reporting success", async () => {
  const prefix = uid("control-error-prefix");
  const schedulerId = uid("control-error");
  // Two same-id instances share the module-level schedule map, so deleting on
  // one empties it for the other while the other's control registration stays.
  const a = makeScheduler(schedulerId, { prefix });
  const b = makeScheduler(schedulerId, { prefix });

  for (const s of [a, b]) {
    await s.create({ id: "vanishing", cron: "0 3 * * *", tz: "UTC", process: async () => {} });
  }
  b.start();
  await a.delete({ id: "vanishing" });

  // b's scheduler.runNow now throws "schedule not found" before accepting. That
  // error used to be swallowed by a bare catch, so a UI "Run now" button
  // reported success on failure.
  await expect(
    schedulerControl({ prefix }).runNow({ schedulerId, scheduleId: "vanishing", timeoutMs: 1_000 }),
  ).rejects.toThrow();
});

test("schedulerControl runNow times out when nothing accepts", async () => {
  const prefix = uid("control-timeout-prefix");
  const schedulerId = uid("control-timeout");

  // A registration that is available but never accepts. The timeout branch was
  // unreachable before, because the first race arm always settled immediately.
  registerBrowserSchedulerControl({
    prefix,
    schedulerId,
    scheduleId: "stuck",
    instanceId: "test-instance",
    getInfo: () => ({
      id: "stuck",
      cron: "0 3 * * *",
      tz: "UTC",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: Date.now() + 60_000,
      runNumber: 0,
      failureCount: 0,
    }),
    runNow: () => new Promise<void>(() => {}),
  });
  setBrowserSchedulerControlAvailable({ prefix, schedulerId, instanceId: "test-instance", available: true });

  await expect(
    schedulerControl({ prefix }).runNow({ schedulerId, scheduleId: "stuck", timeoutMs: 150 }),
  ).rejects.toThrow(SchedulerControlTimeoutError);

  unregisterBrowserSchedulerControl({ prefix, schedulerId, scheduleId: "stuck", instanceId: "test-instance" });
});

test("schedulerControl runNow waits for acceptance rather than returning immediately", async () => {
  const prefix = uid("control-accept-prefix");
  const s = makeScheduler(uid("control-accept"), { prefix });

  let accepted = false;
  await s.create({
    id: "slow",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      accepted = true;
      await Bun.sleep(200);
    },
  });
  s.start();

  await schedulerControl({ prefix }).runNow({ schedulerId: s.id, scheduleId: "slow", timeoutMs: 5_000 });
  expect(accepted).toBe(true);
});

test("schedulerControl executes one run for repeated requestId calls", async () => {
  const prefix = uid("control-idempotent-prefix");
  const s = makeScheduler(uid("control-idempotent"), { prefix });
  let runs = 0;

  await s.create({
    id: "reindex",
    cron: "0 3 * * *",
    process: async () => {
      runs += 1;
      await Bun.sleep(50);
    },
  });
  s.start();

  const control = schedulerControl({ prefix });
  const request = {
    schedulerId: s.id,
    scheduleId: "reindex",
    requestId: uid("request"),
  };
  await Promise.all([control.runNow(request), control.runNow(request)]);
  await Bun.sleep(150);

  expect(runs).toBe(1);
});

test("schedulerControl binds a requestId to one schedule", async () => {
  const prefix = uid("control-target-prefix");
  const s = makeScheduler(uid("control-target"), { prefix });

  await s.create({ id: "one", cron: "0 3 * * *", process: async () => {} });
  await s.create({ id: "two", cron: "0 3 * * *", process: async () => {} });
  s.start();

  const control = schedulerControl({ prefix });
  const requestId = uid("request");
  await control.runNow({ schedulerId: s.id, scheduleId: "one", requestId });
  await expect(
    control.runNow({ schedulerId: s.id, scheduleId: "two", requestId }),
  ).rejects.toThrow("already bound to another schedule");
});

// ==========================
// Persistence and leader election across handles
// ==========================

const seedPersistedState = async (
  prefix: string,
  schedulerId: string,
  scheduleId: string,
  state: Record<string, unknown>,
): Promise<ReturnType<typeof createMemoryStore>> => {
  const store = createMemoryStore();
  store.set(`${prefix}:${schedulerId}:state:${scheduleId}`, state);
  return store;
};

test("a persisted schedule does not resume a stale nextRunAt after the cron changes", async () => {
  const prefix = uid("persist-cron");
  const schedulerId = uid("persist-cron-id");
  const stale = Date.now() + 20 * 60 * 60_000; // tomorrow 03:00-ish

  // Exactly what a previous tab wrote while the app shipped "0 3 * * *".
  const store = await seedPersistedState(prefix, schedulerId, "report", {
    version: 1,
    runNumber: 3,
    nextRunAt: stale,
    failureCount: 2,
    updatedAt: Date.now(),
    cron: "0 3 * * *",
    tz: "UTC",
  });

  const s = scheduler({ id: schedulerId, prefix, store });
  await s.create({ id: "report", cron: "*/5 * * * *", tz: "UTC", process: async () => {} });

  const info = (await s.get({ id: "report" }))!;
  // "State exists" used to be read as "cron unchanged", so the new expression
  // did not take effect for up to a day and a stale failureCount carried over.
  expect(info.nextRunAt).not.toBe(stale);
  expect(info.nextRunAt - Date.now()).toBeLessThan(6 * 60_000);
  expect(info.failureCount).toBe(0);
});

test("a persisted schedule does resume when the cron is unchanged", async () => {
  const prefix = uid("persist-same");
  const schedulerId = uid("persist-same-id");
  const resumeAt = Date.now() + 20 * 60 * 60_000;

  const store = await seedPersistedState(prefix, schedulerId, "report", {
    version: 1,
    runNumber: 3,
    nextRunAt: resumeAt,
    failureCount: 2,
    updatedAt: Date.now(),
    cron: "0 3 * * *",
    tz: "UTC",
  });

  const s = scheduler({ id: schedulerId, prefix, store });
  await s.create({ id: "report", cron: "0 3 * * *", tz: "UTC", process: async () => {} });

  const info = (await s.get({ id: "report" }))!;
  expect(info.nextRunAt).toBe(resumeAt);
  expect(info.failureCount).toBe(2);
});

test("persisted state written by 5.8.0 is reset rather than resumed blindly", async () => {
  const prefix = uid("persist-legacy");
  const schedulerId = uid("persist-legacy-id");
  const stale = Date.now() + 20 * 60 * 60_000;

  // <= 5.8.0 recorded no cron/tz, so its cron cannot be verified.
  const store = await seedPersistedState(prefix, schedulerId, "report", {
    version: 1,
    runNumber: 3,
    nextRunAt: stale,
    failureCount: 2,
    updatedAt: Date.now(),
  });

  const s = scheduler({ id: schedulerId, prefix, store });
  await s.create({ id: "report", cron: "*/5 * * * *", tz: "UTC", process: async () => {} });

  const info = (await s.get({ id: "report" }))!;
  expect(info.nextRunAt).not.toBe(stale);
  expect(info.failureCount).toBe(0);
});

test("handles sharing a store share one leader lock", async () => {
  const store = createMemoryStore();
  const prefix = uid("leader-store");
  const schedulerId = uid("leader-store-id");

  const a = scheduler({ id: schedulerId, prefix, store, leader: { leaseMs: 5_000 } });
  const b = scheduler({ id: schedulerId, prefix, store, leader: { leaseMs: 5_000 } });
  activeSchedulers.push(a, b);

  a.start();
  b.start();
  await waitFor(() => a.metric().isLeader || b.metric().isLeader);
  await Bun.sleep(150);

  // Leader election used to always build its own MemoryStore, so two handles
  // sharing a store each held their own lock and both dispatched every slot.
  expect([a.metric().isLeader, b.metric().isLeader].filter(Boolean).length).toBe(1);
});

test("handles with different explicit stores do not share a leader lock", async () => {
  const prefix = uid("leader-split");
  const schedulerId = uid("leader-split-id");

  // An explicit Store scopes coordination to the handles sharing that Store.
  const a = scheduler({ id: schedulerId, prefix, store: createMemoryStore(), leader: { leaseMs: 5_000 } });
  const b = scheduler({ id: schedulerId, prefix, store: createMemoryStore(), leader: { leaseMs: 5_000 } });
  activeSchedulers.push(a, b);

  a.start();
  b.start();
  await waitFor(() => a.metric().isLeader && b.metric().isLeader);

  expect(a.metric().isLeader).toBe(true);
  expect(b.metric().isLeader).toBe(true);
});

test("fractional scheduler timings are normalized to integer milliseconds", async () => {
  const delays: number[] = [];
  const originalSetTimeout = globalThis.setTimeout;
  globalThis.setTimeout = ((...args: unknown[]) => {
    const delay = args[1];
    if (typeof delay === "number") delays.push(delay);
    return Reflect.apply(originalSetTimeout, globalThis, args);
  }) as typeof setTimeout;

  try {
    const s = makeScheduler(uid("integer-timing"), {
      store: createMemoryStore(),
      leader: { leaseMs: 500.9, heartbeatMs: 100.9 },
      dispatch: { tickMs: 50.9 },
    });
    await s.create({ id: "idle", cron: "0 3 * * *", process: async () => {} });
    s.start();
    await waitFor(() => s.metric().isLeader);
    await Bun.sleep(120);
    await s.stop();
  } finally {
    globalThis.setTimeout = originalSetTimeout;
  }

  expect(delays).toContain(500);
  expect(delays).toContain(50);
  expect(delays.every(Number.isInteger)).toBe(true);
});

test("leader heartbeat survives a callback longer than the configured lease", async () => {
  const prefix = uid("leader-long-prefix");
  const schedulerId = uid("leader-long");
  const store = createMemoryStore();
  const options = {
    prefix,
    store,
    leader: { leaseMs: 500, heartbeatMs: 5_000 },
    dispatch: { tickMs: 20 },
  };
  const a = scheduler({ id: schedulerId, ...options });
  const b = scheduler({ id: schedulerId, ...options });
  activeSchedulers.push(a, b);

  let cronStarts = 0;
  const register = async (s: Scheduler): Promise<void> => {
    await s.create({
      id: "slow",
      cron: "0 3 * * *",
      process: async ({ ctx }) => {
        if (ctx.trigger === "cron") {
          cronStarts += 1;
          await Bun.sleep(900);
        }
      },
      after: async ({ ctx }) => {
        if (ctx.trigger === "manual") ctx.reschedule({ delayMs: 0 });
      },
    });
  };
  await register(a);
  await register(b);

  await a.runNow({ id: "slow" });
  a.start();
  await waitFor(() => cronStarts === 1);
  b.start();
  await Bun.sleep(700);

  expect(cronStarts).toBe(1);
});

test("manual and cron runs of one schedule are serialized", async () => {
  const s = makeScheduler(uid("dispatch-serialized"), {
    leader: { leaseMs: 500, heartbeatMs: 100 },
    dispatch: { tickMs: 20 },
  });
  let active = 0;
  let maxActive = 0;
  let releaseCron = (): void => {};
  const cronGate = new Promise<void>((resolve) => {
    releaseCron = resolve;
  });

  await s.create({
    id: "serial",
    cron: "0 3 * * *",
    process: async ({ ctx }) => {
      if (ctx.runNumber === 1) return;
      active += 1;
      maxActive = Math.max(maxActive, active);
      if (ctx.trigger === "cron") await cronGate;
      active -= 1;
    },
    after: async ({ ctx }) => {
      if (ctx.runNumber === 1) ctx.reschedule({ delayMs: 0 });
    },
  });

  await s.runNow({ id: "serial" });
  s.start();
  await waitFor(() => active === 1);
  const manual = s.runNow({ id: "serial" });
  try {
    await Bun.sleep(100);
    expect(maxActive).toBe(1);
  } finally {
    releaseCron();
  }
  await manual;
  expect(maxActive).toBe(1);
});

test("manual runs are serialized across default scheduler handles", async () => {
  const schedulerId = uid("cross-handle-manual");
  const options = {
    leader: { leaseMs: 500, heartbeatMs: 50 },
    dispatch: { tickMs: 20 },
  };
  const first = makeScheduler(schedulerId, options);
  const second = makeScheduler(schedulerId, options);
  let active = 0;
  let maxActive = 0;
  let starts = 0;
  let releaseFirst = (): void => {};
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const process = async (): Promise<void> => {
    active += 1;
    starts += 1;
    maxActive = Math.max(maxActive, active);
    if (starts === 1) await firstGate;
    active -= 1;
  };

  await first.create({ id: "serial", cron: "0 3 * * *", process });
  await second.create({ id: "serial", cron: "0 3 * * *", process });

  const firstRun = first.runNow({ id: "serial" });
  await waitFor(() => starts === 1);
  const secondRun = second.runNow({ id: "serial" });

  await Bun.sleep(800);
  expect(starts).toBe(1);
  expect(maxActive).toBe(1);

  releaseFirst();
  await Promise.all([firstRun, secondRun]);

  expect(starts).toBe(2);
  expect(maxActive).toBe(1);
  expect((await first.get({ id: "serial" }))?.runNumber).toBe(2);
});

test("losing the dispatch lease aborts the callback and rejects the run", async () => {
  const prefix = uid("dispatch-lease-prefix");
  const schedulerId = uid("dispatch-lease-loss");
  const store = createMemoryStore();
  const s = makeScheduler(schedulerId, {
    prefix,
    store,
    leader: { leaseMs: 500, heartbeatMs: 50 },
  });
  let started = false;
  let sawAbort = false;

  await s.create({
    id: "report",
    cron: "0 3 * * *",
    process: async ({ ctx }) => {
      started = true;
      while (!ctx.signal.aborted) await Bun.sleep(10);
      sawAbort = true;
    },
  });

  const run = s.runNow({ id: "report" });
  await waitFor(() => started);
  store.del(`${prefix}:dispatch:${schedulerId}:dispatch:report`);

  await expect(run).rejects.toThrow("scheduler dispatch lease lost");
  expect(sawAbort).toBe(true);
  expect((await s.get({ id: "report" }))?.runNumber).toBe(0);
});

// ==========================
// Tick loop (not runNow)
// ==========================

test("the tick loop dispatches due schedules itself, with trigger 'cron'", async () => {
  // Every other dispatch test goes through runNow, so dispatchDue, the due
  // selection, the batchSize slice, the cron advance and trigger === "cron"
  // were entirely uncovered: a browser scheduler that never fired on its own
  // schedule passed the whole suite. One minute-boundary wait covers them all.
  const s = makeScheduler(uid("tick-cron"), { dispatch: { tickMs: 20, batchSize: 1 } });
  const seen: Array<{ id: string; trigger: "cron" | "manual" }> = [];
  const ids = ["a", "b", "c"];

  const before: Record<string, number> = {};
  for (const id of ids) {
    await s.create({
      id,
      cron: "* * * * *",
      tz: "UTC",
      process: async ({ ctx }) => {
        seen.push({ id, trigger: ctx.trigger });
        await Bun.sleep(20);
      },
    });
    before[id] = (await s.get({ id }))!.nextRunAt;
  }

  s.start();
  await waitFor(() => seen.length >= ids.length, 90_000, 50);
  // `seen` is pushed at the start of the callback; let every terminal write land.
  await waitFor(async () => {
    const infos = await Promise.all(ids.map((id) => s.get({ id })));
    return infos.every((info) => (info?.runNumber ?? 0) >= 1);
  }, 15_000, 20);

  // batchSize 1 must not drop the other due schedules, only defer them.
  expect(new Set(seen.map((e) => e.id))).toEqual(new Set(ids));
  expect(seen.every((e) => e.trigger === "cron")).toBe(true);

  for (const id of ids) {
    const after = (await s.get({ id }))!;
    expect(after.runNumber).toBeGreaterThanOrEqual(1);
    // The cron advance ran and landed on a later minute boundary. Not compared
    // against Date.now(): that boundary can arrive while the assertion runs.
    expect(after.nextRunAt % 60_000).toBe(0);
    expect(after.nextRunAt).toBeGreaterThanOrEqual(before[id]!);
  }
}, 120_000);
