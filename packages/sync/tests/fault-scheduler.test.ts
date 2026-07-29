import { beforeEach, afterEach, expect, test } from "bun:test";
import { redis } from "bun";
import { scheduler, type Scheduler } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const waitFor = async (pred: () => boolean | Promise<boolean>, timeoutMs = 10_000, pollMs = 20): Promise<void> => {
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
// Leader handoff
// ==========================

test("when leader stops, a second pod takes over and continues dispatch", async () => {
  const schedId = uid("leader-handoff");
  const s1 = makeScheduler(schedId, { leader: { leaseMs: 500, heartbeatMs: 50 } });
  const s2 = makeScheduler(schedId, { leader: { leaseMs: 500, heartbeatMs: 50 } });

  let runsOnS1 = 0;
  let runsOnS2 = 0;

  await s1.create({
    id: "x",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runsOnS1 += 1;
    },
  });
  await s2.create({
    id: "x",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runsOnS2 += 1;
    },
  });

  const keyPrefix = `sync:scheduler:${schedId}`;
  const raw = await redis.get(`${keyPrefix}:schedule:x`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:x`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "x"]);

  s1.start();
  s2.start();

  await waitFor(() => runsOnS1 + runsOnS2 >= 1, 5_000);

  const initialLeader = s1.metric().isLeader ? "s1" : "s2";
  if (initialLeader === "s1") {
    await s1.stop();
  } else {
    await s2.stop();
  }

  const raw2 = await redis.get(`${keyPrefix}:schedule:x`);
  const parsed2 = JSON.parse(raw2 as string);
  parsed2.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:x`, JSON.stringify(parsed2));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed2.nextRunAt), "x"]);

  const initialTotal = runsOnS1 + runsOnS2;
  await waitFor(() => runsOnS1 + runsOnS2 > initialTotal, 5_000);
  expect(runsOnS1 + runsOnS2).toBeGreaterThan(initialTotal);
}, 20_000);

// ==========================
// Orphaned schedule record
// ==========================

test("a leader without the handler hands the slot to a pod that has it", async () => {
  const schedId = uid("orphan");
  const owner = makeScheduler(schedId);

  let runs = 0;
  await owner.create({
    id: "oh",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      runs += 1;
    },
  });

  // Force the slot due.
  const keyPrefix = `sync:scheduler:${schedId}`;
  const raw = await redis.get(`${keyPrefix}:schedule:oh`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1_000;
  await redis.set(`${keyPrefix}:schedule:oh`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "oh"]);

  // A pod that registered no handler. Leases are sticky, so if it advances past
  // slots it cannot serve it starves the schedule forever while `get()` keeps
  // reporting a healthy record with a frozen runNumber.
  const handlerless = scheduler({
    id: schedId,
    leader: { leaseMs: 1_000, heartbeatMs: 100 },
    dispatch: { tickMs: 100 },
  });
  activeSchedulers.push(handlerless);
  handlerless.start();
  owner.start();

  await waitFor(() => runs >= 1, 20_000);

  const info = await owner.get({ id: "oh" });
  expect(runs).toBeGreaterThanOrEqual(1);
  expect(info?.runNumber).toBeGreaterThanOrEqual(1);
  // The slot was served, not skipped: nextRunAt advanced past a real run.
  expect(info?.nextRunAt).toBeGreaterThan(Date.now());
  expect(handlerless.metric().unservedSlots).toBeGreaterThan(0);
}, 30_000);

// ==========================
// Failing process
// ==========================

test("failing process does not halt the scheduler; subsequent dispatches still occur", async () => {
  const s = makeScheduler(uid("failing-process"));
  let badRuns = 0;
  let goodRuns = 0;

  await s.create({
    id: "bad",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      badRuns += 1;
      throw new Error("boom");
    },
  });
  await s.create({
    id: "good",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      goodRuns += 1;
    },
  });

  s.start();
  await s.runNow({ id: "bad" });
  await s.runNow({ id: "good" });

  expect(badRuns).toBe(1);
  expect(goodRuns).toBe(1);

  await s.runNow({ id: "good" });
  expect(goodRuns).toBe(2);
});

// ==========================
// Broken schedule record
// ==========================

test("broken (unparseable) schedule record is cleaned up by the dispatch loop", async () => {
  const schedId = uid("broken-record");
  const s = makeScheduler(schedId);

  await s.create({
    id: "b",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {},
  });

  const keyPrefix = `sync:scheduler:${schedId}`;
  await redis.set(`${keyPrefix}:schedule:b`, "not valid json");
  await redis.send("ZADD", [`${keyPrefix}:due`, String(Date.now() - 1000), "b"]);

  s.start();
  await waitFor(async () => {
    const raw = await redis.get(`${keyPrefix}:schedule:b`);
    return raw === null;
  }, 5_000);

  const info = await s.get({ id: "b" });
  expect(info).toBeNull();
});

// ==========================
// Multiple schedules: one failing, others unaffected
// ==========================

test("one failing schedule does not block dispatch of other schedules", async () => {
  const s = makeScheduler(uid("isolation"));
  let aRuns = 0;
  let bRuns = 0;

  await s.create({
    id: "a",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      aRuns += 1;
      throw new Error("a fails");
    },
  });
  await s.create({
    id: "b",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      bRuns += 1;
    },
  });

  s.start();
  await s.runNow({ id: "a" });
  await s.runNow({ id: "b" });
  await s.runNow({ id: "a" });
  await s.runNow({ id: "b" });

  expect(aRuns).toBe(2);
  expect(bRuns).toBe(2);
});

// ==========================
// Reschedule persists across leader handoff
// ==========================

test("ctx.reschedule persists nextRunAt so another pod can continue", async () => {
  const schedId = uid("reschedule-persists");
  const s1 = makeScheduler(schedId, { leader: { leaseMs: 500, heartbeatMs: 50 } });

  let s1Runs = 0;
  await s1.create({
    id: "r",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      s1Runs += 1;
    },
    after: async ({ ctx }) => {
      if (ctx.runNumber === 1) ctx.reschedule({ delayMs: 300 });
    },
  });

  s1.start();
  await s1.runNow({ id: "r" });
  expect(s1Runs).toBe(1);

  await s1.stop();

  const s2 = makeScheduler(schedId, { leader: { leaseMs: 500, heartbeatMs: 50 } });
  let s2Runs = 0;
  await s2.create({
    id: "r",
    cron: "0 3 * * *",
    tz: "UTC",
    process: async () => {
      s2Runs += 1;
    },
  });
  s2.start();

  await waitFor(() => s2Runs >= 1, 5_000);
  expect(s2Runs).toBeGreaterThanOrEqual(1);
});

// ==========================
// Fenced state transitions
// ==========================

test("a callback longer than the lease neither loses leadership nor clobbers state", async () => {
  const schedId = uid("long-callback");
  const runs: string[] = [];

  const mk = (tag: string): Scheduler => {
    const s = scheduler({
      id: schedId,
      // A heartbeat larger than the lease is clamped to a safe cadence.
      leader: { leaseMs: 600, heartbeatMs: 10_000 },
      dispatch: { tickMs: 100 },
    });
    activeSchedulers.push(s);
    return s;
  };

  const a = mk("a");
  const b = mk("b");

  for (const [tag, s] of [["a", a], ["b", b]] as const) {
    await s.create({
      id: "nightly",
      cron: "* * * * *",
      tz: "UTC",
      process: async () => {
        runs.push(tag);
        // Four times the lease: on the old tick loop the heartbeat could not run
        // at all while this was awaited, so the lease lapsed mid-run.
        await Bun.sleep(2_400);
      },
    });
  }

  const keyPrefix = `sync:scheduler:${schedId}`;
  const raw = await redis.get(`${keyPrefix}:schedule:nightly`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1_000;
  await redis.set(`${keyPrefix}:schedule:nightly`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "nightly"]);

  a.start();
  b.start();

  await waitFor(() => runs.length >= 1, 20_000);
  await Bun.sleep(3_500); // outlive the callback and several leases

  // The slot ran once, and runNumber advanced exactly once — never regressing
  // and never colliding through a stale pod's terminal write.
  expect(runs.length).toBe(1);
  const info = await a.get({ id: "nightly" });
  expect(info?.runNumber).toBe(1);
  expect(info?.nextRunAt).toBeGreaterThan(Date.now());
}, 40_000);

test("deleting a schedule mid-run does not resurrect it", async () => {
  const schedId = uid("delete-mid-run");
  const s = makeScheduler(schedId);

  let started = false;
  await s.create({
    id: "doomed",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      started = true;
      await Bun.sleep(800);
    },
  });

  const keyPrefix = `sync:scheduler:${schedId}`;
  const raw = await redis.get(`${keyPrefix}:schedule:doomed`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1_000;
  await redis.set(`${keyPrefix}:schedule:doomed`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "doomed"]);

  s.start();
  await waitFor(() => started, 20_000);

  await s.delete({ id: "doomed" });
  await Bun.sleep(1_500); // let the in-flight run reach its terminal write

  // The terminal write must not re-create a record the index set no longer
  // lists: that record would be invisible to every listing API, unbounded in
  // Redis, and still dispatchable by any pod holding the handler.
  expect(await redis.send("EXISTS", [`${keyPrefix}:schedule:doomed`])).toBe(0);
  expect(await redis.send("ZSCORE", [`${keyPrefix}:due`, "doomed"])).toBeNull();
  expect(await s.get({ id: "doomed" })).toBeNull();
  expect(await s.list()).toEqual([]);
}, 30_000);

test("runNow concurrent with cron dispatch never rewinds nextRunAt or runNumber", async () => {
  const schedId = uid("runnow-race");
  const s = makeScheduler(schedId);

  await s.create({
    id: "cleanup",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {
      await Bun.sleep(300);
    },
  });

  const keyPrefix = `sync:scheduler:${schedId}`;
  const before = JSON.parse((await redis.get(`${keyPrefix}:schedule:cleanup`)) as string);
  const slot = Date.now() - 1_000;
  await redis.set(`${keyPrefix}:schedule:cleanup`, JSON.stringify({ ...before, nextRunAt: slot }));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(slot), "cleanup"]);

  // Sample the durable record throughout the race.
  const samples: Array<{ runNumber: number; nextRunAt: number }> = [];
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      const info = await s.get({ id: "cleanup" });
      if (info) samples.push({ runNumber: info.runNumber, nextRunAt: info.nextRunAt });
      await Bun.sleep(20);
    }
  })();

  s.start();
  // Fire a manual run into the same window the tick loop is dispatching.
  await s.runNow({ id: "cleanup" }).catch(() => {});
  await Bun.sleep(1_500);
  sampling = false;
  await sampler;

  expect(samples.length).toBeGreaterThan(5);
  // An unsynchronised read-modify-write let the manual run persist a stale
  // snapshot, rewinding nextRunAt to a slot that was already due again and
  // reusing a runNumber the cron run had taken.
  for (let i = 1; i < samples.length; i++) {
    expect(samples[i]!.runNumber).toBeGreaterThanOrEqual(samples[i - 1]!.runNumber);
    expect(samples[i]!.nextRunAt).toBeGreaterThanOrEqual(samples[i - 1]!.nextRunAt);
  }

  const after = samples[samples.length - 1]!;
  expect(after.runNumber).toBeGreaterThanOrEqual(1);
  expect(after.nextRunAt).toBeGreaterThan(slot);
}, 30_000);

test("a run whose record advanced underneath it cannot overwrite the newer state", async () => {
  const schedId = uid("stale-persist");
  const s = makeScheduler(schedId);
  const keyPrefix = `sync:scheduler:${schedId}`;

  let inProcess = false;
  let release = (): void => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });

  await s.create({
    id: "report",
    cron: "0 3 * * *", // never naturally due; this run is manual only
    tz: "UTC",
    process: async () => {
      inProcess = true;
      await held;
    },
  });

  // Start a manual run and park it inside the callback. It captured
  // runNumber = 0 and will try to persist runNumber = 1.
  const manual = s.runNow({ id: "report" });
  await waitFor(() => inProcess, 10_000);

  // Meanwhile another dispatcher completes a run for the same schedule.
  const current = JSON.parse((await redis.get(`${keyPrefix}:schedule:report`)) as string);
  const newer = { ...current, runNumber: 7, nextRunAt: Date.now() + 3_600_000, updatedAt: Date.now() };
  await redis.set(`${keyPrefix}:schedule:report`, JSON.stringify(newer));

  release();
  await manual;

  // The parked run's terminal write was derived from runNumber = 0, so it must
  // be refused rather than rewinding the record to its own stale snapshot.
  const after = await s.get({ id: "report" });
  expect(after?.runNumber).toBe(7);
  expect(after?.nextRunAt).toBe(newer.nextRunAt);
  expect(s.metric().staleWrites).toBeGreaterThan(0);
}, 30_000);

test("stop cancels the in-flight schedule callback via ctx.signal", async () => {
  const schedId = uid("stop-signal");
  const s = makeScheduler(schedId);

  let abortedDuringRun = false;
  let ranToCompletion = false;
  let started = false;

  await s.create({
    id: "long",
    cron: "0 3 * * *", // manual only
    tz: "UTC",
    process: async ({ ctx }) => {
      started = true;
      // `ctx.signal.aborted` used to be false for the whole life of process.
      for (let i = 0; i < 100; i++) {
        if (ctx.signal.aborted) {
          abortedDuringRun = true;
          return;
        }
        await Bun.sleep(20);
      }
      ranToCompletion = true;
    },
  });

  s.start();
  const run = s.runNow({ id: "long" }).catch(() => {});
  await waitFor(() => started, 10_000);
  await s.stop();
  await run;

  expect(abortedDuringRun).toBe(true);
  expect(ranToCompletion).toBe(false);
}, 30_000);
