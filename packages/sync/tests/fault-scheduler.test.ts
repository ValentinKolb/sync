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

test("orphaned due schedule (no handler) advances past slot and doesn't spin", async () => {
  const schedId = uid("orphan");
  const s = makeScheduler(schedId);

  await s.create({
    id: "oh",
    cron: "* * * * *",
    tz: "UTC",
    process: async () => {},
  });

  const keyPrefix = `sync:scheduler:${schedId}`;
  const raw = await redis.get(`${keyPrefix}:schedule:oh`);
  const parsed = JSON.parse(raw as string);
  parsed.nextRunAt = Date.now() - 1000;
  await redis.set(`${keyPrefix}:schedule:oh`, JSON.stringify(parsed));
  await redis.send("ZADD", [`${keyPrefix}:due`, String(parsed.nextRunAt), "oh"]);

  const fresh = scheduler({
    id: schedId,
    leader: { leaseMs: 1_000, heartbeatMs: 100 },
    dispatch: { tickMs: 100 },
  });
  activeSchedulers.push(fresh);
  fresh.start();

  await waitFor(
    async () => {
      const info = await fresh.get({ id: "oh" });
      return info !== null && info.nextRunAt > Date.now();
    },
    5_000,
  );

  const info = await fresh.get({ id: "oh" });
  expect(info?.runNumber).toBe(0);
});

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
