import { expect, setDefaultTimeout, test } from "bun:test";
import { z } from "zod";
import { ephemeral } from "../../src/browser/ephemeral";
import { mutex } from "../../src/browser/mutex";
import { queue } from "../../src/browser/queue";
import { ratelimit } from "../../src/browser/ratelimit";
import { registry } from "../../src/browser/registry";
import { retry } from "../../src/browser/retry";
import { createMemoryStore } from "../../src/browser/store";
import { topic } from "../../src/browser/topic";

setDefaultTimeout(45_000);

let counter = 0;
const uid = (label: string): string => `${label}-${++counter}-${Date.now()}`;

const waitUntil = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await Bun.sleep(20);
  }
  throw new Error("waitUntil timeout");
};

const alignNearWindowEnd = async (windowMs: number, minRemainderMs: number): Promise<void> => {
  await waitUntil(() => Date.now() % windowMs >= minRemainderMs, windowMs);
};

test("browser ratelimit stays stable across repeated sliding-window cycles", async () => {
  const limiter = ratelimit({
    id: uid("browser-rl"),
    limit: 2,
    windowSecs: 1,
    store: createMemoryStore(),
  });

  for (let cycle = 1; cycle <= 3; cycle += 1) {
    const alpha = `alpha-${cycle}`;
    const betaId = `beta-${cycle}`;
    await alignNearWindowEnd(1_000, 950);

    const first = await limiter.check(alpha);
    const beta = await limiter.check(betaId);

    expect(first.limited).toBe(false);
    expect(beta.limited).toBe(false);

    await Bun.sleep(120);

    const carry = await limiter.check(alpha);
    expect(carry.limited).toBe(false);
    expect(carry.remaining).toBeLessThan(2);

    const limited = await limiter.check(alpha);
    expect(limited.limited).toBe(true);

    await Bun.sleep(2_100);

    const fresh = await limiter.check(alpha);
    expect(fresh.limited).toBe(false);
    expect(fresh.remaining).toBe(1);
  }
});

test("browser mutex handoff remains consistent across repeated rounds", async () => {
  const sharedStore = createMemoryStore();
  const mutexId = uid("browser-mutex");
  const m1 = mutex({ id: mutexId, retryCount: 0, defaultTtl: 120, store: sharedStore });
  const m2 = mutex({ id: mutexId, retryCount: 0, defaultTtl: 120, store: sharedStore });
  const resource = "shared-resource";

  for (let round = 0; round < 6; round += 1) {
    const owner = round % 2 === 0 ? m1 : m2;
    const other = owner === m1 ? m2 : m1;

    const lock = await owner.acquire(resource, 120);
    expect(lock).not.toBeNull();
    expect(await other.acquire(resource, 50)).toBeNull();

    if (round % 2 === 0) {
      expect(await owner.extend(lock!, 180)).toBe(true);
    }

    await owner.release(lock!);

    const takeover = await other.acquire(resource, 120);
    expect(takeover).not.toBeNull();
    expect(await owner.extend(lock!, 120)).toBe(false);
    await owner.release(lock!);
    expect(await owner.acquire(resource, 50)).toBeNull();

    await other.release(takeover!);
  }
});

test("browser queue survives repeated nack-delay cycles and reader restart mid-batch", async () => {
  const q = queue({
    id: uid("browser-queue"),
    prefix: "manual:bq",
    schema: z.object({ msg: z.string() }),
  });

  await q.send({ data: { msg: "cycle" } });
  await q.send({ data: { msg: "a" } });
  await q.send({ data: { msg: "b" } });

  const first = await q.recv({ wait: false, leaseMs: 120 });
  expect(first?.data.msg).toBe("cycle");
  expect(first?.attempt).toBe(1);
  expect(await first?.nack({ delayMs: 50, reason: "retry-1" })).toBe(true);

  const a = await q.reader().recv({ wait: false });
  expect(a?.data.msg).toBe("a");
  expect(await a?.ack()).toBe(true);

  await Bun.sleep(1_200);

  const reader2 = q.reader();
  const b = await reader2.recv({ wait: false });
  expect(b?.data.msg).toBe("b");
  expect(await b?.ack()).toBe(true);

  const second = await reader2.recv({ wait: false, leaseMs: 120 });
  expect(second?.data.msg).toBe("cycle");
  expect(second?.attempt).toBe(2);
  expect(await second?.touch({ leaseMs: 200 })).toBe(true);

  await Bun.sleep(100);
  expect(await q.reader().recv({ wait: false })).toBeNull();

  expect(await second?.nack({ delayMs: 50, reason: "retry-2" })).toBe(true);
  await q.send({ data: { msg: "tail" } });

  const tail = await q.reader().recv({ wait: false });
  expect(tail?.data.msg).toBe("tail");
  expect(await tail?.ack()).toBe(true);

  await Bun.sleep(1_200);

  const third = await q.reader().recv({ wait: false });
  expect(third?.data.msg).toBe("cycle");
  expect(third?.attempt).toBe(3);
  expect(await third?.ack()).toBe(true);

  expect(await q.recv({ wait: false })).toBeNull();
});

test("browser topic live reopen stays gap-free across multiple publish waves", async () => {
  const t = topic({
    id: uid("browser-topic"),
    prefix: "manual:bt",
    schema: z.object({ n: z.number() }),
    store: createMemoryStore(),
  });

  await t.pub({ data: { n: 1 } });
  await t.pub({ data: { n: 2 } });

  let after = "0";
  const liveSeen: number[] = [];

  const collectBurst = async (expectedCount: number): Promise<void> => {
    const burst: number[] = [];
    const ac = new AbortController();

    for await (const event of t.live({ after, signal: ac.signal, timeoutMs: 2_000 })) {
      burst.push(event.data.n);
      liveSeen.push(event.data.n);
      after = event.cursor;
      if (burst.length >= expectedCount) {
        ac.abort();
      }
    }
  };

  await collectBurst(2);
  expect(liveSeen).toEqual([1, 2]);

  await t.pub({ data: { n: 3 } });
  await t.pub({ data: { n: 4 } });
  await collectBurst(2);

  await t.pub({ data: { n: 5 } });
  await t.pub({ data: { n: 6 } });
  await collectBurst(2);

  expect(liveSeen).toEqual([1, 2, 3, 4, 5, 6]);

  const audit = t.reader("audit");
  const auditSeen: number[] = [];
  for await (const event of audit.stream({ wait: false })) {
    auditSeen.push(event.data.n);
    await event.commit();
  }
  expect(auditSeen).toEqual([1, 2, 3, 4, 5, 6]);
});

test("browser registry survives repeated handoff, expiry, recreate, cas, and delete cycles", async () => {
  const reg = registry({
    id: uid("browser-registry"),
    schema: z.object({ name: z.string() }),
    limits: {
      tombstoneRetentionMs: 500,
      eventRetentionMs: 5_000,
    },
  });

  const key = "apps/contacts/instances/i-1";
  let cursor = (await reg.list()).cursor;
  const seenTypes: string[] = [];

  const nextEvent = async (): Promise<string> => {
    const event = await reg.reader({ after: cursor }).recv({ wait: true, timeoutMs: 1_500 });
    expect(event).not.toBeNull();
    cursor = event!.cursor;
    seenTypes.push(event!.type);
    return event!.type;
  };

  for (let round = 1; round <= 3; round += 1) {
    const entry = await reg.upsert({ key, value: { name: `svc-${round}` }, ttlMs: 80 });
    expect(entry.status).toBe("active");
    expect(await nextEvent()).toBe("upsert");

    const touched = await reg.touch({ key, ttlMs: 90 });
    expect(touched.ok).toBe(true);
    expect(touched.version).toBe(entry.version);
    expect(await nextEvent()).toBe("touch");

    await Bun.sleep(120);
    const expired = await reg.get({ key, includeExpired: true });
    expect(expired?.status).toBe("expired");
    expect(await nextEvent()).toBe("expire");

    const recreated = await reg.upsert({ key, value: { name: `svc-${round}-recreated` } });
    expect(await nextEvent()).toBe("upsert");

    const cas = await reg.cas({ key, version: recreated.version, value: { name: `svc-${round}-cas` } });
    expect(cas.ok).toBe(true);
    expect(cas.entry?.value.name).toBe(`svc-${round}-cas`);
    expect(await nextEvent()).toBe("upsert");

    expect(await reg.remove({ key, reason: `cleanup-${round}` })).toBe(true);
    expect(await nextEvent()).toBe("delete");
    expect(await reg.get({ key })).toBeNull();
  }

  expect(seenTypes).toEqual([
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
  ]);
});

test("browser ephemeral handles repeated lifecycle churn on the same key without gaps", async () => {
  const store = ephemeral({
    id: uid("browser-ephemeral"),
    schema: z.object({ status: z.string() }),
    ttlMs: 80,
    limits: { eventRetentionMs: 5_000 },
  });

  const key = "presence/u1";
  let cursor = (await store.snapshot()).cursor;
  const seenTypes: string[] = [];

  const nextEvent = async (): Promise<string> => {
    const event = await store.reader({ after: cursor }).recv({ wait: true, timeoutMs: 1_500 });
    expect(event).not.toBeNull();
    cursor = event!.cursor;
    seenTypes.push(event!.type);
    return event!.type;
  };

  for (let round = 1; round <= 3; round += 1) {
    await store.upsert({ key, value: { status: `online-${round}` } });
    expect(await nextEvent()).toBe("upsert");

    const touched = await store.touch({ key, ttlMs: 90 });
    expect(touched.ok).toBe(true);
    expect(await nextEvent()).toBe("touch");

    if (round % 2 === 0) {
      await Bun.sleep(120);
      const snapshot = await store.snapshot();
      expect(snapshot.entries.find((entry) => entry.key === key)).toBeUndefined();
      expect(await nextEvent()).toBe("expire");
    } else {
      expect(await store.remove({ key, reason: `manual-${round}` })).toBe(true);
      expect(await nextEvent()).toBe("delete");
    }
  }

  await store.upsert({ key: "presence/u2", value: { status: "online-final" } });
  expect(await nextEvent()).toBe("upsert");

  const snapshot = await store.snapshot();
  expect(snapshot.entries.map((entry) => entry.key)).toEqual(["presence/u2"]);
  expect(seenTypes).toEqual([
    "upsert", "touch", "delete",
    "upsert", "touch", "expire",
    "upsert", "touch", "delete",
    "upsert",
  ]);
});

test("browser retry keeps repeated flaky runs isolated and abort does not poison later calls", async () => {
  const attemptsPerRun: number[] = [];

  for (let run = 1; run <= 3; run += 1) {
    let attempts = 0;
    const value = await retry(
      async () => {
        attempts += 1;
        if (attempts < 3) {
          const err = new Error(`socket reset run ${run}`);
          (err as Error & { code?: string }).code = "ECONNRESET";
          throw err;
        }
        return run;
      },
      { attempts: 5, minDelayMs: 5, maxDelayMs: 20, jitter: 0 },
    );
    attemptsPerRun.push(attempts);
    expect(value).toBe(run);
  }

  expect(attemptsPerRun).toEqual([3, 3, 3]);

  const ac = new AbortController();
  let abortAttempts = 0;
  const abortPromise = retry(
    async () => {
      abortAttempts += 1;
      const err = new Error("socket closed");
      (err as Error & { code?: string }).code = "ECONNRESET";
      throw err;
    },
    {
      attempts: 10,
      minDelayMs: 200,
      maxDelayMs: 200,
      jitter: 0,
      signal: ac.signal,
    },
  );

  setTimeout(() => ac.abort(), 20);
  await expect(abortPromise).rejects.toMatchObject({ name: "AbortError" });
  expect(abortAttempts).toBe(1);

  let recoveryAttempts = 0;
  const recovered = await retry(
    async () => {
      recoveryAttempts += 1;
      if (recoveryAttempts < 2) {
        const err = new Error("network blip");
        (err as Error & { code?: string }).code = "ECONNRESET";
        throw err;
      }
      return "recovered";
    },
    { attempts: 4, minDelayMs: 5, maxDelayMs: 20, jitter: 0 },
  );

  expect(recovered).toBe("recovered");
  expect(recoveryAttempts).toBe(2);
});
