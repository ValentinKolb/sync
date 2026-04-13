import { expect, setDefaultTimeout, test } from "bun:test";
import { z } from "zod";
import {
  ephemeral,
  mutex,
  queue,
  ratelimit,
  registry,
  retry,
  topic,
} from "../../index";

setDefaultTimeout(45_000);

const uid = (label: string): string => `${label}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

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

test("ratelimit stays stable across repeated sliding-window cycles", async () => {
  const limiter = ratelimit({
    id: uid("manual-rl"),
    limit: 2,
    windowSecs: 1,
    prefix: "manual:rl",
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
    expect(carry.remaining).toBeGreaterThanOrEqual(0);
    expect(carry.remaining).toBeLessThan(2);

    const followUp = await limiter.check(alpha);
    expect(followUp.remaining).toBeLessThanOrEqual(carry.remaining);
    expect(followUp.resetIn).toBeGreaterThan(0);
    expect(followUp.resetIn).toBeLessThanOrEqual(1_000);

    await Bun.sleep(2_100);

    const fresh = await limiter.check(alpha);
    expect(fresh.limited).toBe(false);
    expect(fresh.remaining).toBe(1);
  }
});

test("mutex ownership handoff remains consistent across repeated rounds", async () => {
  const mutexId = uid("manual-mutex");
  const m1 = mutex({ id: mutexId, prefix: "manual:mx", retryCount: 0, defaultTtl: 120 });
  const m2 = mutex({ id: mutexId, prefix: "manual:mx", retryCount: 0, defaultTtl: 120 });
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

test("queue survives repeated nack-delay cycles and consumer restart mid-batch", async () => {
  const q = queue({
    id: uid("manual-queue"),
    prefix: "manual:q",
    schema: z.object({ msg: z.string() }),
  });

  await q.send({ data: { msg: "cycle" } });
  await q.send({ data: { msg: "a" } });
  await q.send({ data: { msg: "b" } });

  const first = await q.recv({ wait: false, leaseMs: 120 });
  expect(first?.data.msg).toBe("cycle");
  expect(first?.attempt).toBe(1);
  expect(await first?.nack({ delayMs: 80, reason: "retry-1" })).toBe(true);

  const a = await q.reader().recv({ wait: false });
  expect(a?.data.msg).toBe("a");
  expect(await a?.ack()).toBe(true);

  await Bun.sleep(120);

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

  expect(await second?.nack({ delayMs: 80, reason: "retry-2" })).toBe(true);

  await q.send({ data: { msg: "tail" } });
  const tail = await q.reader().recv({ wait: false });
  expect(tail?.data.msg).toBe("tail");
  expect(await tail?.ack()).toBe(true);

  await Bun.sleep(120);

  const third = await q.reader().recv({ wait: false });
  expect(third?.data.msg).toBe("cycle");
  expect(third?.attempt).toBe(3);
  expect(await third?.ack()).toBe(true);

  expect(await q.recv({ wait: false })).toBeNull();
});

test("topic reader reopen and live replay stay gap-free across multiple publish waves", async () => {
  const t = topic({
    id: uid("manual-topic"),
    prefix: "manual:t",
    schema: z.object({ n: z.number() }),
  });

  await t.pub({ data: { n: 1 } });
  await t.pub({ data: { n: 2 } });

  const r1 = t.reader("worker");
  const e1 = await r1.recv({ wait: false });
  expect(e1?.data.n).toBe(1);
  expect(await e1?.commit()).toBe(true);

  const r2 = t.reader("worker");
  const e2 = await r2.recv({ wait: false });
  expect(e2?.data.n).toBe(2);
  expect(await e2?.commit()).toBe(true);

  await t.pub({ data: { n: 3 } });
  await t.pub({ data: { n: 4 } });

  const liveSeen: number[] = [];
  const liveAc = new AbortController();
  const liveLoop = (async () => {
    for await (const event of t.live({ after: e2?.cursor ?? "0-0", signal: liveAc.signal, timeoutMs: 2_000 })) {
      liveSeen.push(event.data.n);
      if (liveSeen.length >= 4) {
        liveAc.abort();
      }
    }
  })();

  await Bun.sleep(50);
  await t.pub({ data: { n: 5 } });
  await t.pub({ data: { n: 6 } });
  await liveLoop;

  expect(liveSeen).toEqual([3, 4, 5, 6]);

  const r3 = t.reader("worker");
  const rest: number[] = [];
  for await (const event of r3.stream({ wait: false })) {
    rest.push(event.data.n);
    await event.commit();
  }
  expect(rest).toEqual([3, 4, 5, 6]);

  const audit = t.reader("audit");
  const auditSeen: number[] = [];
  for await (const event of audit.stream({ wait: false })) {
    auditSeen.push(event.data.n);
    await event.commit();
  }
  expect(auditSeen).toEqual([1, 2, 3, 4, 5, 6]);
});

test("registry survives repeated handoff, expiry, recreate, cas, and delete cycles", async () => {
  const reg = registry({
    id: uid("manual-registry"),
    schema: z.object({ name: z.string() }),
    limits: {
      tombstoneRetentionMs: 500,
      eventRetentionMs: 5_000,
    },
  });

  const key = "apps/contacts/instances/i-1";
  await reg.upsert({ key: "bootstrap/init", value: { name: "seed" } });
  await reg.remove({ key: "bootstrap/init", reason: "seed-cleanup" });
  let cursor = (await reg.list()).cursor;
  const seenTypes: string[] = [];

  const expectEvent = async (run: () => Promise<unknown>, expectedType: string): Promise<void> => {
    const pending = reg.reader({ after: cursor }).recv({ wait: true, timeoutMs: 1_500 });
    await run();
    const event = await pending;
    expect(event).not.toBeNull();
    cursor = event!.cursor;
    seenTypes.push(event!.type);
    expect(event!.type).toBe(expectedType);
  };

  for (let round = 1; round <= 3; round += 1) {
    let entryVersion = "";
    await expectEvent(async () => {
      const entry = await reg.upsert({ key, value: { name: `svc-${round}` }, ttlMs: 80 });
      expect(entry.status).toBe("active");
      entryVersion = entry.version;
    }, "upsert");

    let touched: Awaited<ReturnType<typeof reg.touch>>;
    await expectEvent(async () => {
      touched = await reg.touch({ key, ttlMs: 90 });
    }, "touch");
    expect(touched.ok).toBe(true);
    expect(touched.version).toBe(entryVersion);

    await expectEvent(async () => {
      await Bun.sleep(120);
      const expired = await reg.get({ key, includeExpired: true });
      expect(expired?.status).toBe("expired");
    }, "expire");

    let recreatedVersion = "";
    await expectEvent(async () => {
      const recreated = await reg.upsert({ key, value: { name: `svc-${round}-recreated` } });
      recreatedVersion = recreated.version;
    }, "upsert");

    let cas: Awaited<ReturnType<typeof reg.cas>>;
    await expectEvent(async () => {
      cas = await reg.cas({ key, version: recreatedVersion, value: { name: `svc-${round}-cas` } });
    }, "upsert");
    expect(cas.ok).toBe(true);
    expect(cas.entry?.value.name).toBe(`svc-${round}-cas`);

    let removed = false;
    await expectEvent(async () => {
      removed = await reg.remove({ key, reason: `cleanup-${round}` });
    }, "delete");
    expect(removed).toBe(true);
    expect(await reg.get({ key })).toBeNull();
  }

  expect(seenTypes).toEqual([
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
    "upsert", "touch", "expire", "upsert", "upsert", "delete",
  ]);
});

test("ephemeral handles repeated lifecycle churn on the same key without gaps", async () => {
  const store = ephemeral({
    id: uid("manual-ephemeral"),
    schema: z.object({ status: z.string() }),
    ttlMs: 80,
    limits: { eventRetentionMs: 5_000 },
  });

  const key = "presence/u1";
  let cursor = (await store.snapshot()).cursor;
  const seenTypes: string[] = [];

  const expectEvent = async (run: () => Promise<unknown>, expectedType: string): Promise<void> => {
    const pending = store.reader({ after: cursor }).recv({ wait: true, timeoutMs: 1_500 });
    await run();
    const event = await pending;
    expect(event).not.toBeNull();
    cursor = event!.cursor;
    seenTypes.push(event!.type);
    expect(event!.type).toBe(expectedType);
  };

  for (let round = 1; round <= 3; round += 1) {
    await expectEvent(async () => {
      await store.upsert({ key, value: { status: `online-${round}` } });
    }, "upsert");

    let touched: Awaited<ReturnType<typeof store.touch>>;
    await expectEvent(async () => {
      touched = await store.touch({ key, ttlMs: 90 });
    }, "touch");
    expect(touched.ok).toBe(true);

    if (round % 2 === 0) {
      await expectEvent(async () => {
        await Bun.sleep(120);
        const snapshot = await store.snapshot();
        expect(snapshot.entries.find((entry) => entry.key === key)).toBeUndefined();
      }, "expire");
    } else {
      let removed = false;
      await expectEvent(async () => {
        removed = await store.remove({ key, reason: `manual-${round}` });
      }, "delete");
      expect(removed).toBe(true);
    }
  }

  await expectEvent(async () => {
    await store.upsert({ key: "presence/u2", value: { status: "online-final" } });
  }, "upsert");

  const snapshot = await store.snapshot();
  expect(snapshot.entries.map((entry) => entry.key)).toEqual(["presence/u2"]);
  expect(seenTypes).toEqual([
    "upsert", "touch", "delete",
    "upsert", "touch", "expire",
    "upsert", "touch", "delete",
    "upsert",
  ]);
});

test("retry keeps repeated flaky runs isolated and abort does not poison later calls", async () => {
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
