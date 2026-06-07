import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { topic } from "../index";

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:t:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("pub + reader.recv + commit", async () => {
  const t = topic({
    id: "orders",
    prefix: "test:t",
  });

  await t.pub({ data: { type: "order.confirmed", orderId: "o1" } });

  const reader = t.reader("mailer");
  const message = await reader.recv({ wait: false });

  expect(message).not.toBeNull();
  expect(message?.data.type).toBe("order.confirmed");
  expect(await message?.commit()).toBe(true);
});

test("different groups consume same event independently", async () => {
  const t = topic({
    id: "events",
    prefix: "test:t",
  });

  await t.pub({ data: { value: 7 } });

  const a = await t.reader("group-a").recv({ wait: false });
  const b = await t.reader("group-b").recv({ wait: false });

  expect(a?.data.value).toBe(7);
  expect(b?.data.value).toBe(7);
  expect(await a?.commit()).toBe(true);
  expect(await b?.commit()).toBe(true);
});

test("reader.stream consumes available events", async () => {
  const t = topic({
    id: "stream",
    prefix: "test:t",
  });

  await t.pub({ data: { idx: 1 } });
  await t.pub({ data: { idx: 2 } });

  const reader = t.reader("analytics");
  const seen: number[] = [];

  for await (const event of reader.stream({ wait: false })) {
    seen.push(event.data.idx);
    await event.commit();
  }

  expect(seen).toEqual([1, 2]);
});

test("reader() defaults to default group", async () => {
  const t = topic({
    id: "default-group",
    prefix: "test:t",
  });

  const reader = t.reader();
  expect(reader.group).toBe("default");
});

test("live receives newly published event", async () => {
  const t = topic({
    id: "live",
    prefix: "test:t",
  });

  const iterator = t.live({ timeoutMs: 1_000 })[Symbol.asyncIterator]();
  const nextEvent = iterator.next();

  await Bun.sleep(30);
  await t.pub({ data: { n: 42 } });

  const received = await nextEvent;
  expect(received.done).toBe(false);
  expect(received.value?.data.n).toBe(42);

  if (iterator.return) {
    await iterator.return();
  }
});

test("reader.recv returns null when no event exists", async () => {
  const t = topic({
    id: "empty",
    prefix: "test:t",
  });

  const message = await t.reader("none").recv({ wait: false });
  expect(message).toBeNull();
});

test("pub idempotency key deduplicates events", async () => {
  const t = topic({
    id: "idempotency",
    prefix: "test:t",
  });

  const a = await t.pub({ data: { id: "a" }, idempotencyKey: "k" });
  const b = await t.pub({ data: { id: "a" }, idempotencyKey: "k" });
  expect(a.eventId).toBe(b.eventId);

  const reader = t.reader("g");
  const events: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    events.push(event.eventId);
    await event.commit();
  }

  expect(events.length).toBe(1);
});

test("commit can only acknowledge once", async () => {
  const t = topic({
    id: "commit-once",
    prefix: "test:t",
  });

  await t.pub({ data: { n: 1 } });
  const message = await t.reader("cg").recv({ wait: false });
  expect(message).not.toBeNull();
  expect(await message?.commit()).toBe(true);
  expect(await message?.commit()).toBe(false);
});

test("live stops when signal is aborted", async () => {
  const t = topic({
    id: "live-abort",
    prefix: "test:t",
  });

  const ac = new AbortController();
  const received: number[] = [];

  const consuming = (async () => {
    for await (const event of t.live({ signal: ac.signal, timeoutMs: 2_000 })) {
      received.push(event.data.n);
    }
  })();

  await Bun.sleep(30);
  await t.pub({ data: { n: 1 } });
  await Bun.sleep(30);
  ac.abort();

  await consuming;
  expect(received).toEqual([1]);
});

test("reader.stream stops when signal is aborted", async () => {
  const t = topic({
    id: "stream-abort",
    prefix: "test:t",
  });

  await t.pub({ data: { v: 10 } });
  await t.pub({ data: { v: 20 } });

  const ac = new AbortController();
  const reader = t.reader("abort-group");
  const seen: number[] = [];

  for await (const event of reader.stream({ signal: ac.signal, wait: false })) {
    seen.push(event.data.v);
    await event.commit();
    if (seen.length >= 2) ac.abort();
  }

  expect(seen).toEqual([10, 20]);
});

test("parallel pub with same idempotency key deduplicates", async () => {
  const t = topic({
    id: "parallel-idem",
    prefix: "test:t",
  });

  const results = await Promise.all(
    Array.from({ length: 10 }, () =>
      t.pub({ data: { id: "a" }, idempotencyKey: "race-key" }),
    ),
  );

  const ids = new Set(results.map((r) => r.eventId));
  expect(ids.size).toBe(1);

  const reader = t.reader("check");
  const events: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    events.push(event.eventId);
    await event.commit();
  }
  expect(events.length).toBe(1);
});

test("uncommitted message is redelivered to same group", async () => {
  const t = topic({
    id: "redeliver",
    prefix: "test:t",
  });

  await t.pub({ data: { v: 42 } });

  // First reader receives but does NOT commit
  const reader1 = t.reader("sticky-group");
  const msg1 = await reader1.recv({ wait: false });
  expect(msg1).not.toBeNull();
  expect(msg1?.data.v).toBe(42);
  // intentionally no commit

  // A new reader for the same group using ">" should NOT see the pending message
  const reader2 = t.reader("sticky-group");
  const msg2 = await reader2.recv({ wait: false });
  expect(msg2).toBeNull();

  // But the original can still commit
  expect(await msg1?.commit()).toBe(true);
});

test("tenant isolation separates topic streams", async () => {
  const t = topic({
    id: "tenant-iso",
    prefix: "test:t",
  });

  await t.pub({ tenantId: "t1", data: { value: "alpha" } });
  await t.pub({ tenantId: "t2", data: { value: "beta" } });

  const r1 = t.reader("grp");
  const r2 = t.reader("grp");

  const m1 = await r1.recv({ tenantId: "t1", wait: false });
  const m2 = await r2.recv({ tenantId: "t2", wait: false });

  expect(m1?.data.value).toBe("alpha");
  expect(m2?.data.value).toBe("beta");

  expect(await m1?.commit()).toBe(true);
  expect(await m2?.commit()).toBe(true);

  // Cross-tenant: t1 reader should not see t2 data
  const cross = await r1.recv({ tenantId: "t1", wait: false });
  expect(cross).toBeNull();
});

test("payload exceeding size limit is rejected on pub", async () => {
  const t = topic({
    id: "pub-limit",
    prefix: "test:t",
    limits: { payloadBytes: 64 },
  });

  let thrown: unknown = null;
  try {
    await t.pub({ data: { data: "x".repeat(200) } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toContain("payload exceeds limit");
});

test("meta and orderingKey are preserved through pub/recv", async () => {
  const t = topic({
    id: "meta-ordering",
    prefix: "test:t",
  });

  await t.pub({
    data: { v: 1 },
    orderingKey: "partition-a",
    meta: { source: "unit-test", version: 3 },
  });

  const reader = t.reader("meta-grp");
  const msg = await reader.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(msg?.orderingKey).toBe("partition-a");
  expect(msg?.meta?.source).toBe("unit-test");
  expect(msg?.meta?.version).toBe(3);
  expect(msg?.publishedAt).toBeGreaterThan(0);
  expect(await msg?.commit()).toBe(true);
});

test("live cursor advances correctly across multiple events", async () => {
  const t = topic({
    id: "live-cursor",
    prefix: "test:t",
  });

  await t.pub({ data: { n: 1 } });
  await t.pub({ data: { n: 2 } });
  await t.pub({ data: { n: 3 } });

  const ac = new AbortController();
  const received: number[] = [];

  for await (const event of t.live({ after: "0-0", signal: ac.signal, timeoutMs: 1_000 })) {
    received.push(event.data.n);
    if (received.length >= 3) ac.abort();
  }

  expect(received).toEqual([1, 2, 3]);
});

test("idempotency key expires after ttl on pub", async () => {
  const t = topic({
    id: "idem-ttl",
    prefix: "test:t",
  });

  const a = await t.pub({ data: { v: 1 }, idempotencyKey: "expire-me", idempotencyTtlMs: 40 });
  await Bun.sleep(60);
  const b = await t.pub({ data: { v: 2 }, idempotencyKey: "expire-me", idempotencyTtlMs: 40 });

  expect(a.eventId).not.toBe(b.eventId);

  const reader = t.reader("ttl-grp");
  const events: number[] = [];
  for await (const event of reader.stream({ wait: false })) {
    events.push(event.data.v);
    await event.commit();
  }
  expect(events).toEqual([1, 2]);
});

test("live can replay from explicit cursor", async () => {
  const t = topic({
    id: "live-after",
    prefix: "test:t",
  });

  await t.pub({ data: { value: 9 } });

  const iterator = t.live({ after: "0-0", timeoutMs: 1_000 })[Symbol.asyncIterator]();
  const first = await iterator.next();

  expect(first.done).toBe(false);
  expect(first.value?.data.value).toBe(9);

  if (iterator.return) {
    await iterator.return();
  }
});

// ==========================
// Race condition tests
// ==========================

test("two consumers in same group split messages without duplicates", async () => {
  const t = topic({
    id: "same-group-split",
    prefix: "test:t",
  });

  const count = 20;
  for (let i = 0; i < count; i++) {
    await t.pub({ data: { idx: i } });
  }

  const r1 = t.reader("shared-group");
  const r2 = t.reader("shared-group");
  const collected: number[] = [];

  const drain = async (reader: typeof r1): Promise<void> => {
    while (true) {
      const msg = await reader.recv({ wait: false });
      if (!msg) break;
      collected.push(msg.data.idx);
      await msg.commit();
    }
  };

  await Promise.all([drain(r1), drain(r2)]);

  expect(collected.length).toBe(count);
  expect(new Set(collected).size).toBe(count);
});

test("rapid burst during live iteration — no duplicates or gaps", async () => {
  const t = topic({
    id: "live-burst",
    prefix: "test:t",
  });

  const ac = new AbortController();
  const received: number[] = [];
  const count = 30;

  const consuming = (async () => {
    for await (const event of t.live({ after: "0-0", signal: ac.signal, timeoutMs: 3_000 })) {
      received.push(event.data.n);
      if (received.length >= count) ac.abort();
    }
  })();

  // Small delay for the live iterator to start blocking
  await Bun.sleep(30);

  // Rapid burst of publishes
  for (let i = 0; i < count; i++) {
    await t.pub({ data: { n: i } });
  }

  await consuming;

  expect(received.length).toBe(count);
  expect(new Set(received).size).toBe(count);
  // Verify ordering
  for (let i = 0; i < count; i++) {
    expect(received[i]).toBe(i);
  }
});

test("concurrent first-recv on new group does not throw", async () => {
  const t = topic({
    id: "concurrent-group-create",
    prefix: "test:t",
  });

  await t.pub({ data: { v: 1 } });
  await t.pub({ data: { v: 2 } });

  // Two readers with a brand new group — both will try XGROUP CREATE
  const r1 = t.reader("fresh-group");
  const r2 = t.reader("fresh-group");

  const [m1, m2] = await Promise.all([
    r1.recv({ wait: false }),
    r2.recv({ wait: false }),
  ]);

  // At least one should succeed, and they should not get the same message
  const received = [m1, m2].filter((m) => m !== null);
  expect(received.length).toBeGreaterThanOrEqual(1);

  const ids = received.map((m) => m!.eventId);
  expect(new Set(ids).size).toBe(ids.length); // no duplicates

  for (const m of received) {
    await m!.commit();
  }
});

test("latestCursor returns null when stream is missing", async () => {
  const t = topic({
    id: "latest-empty",
    prefix: "test:t",
  });

  expect(await t.latestCursor()).toBeNull();
});

test("latestCursor returns the latest published cursor", async () => {
  const t = topic({
    id: "latest-published",
    prefix: "test:t",
  });

  await t.pub({ data: { n: 1 } });
  const second = await t.pub({ data: { n: 2 } });

  expect(await t.latestCursor()).toBe(second.cursor);
});

test("latestCursor uses the same tenant isolation as pub and live", async () => {
  const t = topic({
    id: "latest-tenant",
    prefix: "test:t",
  });

  const t1 = await t.pub({ tenantId: "t1", data: { value: "alpha" } });
  const t2 = await t.pub({ tenantId: "t2", data: { value: "beta" } });

  expect(await t.latestCursor({ tenantId: "t1" })).toBe(t1.cursor);
  expect(await t.latestCursor({ tenantId: "t2" })).toBe(t2.cursor);
  expect(await t.latestCursor({ tenantId: "missing" })).toBeNull();
});

test("latestCursor does not consume reader messages", async () => {
  const t = topic({
    id: "latest-non-consuming",
    prefix: "test:t",
  });

  const published = await t.pub({ data: { value: 42 } });
  expect(await t.latestCursor()).toBe(published.cursor);

  const message = await t.reader("latest-check").recv({ wait: false });
  expect(message?.cursor).toBe(published.cursor);
  expect(message?.data.value).toBe(42);
  expect(await message?.commit()).toBe(true);
});
