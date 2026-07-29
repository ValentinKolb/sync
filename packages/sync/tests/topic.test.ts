import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { TopicPayloadError, topic } from "../index";

const connectedClients = async (): Promise<number> => {
  const info = (await redis.send("INFO", ["clients"])) as string;
  return Number(/connected_clients:(\d+)/.exec(info)?.[1] ?? 0);
};

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

test("reader reclaims an abandoned delivery through the public API", async () => {
  const t = topic<{ value: number }>({ id: "reclaim", prefix: "test:t" });
  await t.pub({ data: { value: 42 } });

  const original = await t.reader("workers").recv({ wait: false });
  expect(original?.data.value).toBe(42);

  const recovered = await t.reader("workers").reclaim?.({ minIdleMs: 0 });
  if (!recovered) throw new Error("Expected topic reclaim support");
  expect(recovered.nextCursor).toBe("0-0");
  expect(recovered.entries).toHaveLength(1);
  const entry = recovered.entries[0];
  expect(entry?.kind).toBe("delivery");
  if (entry?.kind !== "delivery") throw new Error("Expected a recovered delivery");
  expect(entry.delivery.data.value).toBe(42);
  expect(await entry.delivery.commit()).toBe(true);
});

test("invalid transport payload remains pending and is reclaimable", async () => {
  const t = topic<{ value: number }>({ id: "invalid-reclaim", prefix: "test:t" });
  const key = "test:t:default:invalid-reclaim:stream";
  await redis.send("XADD", [key, "*", "payload", "{broken"]);

  const reader = t.reader("workers");
  await expect(reader.recv({ wait: false, invalidPayload: "throw" })).rejects.toBeInstanceOf(TopicPayloadError);

  const recovered = await reader.reclaim?.({ minIdleMs: 0 });
  if (!recovered) throw new Error("Expected topic reclaim support");
  const entry = recovered.entries[0];
  expect(entry).toMatchObject({ kind: "invalid", error: "payload is not valid JSON", rawPayload: "{broken" });
  if (entry?.kind !== "invalid") throw new Error("Expected an invalid recovered delivery");
  expect(await entry.commit()).toBe(true);
});

test("reader keeps legacy acknowledgement for invalid payloads by default", async () => {
  const t = topic({ id: "invalid-default", prefix: "test:t" });
  const key = "test:t:default:invalid-default:stream";
  await redis.send("XADD", [key, "*", "payload", "{broken"]);

  const reader = t.reader("workers");
  expect(await reader.recv({ wait: false })).toBeNull();
  const recovered = await reader.reclaim?.({ minIdleMs: 0 });
  expect(recovered?.entries).toHaveLength(0);
});

test("topic preserves undefined data", async () => {
  const t = topic<undefined>({ id: "undefined-data", prefix: "test:t" });
  await t.pub({ data: undefined });

  const delivery = await t.reader("workers").recv({ wait: false, invalidPayload: "throw" });
  expect(delivery?.data).toBeUndefined();
  expect(await delivery?.commit()).toBe(true);
});

test("reclaim cursor advances beyond more than one poison batch", async () => {
  const t = topic<{ value: number }>({ id: "poison-prefix", prefix: "test:t" });
  const key = "test:t:default:poison-prefix:stream";
  const reader = t.reader("workers");

  for (let index = 0; index < 30; index++) {
    await redis.send("XADD", [key, "*", "payload", index % 2 === 0 ? "not-json" : JSON.stringify({ publishedAt: "invalid" })]);
  }
  await t.pub({ data: { value: 99 } });

  for (let index = 0; index < 30; index++) {
    await expect(reader.recv({ wait: false, invalidPayload: "throw" })).rejects.toBeInstanceOf(TopicPayloadError);
  }
  const valid = await reader.recv({ wait: false });
  expect(valid?.data.value).toBe(99);

  const kinds: string[] = [];
  let cursor = "0-0";
  do {
    const batch = await reader.reclaim?.({ minIdleMs: 0, cursor, count: 7 });
    if (!batch) throw new Error("Expected topic reclaim support");
    kinds.push(...batch.entries.map((entry) => entry.kind));
    cursor = batch.nextCursor;
  } while (cursor !== "0-0");

  expect(kinds.filter((kind) => kind === "invalid")).toHaveLength(30);
  expect(kinds.filter((kind) => kind === "delivery")).toHaveLength(1);
});

test("reclaim validates its batch controls", async () => {
  const reader = topic({ id: "reclaim-config", prefix: "test:t" }).reader("workers");
  if (!reader.reclaim) throw new Error("Expected topic reclaim support");
  await expect(reader.reclaim({ minIdleMs: -1 })).rejects.toThrow("minIdleMs must be a non-negative number");
  await expect(reader.reclaim({ count: 0 })).rejects.toThrow("count must be an integer between 1 and 1000");
  await expect(reader.reclaim({ count: 1_001 })).rejects.toThrow("count must be an integer between 1 and 1000");
});

test("reclaim advances across an ineligible pending prefix", async () => {
  const t = topic<{ value: number }>({ id: "ineligible-prefix", prefix: "test:t" });
  const key = "test:t:default:ineligible-prefix:stream";
  const group = "workers";
  const originalReader = t.reader(group);
  const eventIds: string[] = [];

  for (let index = 0; index < 25; index++) {
    eventIds.push((await t.pub({ data: { value: index } })).eventId);
    await originalReader.recv({ wait: false });
  }
  for (const eventId of eventIds.slice(0, 20)) {
    await redis.send("XCLAIM", [key, group, "fresh-owner", "0", eventId, "IDLE", "0", "JUSTID"]);
  }
  for (const eventId of eventIds.slice(20)) {
    await redis.send("XCLAIM", [key, group, "stale-owner", "0", eventId, "IDLE", "60000", "JUSTID"]);
  }

  const reader = t.reader(group);
  const first = await reader.reclaim?.({ minIdleMs: 30_000, cursor: "0-0", count: 1 });
  if (!first) throw new Error("Expected topic reclaim support");
  expect(first.entries).toHaveLength(0);
  expect(first.nextCursor).not.toBe("0-0");

  const second = await reader.reclaim?.({ minIdleMs: 30_000, cursor: first.nextCursor, count: 10 });
  if (!second) throw new Error("Expected topic reclaim support");
  expect(second.entries).toHaveLength(5);
  expect(second.entries.every((entry) => entry.kind === "delivery")).toBe(true);
});

test("reclaim recreates a consumer group removed after it was cached", async () => {
  const id = "reclaim-nogroup";
  const key = `test:t:default:${id}:stream`;
  const group = "workers";
  const t = topic<{ value: number }>({ id, prefix: "test:t" });
  const reader = t.reader(group, { consumerId: "recovery" });

  await t.pub({ data: { value: 1 } });
  const first = await reader.recv({ wait: false });
  expect(await first?.commit()).toBe(true);
  await redis.send("XGROUP", ["DESTROY", key, group]);

  await expect(reader.reclaim({ minIdleMs: 0 })).resolves.toEqual({
    nextCursor: "0-0",
    entries: [],
  });
  expect((await reader.recv({ wait: false }))?.data.value).toBe(1);
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

  try {
    const [m1, m2] = await Promise.all([
      r1.recv({ wait: false }),
      r2.recv({ wait: false }),
    ]);

    const received = [m1, m2].filter((m) => m !== null);
    expect(received).toHaveLength(2);
    expect(received.map((m) => m!.data.v).sort()).toEqual([1, 2]);
    expect(new Set(received.map((m) => m!.eventId)).size).toBe(2);

    for (const message of received) {
      expect(await message!.commit()).toBe(true);
    }
  } finally {
    await Promise.all([r1.close(), r2.close()]);
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

test("a topic retention longer than the epoch does not break pub", async () => {
  const t = topic<{ v: number }>({
    id: `retention-clamp-${Date.now()}`,
    prefix: "test:topic",
    retentionMs: 100 * 365 * 24 * 60 * 60 * 1000, // ~100 years
  });

  // XTRIM ran after the XADD in the same script, so a negative MINID meant the
  // event was written and pub() still threw — and the caller's retry duplicated it.
  const { eventId } = await t.pub({ data: { v: 1 } });
  expect(eventId).toBeTruthy();

  const reader = t.reader("clamp");
  const message = await reader.recv({ wait: false });
  expect(message?.data).toEqual({ v: 1 });
});

// ==========================
// Delivery ownership and reader lifecycle
// ==========================

test("a stale consumer cannot commit an entry another consumer reclaimed", async () => {
  const t = topic<{ v: number }>({ id: `fenced-commit-${Date.now()}`, prefix: "test:t" });

  await t.pub({ data: { v: 1 } });

  const a = t.reader("workers", { consumerId: "consumer-a" });
  const b = t.reader("workers", { consumerId: "consumer-b" });

  const delivered = await a.recv({ wait: false });
  expect(delivered).not.toBeNull();

  // B takes the stalled delivery over.
  const reclaimed = await b.reclaim({ minIdleMs: 0 });
  expect(reclaimed.entries.length).toBe(1);

  // A wakes and commits. A bare XACK succeeded here and removed the entry from
  // the group PEL entirely, so if B then died it was in nobody's PEL and could
  // never be redelivered or reclaimed again.
  expect(await delivered!.commit()).toBe(false);

  // Still owned by B, so still recoverable.
  const stillThere = await b.reclaim({ minIdleMs: 0 });
  expect(stillThere.entries.length).toBe(1);

  await a.close();
  await b.close();
});

test("stream({ wait: false }) drains past a poison payload instead of stopping", async () => {
  const id = `poison-drain-${Date.now()}`;
  const t = topic<{ v: number }>({ id, prefix: "test:t" });
  const key = `test:t:default:${id}:stream`;

  // A malformed envelope at the head of a backlog.
  await redis.send("XADD", [key, "*", "payload", "not-json"]);
  for (const v of [1, 2, 3]) await t.pub({ data: { v } });

  const reader = t.reader("drain");
  const seen: number[] = [];
  for await (const message of reader.stream({ wait: false })) {
    seen.push(message.data.v);
    await message.commit();
  }
  await reader.close();

  // One bad envelope used to end the drain, yielding nothing at all.
  expect(seen).toEqual([1, 2, 3]);
});

test("reader.close releases the blocking connection", async () => {
  const t = topic<{ v: number }>({ id: `close-conn-${Date.now()}`, prefix: "test:t" });

  const clientsBefore = await connectedClients();
  const readers = Array.from({ length: 6 }, (_, i) => t.reader(`g${i}`));
  for (const reader of readers) {
    await reader.recv({ wait: true, timeoutMs: 50 });
  }
  const clientsDuring = await connectedClients();
  expect(clientsDuring).toBeGreaterThan(clientsBefore);

  await Promise.all(readers.map((reader) => reader.close()));
  await Bun.sleep(200);

  // Without close() there was no way to reach the socket at all, so a reader
  // created per request leaked one connection per request.
  expect(await connectedClients()).toBeLessThan(clientsDuring);
});

test("reader.close cleans every used tenant without deleting pending consumers", async () => {
  const id = "close-tenants";
  const group = "workers";
  const consumerId = "tenant-reader";
  const t = topic<{ value: number }>({ id, prefix: "test:t" });
  const reader = t.reader(group, { consumerId });

  await t.pub({ tenantId: "committed", data: { value: 1 } });
  await t.pub({ tenantId: "pending", data: { value: 2 } });
  const committed = await reader.recv({ tenantId: "committed", wait: false });
  const pending = await reader.recv({ tenantId: "pending", wait: false });
  expect(await committed?.commit()).toBe(true);
  expect(pending).not.toBeNull();

  await reader.close();

  const committedConsumers = await redis.send("XINFO", [
    "CONSUMERS",
    `test:t:committed:${id}:stream`,
    group,
  ]);
  const pendingConsumers = await redis.send("XINFO", [
    "CONSUMERS",
    `test:t:pending:${id}:stream`,
    group,
  ]);
  expect(committedConsumers).toEqual([]);
  expect(pendingConsumers).toEqual([
    expect.objectContaining({ name: consumerId, pending: 1 }),
  ]);
});

test("close is terminal for a reader", async () => {
  const id = "closed-reader";
  const key = `test:t:default:${id}:stream`;
  const t = topic({ id, prefix: "test:t" });
  await t.pub({ data: { value: 1 } });
  const reader = t.reader("workers");
  await reader.close();
  await reader.close();

  await expect(reader.recv({ wait: false })).rejects.toThrow("topic reader is closed");
  await expect(reader.reclaim({ minIdleMs: 0 })).rejects.toThrow("topic reader is closed");
  expect(await redis.send("XINFO", ["GROUPS", key])).toEqual([]);

  const seen: unknown[] = [];
  for await (const message of reader.stream({ wait: false })) {
    seen.push(message);
  }
  expect(seen).toEqual([]);
});

test("two concurrent streams from one reader do not close each other's connection", async () => {
  const id = `shared-stream-${Date.now()}`;
  const t = topic<{ v: number }>({ id, prefix: "test:t" });
  const reader = t.reader("shared");

  const firstAc = new AbortController();
  const collected: number[] = [];

  const consume = async (signal?: AbortSignal): Promise<void> => {
    for await (const message of reader.stream({ wait: true, timeoutMs: 200, signal })) {
      collected.push(message.data.v);
      await message.commit();
    }
  };

  const first = consume(firstAc.signal);
  const secondAc = new AbortController();
  const second = consume(secondAc.signal);

  await Bun.sleep(100);
  firstAc.abort();
  await first;

  // The surviving loop must keep working on a socket the other one closed.
  await t.pub({ data: { v: 42 } });
  await Bun.sleep(400);
  secondAc.abort();
  await second;
  await reader.close();

  expect(collected).toContain(42);
});
