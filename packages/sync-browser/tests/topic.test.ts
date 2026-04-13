import { test, expect } from "bun:test";
import { z } from "zod";
import { topic } from "../src/topic";
import { createMemoryStore } from "../src/store";

// ==========================
// pub + recv basics
// ==========================

test("pub and recv basic flow", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "basic",
    prefix: "test:bt",
    schema: z.object({ type: z.string(), orderId: z.string() }),
    store,
  });

  const result = await t.pub({ data: { type: "order.confirmed", orderId: "o1" } });
  expect(result.eventId).toBeDefined();
  expect(result.cursor).toBeDefined();

  const reader = t.reader("mailer");
  const message = await reader.recv({ wait: false });

  expect(message).not.toBeNull();
  expect(message!.data.type).toBe("order.confirmed");
  expect(message!.data.orderId).toBe("o1");
  expect(message!.eventId).toBe(result.eventId);
  expect(message!.cursor).toBe(result.cursor);
  expect(message!.publishedAt).toBeGreaterThan(0);
});

// ==========================
// Schema validation
// ==========================

test("pub validates schema and rejects invalid data", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "validate",
    prefix: "test:bt",
    schema: z.object({ count: z.number().min(0) }),
    store,
  });

  let thrown: unknown = null;
  try {
    // @ts-expect-error intentional invalid type
    await t.pub({ data: { count: "not-a-number" } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).not.toBeNull();
});

// ==========================
// recv with wait: false
// ==========================

test("reader recv with wait: false returns null when empty", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "empty-recv",
    prefix: "test:bt",
    schema: z.object({ id: z.string() }),
    store,
  });

  const message = await t.reader("none").recv({ wait: false });
  expect(message).toBeNull();
});

// ==========================
// reader.stream
// ==========================

test("reader stream yields events", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "stream",
    prefix: "test:bt",
    schema: z.object({ idx: z.number() }),
    store,
  });

  await t.pub({ data: { idx: 1 } });
  await t.pub({ data: { idx: 2 } });
  await t.pub({ data: { idx: 3 } });

  const reader = t.reader("analytics");
  const seen: number[] = [];

  for await (const event of reader.stream({ wait: false })) {
    seen.push(event.data.idx);
    await event.commit();
  }

  expect(seen).toEqual([1, 2, 3]);
});

test("reader stream stops on AbortSignal", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "stream-abort",
    prefix: "test:bt",
    schema: z.object({ v: z.number() }),
    store,
  });

  await t.pub({ data: { v: 10 } });
  await t.pub({ data: { v: 20 } });
  await t.pub({ data: { v: 30 } });

  const ac = new AbortController();
  const reader = t.reader("abort-group");
  const seen: number[] = [];

  for await (const event of reader.stream({ signal: ac.signal, wait: false })) {
    seen.push(event.data.v);
    await event.commit();
    if (seen.length >= 2) ac.abort();
  }

  // Should have stopped after 2 events due to abort
  expect(seen).toEqual([10, 20]);
});

// ==========================
// live
// ==========================

test("live yields events after subscription", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "live",
    prefix: "test:bt",
    schema: z.object({ n: z.number() }),
    store,
  });

  const ac = new AbortController();
  const iterator = t.live({ signal: ac.signal, timeoutMs: 2_000 })[Symbol.asyncIterator]();
  const nextEvent = iterator.next();

  // Small delay so the live iterator is listening
  await Bun.sleep(20);
  await t.pub({ data: { n: 42 } });

  const received = await nextEvent;
  expect(received.done).toBe(false);
  expect(received.value?.data.n).toBe(42);
  expect(received.value?.eventId).toBeDefined();
  expect(received.value?.cursor).toBeDefined();
  expect(received.value?.publishedAt).toBeGreaterThan(0);

  ac.abort();
  if (iterator.return) {
    await iterator.return();
  }
});

test("live with after cursor replays events", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "live-replay",
    prefix: "test:bt",
    schema: z.object({ value: z.number() }),
    store,
  });

  // Publish events before starting live
  await t.pub({ data: { value: 1 } });
  await t.pub({ data: { value: 2 } });
  await t.pub({ data: { value: 3 } });

  const ac = new AbortController();
  const received: number[] = [];

  // Use after: "0" to replay from the beginning
  for await (const event of t.live({ after: "0", signal: ac.signal, timeoutMs: 1_000 })) {
    received.push(event.data.value);
    if (received.length >= 3) ac.abort();
  }

  expect(received).toEqual([1, 2, 3]);
});

// ==========================
// Consumer group isolation
// ==========================

test("consumer group isolation - different groups get same events", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "groups",
    prefix: "test:bt",
    schema: z.object({ value: z.number() }),
    store,
  });

  await t.pub({ data: { value: 7 } });

  const a = await t.reader("group-a").recv({ wait: false });
  const b = await t.reader("group-b").recv({ wait: false });

  expect(a).not.toBeNull();
  expect(b).not.toBeNull();
  expect(a!.data.value).toBe(7);
  expect(b!.data.value).toBe(7);
  expect(a!.deliveryId).not.toBe(b!.deliveryId); // different delivery IDs
});

// ==========================
// Idempotency
// ==========================

test("idempotency key prevents duplicate publishing", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "idempotency",
    prefix: "test:bt",
    schema: z.object({ id: z.string() }),
    store,
  });

  const a = await t.pub({ data: { id: "a" }, idempotencyKey: "k1" });
  const b = await t.pub({ data: { id: "a" }, idempotencyKey: "k1" });

  // Same idempotency key should return same eventId
  expect(a.eventId).toBe(b.eventId);

  // Only one event should exist
  const reader = t.reader("check");
  const events: string[] = [];
  for await (const event of reader.stream({ wait: false })) {
    events.push(event.eventId);
    await event.commit();
  }

  expect(events.length).toBe(1);
});

// ==========================
// Payload size limit
// ==========================

test("payload size limit is enforced", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "size-limit",
    prefix: "test:bt",
    schema: z.object({ data: z.string() }),
    limits: { payloadBytes: 64 },
    store,
  });

  let thrown: unknown = null;
  try {
    await t.pub({ data: { data: "x".repeat(200) } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toContain("payload exceeds limit");

  // Small payload should succeed
  const result = await t.pub({ data: { data: "ok" } });
  expect(result.eventId).toBeDefined();
});

// ==========================
// Ordering key
// ==========================

test("ordering key is preserved", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "ordering",
    prefix: "test:bt",
    schema: z.object({ v: z.number() }),
    store,
  });

  await t.pub({ data: { v: 1 }, orderingKey: "partition-a" });

  const reader = t.reader("ordering-grp");
  const msg = await reader.recv({ wait: false });

  expect(msg).not.toBeNull();
  expect(msg!.orderingKey).toBe("partition-a");
});

// ==========================
// Meta
// ==========================

test("meta is preserved", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "meta",
    prefix: "test:bt",
    schema: z.object({ v: z.number() }),
    store,
  });

  await t.pub({
    data: { v: 1 },
    meta: { source: "unit-test", version: 3, nested: { ok: true } },
  });

  const reader = t.reader("meta-grp");
  const msg = await reader.recv({ wait: false });

  expect(msg).not.toBeNull();
  expect(msg!.meta?.source).toBe("unit-test");
  expect(msg!.meta?.version).toBe(3);
  expect((msg!.meta?.nested as { ok: boolean }).ok).toBe(true);
});

// ==========================
// Commit
// ==========================

test("commit returns true", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "commit",
    prefix: "test:bt",
    schema: z.object({ n: z.number() }),
    store,
  });

  await t.pub({ data: { n: 1 } });

  const reader = t.reader("commit-grp");
  const msg = await reader.recv({ wait: false });

  expect(msg).not.toBeNull();
  const result = await msg!.commit();
  expect(result).toBe(true);
});

// ==========================
// Tenant isolation
// ==========================

test("different tenants are isolated", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "tenant-iso",
    prefix: "test:bt",
    schema: z.object({ value: z.string() }),
    store,
  });

  await t.pub({ tenantId: "t1", data: { value: "alpha" } });
  await t.pub({ tenantId: "t2", data: { value: "beta" } });

  const r1 = t.reader("grp");
  const r2 = t.reader("grp");

  const m1 = await r1.recv({ tenantId: "t1", wait: false });
  const m2 = await r2.recv({ tenantId: "t2", wait: false });

  expect(m1).not.toBeNull();
  expect(m2).not.toBeNull();
  expect(m1!.data.value).toBe("alpha");
  expect(m2!.data.value).toBe("beta");

  // Cross-tenant: t1 reader should not see t2 data
  const cross = await r1.recv({ tenantId: "t1", wait: false });
  expect(cross).toBeNull();
});

// ==========================
// recv with wait + timeout
// ==========================

test("recv with wait true and timeoutMs returns null after timeout", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "timeout-test",
    schema: z.object({ msg: z.string() }),
    store,
  });
  const r = t.reader("timeout-group");
  const start = Date.now();
  const result = await r.recv({ wait: true, timeoutMs: 200 });
  const elapsed = Date.now() - start;
  expect(result).toBeNull();
  expect(elapsed).toBeGreaterThanOrEqual(150);
  expect(elapsed).toBeLessThan(2000);
});

test("commit returns true on first call", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "commit-test",
    schema: z.object({ v: z.number() }),
    store,
  });
  await t.pub({ data: { v: 1 } });
  const r = t.reader("g1");
  const msg = await r.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(await msg!.commit()).toBe(true);
});
