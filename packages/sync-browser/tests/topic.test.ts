import { test, expect } from "bun:test";
import { TopicPayloadError, topic } from "../src/topic";
import { createMemoryStore, type Store } from "../src/store";
import { sharedState } from "../src/internal/shared-state";
import type { EventLog } from "../src/internal/event-log";

const jsonStore = (backing: Map<string, string>): Store => ({
  get(key) {
    const raw = backing.get(key);
    if (!raw) return undefined;
    const entry = JSON.parse(raw) as { value: unknown; expiresAt: number | null };
    if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
      backing.delete(key);
      return undefined;
    }
    return entry.value;
  },
  set(key, value, ttlMs) {
    backing.set(
      key,
      JSON.stringify({
        value,
        expiresAt: ttlMs === undefined ? null : Date.now() + ttlMs,
      }),
    );
  },
  del(key) {
    backing.delete(key);
  },
  keys(prefix) {
    return [...backing.keys()].filter((key) => prefix === undefined || key.startsWith(prefix));
  },
});

const topicKeyKind = (key: string, kind: string): boolean =>
  key.startsWith("sync:topic:browser:v2:") && decodeURIComponent(key).includes(`"${kind}"`);

// ==========================
// pub + recv basics
// ==========================

test("pub and recv basic flow", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "basic",
    prefix: "test:bt",
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
// recv with wait: false
// ==========================

test("reader recv with wait: false returns null when empty", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "empty-recv",
    prefix: "test:bt",
    store,
  });

  const message = await t.reader("none").recv({ wait: false });
  expect(message).toBeNull();
});

test("reader recv returns promptly for an already-aborted signal", async () => {
  const reader = topic({ id: `pre-aborted-${Date.now()}` }).reader("none");
  const ac = new AbortController();
  ac.abort();
  const startedAt = Date.now();

  expect(await reader.recv({ wait: true, timeoutMs: 10_000, signal: ac.signal })).toBeNull();
  expect(Date.now() - startedAt).toBeLessThan(100);
});

test("reader reclaim is empty when the group has no pending deliveries", async () => {
  const store = createMemoryStore();
  const reader = topic({ id: "reclaim", prefix: "test:bt", store }).reader("workers");
  if (!reader.reclaim) throw new Error("Expected topic reclaim support");

  await expect(reader.reclaim({ minIdleMs: 0, count: 25 })).resolves.toEqual({
    nextCursor: "0-0",
    entries: [],
  });
  await expect(reader.reclaim({ minIdleMs: -1 })).rejects.toThrow("minIdleMs must be a non-negative safe integer");
  await expect(reader.reclaim({ count: 0 })).rejects.toThrow("count must be an integer between 1 and 1000");
});

test("reclaim advances from the returned cursor", async () => {
  const store = createMemoryStore();
  const t = topic<{ value: number }>({ id: "reclaim-cursor", prefix: "test:bt", store });
  const original = t.reader("workers", { consumerId: "original" });

  for (const value of [1, 2, 3]) {
    await t.pub({ data: { value } });
    expect(await original.recv({ wait: false })).not.toBeNull();
  }

  const recovery = t.reader("workers", { consumerId: "recovery" });
  const first = await recovery.reclaim({ minIdleMs: 0, cursor: "0-0", count: 1 });
  expect(first.entries).toHaveLength(1);
  expect(first.entries[0]?.kind === "delivery" && first.entries[0].delivery.data.value).toBe(1);

  const second = await recovery.reclaim({ minIdleMs: 0, cursor: first.nextCursor, count: 1 });
  expect(second.entries).toHaveLength(1);
  expect(second.entries[0]?.kind === "delivery" && second.entries[0].delivery.data.value).toBe(2);

  const third = await recovery.reclaim({ minIdleMs: 0, cursor: second.nextCursor, count: 1 });
  expect(third.entries).toHaveLength(1);
  expect(third.entries[0]?.kind === "delivery" && third.entries[0].delivery.data.value).toBe(3);
  expect(third.nextCursor).toBe("0-0");
});

test("close is terminal for a reader", async () => {
  const reader = topic({ id: "closed-reader", prefix: "test:bt", store: createMemoryStore() }).reader("workers");
  await reader.close();
  await reader.close();

  await expect(reader.recv({ wait: false })).rejects.toThrow("topic reader is closed");
  await expect(reader.reclaim({ minIdleMs: 0 })).rejects.toThrow("topic reader is closed");

  const seen: unknown[] = [];
  for await (const message of reader.stream({ wait: false })) {
    seen.push(message);
  }
  expect(seen).toEqual([]);
});

test("topic preserves undefined data", async () => {
  const store = createMemoryStore();
  const t = topic<undefined>({ id: "undefined-data", prefix: "test:bt", store });
  await t.pub({ data: undefined });

  const delivery = await t.reader("workers").recv({ wait: false, invalidPayload: "throw" });
  expect(delivery?.data).toBeUndefined();
});

// ==========================
// reader.stream
// ==========================

test("reader stream yields events", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "stream",
    prefix: "test:bt",
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

test("invalid idempotency TTL is rejected before publishing", async () => {
  const t = topic<{ v: number }>({
    id: `invalid-idem-ttl-${Date.now()}`,
    store: createMemoryStore(),
  });

  await expect(
    t.pub({ data: { v: 1 }, idempotencyKey: "same", idempotencyTtlMs: Number.NaN }),
  ).rejects.toThrow(/idempotencyTtlMs must be a positive integer/);
  expect(await t.latestCursor()).toBeNull();
});

test("store persists events, idempotency, and group recovery across reloads", async () => {
  const backing = new Map<string, string>();
  const id = `persisted-${Date.now()}`;
  const firstTopic = topic<{ value: number }>({
    id,
    prefix: "test:bt",
    store: jsonStore(backing),
  });
  const first = await firstTopic.pub({
    data: { value: 1 },
    idempotencyKey: "first",
  });
  const second = await firstTopic.pub({ data: { value: 2 } });
  const original = firstTopic.reader("workers", { consumerId: "before-reload" });
  expect(await (await original.recv({ wait: false }))?.commit()).toBe(true);
  expect((await original.recv({ wait: false }))?.eventId).toBe(second.eventId);

  // A distinct Store object gives sharedState a fresh scope, matching a reload.
  const reloaded = topic<{ value: number }>({
    id,
    prefix: "test:bt",
    store: jsonStore(backing),
  });
  expect(await reloaded.latestCursor()).toBe(second.cursor);
  expect(await reloaded.pub({ data: { value: 999 }, idempotencyKey: "first" })).toEqual(first);

  const recovered = await reloaded
    .reader("workers", { consumerId: "after-reload" })
    .reclaim({ minIdleMs: 0 });
  expect(recovered.entries).toHaveLength(1);
  expect(
    recovered.entries[0]?.kind === "delivery" && recovered.entries[0].delivery.data,
  ).toEqual({ value: 2 });
});

test("same handle and reload replace an idempotency key whose retained event expired", async () => {
  const backing = new Map<string, string>();
  const id = `stale-idempotency-${Date.now()}`;
  const firstTopic = topic<{ value: number }>({
    id,
    prefix: "test:bt",
    retentionMs: 200,
    store: jsonStore(backing),
  });
  const first = await firstTopic.pub({ data: { value: 1 }, idempotencyKey: "same" });
  await Bun.sleep(250);
  const fresh = await firstTopic.pub({ data: { value: 2 }, idempotencyKey: "same" });
  expect(fresh.eventId).not.toBe(first.eventId);

  const reloaded = topic<{ value: number }>({
    id,
    prefix: "test:bt",
    retentionMs: 200,
    store: jsonStore(backing),
  });

  expect(await reloaded.latestCursor()).toBe(fresh.cursor);
  expect(await reloaded.pub({ data: { value: 999 }, idempotencyKey: "same" })).toEqual(fresh);
  expect((await reloaded.reader("check").recv({ wait: false }))?.data).toEqual({ value: 2 });
});

test("failed event persistence is rolled back before readers can observe it", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  let failWrite = true;
  const store: Store = {
    ...base,
    set(key, value, ttlMs) {
      if (topicKeyKind(key, "event-log") && failWrite) {
        failWrite = false;
        throw new Error("storage full");
      }
      base.set(key, value, ttlMs);
    },
  };
  const t = topic<{ value: number }>({
    id: `failed-persistence-${Date.now()}`,
    prefix: "test:bt",
    store,
  });

  await expect(
    t.pub({ data: { value: 1 }, idempotencyKey: "same" }),
  ).rejects.toThrow("storage full");
  expect([...backing.keys()].some((key) => topicKeyKind(key, "idempotency"))).toBe(false);
  expect(await t.latestCursor()).toBeNull();
  expect(await t.reader("check").recv({ wait: false })).toBeNull();

  const published = await t.pub({ data: { value: 1 }, idempotencyKey: "same" });
  expect((await t.reader("retry").recv({ wait: false }))?.eventId).toBe(published.eventId);
  expect(await t.reader("retry").recv({ wait: false })).toBeNull();
});

test("failed idempotency persistence rolls the event snapshot back", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  let failKeyWrite = true;
  const store: Store = {
    ...base,
    set(key, value, ttlMs) {
      if (topicKeyKind(key, "idempotency") && failKeyWrite) {
        failKeyWrite = false;
        throw new Error("key write failed");
      }
      base.set(key, value, ttlMs);
    },
  };
  const t = topic<{ value: number }>({
    id: `failed-idempotency-${Date.now()}`,
    prefix: "test:bt",
    store,
  });

  await expect(
    t.pub({ data: { value: 1 }, idempotencyKey: "same" }),
  ).rejects.toThrow("key write failed");
  expect(await t.latestCursor()).toBeNull();

  const published = await t.pub({ data: { value: 1 }, idempotencyKey: "same" });
  const reader = t.reader("check");
  expect((await reader.recv({ wait: false }))?.eventId).toBe(published.eventId);
  expect(await reader.recv({ wait: false })).toBeNull();
});

test("write-then-throw event persistence is rolled back and remains retryable", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  let eventWrites = 0;
  const store: Store = {
    ...base,
    set(key, value, ttlMs) {
      base.set(key, value, ttlMs);
      if (topicKeyKind(key, "event-log") && ++eventWrites === 1) {
        throw new Error("ambiguous event write");
      }
    },
  };
  const t = topic<{ value: number }>({
    id: `ambiguous-event-${Date.now()}`,
    prefix: "test:bt",
    store,
  });

  await expect(t.pub({ data: { value: 1 }, idempotencyKey: "same" })).rejects.toThrow(
    "ambiguous event write",
  );
  expect(await t.latestCursor()).toBeNull();
  expect([...backing.keys()].some((key) => topicKeyKind(key, "idempotency"))).toBe(false);

  const retried = await t.pub({ data: { value: 1 }, idempotencyKey: "same" });
  expect((await t.reader("check").recv({ wait: false }))?.eventId).toBe(retried.eventId);
  expect(await t.reader("check").recv({ wait: false })).toBeNull();
});

test("uncertain event rollback preserves the idempotency fence", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  let eventWrites = 0;
  const faultyStore: Store = {
    ...base,
    set(key, value, ttlMs) {
      if (topicKeyKind(key, "event-log")) {
        eventWrites += 1;
        if (eventWrites === 1) {
          base.set(key, value, ttlMs);
          throw new Error("ambiguous event write");
        }
        if (eventWrites === 2) throw new Error("rollback write failed");
      }
      base.set(key, value, ttlMs);
    },
  };
  const id = `uncertain-event-${Date.now()}`;
  const failed = topic<{ value: number }>({ id, prefix: "test:bt", store: faultyStore });

  await expect(failed.pub({ data: { value: 1 }, idempotencyKey: "same" })).rejects.toThrow(
    "ambiguous event write",
  );
  expect([...backing.keys()].some((key) => topicKeyKind(key, "idempotency"))).toBe(true);
  expect(await failed.pub({ data: { value: 2 }, idempotencyKey: "same" })).toEqual({
    eventId: "1",
    cursor: "1",
  });
  expect(await failed.latestCursor()).toBe("1");

  const recovered = topic<{ value: number }>({ id, prefix: "test:bt", store: jsonStore(backing) });
  const existing = await recovered.pub({ data: { value: 2 }, idempotencyKey: "same" });
  expect(existing.eventId).toBe("1");
  expect((await recovered.reader("check").recv({ wait: false }))?.data.value).toBe(1);
  expect(await recovered.reader("check").recv({ wait: false })).toBeNull();
});

test("uncertain event recovery keeps active live subscribers attached", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  let eventWrites = 0;
  const store: Store = {
    ...base,
    set(key, value, ttlMs) {
      if (topicKeyKind(key, "event-log")) {
        eventWrites += 1;
        if (eventWrites === 1) {
          base.set(key, value, ttlMs);
          throw new Error("ambiguous event write");
        }
        if (eventWrites === 2) throw new Error("rollback write failed");
      }
      base.set(key, value, ttlMs);
    },
  };
  const t = topic<{ value: number }>({
    id: `live-recovery-${Date.now()}`,
    prefix: "test:bt",
    store,
  });
  const ac = new AbortController();
  const next = t.live({ after: "0", signal: ac.signal })[Symbol.asyncIterator]().next();
  await Bun.sleep(0);

  await expect(t.pub({ data: { value: 1 }, idempotencyKey: "same" })).rejects.toThrow(
    "ambiguous event write",
  );
  await t.pub({ data: { value: 2 }, idempotencyKey: "same" });
  expect(await next).toMatchObject({ done: false, value: { data: { value: 1 }, eventId: "1" } });
  ac.abort();
});

test("a persisted high-water mark prevents cursor reuse when only the fence was written", async () => {
  const backing = new Map<string, string>();
  const base = jsonStore(backing);
  const faultyStore: Store = {
    ...base,
    set(key, value, ttlMs) {
      if (topicKeyKind(key, "event-log")) throw new Error("event snapshot unavailable");
      base.set(key, value, ttlMs);
    },
  };
  const id = `high-water-${Date.now()}`;
  const failed = topic<{ value: number }>({ id, prefix: "test:bt", store: faultyStore });
  await expect(failed.pub({ data: { value: 1 }, idempotencyKey: "first" })).rejects.toThrow(
    "event snapshot unavailable",
  );

  const recovered = topic<{ value: number }>({
    id,
    prefix: "test:bt",
    store: jsonStore(backing),
  });
  const published = await recovered.pub({ data: { value: 2 }, idempotencyKey: "second" });
  expect(published.eventId).toBe("2");
  expect((await recovered.reader("check").recv({ wait: false }))?.data).toEqual({ value: 2 });
});

test("topic validates numeric configuration and operation timings", async () => {
  for (const retentionMs of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => topic({ id: `invalid-retention-${retentionMs}`, retentionMs })).toThrow(
      "retentionMs must be a positive safe integer",
    );
  }
  for (const payloadBytes of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => topic({ id: `invalid-payload-${payloadBytes}`, limits: { payloadBytes } })).toThrow(
      "limits.payloadBytes must be a positive safe integer",
    );
  }

  const t = topic<{ value: number }>({ id: `numeric-operations-${Date.now()}` });
  const reader = t.reader("validation");
  for (const timeoutMs of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
    await expect(reader.recv({ wait: false, timeoutMs })).rejects.toThrow(
      "timeoutMs must be a non-negative safe integer",
    );
    await expect(t.live({ timeoutMs })[Symbol.asyncIterator]().next()).rejects.toThrow(
      "timeoutMs must be a non-negative safe integer",
    );
  }
  for (const minIdleMs of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
    await expect(reader.reclaim({ minIdleMs })).rejects.toThrow(
      "minIdleMs must be a non-negative safe integer",
    );
  }
  expect(await reader.recv({ wait: false, timeoutMs: 0 })).toBeNull();
  expect((await reader.reclaim({ minIdleMs: 0 })).entries).toEqual([]);
  await reader.close();
});

test("topic persistence keys are injective across prefix, id, tenant, group, and idempotency segments", async () => {
  const backing = new Map<string, string>();
  const first = topic<{ source: string }>({
    prefix: "collision",
    id: "topic:tail",
    tenantId: "tenant",
    store: jsonStore(backing),
  });
  const second = topic<{ source: string }>({
    prefix: "collision:tenant",
    id: "tail",
    tenantId: "topic",
    store: jsonStore(backing),
  });

  await first.pub({ data: { source: "first" }, idempotencyKey: "same:key" });
  expect(await second.latestCursor()).toBeNull();
  expect(await second.reader("workers").recv({ wait: false })).toBeNull();
  await second.pub({ data: { source: "second" }, idempotencyKey: "same:key" });

  expect((await first.reader("workers").recv({ wait: false }))?.data.source).toBe("first");
  expect((await second.reader("workers").recv({ wait: false }))?.data.source).toBe("second");
});

test("topic does not import legacy state without identity proof", async () => {
  const backing = new Map<string, string>();
  const store = jsonStore(backing);
  const prefix = `legacy-topic-${Date.now()}`;
  const id = "events";
  const tenantId = "tenant";
  const group = "workers";
  const publishedAt = Date.now();
  const legacyEventKey = `${prefix}:${encodeURIComponent(tenantId)}:${id}:browser:event-log`;
  const legacyGroupKey = `${prefix}:${encodeURIComponent(tenantId)}:${id}:browser:group:${encodeURIComponent(group)}`;
  const legacyIdempotencyKey = `${prefix}:${tenantId}:${id}:idempotency:first`;
  store.set(legacyEventKey, {
    entries: [{
      id: "7",
      ts: publishedAt,
      fields: { payload: JSON.stringify({ data: { value: 1 }, publishedAt }) },
    }],
  });
  store.set(legacyGroupKey, {
    committed: "0",
    delivered: "7",
    inFlight: [["7", { at: publishedAt, consumerId: "old" }]],
  });
  store.set(legacyIdempotencyKey, "7", 60_000);

  const t = topic<{ value: number }>({ prefix, id, tenantId, store });
  expect(await t.latestCursor()).toBeNull();
  expect(await t.pub({ data: { value: 999 }, idempotencyKey: "first" })).toEqual({
    eventId: "1",
    cursor: "1",
  });
  const reclaimed = await t.reader(group, { consumerId: "new" }).reclaim({ minIdleMs: 0 });
  expect(reclaimed.entries).toHaveLength(0);

  expect(backing.has(legacyEventKey)).toBe(true);
  expect(backing.has(legacyGroupKey)).toBe(true);
  expect(backing.has(legacyIdempotencyKey)).toBe(true);
  expect([...backing.keys()].filter((key) => key.startsWith("sync:topic:browser:v2:"))).toHaveLength(3);
});

test("colliding topic identities both ignore an ambiguous legacy event log", async () => {
  const backing = new Map<string, string>();
  const store = jsonStore(backing);
  const publishedAt = Date.now();
  const legacyEventKey = "legacy-collision:tenant:topic:tail:browser:event-log";
  store.set(legacyEventKey, {
    entries: [{
      id: "1",
      ts: publishedAt,
      fields: { payload: JSON.stringify({ data: { source: "legacy" }, publishedAt }) },
    }],
  });

  const owner = topic<{ source: string }>({
    prefix: "legacy-collision",
    id: "topic:tail",
    tenantId: "tenant",
    store,
  });
  const nonOwner = topic<{ source: string }>({
    prefix: "legacy-collision:tenant",
    id: "tail",
    tenantId: "topic",
    store,
  });

  expect(await owner.latestCursor()).toBeNull();
  expect(await nonOwner.latestCursor()).toBeNull();
  expect(backing.has(legacyEventKey)).toBe(true);
  expect([...backing.keys()].filter((key) => key.startsWith("sync:topic:browser:v2:"))).toHaveLength(0);
});

test("failed group persistence never advances delivery, commit, or reclaim state", async () => {
  const backing = createMemoryStore();
  let failGroupWrite = false;
  const store: Store = {
    get: (key) => backing.get(key),
    set: (key, value, ttlMs) => {
      if (topicKeyKind(key, "group") && failGroupWrite) throw new Error("group write failed");
      backing.set(key, value, ttlMs);
    },
    del: (key) => backing.del(key),
    keys: (prefix) => backing.keys(prefix),
  };
  const t = topic<{ value: number }>({ id: `group-faults-${Date.now()}`, store });
  await t.pub({ data: { value: 1 } });
  const owner = t.reader("workers", { consumerId: "owner" });

  failGroupWrite = true;
  await expect(owner.recv({ wait: false })).rejects.toThrow("group write failed");
  failGroupWrite = false;
  const delivery = await owner.recv({ wait: false });
  expect(delivery?.data.value).toBe(1);

  failGroupWrite = true;
  await expect(delivery!.commit()).rejects.toThrow("group write failed");
  failGroupWrite = false;
  expect(await delivery!.commit()).toBe(true);

  await t.pub({ data: { value: 2 } });
  const pending = await owner.recv({ wait: false });
  const recovering = t.reader("workers", { consumerId: "recovering" });
  failGroupWrite = true;
  await expect(recovering.reclaim({ minIdleMs: 0 })).rejects.toThrow("group write failed");
  failGroupWrite = false;
  expect(await pending!.commit()).toBe(true);
});

// ==========================
// Payload size limit
// ==========================

test("payload size limit is enforced", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "size-limit",
    prefix: "test:bt",
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
    store,
  });
  await t.pub({ data: { v: 1 } });
  const r = t.reader("g1");
  const msg = await r.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(await msg!.commit()).toBe(true);
});

test("latestCursor returns null when log is empty", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "latest-empty",
    store,
  });

  expect(await t.latestCursor()).toBeNull();
});

test("latestCursor returns the latest published cursor", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "latest-published",
    store,
  });

  await t.pub({ data: { n: 1 } });
  const second = await t.pub({ data: { n: 2 } });

  expect(await t.latestCursor()).toBe(second.cursor);
});

test("latestCursor uses the same tenant isolation as pub and live", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "latest-tenant",
    store,
  });

  const t1 = await t.pub({ tenantId: "t1", data: { value: "alpha" } });
  const t2 = await t.pub({ tenantId: "t2", data: { value: "beta" } });

  expect(await t.latestCursor({ tenantId: "t1" })).toBe(t1.cursor);
  expect(await t.latestCursor({ tenantId: "t2" })).toBe(t2.cursor);
  expect(await t.latestCursor({ tenantId: "missing" })).toBeNull();
});

test("latestCursor does not consume reader messages", async () => {
  const store = createMemoryStore();
  const t = topic({
    id: "latest-non-consuming",
    store,
  });

  const published = await t.pub({ data: { value: 42 } });
  expect(await t.latestCursor()).toBe(published.cursor);

  const message = await t.reader("latest-check").recv({ wait: false });
  expect(message?.cursor).toBe(published.cursor);
  expect(message?.data.value).toBe(42);
  expect(await message?.commit()).toBe(true);
});

// ==========================
// Delivery semantics (at-least-once)
// ==========================

test("an uncommitted delivery is redelivered, not skipped", async () => {
  const t = topic<{ v: number }>({ id: `uncommitted-${Date.now()}` });
  await t.pub({ data: { v: 1 } });
  await t.pub({ data: { v: 2 } });

  const reader = t.reader("workers");
  const first = await reader.recv({ wait: false });
  expect(first?.data).toEqual({ v: 1 });

  // Handler failed: nothing was committed. The cursor used to advance at
  // delivery, so the event was gone and the next recv returned the *next* one.
  const retry = await reader.reclaim({ minIdleMs: 0 });
  expect(retry.entries.length).toBe(1);
  const redelivered = retry.entries[0];
  expect(redelivered?.kind).toBe("delivery");
  if (redelivered?.kind === "delivery") {
    expect(redelivered.delivery.eventId).toBe(first!.eventId);
    expect(await redelivered.delivery.commit()).toBe(true);
  }

  const second = await reader.recv({ wait: false });
  expect(second?.data).toEqual({ v: 2 });
  expect(await second?.commit()).toBe(true);
});

test("two readers in one group distribute rather than broadcast", async () => {
  const t = topic<{ v: number }>({ id: `group-distribute-${Date.now()}` });
  await t.pub({ data: { v: 1 } });

  const a = t.reader("g");
  const b = t.reader("g");

  const received = [await a.recv({ wait: false }), await b.recv({ wait: false })];
  const nonNull = received.filter((m) => m !== null);

  // `group` used to be a display string only, so every worker saw every event
  // and every side effect ran twice.
  expect(nonNull.length).toBe(1);
  expect(nonNull[0]?.data).toEqual({ v: 1 });
});

test("a recreated reader resumes at the group's committed position", async () => {
  const id = `group-resume-${Date.now()}`;
  const t = topic<{ v: number }>({ id });
  for (const v of [1, 2, 3]) await t.pub({ data: { v } });

  const first = t.reader("g");
  const one = await first.recv({ wait: false });
  await one?.commit();
  await first.close();

  // A route remount or a StrictMode double-mount used to replay the whole log.
  const second = t.reader("g");
  const next = await second.recv({ wait: false });
  expect(next?.data).toEqual({ v: 2 });
});

test("browser topic logs retain at most 256 events", async () => {
  const id = `bounded-log-${Date.now()}`;
  const t = topic<{ v: number }>({ id, retentionMs: 60_000 });
  for (let v = 0; v < 300; v += 1) await t.pub({ data: { v } });

  const logs = sharedState(
    JSON.stringify(["topic:logs", "sync:topic", id]),
    undefined,
    () => new Map<string, EventLog>(),
  );
  const snapshot = logs.get(JSON.stringify(["sync:topic", id, "default"]))!.snapshot();
  expect(snapshot).toHaveLength(256);
  expect(snapshot[0]?.id).toBe("45");
  expect(snapshot.at(-1)?.id).toBe("300");
});

test("invalidPayload: 'throw' raises TopicPayloadError on a malformed entry", async () => {
  const id = `invalid-throw-${Date.now()}`;
  const t = topic<{ v: number }>({ id });
  await t.pub({ data: { v: 1 } });

  // Inject a malformed envelope the way a foreign or older writer would.
  const logs = sharedState(
    JSON.stringify(["topic:logs", "sync:topic", id]),
    undefined,
    () => new Map<string, EventLog>(),
  );
  logs.get(JSON.stringify(["sync:topic", id, "default"]))!.append({ payload: "not-json" });

  const reader = t.reader("g");
  expect(await reader.recv({ wait: false }))?.data;

  // The option was accepted and never read, so `catch (e) { if (e instanceof
  // TopicPayloadError) ... }`, written against the documented contract, was
  // dead code in the browser.
  await expect(reader.recv({ wait: false, invalidPayload: "throw" })).rejects.toThrow(TopicPayloadError);

  // Default behaviour is still to skip it and keep draining.
  const skipping = t.reader("g2");
  expect((await skipping.recv({ wait: false }))?.data).toEqual({ v: 1 });
  expect(await skipping.recv({ wait: false })).toBeNull();
});

test("a stale consumer cannot commit a delivery another reader reclaimed", async () => {
  const t = topic<{ v: number }>({ id: `fenced-commit-${Date.now()}` });
  await t.pub({ data: { v: 1 } });

  const a = t.reader("g");
  const b = t.reader("g");

  const delivered = await a.recv({ wait: false });
  expect(delivered).not.toBeNull();

  const reclaimed = await b.reclaim({ minIdleMs: 0 });
  expect(reclaimed.entries.length).toBe(1);

  // Mirrors the server's fenced XACK.
  expect(await delivered!.commit()).toBe(false);
});
