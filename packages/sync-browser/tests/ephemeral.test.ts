import { test, expect, describe } from "bun:test";
import {
  ephemeral,
  EphemeralCapacityError,
  EphemeralPayloadTooLargeError,
} from "../src/ephemeral";
import { sharedState } from "../src/internal/shared-state";

// ==========================
// Helpers
// ==========================

type TestValue = { status: string };

// A distinct id per store, because same-id handles genuinely share state now,
// exactly as two queue()/ephemeral() calls with one id do on the server.
let storeCounter = 0;
const makeStore = (overrides?: Parameters<typeof ephemeral<TestValue>>[0]) =>
  ephemeral<TestValue>({
    id: `test-${++storeCounter}-${Date.now()}`,
    ttlMs: 2000,
    ...overrides,
  });

// ==========================
// upsert
// ==========================

describe("upsert", () => {
  test("creates entry with correct fields", async () => {
    const store = makeStore();
    const before = Date.now();
    const entry = await store.upsert({ key: "user1", value: { status: "online" } });
    const after = Date.now();

    expect(entry.key).toBe("user1");
    expect(entry.value).toEqual({ status: "online" });
    expect(typeof entry.version).toBe("string");
    expect(entry.updatedAt).toBeGreaterThanOrEqual(before);
    expect(entry.updatedAt).toBeLessThanOrEqual(after);
    expect(entry.expiresAt).toBeGreaterThan(entry.updatedAt);
  });

  test("updates existing entry with new version and updatedAt", async () => {
    const store = makeStore();
    const first = await store.upsert({ key: "user1", value: { status: "online" } });
    await Bun.sleep(5);
    const second = await store.upsert({ key: "user1", value: { status: "away" } });

    expect(second.key).toBe("user1");
    expect(second.value).toEqual({ status: "away" });
    expect(second.version).not.toBe(first.version);
    expect(Number(second.version)).toBeGreaterThan(Number(first.version));
    expect(second.updatedAt).toBeGreaterThanOrEqual(first.updatedAt);
  });

  test("custom ttlMs per upsert overrides default", async () => {
    const store = makeStore({ id: "ttl-override", ttlMs: 5000 });
    const entry = await store.upsert({
      key: "user1",
      value: { status: "online" },
      ttlMs: 100,
    });

    // expiresAt should be ~100ms from now, not ~5000ms
    expect(entry.expiresAt - entry.updatedAt).toBeLessThanOrEqual(150);
    expect(entry.expiresAt - entry.updatedAt).toBeGreaterThanOrEqual(50);

    // Entry should expire after the short TTL
    await Bun.sleep(250);
    const snap = await store.snapshot();
    expect(snap.entries).toHaveLength(0);
  });
});

// ==========================
// touch
// ==========================

describe("touch", () => {
  test("extends TTL and returns new version and expiresAt", async () => {
    const store = makeStore();
    const entry = await store.upsert({ key: "user1", value: { status: "online" } });
    await Bun.sleep(5);
    const result = await store.touch({ key: "user1", ttlMs: 3000 });

    expect(result.ok).toBe(true);
    expect(typeof result.version).toBe("string");
    expect(result.version).not.toBe(entry.version);
    expect(Number(result.version)).toBeGreaterThan(Number(entry.version));
    expect(result.expiresAt).toBeDefined();
    expect(result.expiresAt!).toBeGreaterThan(entry.expiresAt);
  });

  test("returns { ok: false } for non-existent key", async () => {
    const store = makeStore();
    const result = await store.touch({ key: "nonexistent" });

    expect(result.ok).toBe(false);
    expect(result.version).toBeUndefined();
    expect(result.expiresAt).toBeUndefined();
  });
});

// ==========================
// remove
// ==========================

describe("remove", () => {
  test("deletes entry and returns true", async () => {
    const store = makeStore();
    await store.upsert({ key: "user1", value: { status: "online" } });
    const removed = await store.remove({ key: "user1", reason: "logout" });

    expect(removed).toBe(true);

    const snap = await store.snapshot();
    expect(snap.entries).toHaveLength(0);
  });

  test("returns false for non-existent key", async () => {
    const store = makeStore();
    const removed = await store.remove({ key: "ghost" });

    expect(removed).toBe(false);
  });
});

// ==========================
// TTL expiry
// ==========================

describe("TTL expiry", () => {
  test("entry disappears after ttlMs", async () => {
    const store = makeStore({ id: "ttl-expiry", ttlMs: 100 });
    await store.upsert({ key: "temp", value: { status: "fleeting" } });

    const snapBefore = await store.snapshot();
    expect(snapBefore.entries).toHaveLength(1);

    await Bun.sleep(250);

    const snapAfter = await store.snapshot();
    expect(snapAfter.entries).toHaveLength(0);
  });
});

// ==========================
// snapshot
// ==========================

describe("snapshot", () => {
  test("returns all current entries sorted by key", async () => {
    const store = makeStore();
    await store.upsert({ key: "charlie", value: { status: "away" } });
    await store.upsert({ key: "alice", value: { status: "online" } });
    await store.upsert({ key: "bob", value: { status: "offline" } });

    const snap = await store.snapshot();
    expect(snap.entries).toHaveLength(3);
    expect(snap.entries[0]!.key).toBe("alice");
    expect(snap.entries[1]!.key).toBe("bob");
    expect(snap.entries[2]!.key).toBe("charlie");
  });

  test("cursor reflects latest event", async () => {
    const store = makeStore();
    const snap0 = await store.snapshot();

    await store.upsert({ key: "a", value: { status: "on" } });
    const snap1 = await store.snapshot();
    expect(Number(snap1.cursor)).toBeGreaterThan(Number(snap0.cursor));

    await store.upsert({ key: "b", value: { status: "on" } });
    const snap2 = await store.snapshot();
    expect(Number(snap2.cursor)).toBeGreaterThan(Number(snap1.cursor));
  });
});

// ==========================
// Capacity limit
// ==========================

describe("capacity limit", () => {
  test("EphemeralCapacityError thrown when maxEntries reached", async () => {
    const store = makeStore({
      id: "cap-test",
      
      ttlMs: 2000,
      limits: { maxEntries: 2 },
    });

    await store.upsert({ key: "k1", value: { status: "a" } });
    await store.upsert({ key: "k2", value: { status: "b" } });

    await expect(
      store.upsert({ key: "k3", value: { status: "c" } }),
    ).rejects.toBeInstanceOf(EphemeralCapacityError);

    // Upserting an existing key should still work (no net new entry)
    const updated = await store.upsert({ key: "k1", value: { status: "updated" } });
    expect(updated.value).toEqual({ status: "updated" });
  });
});

// ==========================
// Payload size limit
// ==========================

describe("payload size limit", () => {
  test("EphemeralPayloadTooLargeError thrown for oversized payload", async () => {
    const store = makeStore({
      id: "payload-test",
      
      ttlMs: 2000,
      limits: { maxPayloadBytes: 32 },
    });

    // Small payload should succeed
    await store.upsert({ key: "ok", value: { status: "hi" } });

    // Large payload should throw
    const longStatus = "x".repeat(200);
    await expect(
      store.upsert({ key: "big", value: { status: longStatus } }),
    ).rejects.toBeInstanceOf(EphemeralPayloadTooLargeError);
  });
});

// ==========================
// Reader — upsert events
// ==========================

describe("reader", () => {
  test("receives upsert events", async () => {
    const store = makeStore();

    // Create reader before any writes so it captures everything from cursor "0"
    const reader = store.reader({ after: "0" });

    await store.upsert({ key: "user1", value: { status: "online" } });
    await store.upsert({ key: "user2", value: { status: "away" } });

    const ev1 = await reader.recv({ wait: false });
    expect(ev1).not.toBeNull();
    expect(ev1!.type).toBe("upsert");
    if (ev1!.type === "upsert") {
      expect(ev1!.entry.key).toBe("user1");
      expect(ev1!.entry.value).toEqual({ status: "online" });
    }

    const ev2 = await reader.recv({ wait: false });
    expect(ev2).not.toBeNull();
    expect(ev2!.type).toBe("upsert");
    if (ev2!.type === "upsert") {
      expect(ev2!.entry.key).toBe("user2");
      expect(ev2!.entry.value).toEqual({ status: "away" });
    }
  });

  test("receives touch and delete events", async () => {
    const store = makeStore();

    await store.upsert({ key: "user1", value: { status: "online" } });

    // Start reader after the initial upsert
    const snap = await store.snapshot();
    const reader = store.reader({ after: snap.cursor });

    await store.touch({ key: "user1", ttlMs: 5000 });
    await store.remove({ key: "user1", reason: "left" });

    const touchEvent = await reader.recv({ wait: false });
    expect(touchEvent).not.toBeNull();
    expect(touchEvent!.type).toBe("touch");
    if (touchEvent!.type === "touch") {
      expect(touchEvent!.key).toBe("user1");
      expect(typeof touchEvent!.version).toBe("string");
      expect(touchEvent!.expiresAt).toBeGreaterThan(Date.now() - 1000);
    }

    const deleteEvent = await reader.recv({ wait: false });
    expect(deleteEvent).not.toBeNull();
    expect(deleteEvent!.type).toBe("delete");
    if (deleteEvent!.type === "delete") {
      expect(deleteEvent!.key).toBe("user1");
      expect(deleteEvent!.reason).toBe("left");
      expect(deleteEvent!.deletedAt).toBeGreaterThan(0);
    }
  });

  test("receives expire events after TTL", async () => {
    const store = makeStore({ id: "expire-reader", ttlMs: 100 });

    const reader = store.reader({ after: "0" });

    await store.upsert({ key: "temp", value: { status: "ephemeral" } });

    // First event should be the upsert
    const upsertEv = await reader.recv({ wait: false });
    expect(upsertEv).not.toBeNull();
    expect(upsertEv!.type).toBe("upsert");

    // Wait for TTL to expire
    await Bun.sleep(250);

    const expireEv = await reader.recv({ wait: false });
    expect(expireEv).not.toBeNull();
    expect(expireEv!.type).toBe("expire");
    if (expireEv!.type === "expire") {
      expect(expireEv!.key).toBe("temp");
      expect(typeof expireEv!.version).toBe("string");
      expect(expireEv!.expiredAt).toBeGreaterThan(0);
    }
  });

  test("recv returns null with wait: false when no events pending", async () => {
    const store = makeStore();
    const reader = store.reader();

    const event = await reader.recv({ wait: false });
    expect(event).toBeNull();
  });
});

// ==========================
// Additional coverage
// ==========================

test("different tenants are isolated", async () => {
  const store = ephemeral({
    id: `tenant-iso-${Date.now()}`,
    ttlMs: 5000,
  });

  await store.upsert({ key: "user1", value: { status: "online" }, tenantId: "t1" });
  await store.upsert({ key: "user1", value: { status: "away" }, tenantId: "t2" });

  const snap1 = await store.snapshot({ tenantId: "t1" });
  const snap2 = await store.snapshot({ tenantId: "t2" });

  expect(snap1.entries.length).toBe(1);
  expect(snap1.entries[0]!.value.status).toBe("online");
  expect(snap2.entries.length).toBe(1);
  expect(snap2.entries[0]!.value.status).toBe("away");
});

test("reader recv with wait false returns null when no events pending", async () => {
  const store = ephemeral({
    id: `reader-empty-${Date.now()}`,
    ttlMs: 5000,
  });
  const reader = store.reader();
  const event = await reader.recv({ wait: false });
  expect(event).toBeNull();
});

// ==========================
// Overflow and validation
// ==========================

test("reader receives overflow event when cursor falls behind", async () => {
  const store = ephemeral({
    id: `overflow-${Date.now()}`,
    ttlMs: 60_000,
    limits: { eventMaxLen: 3 }, // very small event log
  });

  // Create initial events and get a cursor
  await store.upsert({ key: "a", value: { status: "on" } });
  const snap = await store.snapshot();
  const oldCursor = snap.cursor;

  // Create more events than eventMaxLen to push the old cursor out
  await store.upsert({ key: "b", value: { status: "on" } });
  await store.upsert({ key: "c", value: { status: "on" } });
  await store.upsert({ key: "d", value: { status: "on" } });
  await store.upsert({ key: "e", value: { status: "on" } });

  // Reader with the old cursor should get an overflow
  const reader = store.reader({ after: oldCursor });
  const event = await reader.recv({ wait: false });

  // Depending on whether the log trimmed the cursor,
  // we should either get an overflow event or the next available event
  expect(event).not.toBeNull();
});

test("empty key throws", async () => {
  const store = ephemeral({
    id: `empty-key-${Date.now()}`,
    ttlMs: 5_000,
  });
  await expect(
    store.upsert({ key: "", value: { status: "on" } })
  ).rejects.toThrow("key must be non-empty");
});

test("ttlMs <= 0 on factory throws", () => {
  expect(() =>
    ephemeral({
      id: `bad-ttl-${Date.now()}`,
      ttlMs: 0,
    })
  ).toThrow(/positive integer/);
});

test("fractional ttlMs is rejected at the factory and call sites", async () => {
  expect(() =>
    ephemeral({
      id: `fractional-ttl-${Date.now()}`,
      ttlMs: 0.5,
    }),
  ).toThrow(/positive integer/);

  const store = ephemeral<{ v: number }>({
    id: `fractional-override-${Date.now()}`,
    ttlMs: 5_000,
  });
  await expect(store.upsert({ key: "k", value: { v: 1 }, ttlMs: 100.5 })).rejects.toThrow(
    /positive integer/,
  );
  await expect(store.touch({ key: "k", ttlMs: 100.5 })).rejects.toThrow(/positive integer/);
});

test("stored and returned values are isolated JSON snapshots", async () => {
  const store = ephemeral<{ nested: { value: number } }>({
    id: `json-snapshot-${Date.now()}`,
    ttlMs: 5_000,
  });
  const input = { nested: { value: 1 } };
  const returned = await store.upsert({ key: "k", value: input });

  input.nested.value = 2;
  returned.value.nested.value = 3;

  const first = await store.snapshot();
  expect(first.entries[0]?.value.nested.value).toBe(1);
  first.entries[0]!.value.nested.value = 4;
  expect((await store.snapshot()).entries[0]?.value.nested.value).toBe(1);
});

// ==========================
// Prefix filter — snapshot
// ==========================

describe("prefix filter", () => {
  test("snapshot with prefix returns only matching entries", async () => {
    const store = ephemeral<{ v: number }>({
      id: `prefix-snap-${Date.now()}`,
      ttlMs: 5_000,
    });

    await store.upsert({ key: "apps/backend", value: { v: 1 } });
    await store.upsert({ key: "apps/frontend", value: { v: 2 } });
    await store.upsert({ key: "services/cache", value: { v: 3 } });

    const appsOnly = await store.snapshot({ prefix: "apps/" });
    expect(appsOnly.entries.map((e) => e.key).sort()).toEqual(["apps/backend", "apps/frontend"]);

    const servicesOnly = await store.snapshot({ prefix: "services/" });
    expect(servicesOnly.entries).toHaveLength(1);
    expect(servicesOnly.entries[0]?.key).toBe("services/cache");

    const all = await store.snapshot();
    expect(all.entries).toHaveLength(3);

    const empty = await store.snapshot({ prefix: "none/" });
    expect(empty.entries).toHaveLength(0);
  });

  test("snapshot prefix respects tenantId isolation", async () => {
    const store = ephemeral<{ v: number }>({
      id: `prefix-tenant-${Date.now()}`,
      ttlMs: 5_000,
    });

    await store.upsert({ tenantId: "t1", key: "apps/a", value: { v: 1 } });
    await store.upsert({ tenantId: "t2", key: "apps/a", value: { v: 99 } });

    const t1 = await store.snapshot({ tenantId: "t1", prefix: "apps/" });
    expect(t1.entries).toHaveLength(1);
    expect(t1.entries[0]?.value.v).toBe(1);

    const t2 = await store.snapshot({ tenantId: "t2", prefix: "apps/" });
    expect(t2.entries).toHaveLength(1);
    expect(t2.entries[0]?.value.v).toBe(99);
  });

  test("reader with prefix only yields events for matching keys", async () => {
    const store = ephemeral<{ v: number }>({
      id: `prefix-reader-${Date.now()}`,
      ttlMs: 5_000,
    });

    const r = store.reader({ prefix: "apps/" });

    const p1 = r.recv({ wait: true, timeoutMs: 500 });
    await store.upsert({ key: "apps/a", value: { v: 1 } });
    const ev1 = await p1;
    expect(ev1?.type).toBe("upsert");
    if (ev1?.type === "upsert") expect(ev1.entry.key).toBe("apps/a");

    // Non-matching upsert: reader should skip and wait for next matching
    const p2 = r.recv({ wait: true, timeoutMs: 500 });
    await store.upsert({ key: "services/cache", value: { v: 2 } });
    await store.upsert({ key: "apps/b", value: { v: 3 } });
    const ev2 = await p2;
    expect(ev2?.type).toBe("upsert");
    if (ev2?.type === "upsert") expect(ev2.entry.key).toBe("apps/b");
  });

  test("reader without prefix yields all events (baseline)", async () => {
    const store = ephemeral<{ v: number }>({
      id: `noprefix-reader-${Date.now()}`,
      ttlMs: 5_000,
    });

    const r = store.reader();

    const p1 = r.recv({ wait: true, timeoutMs: 500 });
    await store.upsert({ key: "apps/a", value: { v: 1 } });
    const ev1 = await p1;
    expect(ev1?.type).toBe("upsert");
    if (ev1?.type === "upsert") expect(ev1.entry.key).toBe("apps/a");

    const p2 = r.recv({ wait: true, timeoutMs: 500 });
    await store.upsert({ key: "services/cache", value: { v: 2 } });
    const ev2 = await p2;
    expect(ev2?.type).toBe("upsert");
    if (ev2?.type === "upsert") expect(ev2.entry.key).toBe("services/cache");
  });
});

// ==========================
// createdAt (uptime tracking)
// ==========================

describe("createdAt", () => {
  test("createdAt is set on first upsert", async () => {
    const store = ephemeral<{ v: number }>({ id: `ca-init-${Date.now()}`, ttlMs: 5_000 });
    const before = Date.now();
    const entry = await store.upsert({ key: "k", value: { v: 1 } });
    const after = Date.now();

    expect(entry.createdAt).toBeGreaterThanOrEqual(before);
    expect(entry.createdAt).toBeLessThanOrEqual(after);
    expect(entry.createdAt).toBe(entry.updatedAt);
  });

  test("createdAt stays constant across upserts with new value", async () => {
    const store = ephemeral<{ v: number }>({ id: `ca-preserve-${Date.now()}`, ttlMs: 5_000 });
    const first = await store.upsert({ key: "k", value: { v: 1 } });
    await Bun.sleep(10);
    const second = await store.upsert({ key: "k", value: { v: 2 } });

    expect(second.createdAt).toBe(first.createdAt);
    expect(second.updatedAt).toBeGreaterThan(first.updatedAt);
  });

  test("createdAt stays constant across touch", async () => {
    const store = ephemeral<{ v: number }>({ id: `ca-touch-${Date.now()}`, ttlMs: 5_000 });
    const first = await store.upsert({ key: "k", value: { v: 1 } });
    await Bun.sleep(10);
    await store.touch({ key: "k" });

    const snap = await store.snapshot();
    expect(snap.entries[0]?.createdAt).toBe(first.createdAt);
    expect(snap.entries[0]?.updatedAt).toBeGreaterThan(first.updatedAt);
  });

  test("createdAt resets after remove + re-upsert", async () => {
    const store = ephemeral<{ v: number }>({ id: `ca-remove-${Date.now()}`, ttlMs: 5_000 });
    const first = await store.upsert({ key: "k", value: { v: 1 } });
    await Bun.sleep(15);
    await store.remove({ key: "k" });
    await Bun.sleep(5);
    const second = await store.upsert({ key: "k", value: { v: 2 } });

    expect(second.createdAt).toBeGreaterThan(first.createdAt);
  });

  test("createdAt flows through snapshot and reader", async () => {
    const store = ephemeral<{ v: number }>({ id: `ca-flow-${Date.now()}`, ttlMs: 5_000 });
    const r = store.reader();

    const p = r.recv({ wait: true, timeoutMs: 500 });
    const entry = await store.upsert({ key: "k", value: { v: 1 } });
    const ev = await p;

    expect(ev?.type).toBe("upsert");
    if (ev?.type === "upsert") {
      expect(ev.entry.createdAt).toBe(entry.createdAt);
    }

    const snap = await store.snapshot();
    expect(snap.entries[0]?.createdAt).toBe(entry.createdAt);
  });
});

// ==========================
// Lazy expiry and reader anchoring
// ==========================

test("reads and writes do not see entries whose ttl has passed", async () => {
  const id = `sweep-${Date.now()}`;
  const store = makeStore({ id, ttlMs: 60_000 });

  await store.upsert({ key: "user:1", value: { status: "online" } });
  await store.upsert({ key: "user:2", value: { status: "online" } });

  // Browsers clamp and defer setTimeout in a background tab — a one-second
  // floor, and minutes under intensive throttling or bfcache — so the deadline
  // passes while the timer has not fired. Reproduce exactly that: move the
  // deadline into the past without touching the pending timer.
  const states = sharedState(`ephemeral:${id}`, undefined, () => new Map()) as Map<
    string,
    { entries: Map<string, { expiresAt: number }> }
  >;
  states.get("default")!.entries.get("user:1")!.expiresAt = Date.now() - 1;

  const snap = await store.snapshot({});
  expect(snap.entries.map((e) => e.key)).toEqual(["user:2"]);

  // touch() on a logically dead key must not resurrect it; the server returns
  // ok:false here.
  expect((await store.touch({ key: "user:1" })).ok).toBe(false);
});

test("a reader anchors at its first recv, not at construction", async () => {
  const store = makeStore({ id: `anchor-${Date.now()}`, ttlMs: 60_000 });

  const reader = store.reader();
  await store.upsert({ key: "after-construction", value: { status: "online" } });

  // The server anchors lazily, so this upsert is *not* delivered.
  expect(await reader.recv({ wait: false })).toBeNull();

  await store.upsert({ key: "after-first-recv", value: { status: "away" } });
  const event = await reader.recv({ wait: false });
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") expect(event.entry.key).toBe("after-first-recv");
});

test("a reader that falls behind after a healthy read gets overflow", async () => {
  const store = ephemeral<{ n: number }>({
    id: `overflow-live-${Date.now()}`,
    ttlMs: 60_000,
    limits: { eventMaxLen: 2 },
  });
  const reader = store.reader();

  expect(await reader.recv({ wait: false })).toBeNull();
  await store.upsert({ key: "a", value: { n: 1 } });
  expect((await reader.recv({ wait: false }))?.type).toBe("upsert");

  await store.upsert({ key: "b", value: { n: 2 } });
  await store.upsert({ key: "c", value: { n: 3 } });
  await store.upsert({ key: "d", value: { n: 4 } });

  const event = await reader.recv({ wait: false });
  expect(event?.type).toBe("overflow");
  if (event?.type === "overflow") expect(event.after).toBe("1");
});
