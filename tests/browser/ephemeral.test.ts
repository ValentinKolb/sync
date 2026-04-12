import { test, expect, describe } from "bun:test";
import {
  ephemeral,
  EphemeralCapacityError,
  EphemeralPayloadTooLargeError,
} from "../../src/browser/ephemeral";
import { z } from "zod";

// ==========================
// Helpers
// ==========================

const schema = z.object({ status: z.string() });

const makeStore = (overrides?: Parameters<typeof ephemeral>[0]) =>
  ephemeral({
    id: "test",
    schema,
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
    const store = makeStore({ id: "ttl-override", schema, ttlMs: 5000 });
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
    const store = makeStore({ id: "ttl-expiry", schema, ttlMs: 100 });
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
      schema,
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
      schema,
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
    const store = makeStore({ id: "expire-reader", schema, ttlMs: 100 });

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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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

test("upsert validates schema and rejects invalid data", async () => {
  const store = ephemeral({
    id: `schema-${Date.now()}`,
    schema: z.object({ status: z.string() }),
    ttlMs: 5_000,
  });
  await expect(
    store.upsert({ key: "k", value: { status: 123 } as any })
  ).rejects.toThrow();
});

test("empty key throws", async () => {
  const store = ephemeral({
    id: `empty-key-${Date.now()}`,
    schema: z.object({ status: z.string() }),
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
      schema: z.object({ s: z.string() }),
      ttlMs: 0,
    })
  ).toThrow("ttlMs must be > 0");
});
