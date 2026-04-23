import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import {
  ephemeral,
  EphemeralCapacityError,
  EphemeralPayloadTooLargeError,
} from "../index";

const testId = (suffix: string): string => `test-eph-${suffix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["sync:e:*:test-eph-*:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("upsert + snapshot returns current state", async () => {
  const store = ephemeral({
    id: testId("snapshot"),
    ttlMs: 2_000,
  });

  const saved = await store.upsert({
    key: "user:u1",
    value: { status: "online", typing: false },
  });

  expect(saved.key).toBe("user:u1");
  expect(saved.value.status).toBe("online");

  const snap = await store.snapshot();
  expect(snap.entries.length).toBe(1);
  expect(snap.entries[0]?.key).toBe("user:u1");
  expect(snap.entries[0]?.value.typing).toBe(false);
  expect(typeof snap.cursor).toBe("string");
});

test("touch extends ttl and key survives until new expiry", async () => {
  const store = ephemeral({
    id: testId("touch"),
    ttlMs: 80,
  });

  await store.upsert({ key: "user:u1", value: { status: "online" } });
  await Bun.sleep(40);

  const touched = await store.touch({ key: "user:u1", ttlMs: 120 });
  expect(touched.ok).toBe(true);

  await Bun.sleep(70);
  const snap = await store.snapshot();
  expect(snap.entries.length).toBe(1);
  expect(snap.entries[0]?.key).toBe("user:u1");
});

test("remove deletes entry and is idempotent", async () => {
  const store = ephemeral({
    id: testId("remove"),
    ttlMs: 5_000,
  });

  await store.upsert({ key: "user:u1", value: { status: "online" } });

  expect(await store.remove({ key: "user:u1", reason: "logout" })).toBe(true);
  expect(await store.remove({ key: "user:u1" })).toBe(false);

  const snap = await store.snapshot();
  expect(snap.entries.length).toBe(0);
});

test("reader receives upsert/touch/delete events", async () => {
  const store = ephemeral({
    id: testId("events"),
    ttlMs: 1_000,
  });

  const reader = store.reader();

  const p1 = reader.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({ key: "user:u1", value: { status: "online" } });
  const ev1 = await p1;
  expect(ev1?.type).toBe("upsert");

  const p2 = reader.recv({ wait: true, timeoutMs: 500 });
  await store.touch({ key: "user:u1" });
  const ev2 = await p2;
  expect(ev2?.type).toBe("touch");

  const p3 = reader.recv({ wait: true, timeoutMs: 500 });
  await store.remove({ key: "user:u1" });
  const ev3 = await p3;
  expect(ev3?.type).toBe("delete");
  if (ev3?.type === "delete") {
    expect(typeof ev3.deletedAt).toBe("number");
  }
});

test("ttl expiry produces expire event and removes key from snapshot", async () => {
  const store = ephemeral({
    id: testId("expire"),
    ttlMs: 40,
  });

  await store.upsert({ key: "user:u1", value: { status: "online" } });

  const snapBefore = await store.snapshot();
  const reader = store.reader({ after: snapBefore.cursor });
  const readPromise = reader.recv({ wait: true, timeoutMs: 500 });

  await Bun.sleep(80);
  await store.snapshot();

  const ev = await readPromise;
  expect(ev?.type).toBe("expire");
  if (ev?.type === "expire") {
    expect(typeof ev.expiredAt).toBe("number");
  }

  const snap = await store.snapshot();
  expect(snap.entries.length).toBe(0);
});

test("reader({ after }) replays deltas since cursor", async () => {
  const store = ephemeral({
    id: testId("replay"),
    ttlMs: 2_000,
  });

  const live = store.reader();

  const firstPromise = live.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({ key: "user:u1", value: { status: "online" } });
  const first = await firstPromise;
  expect(first?.type).toBe("upsert");
  expect(first && "cursor" in first ? first.cursor : "").not.toBe("");

  await store.upsert({ key: "user:u2", value: { status: "away" } });

  const replay = store.reader({ after: first?.cursor });
  const next = await replay.recv({ wait: false });

  expect(next?.type).toBe("upsert");
  if (next?.type === "upsert") {
    expect(next.entry.key).toBe("user:u2");
  }
});

test("overflow event when replay cursor is older than retention", async () => {
  const store = ephemeral({
    id: testId("overflow"),
    ttlMs: 2_000,
    limits: {
      eventRetentionMs: 30,
    },
  });

  await store.upsert({ key: "user:u1", value: { status: "online" } });
  await Bun.sleep(60);
  await store.upsert({ key: "user:u2", value: { status: "away" } });

  const replay = store.reader({ after: "0-0" });
  const first = await replay.recv({ wait: false });

  expect(first?.type).toBe("overflow");

  await store.upsert({ key: "user:u3", value: { status: "online" } });
  const second = await replay.recv({ wait: false });
  expect(second?.type).toBe("upsert");
  if (second?.type === "upsert") {
    expect(second.entry.key).toBe("user:u3");
  }
});

test("maxEntries rejects new keys when capacity is reached", async () => {
  const store = ephemeral({
    id: testId("capacity"),
    ttlMs: 2_000,
    limits: {
      maxEntries: 1,
    },
  });

  await store.upsert({ key: "user:u1", value: { status: "online" } });

  await expect(
    store.upsert({ key: "user:u2", value: { status: "away" } }),
  ).rejects.toBeInstanceOf(EphemeralCapacityError);

  // updates to existing key must still work at capacity
  await expect(
    store.upsert({ key: "user:u1", value: { status: "away" } }),
  ).resolves.toBeDefined();
});

test("payload size limit rejects oversized values", async () => {
  const store = ephemeral({
    id: testId("payload"),
    ttlMs: 2_000,
    limits: {
      maxPayloadBytes: 64,
    },
  });

  await expect(
    store.upsert({ key: "k", value: { text: "x".repeat(1_024) } }),
  ).rejects.toBeInstanceOf(EphemeralPayloadTooLargeError);
});

test("tenant isolation separates state and events", async () => {
  const store = ephemeral({
    id: testId("tenant"),
    ttlMs: 2_000,
  });

  await store.upsert({ tenantId: "t1", key: "user:u1", value: { status: "online" } });
  await store.upsert({ tenantId: "t2", key: "user:u1", value: { status: "away" } });

  const s1 = await store.snapshot({ tenantId: "t1" });
  const s2 = await store.snapshot({ tenantId: "t2" });

  expect(s1.entries.length).toBe(1);
  expect(s2.entries.length).toBe(1);
  expect(s1.entries[0]?.value.status).toBe("online");
  expect(s2.entries[0]?.value.status).toBe("away");
});

test("tenant and id delimiter combinations do not collide", async () => {
  const storeA = ephemeral({
    id: "room:x",
    tenantId: "t:a",
    ttlMs: 2_000,
  });
  const storeB = ephemeral({
    id: "x",
    tenantId: "t:a:room",
    ttlMs: 2_000,
  });

  await storeA.upsert({ key: "user1", value: { status: "online" } });
  await storeB.upsert({ key: "user2", value: { status: "away" } });

  const snapA = await storeA.snapshot();
  const snapB = await storeB.snapshot();

  expect(snapA.entries.map((e) => e.key)).toEqual(["user1"]);
  expect(snapB.entries.map((e) => e.key)).toEqual(["user2"]);
});

test("invalid keys are rejected", async () => {
  const store = ephemeral({
    id: testId("invalid-key"),
    ttlMs: 2_000,
  });

  await expect(
    store.upsert({ key: "", value: { status: "online" } }),
  ).rejects.toThrow("key must be non-empty");

  await expect(
    store.upsert({ key: "x".repeat(600), value: { status: "online" } }),
  ).rejects.toThrow("key exceeds max length");
});

test("touch returns ok=false for missing key", async () => {
  const store = ephemeral({
    id: testId("touch-missing"),
    ttlMs: 2_000,
  });

  const res = await store.touch({ key: "missing" });
  expect(res.ok).toBe(false);
});

test("recv respects abort signal", async () => {
  const store = ephemeral({
    id: testId("abort"),
    ttlMs: 2_000,
  });

  const ac = new AbortController();
  ac.abort();
  const ev = await store.reader().recv({ wait: true, timeoutMs: 5_000, signal: ac.signal });
  expect(ev).toBeNull();
});

// ==========================
// Prefix filter — snapshot
// ==========================

test("snapshot with prefix returns only matching entries", async () => {
  const store = ephemeral<{ v: number }>({
    id: testId("prefix-snap"),
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
    id: testId("prefix-tenant"),
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

// ==========================
// Prefix filter — reader
// ==========================

test("reader with prefix only yields events for matching keys", async () => {
  const store = ephemeral<{ v: number }>({
    id: testId("prefix-reader"),
    ttlMs: 5_000,
  });

  const r = store.reader({ prefix: "apps/" });

  // First matching event
  const p1 = r.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({ key: "apps/a", value: { v: 1 } });
  const ev1 = await p1;
  expect(ev1?.type).toBe("upsert");
  if (ev1?.type === "upsert") expect(ev1.entry.key).toBe("apps/a");

  // Non-matching upsert: reader should skip this and wait for next matching
  const p2 = r.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({ key: "services/cache", value: { v: 2 } });
  await store.upsert({ key: "apps/b", value: { v: 3 } });
  const ev2 = await p2;
  expect(ev2?.type).toBe("upsert");
  if (ev2?.type === "upsert") expect(ev2.entry.key).toBe("apps/b");
});

test("reader without prefix yields all events (baseline)", async () => {
  const store = ephemeral<{ v: number }>({
    id: testId("noprefix-reader"),
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

// ==========================
// createdAt (uptime tracking)
// ==========================

test("createdAt is set on first upsert", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-init"), ttlMs: 5_000 });
  const before = Date.now();
  const entry = await store.upsert({ key: "k", value: { v: 1 } });
  const after = Date.now();

  expect(entry.createdAt).toBeGreaterThanOrEqual(before);
  expect(entry.createdAt).toBeLessThanOrEqual(after);
  expect(entry.createdAt).toBe(entry.updatedAt);
});

test("createdAt stays constant across upserts with new value", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-preserve"), ttlMs: 5_000 });
  const first = await store.upsert({ key: "k", value: { v: 1 } });
  await Bun.sleep(10);
  const second = await store.upsert({ key: "k", value: { v: 2 } });

  expect(second.createdAt).toBe(first.createdAt);
  expect(second.updatedAt).toBeGreaterThan(first.updatedAt);
});

test("createdAt stays constant across touch", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-touch"), ttlMs: 5_000 });
  const first = await store.upsert({ key: "k", value: { v: 1 } });
  await Bun.sleep(10);
  await store.touch({ key: "k" });

  const snap = await store.snapshot();
  expect(snap.entries[0]?.createdAt).toBe(first.createdAt);
  expect(snap.entries[0]?.updatedAt).toBeGreaterThan(first.updatedAt);
});

test("createdAt resets after remove + re-upsert", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-remove"), ttlMs: 5_000 });
  const first = await store.upsert({ key: "k", value: { v: 1 } });
  await Bun.sleep(15);
  await store.remove({ key: "k" });
  await Bun.sleep(5);
  const second = await store.upsert({ key: "k", value: { v: 2 } });

  expect(second.createdAt).toBeGreaterThan(first.createdAt);
});

test("createdAt flows through snapshot", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-snapshot"), ttlMs: 5_000 });
  const entry = await store.upsert({ key: "k", value: { v: 1 } });

  const snap = await store.snapshot();
  expect(snap.entries[0]?.createdAt).toBe(entry.createdAt);
});

test("createdAt flows through reader upsert event", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("createdAt-reader"), ttlMs: 5_000 });
  const r = store.reader();

  const p = r.recv({ wait: true, timeoutMs: 500 });
  const entry = await store.upsert({ key: "k", value: { v: 1 } });
  const ev = await p;

  expect(ev?.type).toBe("upsert");
  if (ev?.type === "upsert") {
    expect(ev.entry.createdAt).toBe(entry.createdAt);
  }
});
