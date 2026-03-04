import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
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
    schema: z.object({ status: z.enum(["online", "away"]), typing: z.boolean() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ text: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
    ttlMs: 2_000,
  });
  const storeB = ephemeral({
    id: "x",
    tenantId: "t:a:room",
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
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
    schema: z.object({ status: z.string() }),
    ttlMs: 2_000,
  });

  const res = await store.touch({ key: "missing" });
  expect(res.ok).toBe(false);
});

test("recv respects abort signal", async () => {
  const store = ephemeral({
    id: testId("abort"),
    schema: z.object({ status: z.string() }),
    ttlMs: 2_000,
  });

  const ac = new AbortController();
  ac.abort();
  const ev = await store.reader().recv({ wait: true, timeoutMs: 5_000, signal: ac.signal });
  expect(ev).toBeNull();
});
