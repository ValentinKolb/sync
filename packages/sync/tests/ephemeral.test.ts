import { afterEach, beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import {
  ephemeral,
  EphemeralCapacityError,
  EphemeralPayloadTooLargeError,
  type EphemeralReader,
} from "../index";

const testId = (suffix: string): string => `test-eph-${suffix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const readers: EphemeralReader<unknown>[] = [];

const connectedClients = async (): Promise<number> => {
  const info = (await redis.send("INFO", ["clients"])) as string;
  return Number(/connected_clients:(\d+)/.exec(info)?.[1] ?? 0);
};

const waitFor = async (predicate: () => boolean | Promise<boolean>, timeoutMs = 1_000): Promise<void> => {
  const startedAt = Date.now();
  while (!(await predicate())) {
    if (Date.now() - startedAt >= timeoutMs) throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    await Bun.sleep(5);
  }
};

const trackReader = <T>(reader: EphemeralReader<T>): EphemeralReader<T> => {
  readers.push(reader as EphemeralReader<unknown>);
  return reader;
};

const cleanup = async (): Promise<void> => {
  const keys = await redis.send("KEYS", ["sync:e:*:*test-eph-*:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
};

beforeEach(cleanup);

afterEach(async () => {
  await Promise.all(readers.splice(0).map((reader) => reader.close()));
  await cleanup();
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

  const reader = trackReader(store.reader());

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
  const reader = trackReader(store.reader({ after: snapBefore.cursor }));
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

  const live = trackReader(store.reader());

  const firstPromise = live.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({ key: "user:u1", value: { status: "online" } });
  const first = await firstPromise;
  expect(first?.type).toBe("upsert");
  expect(first && "cursor" in first ? first.cursor : "").not.toBe("");

  await store.upsert({ key: "user:u2", value: { status: "away" } });

  const replay = trackReader(store.reader({ after: first?.cursor }));
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

  const replay = trackReader(store.reader({ after: "0-0" }));
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
  const suffix = testId("delimiter");
  const storeA = ephemeral({
    id: `room:${suffix}:x`,
    tenantId: "t:a",
    ttlMs: 2_000,
  });
  const storeB = ephemeral({
    id: `${suffix}:x`,
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
  const ev = await trackReader(store.reader()).recv({ wait: true, timeoutMs: 5_000, signal: ac.signal });
  expect(ev).toBeNull();
});

test("stream yields events in order and releases its blocking reader", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("stream"),
    ttlMs: 2_000,
  });
  await store.upsert({ key: "seed", value: { value: 0 } });
  const anchor = await store.snapshot();
  const reader = trackReader(store.reader({ after: anchor.cursor }));
  const iterator = reader.stream({ wait: true, timeoutMs: 500 })[Symbol.asyncIterator]();

  try {
    const first = iterator.next();
    await store.upsert({ key: "a", value: { value: 1 } });
    const result = await first;

    expect(result.done).toBe(false);
    expect(result.value).toMatchObject({
      type: "upsert",
      entry: { key: "a", value: { value: 1 } },
    });
  } finally {
    await iterator.return?.();
    await reader.close();
  }
});

test("stream stops promptly when aborted", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("stream-abort"),
    ttlMs: 2_000,
  });
  const reader = trackReader(store.reader());
  const abort = new AbortController();
  const iterator = reader.stream({
    wait: true,
    timeoutMs: 5_000,
    signal: abort.signal,
  })[Symbol.asyncIterator]();

  try {
    const pending = iterator.next();
    await Bun.sleep(20);
    abort.abort();
    expect(await pending).toEqual({ value: undefined, done: true });
  } finally {
    await iterator.return?.();
    await reader.close();
  }
});

test("reader close is terminal and aborts pending reads", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("reader-close"),
    ttlMs: 2_000,
  });
  const recvReader = trackReader(store.reader());
  const pendingRecv = recvReader.recv({ wait: true, timeoutMs: 5_000 });

  await Bun.sleep(20);
  await recvReader.close();
  await recvReader.close();

  expect(await pendingRecv).toBeNull();
  await expect(recvReader.recv({ wait: false })).rejects.toThrow("ephemeral reader is closed");

  const closedStream = recvReader.stream({ wait: true, timeoutMs: 5_000 })[Symbol.asyncIterator]();
  expect(await closedStream.next()).toEqual({ value: undefined, done: true });

  const streamReader = trackReader(store.reader());
  const iterator = streamReader.stream({ wait: true, timeoutMs: 5_000 })[Symbol.asyncIterator]();
  const pendingNext = iterator.next();

  await Bun.sleep(20);
  await streamReader.close();

  expect(await pendingNext).toEqual({ value: undefined, done: true });
});

test("reader close can interrupt a read while its connection is starting", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("reader-close-connect"),
    ttlMs: 2_000,
  });
  const reader = trackReader(store.reader());
  const pending = reader.recv({ wait: true, timeoutMs: 10_000 });

  await reader.close();
  await expect(pending).resolves.toBeNull();
});

test("concurrent streams from one reader use independent connections", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("concurrent-streams"),
    ttlMs: 2_000,
  });
  const reader = trackReader(store.reader());
  const clientsBefore = await connectedClients();
  const first = reader.stream({ wait: true, timeoutMs: 10_000 })[Symbol.asyncIterator]();
  const second = reader.stream({ wait: true, timeoutMs: 10_000 })[Symbol.asyncIterator]();
  const firstPending = first.next();
  const secondPending = second.next();

  await waitFor(async () => (await connectedClients()) >= clientsBefore + 2);

  await reader.close();
  expect(await firstPending).toEqual({ value: undefined, done: true });
  expect(await secondPending).toEqual({ value: undefined, done: true });
});

test("reader close during signal registration aborts the pending read", async () => {
  const store = ephemeral<{ value: number }>({
    id: testId("reader-close-registration"),
    ttlMs: 2_000,
  });
  const reader = trackReader(store.reader());
  const signal = {
    aborted: false,
    addEventListener: () => {
      void reader.close();
    },
    removeEventListener: () => {},
  } as unknown as AbortSignal;

  const startedAt = Date.now();
  expect(await reader.recv({ wait: true, timeoutMs: 5_000, signal })).toBeNull();
  expect(Date.now() - startedAt).toBeLessThan(500);
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

  const r = trackReader(store.reader({ prefix: "apps/" }));

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

  const r = trackReader(store.reader());

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
  const r = trackReader(store.reader());

  const p = r.recv({ wait: true, timeoutMs: 500 });
  const entry = await store.upsert({ key: "k", value: { v: 1 } });
  const ev = await p;

  expect(ev?.type).toBe("upsert");
  if (ev?.type === "upsert") {
    expect(ev.entry.createdAt).toBe(entry.createdAt);
  }
});

// ==========================
// Payload fidelity and argument validity
// ==========================

test("ephemeral values round-trip byte-equivalent JSON", async () => {
  const value = {
    endpoints: [] as string[],
    emptyObj: {},
    unicode: "日本😀",
    big: 9007199254740991,
    nested: [[], {}] as unknown[],
  };
  const store = ephemeral<typeof value>({ id: testId("opaque"), ttlMs: 60_000 });

  const created = await store.upsert({ key: "apps/backend", value });
  expect(created.value).toEqual(value);

  // touch() rewrites the record, so a lossy encode would compound here.
  await store.touch({ key: "apps/backend" });
  const touched = (await store.snapshot({})).entries.find((e) => e.key === "apps/backend");
  expect(touched?.value).toEqual(value);
  expect(Array.isArray(touched?.value.endpoints)).toBe(true);
});

test("the snapshot and the change stream agree on the same value", async () => {
  const value = { tags: [] as string[], n: 9007199254740991 };
  const store = ephemeral<typeof value>({ id: testId("opaque-stream"), ttlMs: 60_000 });

  // Seed first so the snapshot cursor is a real stream id, not "0-0".
  await store.upsert({ key: "svc/seed", value: { tags: [], n: 1 } });
  const anchor = await store.snapshot({});
  const reader = trackReader(store.reader({ after: anchor.cursor }));
  await store.upsert({ key: "svc/a", value });

  const event = await reader.recv({ timeoutMs: 2_000 });
  expect(event?.type).toBe("upsert");
  const fromStream = event?.type === "upsert" ? event.entry.value : null;
  const fromSnapshot = (await store.snapshot({})).entries.find((e) => e.key === "svc/a")?.value;

  // These two paths used to apply different serialization to the same write.
  expect(fromStream).toEqual(fromSnapshot);
  expect(fromSnapshot).toEqual(value);
});

test("entries written in the 5.8.0 record format are still readable", async () => {
  const id = testId("opaque-legacy");
  const store = ephemeral<{ endpoints: string[] }>({ id, ttlMs: 60_000 });
  const stateKey = `sync:e:default:${id}:state`;

  await redis.send("HSET", [
    stateKey,
    "apps/legacy",
    JSON.stringify({
      key: "apps/legacy",
      data: { endpoints: ["a"] },
      version: "1",
      createdAt: Date.now(),
      updatedAt: Date.now(),
      expiresAt: Date.now() + 60_000,
    }),
  ]);

  const snap = await store.snapshot({});
  expect(snap.entries.find((e) => e.key === "apps/legacy")?.value).toEqual({ endpoints: ["a"] });
});

test("snapshot omits expired rows even when reconciliation cannot remove them", async () => {
  const id = testId("snapshot-expired");
  const store = ephemeral<{ v: number }>({ id, ttlMs: 60_000 });
  const stateKey = `sync:e:default:${id}:state`;
  const now = Date.now();

  await redis.send("HSET", [
    stateKey,
    "expired",
    JSON.stringify({
      v: 2,
      key: "expired",
      dataJson: JSON.stringify({ v: 1 }),
      version: "1",
      createdAt: now - 2_000,
      updatedAt: now - 2_000,
      expiresAt: now - 1_000,
    }),
  ]);

  // The row intentionally has no expiration-index member, as can happen with
  // legacy or partially migrated state. Snapshot filtering is the final guard.
  expect(await store.snapshot()).toEqual({ entries: [], cursor: "0-0" });
});

test("a retention longer than the epoch does not break writes", async () => {
  const store = ephemeral<{ v: number }>({
    id: testId("huge-retention"),
    ttlMs: 60_000,
    limits: { eventRetentionMs: 100 * 365 * 24 * 60 * 60 * 1000 }, // ~100 years
  });

  // The trim ran after the XADD in the same script, so a negative MINID meant
  // the event was written and the call still threw.
  const written = await store.upsert({ key: "k", value: { v: 1 } });
  expect(written.value).toEqual({ v: 1 });
});

test("a ttlMs Redis cannot express is rejected at the call site", async () => {
  const store = ephemeral<{ v: number }>({ id: testId("ttl-validation"), ttlMs: 60_000 });

  await expect(store.upsert({ key: "k", value: { v: 1 }, ttlMs: 100.5 })).rejects.toThrow(/positive integer/);
  expect(() => ephemeral({ id: testId("ttl-bad"), ttlMs: 0.5 })).toThrow(/positive integer/);
  // A large but expressible TTL still works.
  await store.upsert({ key: "ok", value: { v: 1 }, ttlMs: 1_000_000_000_000 });
});

test("a reader that falls behind after a healthy start gets an overflow", async () => {
  const id = testId("overflow-live");
  const store = ephemeral<{ n: number }>({ id, ttlMs: 60_000 });
  const eventsKey = `sync:e:default:${id}:events`;

  // Start healthy: anchor on a real cursor and consume one event without gaps.
  await store.upsert({ key: "seed", value: { n: 0 } });
  const anchor = await store.snapshot({});
  const reader = trackReader(store.reader({ after: anchor.cursor }));

  await store.upsert({ key: "a", value: { n: 1 } });
  expect((await reader.recv({ wait: false }))?.type).toBe("upsert");

  // Now stall while writers keep going, until retention discards the reader's
  // cursor. Trimmed exactly here, because production trims approximately and
  // would not drop anything in a stream this small.
  for (let i = 0; i < 5; i++) {
    await store.upsert({ key: `k${i}`, value: { n: i } });
  }
  await redis.send("XTRIM", [eventsKey, "MAXLEN", "2"]);

  // The gap check was a one-shot latch over the constructor cursor, so this
  // silently returned the oldest surviving entry as an ordinary upsert and the
  // consumer's materialised view stayed permanently wrong.
  const next = await reader.recv({ wait: false });
  expect(next?.type).toBe("overflow");
});

test("a blocked reader detects a gap after the entire event stream disappeared", async () => {
  const id = testId("overflow-empty-history");
  const store = ephemeral<{ n: number }>({ id, ttlMs: 60_000 });
  const eventsKey = `sync:e:default:${id}:events`;

  await store.upsert({ key: "seed", value: { n: 0 } });
  const anchor = await store.snapshot();
  const reader = trackReader(store.reader({ after: anchor.cursor }));
  await redis.send("DEL", [eventsKey]);

  const clientsBefore = await connectedClients();
  const pending = reader.recv({ wait: true, timeoutMs: 1_000 });
  await waitFor(async () => (await connectedClients()) > clientsBefore);
  await store.upsert({ key: "after-gap", value: { n: 1 } });

  const event = await pending;
  expect(event?.type).toBe("overflow");
  if (event?.type === "overflow") {
    expect(event.after).toBe(anchor.cursor);
    expect(event.firstAvailable).not.toBe(anchor.cursor);
  }
});

test("a reader that keeps up never sees a spurious overflow", async () => {
  const store = ephemeral<{ n: number }>({
    id: testId("overflow-none"),
    ttlMs: 60_000,
    limits: { eventMaxLen: 1_000 },
  });

  await store.upsert({ key: "seed", value: { n: 0 } });
  const anchor = await store.snapshot({});
  const reader = trackReader(store.reader({ after: anchor.cursor }));

  const types: string[] = [];
  for (let i = 0; i < 5; i++) {
    await store.upsert({ key: `k${i}`, value: { n: i } });
    const event = await reader.recv({ wait: false });
    if (event) types.push(event.type);
  }

  expect(types).toEqual(["upsert", "upsert", "upsert", "upsert", "upsert"]);
});
