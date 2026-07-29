import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { queue } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:fq:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

// ==========================
// High-contention ack/nack/touch races
// ==========================

test("concurrent ack/nack/touch on same delivery — exactly one ack or nack succeeds", async () => {
  for (let round = 0; round < 10; round++) {
    const q = queue({
      id: uid("race-trio"),
      prefix: "test:fq",
    });

    await q.send({ data: { v: round } });
    const msg = await q.recv({ wait: false });
    expect(msg).not.toBeNull();

    const [ack, nack, touched] = await Promise.all([
      msg!.ack(),
      msg!.nack(),
      msg!.touch({ leaseMs: 1000 }),
    ]);

    // Exactly one of ack/nack settles the delivery — the invariant is one, not
    // "at most one", which also holds if the delivery vanished entirely.
    expect((ack ? 1 : 0) + (nack ? 1 : 0)).toBe(1);
    // touch raced the settle, so it may lose; it must never report success
    // after the delivery is gone.
    expect(typeof touched).toBe("boolean");

    // If nack won, clean up the requeued message
    if (nack) {
      const requeued = await q.recv({ wait: false });
      if (requeued) await requeued.ack();
    }
  }
});

test("multiple readers racing on same queue — each message delivered exactly once", async () => {
  const q = queue({
    id: uid("multi-reader"),
    prefix: "test:fq",
  });

  const count = 50;
  for (let i = 0; i < count; i++) {
    await q.send({ data: { idx: i } });
  }

  const collected = new Set<number>();
  const readers = Array.from({ length: 5 }, () => q.reader());

  const drain = async (reader: ReturnType<typeof q.reader>): Promise<void> => {
    while (true) {
      const msg = await reader.recv({ wait: false });
      if (!msg) break;
      collected.add(msg.data.idx);
      await msg.ack();
    }
  };

  await Promise.all(readers.map(drain));

  expect(collected.size).toBe(count);
});

// ==========================
// Maintenance edge: delayed + expired + maxDeliveries in same batch
// ==========================

test("maintenance handles delayed expiry, lease expiry, and max deliveries in one pass", async () => {
  const q = queue({
    id: uid("maint-combo"),
    prefix: "test:fq",
    delivery: { maxDeliveries: 2, defaultLeaseMs: 40 },
    limits: { maxMessageAgeMs: 200 },
  });

  // Message A: will exceed maxDeliveries
  await q.send({ data: { id: "a" } });
  const a1 = await q.recv({ wait: false, leaseMs: 40 });
  expect(a1).not.toBeNull();
  await a1!.nack(); // attempt 1
  const a2 = await q.recv({ wait: false, leaseMs: 40 });
  expect(a2).not.toBeNull();
  await a2!.nack(); // attempt 2 → should DLQ

  // Message B: will be delayed and eventually expire
  await q.send({ data: { id: "b" }, delayMs: 50 });

  // Message C: will have its lease expire during processing
  await q.send({ data: { id: "c" } });
  const c1 = await q.recv({ wait: false, leaseMs: 40 });
  expect(c1).not.toBeNull();
  // Don't ack — let lease expire

  await Bun.sleep(80);

  // Both assertions used to sit inside `if (...)`, so a completely broken
  // maintenance pass left them unexecuted and the test still passed. The DLQ
  // was never read at all, so "A should be in DLQ" was pure comment.
  const drained = new Set<string>();
  for (let i = 0; i < 2; i++) {
    const message = await q.recv({ wait: false });
    expect(message).not.toBeNull();
    drained.add(message!.data.id as string);
    expect(await message!.ack()).toBe(true);
  }

  // B was promoted from delayed, C was requeued from its expired lease.
  expect(drained).toEqual(new Set(["b", "c"]));

  // A exceeded maxDeliveries and must be in the DLQ, with the right reason.
  const dead = await q.dlq();
  expect(dead.length).toBe(1);
  expect(dead[0]?.data).toEqual({ id: "a" });
  expect(dead[0]?.reason).toBe("max_deliveries_exceeded");

  expect(await q.recv({ wait: false })).toBeNull();
});

// ==========================
// Idempotency TTL boundary race
// ==========================

test("idempotency key expiry at send time allows new message", async () => {
  const q = queue({
    id: uid("idem-boundary"),
    prefix: "test:fq",
  });

  const first = await q.send({ data: { v: 1 }, idempotencyKey: "k", idempotencyTtlMs: 50 });

  // Wait for idempotency key to expire
  await Bun.sleep(70);

  const second = await q.send({ data: { v: 2 }, idempotencyKey: "k", idempotencyTtlMs: 50 });

  // Should get different message IDs
  expect(first.messageId).not.toBe(second.messageId);

  // Both messages should be receivable
  const m1 = await q.recv({ wait: false });
  const m2 = await q.recv({ wait: false });
  expect(m1).not.toBeNull();
  expect(m2).not.toBeNull();
  expect(new Set([m1!.data.v, m2!.data.v])).toEqual(new Set([1, 2]));
  await m1!.ack();
  await m2!.ack();
});

test("concurrent sends with same idempotency key during TTL expiry boundary", async () => {
  const q = queue({
    id: uid("idem-race-boundary"),
    prefix: "test:fq",
  });

  // Set a very short TTL
  await q.send({ data: { v: 1 }, idempotencyKey: "race-k", idempotencyTtlMs: 40 });

  // Wait right at the boundary
  await Bun.sleep(35);

  // Race: one send should dedup, the other might create a new message if TTL expired
  const results = await Promise.all([
    q.send({ data: { v: 2 }, idempotencyKey: "race-k", idempotencyTtlMs: 40 }),
    q.send({ data: { v: 3 }, idempotencyKey: "race-k", idempotencyTtlMs: 40 }),
  ]);

  // At TTL boundary, both outcomes are valid:
  // - both dedup to same message ID
  // - one attaches to old ID while the other creates a fresh post-expiry ID
  const uniqueResultIds = new Set(results.map((r) => r.messageId));
  expect(uniqueResultIds.size).toBeGreaterThanOrEqual(1);
  expect(uniqueResultIds.size).toBeLessThanOrEqual(2);

  // Drain and assert no more than 2 physical messages are produced.
  const drainedIds = new Set<string>();
  let drainedCount = 0;
  while (true) {
    const msg = await q.recv({ wait: false });
    if (!msg) break;
    drainedCount += 1;
    drainedIds.add(msg.messageId);
    await msg.ack();
  }
  expect(drainedCount).toBeGreaterThanOrEqual(1);
  expect(drainedCount).toBeLessThanOrEqual(2);
  expect(drainedIds.size).toBe(drainedCount);
});

// ==========================
// Corrupted message recovery
// ==========================

test("recv skips message with corrupted payload and does not block queue", async () => {
  const qId = uid("corrupt");
  const q = queue({
    id: qId,
    prefix: "test:fq",
  });

  // Send a valid message to get a message ID
  await q.send({ data: { v: 1 } });

  // Corrupt the message payload in Redis directly. The guard around this used
  // to let the corruption silently no-op.
  const messagesKey = `test:fq:default:${qId}:messages`;
  const msgIds = (await redis.send("HKEYS", [messagesKey])) as string[];
  expect(msgIds.length).toBe(1);
  const poisonId = String(msgIds[0]);
  await redis.send("HSET", [messagesKey, poisonId, "{{invalid json"]);

  // Send another valid message after the corrupted one
  await q.send({ data: { v: 2 } });

  // Every assertion used to sit inside `if (msg)`, and the outcome was
  // deterministically null, so a regression that made recv throw or wedge the
  // queue on a poison message shipped green.
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(msg!.data.v).toBe(2);
  expect(await msg!.ack()).toBe(true);

  // The poison record is gone and the queue is not wedged.
  expect(await redis.send("HEXISTS", [messagesKey, poisonId])).toBe(0);
  expect(await q.recv({ wait: false })).toBeNull();
  await q.send({ data: { v: 3 } });
  const after = await q.recv({ wait: false });
  expect(after?.data.v).toBe(3);
  await after?.ack();
});

// ==========================
// Backpressure: many delayed messages becoming due simultaneously
// ==========================

test("many delayed messages becoming due at once are all eventually delivered", async () => {
  const q = queue({
    id: uid("burst"),
    prefix: "test:fq",
  });

  const count = 30;
  for (let i = 0; i < count; i++) {
    await q.send({ data: { idx: i }, delayMs: 50 });
  }

  // None should be available immediately
  expect(await q.recv({ wait: false })).toBeNull();

  // Wait for all to become due
  await Bun.sleep(100);

  const received = new Set<number>();
  // Multiple maintenance+recv passes may be needed
  for (let pass = 0; pass < 5 && received.size < count; pass++) {
    while (true) {
      const msg = await q.recv({ wait: false });
      if (!msg) break;
      received.add(msg.data.idx);
      await msg.ack();
    }
    if (received.size < count) await Bun.sleep(20);
  }

  expect(received.size).toBe(count);
});

// ==========================
// Touch after ack/nack
// ==========================

test("touch after ack returns false", async () => {
  const q = queue({
    id: uid("touch-after-ack"),
    prefix: "test:fq",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  expect(await msg!.ack()).toBe(true);
  expect(await msg!.touch({ leaseMs: 1000 })).toBe(false);
});

test("touch after nack returns false", async () => {
  const q = queue({
    id: uid("touch-after-nack"),
    prefix: "test:fq",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  expect(await msg!.nack()).toBe(true);
  expect(await msg!.touch({ leaseMs: 1000 })).toBe(false);

  // Clean up requeued message
  const requeued = await q.recv({ wait: false });
  if (requeued) await requeued.ack();
});

// ==========================
// DLQ receives expired messages from delayed queue
// ==========================

test("message that ages out while in delayed queue goes to DLQ", async () => {
  const qId = uid("delayed-age-dlq");
  const q = queue({
    id: qId,
    prefix: "test:fq",
    limits: { maxMessageAgeMs: 60 },
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  // Nack with delay — message goes to delayed queue
  await msg!.nack({ delayMs: 100 });

  // Wait for message to age past maxMessageAgeMs while delayed
  await Bun.sleep(150);

  // Trigger maintenance — message should be expired
  const result = await q.recv({ wait: false });
  expect(result).toBeNull();

  // Check DLQ
  const dlqKey = `test:fq:default:${qId}:dlq`;
  const dlqRaw = await redis.hget(dlqKey, msg!.messageId);
  expect(dlqRaw).not.toBeNull();
  const entry = JSON.parse(dlqRaw!);
  expect(entry.reason).toBe("expired");
});

// ==========================
// Ordering under nack+requeue
// ==========================

test("nacked message is redelivered after all ready messages", async () => {
  const q = queue({
    id: uid("nack-order"),
    prefix: "test:fq",
  });

  await q.send({ data: { seq: 1 } });
  await q.send({ data: { seq: 2 } });
  await q.send({ data: { seq: 3 } });

  // Receive and nack the first message
  const first = await q.recv({ wait: false });
  expect(first?.data.seq).toBe(1);
  await first!.nack();

  // Receive remaining — should get 2, 3, then 1 (nacked goes to back)
  const order: number[] = [];
  while (true) {
    const msg = await q.recv({ wait: false });
    if (!msg) break;
    order.push(msg.data.seq);
    await msg.ack();
  }

  expect(order).toEqual([2, 3, 1]);
});

// ==========================
// Lease expiry + rapid reprocessing cycle
// ==========================

test("rapid lease expiry cycle — message survives multiple lease expirations", async () => {
  const q = queue({
    id: uid("lease-cycle"),
    prefix: "test:fq",
    delivery: { maxDeliveries: 5, defaultLeaseMs: 30 },
  });

  await q.send({ data: { v: 42 } });

  // Let lease expire 3 times
  for (let i = 0; i < 3; i++) {
    const msg = await q.recv({ wait: false, leaseMs: 30 });
    expect(msg).not.toBeNull();
    // Don't ack — let lease expire
    await Bun.sleep(60);
  }

  // Message should still be deliverable (3 < maxDeliveries=5)
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(msg!.data.v).toBe(42);
  expect(msg!.attempt).toBe(4);
  await msg!.ack();
});

// ==========================
// Crash between dequeue and claim
// ==========================

test("a message orphaned between dequeue and claim is recovered, not lost", async () => {
  const id = uid("orphan-recovery");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });
  const base = `test:fq:default:${id}`;

  const { messageId } = await q.send({ data: { v: 1 } });

  // Exactly what a <= 5.8.0 consumer left behind when it died after its
  // LMOVE and before its claim: parked in `active` with no delivery, no lease.
  const popped = await redis.send("LMOVE", [`${base}:ready`, `${base}:active`, "RIGHT", "LEFT"]);
  expect(popped).toBe(messageId);
  expect(await redis.send("HLEN", [`${base}:deliveries`])).toBe(0);
  expect(await redis.send("LLEN", [`${base}:ready`])).toBe(0);

  // The first bounded pass only records the suspicion. Requeueing immediately
  // would race an older worker between its LMOVE and delivery write.
  expect(await q.recv({ wait: false })).toBeNull();
  const recovered = await q.recv({ wait: false });
  expect(recovered).not.toBeNull();
  expect(recovered?.messageId).toBe(messageId);
  expect(recovered?.data).toEqual({ v: 1 });
  expect(await recovered?.ack()).toBe(true);

  expect(await redis.send("LLEN", [`${base}:active`])).toBe(0);
});

test("the reaper leaves genuinely in-flight deliveries alone", async () => {
  const id = uid("orphan-negative");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });
  const base = `test:fq:default:${id}`;

  await q.send({ data: { v: 1 } });
  const held = await q.recv({ wait: false, leaseMs: 60_000 });
  expect(held).not.toBeNull();
  expect(await redis.send("LLEN", [`${base}:active`])).toBe(1);

  // Force several maintenance passes while the delivery is legitimately held.
  const other = q.reader();
  expect(await other.recv({ wait: false })).toBeNull();
  expect(await other.recv({ wait: false })).toBeNull();

  // Still owned by the original consumer, never requeued behind its back.
  expect(await redis.send("LLEN", [`${base}:ready`])).toBe(0);
  expect(await held?.ack()).toBe(true);
});

test("maintenance backfills a legacy in-flight delivery before orphan recovery", async () => {
  const id = uid("orphan-rolling-upgrade");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });
  const base = `test:fq:default:${id}`;

  await q.send({ data: { v: 1 } });
  const held = await q.recv({ wait: false, leaseMs: 60_000 });
  expect(held).not.toBeNull();

  // Simulate a claim made by a pre-index worker during a rolling upgrade.
  await redis.send("HDEL", [`${base}:delivery-owners`, held!.messageId]);

  expect(await q.reader().recv({ wait: false })).toBeNull();
  expect(await redis.send("HGET", [`${base}:delivery-owners`, held!.messageId])).toBe(held!.deliveryId);
  expect(await redis.send("LLEN", [`${base}:ready`])).toBe(0);
  expect(await held?.ack()).toBe(true);
});

test("an orphan is recovered without requeueing an unrelated live delivery", async () => {
  const id = uid("orphan-with-live-delivery");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });
  const base = `test:fq:default:${id}`;

  await q.send({ data: { v: 2 } });
  const held = await q.recv({ wait: false, leaseMs: 60_000 });
  expect(held?.data).toEqual({ v: 2 });

  const orphan = await q.send({ data: { v: 1 } });
  expect(await redis.send("LMOVE", [`${base}:ready`, `${base}:active`, "RIGHT", "LEFT"])).toBe(orphan.messageId);

  // First pass records the missing owner. The next completed delivery scan
  // proves that only the parked message is orphaned.
  expect(await q.reader().recv({ wait: false })).toBeNull();
  const recovered = await q.reader().recv({ wait: false });

  expect(recovered?.messageId).toBe(orphan.messageId);
  expect(recovered?.data).toEqual({ v: 1 });
  expect(await redis.send("LLEN", [`${base}:ready`])).toBe(0);
  expect(await held?.ack()).toBe(true);
  expect(await recovered?.ack()).toBe(true);
});

test("legacy delivery indexing is incremental instead of scanning the full hash", async () => {
  const id = uid("orphan-bounded-scan");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });
  const base = `test:fq:default:${id}`;
  const future = Date.now() + 60_000;
  const deliveries: string[] = [`${base}:deliveries`];

  for (let i = 0; i < 1_000; i++) {
    deliveries.push(
      `delivery-${i}`,
      JSON.stringify({ messageId: `message-${i}`, leaseUntil: future, attempt: 1 }),
    );
  }
  await redis.send("HSET", deliveries);

  expect(await q.recv({ wait: false })).toBeNull();
  const indexed = Number(await redis.send("HLEN", [`${base}:delivery-owners`]));
  expect(indexed).toBeGreaterThan(0);
  expect(indexed).toBeLessThan(1_000);
});

// ==========================
// Maintenance while a consumer is parked
// ==========================

test("a parked blocking recv still sees a delayed message become due", async () => {
  const id = uid("parked-delayed");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });

  await q.send({ data: { v: 7 }, delayMs: 300 });

  const startedAt = Date.now();
  const message = await q.recv({ wait: true, timeoutMs: 10_000 });
  const elapsedMs = Date.now() - startedAt;

  expect(message).not.toBeNull();
  expect(message?.data).toEqual({ v: 7 });
  // Maintenance runs on its own cadence while parked, so this must not wait out
  // the recv timeout.
  expect(elapsedMs).toBeLessThan(3_000);
  expect(await message?.ack()).toBe(true);
});

test("a parked blocking recv still recovers an expired lease", async () => {
  const id = uid("parked-lease");
  const q = queue<{ v: number }>({ id, prefix: "test:fq" });

  await q.send({ data: { v: 9 } });
  const first = await q.recv({ wait: false, leaseMs: 200 });
  expect(first).not.toBeNull();

  // A second reader parks before the lease expires.
  const startedAt = Date.now();
  const redelivered = await q.reader().recv({ wait: true, timeoutMs: 10_000 });
  const elapsedMs = Date.now() - startedAt;

  expect(redelivered).not.toBeNull();
  expect(redelivered?.messageId).toBe(first?.messageId);
  expect(redelivered?.attempt).toBe(2);
  expect(elapsedMs).toBeLessThan(3_000);
  expect(await redelivered?.ack()).toBe(true);
});
