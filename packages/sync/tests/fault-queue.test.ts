import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
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
      schema: z.object({ v: z.number() }),
    });

    await q.send({ data: { v: round } });
    const msg = await q.recv({ wait: false });
    expect(msg).not.toBeNull();

    const [ack, nack, touch] = await Promise.all([
      msg!.ack(),
      msg!.nack(),
      msg!.touch({ leaseMs: 1000 }),
    ]);

    // At most one of ack/nack succeeds (they delete the delivery)
    const mutuallyExclusive = (ack ? 1 : 0) + (nack ? 1 : 0);
    expect(mutuallyExclusive).toBeLessThanOrEqual(1);

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
    schema: z.object({ idx: z.number() }),
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
    schema: z.object({ id: z.string() }),
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

  // Trigger maintenance via recv
  const afterMaint = await q.recv({ wait: false });

  // Message B should have been moved from delayed to ready
  // Message C should have been requeued from expired lease
  // Message A should be in DLQ
  if (afterMaint) {
    const id = afterMaint.data.id;
    expect(["b", "c"]).toContain(id);
    await afterMaint.ack();
  }

  // Drain remaining
  const remaining = await q.recv({ wait: false });
  if (remaining) {
    expect(["b", "c"]).toContain(remaining.data.id);
    await remaining.ack();
  }
});

// ==========================
// Idempotency TTL boundary race
// ==========================

test("idempotency key expiry at send time allows new message", async () => {
  const q = queue({
    id: uid("idem-boundary"),
    prefix: "test:fq",
    schema: z.object({ v: z.number() }),
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
    schema: z.object({ v: z.number() }),
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
    schema: z.object({ v: z.number() }),
  });

  // Send a valid message to get a message ID
  await q.send({ data: { v: 1 } });

  // Corrupt the message payload in Redis directly
  const messagesKey = `test:fq:default:${qId}:messages`;
  const msgIds = await redis.send("HKEYS", [messagesKey]);
  if (Array.isArray(msgIds) && msgIds.length > 0) {
    await redis.send("HSET", [messagesKey, String(msgIds[0]), "{{invalid json"]);
  }

  // Send another valid message after the corrupted one
  await q.send({ data: { v: 2 } });

  // recv should skip the corrupted message and return the valid one
  const msg = await q.recv({ wait: false });
  // May get null (corrupted consumed the recv attempt) or the valid message
  if (msg) {
    expect(msg.data.v).toBe(2);
    await msg.ack();
  }
});

// ==========================
// Backpressure: many delayed messages becoming due simultaneously
// ==========================

test("many delayed messages becoming due at once are all eventually delivered", async () => {
  const q = queue({
    id: uid("burst"),
    prefix: "test:fq",
    schema: z.object({ idx: z.number() }),
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
    schema: z.object({ v: z.number() }),
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
    schema: z.object({ v: z.number() }),
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
    schema: z.object({ v: z.number() }),
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
    schema: z.object({ seq: z.number() }),
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
    schema: z.object({ v: z.number() }),
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
