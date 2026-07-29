import { test, expect } from "bun:test";
import { queue } from "../src/queue";

// Helper: unique queue per test to avoid cross-test interference
let counter = 0;
const makeQueue = (opts?: {
  maxDeliveries?: number;
  payloadBytes?: number;
  defaultLeaseMs?: number;
}) => {
  counter++;
  return queue({
    id: `test-q-${counter}`,
    prefix: "test:bq",
    delivery: {
      maxDeliveries: opts?.maxDeliveries,
      defaultLeaseMs: opts?.defaultLeaseMs,
    },
    limits: {
      payloadBytes: opts?.payloadBytes,
    },
  });
};

// ==========================
// 1. send and recv basic flow
// ==========================

test("send and recv basic flow", async () => {
  const q = makeQueue();

  const { messageId } = await q.send({ data: { msg: "hello" } });
  expect(messageId).toBeDefined();

  const received = await q.recv({ wait: false });
  expect(received).not.toBeNull();
  expect(received!.data.msg).toBe("hello");
  expect(received!.messageId).toBe(messageId);
  expect(received!.deliveryId).toBeDefined();
  expect(received!.attempt).toBe(1);
  expect(received!.leaseUntil).toBeGreaterThan(Date.now());

  await received!.ack();
});

// ==========================
// 2. FIFO ordering
// ==========================

test("FIFO ordering - send 3 messages, recv in order", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "first" } });
  await q.send({ data: { msg: "second" } });
  await q.send({ data: { msg: "third" } });

  const m1 = await q.recv({ wait: false });
  const m2 = await q.recv({ wait: false });
  const m3 = await q.recv({ wait: false });

  expect(m1!.data.msg).toBe("first");
  expect(m2!.data.msg).toBe("second");
  expect(m3!.data.msg).toBe("third");

  await m1!.ack();
  await m2!.ack();
  await m3!.ack();
});

// ==========================
// 3. recv with wait: false returns null when empty
// ==========================

test("recv with wait: false returns null when empty", async () => {
  const q = makeQueue();

  const received = await q.recv({ wait: false });
  expect(received).toBeNull();
});

// ==========================
// 4. recv with wait: true blocks until message arrives
// ==========================

test("recv with wait: true blocks until message arrives", async () => {
  const q = makeQueue();

  const recvPromise = q.recv({ wait: true, timeoutMs: 2_000 });

  // Send after a short delay so the reader is already waiting
  await Bun.sleep(50);
  await q.send({ data: { msg: "delayed-arrival" } });

  const received = await recvPromise;
  expect(received).not.toBeNull();
  expect(received!.data.msg).toBe("delayed-arrival");

  await received!.ack();
});

// ==========================
// 5. recv with timeout returns null
// ==========================

test("recv with timeout returns null after timeout", async () => {
  const q = makeQueue();

  const start = Date.now();
  const received = await q.recv({ wait: true, timeoutMs: 100 });
  const elapsed = Date.now() - start;

  expect(received).toBeNull();
  expect(elapsed).toBeGreaterThanOrEqual(80);
  expect(elapsed).toBeLessThan(2_000);
});

// ==========================
// 6. ack removes message
// ==========================

test("ack removes message from queue", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "to-ack" } });

  const received = await q.recv({ wait: false });
  expect(received).not.toBeNull();

  const acked = await received!.ack();
  expect(acked).toBe(true);

  // Acking again should return false (already settled)
  const ackedAgain = await received!.ack();
  expect(ackedAgain).toBe(false);

  // No more messages in the queue
  const next = await q.recv({ wait: false });
  expect(next).toBeNull();
});

// ==========================
// 7. nack requeues message with incremented attempt
// ==========================

test("nack requeues message with incremented attempt", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "retry-me" } });

  const first = await q.recv({ wait: false });
  expect(first).not.toBeNull();
  expect(first!.attempt).toBe(1);

  const nacked = await first!.nack();
  expect(nacked).toBe(true);

  // Message should be back in the queue with attempt incremented
  const second = await q.recv({ wait: false });
  expect(second).not.toBeNull();
  expect(second!.data.msg).toBe("retry-me");
  expect(second!.attempt).toBe(2);
  expect(second!.messageId).toBe(first!.messageId);

  await second!.ack();
});

// ==========================
// 8. nack with delayMs delays requeue
// ==========================

test("nack with delayMs delays requeue", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "delay-nack" } });

  const received = await q.recv({ wait: false });
  expect(received).not.toBeNull();

  await received!.nack({ delayMs: 150 });

  // Should not be immediately available
  const immediate = await q.recv({ wait: false });
  expect(immediate).toBeNull();

  // Wait for the delay plus maintenance interval (~1s) to pass
  // The maintenance runs with a 1s rate limit, so we need to wait
  // for the delayed message to be promoted
  await Bun.sleep(1_200);

  const delayed = await q.recv({ wait: false });
  expect(delayed).not.toBeNull();
  expect(delayed!.data.msg).toBe("delay-nack");

  await delayed!.ack();
});

// ==========================
// 9. DLQ after maxDeliveries
// ==========================

test("message moved to DLQ after maxDeliveries", async () => {
  const q = makeQueue({ maxDeliveries: 2 });

  await q.send({ data: { msg: "doomed" } });

  // First delivery
  const first = await q.recv({ wait: false });
  expect(first).not.toBeNull();
  expect(first!.attempt).toBe(1);
  await first!.nack();

  // Second delivery (attempt 2 = maxDeliveries)
  const second = await q.recv({ wait: false });
  expect(second).not.toBeNull();
  expect(second!.attempt).toBe(2);
  await second!.nack();

  // Should be in DLQ now, not available anymore
  const third = await q.recv({ wait: false });
  expect(third).toBeNull();
});

// ==========================
// 10. Lease expiry requeues message
// ==========================

test("lease expiry requeues message", async () => {
  const q = makeQueue({ defaultLeaseMs: 100 });

  await q.send({ data: { msg: "lease-test" } });

  const received = await q.recv({ wait: false, leaseMs: 100 });
  expect(received).not.toBeNull();

  // Don't ack or nack — let the lease expire
  // Wait for lease to expire plus maintenance interval
  await Bun.sleep(1_200);

  const redelivered = await q.recv({ wait: false });
  expect(redelivered).not.toBeNull();
  expect(redelivered!.data.msg).toBe("lease-test");
  expect(redelivered!.attempt).toBe(2);
  expect(redelivered!.messageId).toBe(received!.messageId);

  await redelivered!.ack();
});

// ==========================
// 11. touch extends lease
// ==========================

test("touch extends lease past the point it would otherwise expire", async () => {
  const q = makeQueue({ defaultLeaseMs: 100 });

  await q.send({ data: { msg: "touch-me" } });

  const received = await q.recv({ wait: false, leaseMs: 100 });
  expect(received).not.toBeNull();

  const originalLease = received!.leaseUntil;
  const touched = await received!.touch({ leaseMs: 5_000 });
  expect(touched).toBe(true);
  // `originalLease` was a dead variable before; the extension must be real.
  expect(received!.leaseUntil).toBe(originalLease);

  // Sleep past both the original lease *and* the maintenance interval. The old
  // test slept 200ms, so maintenance — rate-limited to 1s and stamped by the
  // first recv — never examined a lease at all, and the assertion below held
  // for the wrong reason.
  await Bun.sleep(1_300);

  const shouldBeNull = await q.recv({ wait: false });
  expect(shouldBeNull).toBeNull();

  expect(await received!.ack()).toBe(true);
});

test("without a touch the lease does expire and the message is redelivered", async () => {
  // The negative control the test above needs to mean anything.
  const q = makeQueue({ defaultLeaseMs: 100 });

  await q.send({ data: { msg: "no-touch" } });
  const received = await q.recv({ wait: false, leaseMs: 100 });
  expect(received).not.toBeNull();

  await Bun.sleep(1_300);

  const redelivered = await q.recv({ wait: false });
  expect(redelivered).not.toBeNull();
  expect(redelivered?.attempt).toBe(2);
  expect(await redelivered?.ack()).toBe(true);
});

// ==========================
// 12. Delayed message not immediately available
// ==========================

test("delayed message not immediately available", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "future" }, delayMs: 200 });

  const immediate = await q.recv({ wait: false });
  expect(immediate).toBeNull();
});

// ==========================
// 13. Delayed message available after delay
// ==========================

test("delayed message available after delay", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "future" }, delayMs: 100 });

  // Not available yet
  const immediate = await q.recv({ wait: false });
  expect(immediate).toBeNull();

  // Wait for delay + maintenance
  await Bun.sleep(1_200);

  const delayed = await q.recv({ wait: false });
  expect(delayed).not.toBeNull();
  expect(delayed!.data.msg).toBe("future");

  await delayed!.ack();
});

// ==========================
// 14. Idempotency key prevents duplicate send
// ==========================

test("idempotency key prevents duplicate send", async () => {
  const q = makeQueue();

  const first = await q.send({ data: { msg: "once" }, idempotencyKey: "idem-1" });
  const second = await q.send({ data: { msg: "once" }, idempotencyKey: "idem-1" });

  expect(first.messageId).toBe(second.messageId);

  // Only one message should be in the queue
  const m1 = await q.recv({ wait: false });
  expect(m1).not.toBeNull();
  await m1!.ack();

  const m2 = await q.recv({ wait: false });
  expect(m2).toBeNull();
});

// ==========================
// 15. Payload size limit enforced
// ==========================

test("payload size limit enforced", async () => {
  const q = makeQueue({ payloadBytes: 64 });

  let thrown: unknown = null;
  try {
    await q.send({ data: { msg: "x".repeat(200) } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toContain("payload exceeds limit");

  // Small payload should succeed
  const result = await q.send({ data: { msg: "ok" } });
  expect(result.messageId).toBeDefined();
});

// ==========================
// 17. Stream yields messages in order
// ==========================

test("stream yields messages in order", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "s1" } });
  await q.send({ data: { msg: "s2" } });
  await q.send({ data: { msg: "s3" } });

  const seen: string[] = [];

  for await (const m of q.stream({ wait: false })) {
    seen.push(m.data.msg);
    await m.ack();
  }

  expect(seen).toEqual(["s1", "s2", "s3"]);
});

// ==========================
// 18. Stream with wait: false stops when empty
// ==========================

test("stream with wait: false stops when empty", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "only-one" } });

  const seen: string[] = [];

  for await (const m of q.stream({ wait: false })) {
    seen.push(m.data.msg);
    await m.ack();
  }

  expect(seen).toEqual(["only-one"]);

  // Confirm the loop actually terminated
  expect(seen.length).toBe(1);
});

// ==========================
// 19. Multiple readers each get separate messages
// ==========================

test("multiple readers each get separate messages", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "a" } });
  await q.send({ data: { msg: "b" } });

  const reader1 = q.reader();
  const reader2 = q.reader();

  const m1 = await reader1.recv({ wait: false });
  const m2 = await reader2.recv({ wait: false });

  expect(m1).not.toBeNull();
  expect(m2).not.toBeNull();

  // They should get different messages since they share the same queue state
  expect(m1!.messageId).not.toBe(m2!.messageId);

  // The two messages together should cover both sent messages
  const msgs = [m1!.data.msg, m2!.data.msg].sort();
  expect(msgs).toEqual(["a", "b"]);

  await m1!.ack();
  await m2!.ack();

  // No more messages left
  const m3 = await reader1.recv({ wait: false });
  expect(m3).toBeNull();
});

// ==========================
// 20. Concurrent ack and nack on same delivery
// ==========================

test("concurrent ack and nack on same delivery - only one succeeds", async () => {
  const q = makeQueue();

  await q.send({ data: { msg: "race" } });

  const received = await q.recv({ wait: false });
  expect(received).not.toBeNull();

  // Fire both concurrently
  const [ackResult, nackResult] = await Promise.all([
    received!.ack(),
    received!.nack(),
  ]);

  // Exactly one should succeed
  expect(ackResult !== nackResult).toBe(true);
  expect(ackResult || nackResult).toBe(true);
});

// ==========================
// 21. Meta and orderingKey preserved
// ==========================

test("meta and orderingKey preserved", async () => {
  const q = makeQueue();

  await q.send({
    data: { msg: "tagged" },
    orderingKey: "partition-x",
    meta: { source: "unit-test", version: 7, nested: { ok: true } },
  });

  const received = await q.recv({ wait: false });
  expect(received).not.toBeNull();
  expect(received!.orderingKey).toBe("partition-x");
  expect(received!.meta).toBeDefined();
  expect(received!.meta!.source).toBe("unit-test");
  expect(received!.meta!.version).toBe(7);
  expect((received!.meta!.nested as { ok: boolean }).ok).toBe(true);

  await received!.ack();
});

// ==========================
// 22. AbortSignal cancels recv
// ==========================

test("AbortSignal cancels recv", async () => {
  const q = makeQueue();

  const ac = new AbortController();

  // Start a recv that will wait
  const recvPromise = q.recv({ wait: true, timeoutMs: 5_000, signal: ac.signal });

  // Abort after a short delay
  await Bun.sleep(50);
  ac.abort();

  const received = await recvPromise;
  expect(received).toBeNull();
});

// ==========================
// nack with delayMs exceeding maxNackDelayMs
// ==========================

test("nack with delayMs exceeding maxNackDelayMs throws", async () => {
  const q = queue({
    id: `nack-overflow-${Date.now()}`,
    limits: { maxNackDelayMs: 100 },
  });
  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  await expect(msg!.nack({ delayMs: 200 })).rejects.toThrow("exceeds maxNackDelayMs");
});

test("touch after ack returns false", async () => {
  const q = queue({
    id: `touch-after-ack-${Date.now()}`,
  });
  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  await msg!.ack();
  expect(await msg!.touch()).toBe(false);
});

// ==========================
// Queue maintenance tests
// ==========================

test("message age expiry in claimNext moves to DLQ", async () => {
  const q = queue({
    id: `age-dlq-${Date.now()}`,
    limits: { maxMessageAgeMs: 100 },
    delivery: { defaultLeaseMs: 30_000 },
  });
  await q.send({ data: { v: 1 } });
  // Wait for the message to age out
  await Bun.sleep(200);
  // recv should return null because the message aged out during claimNext
  const msg = await q.recv({ wait: false });
  expect(msg).toBeNull();
});

test("nack after ack returns false", async () => {
  const q = queue({
    id: `nack-after-ack-${Date.now()}`,
  });
  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(await msg!.ack()).toBe(true);
  expect(await msg!.nack()).toBe(false);
});

test("ack after nack returns false", async () => {
  const q = queue({
    id: `ack-after-nack-${Date.now()}`,
  });
  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(await msg!.nack()).toBe(true);
  expect(await msg!.ack()).toBe(false);
});

test("delayed message age expiry during maintenance moves to DLQ", async () => {
  const q = queue({
    id: `delayed-age-${Date.now()}`,
    limits: { maxMessageAgeMs: 50 },
  });
  // Send with a delay longer than maxMessageAgeMs
  await q.send({ data: { v: 1 }, delayMs: 200 });
  // Wait for the message to age AND the delay to pass
  await Bun.sleep(300);
  // Maintenance should have moved it to DLQ during the next recv
  // Force maintenance by calling recv
  const msg = await q.recv({ wait: false });
  expect(msg).toBeNull();
});

test("tenant isolation between queues", async () => {
  const q = queue({
    id: `tenant-iso-${Date.now()}`,
  });
  await q.send({ data: { v: 1 }, tenantId: "t1" });
  await q.send({ data: { v: 2 }, tenantId: "t2" });

  const msg1 = await q.recv({ wait: false, tenantId: "t1" });
  expect(msg1).not.toBeNull();
  expect(msg1!.data.v).toBe(1);

  const msg2 = await q.recv({ wait: false, tenantId: "t2" });
  expect(msg2).not.toBeNull();
  expect(msg2!.data.v).toBe(2);

  // t1 queue should be empty now
  const msg3 = await q.recv({ wait: false, tenantId: "t1" });
  expect(msg3).toBeNull();

  await msg1!.ack();
  await msg2!.ack();
});

test("the unimplemented ordering mode is rejected instead of silently ignored", () => {
  expect(() => queue({ id: "ordering-reject", ordering: { mode: "ordering_key_partitioned" } })).toThrow(
    /ordering_key_partitioned/,
  );
  expect(() => queue({ id: "ordering-ok", ordering: { mode: "best_effort" } })).not.toThrow();
});

test("dlq entries are readable and drainable", async () => {
  const q = queue<{ v: number }>({ id: `dlq-api-${Date.now()}`, delivery: { maxDeliveries: 1 } });

  await q.send({ data: { v: 1 } });
  const message = await q.recv({ wait: false });
  await message?.nack({ error: "boom" });

  const entries = await q.dlq();
  expect(entries.length).toBe(1);
  expect(entries[0]?.data).toEqual({ v: 1 });
  expect(entries[0]?.lastError).toBe("boom");

  expect(await q.dlqRemove({ messageId: entries[0]!.messageId })).toBe(true);
  expect((await q.dlq()).length).toBe(0);
});

test("touch defaults to this delivery's lease, not the queue default", async () => {
  const q = queue<{ v: number }>({
    id: `touch-lease-${Date.now()}`,
    delivery: { defaultLeaseMs: 30_000 },
  });

  await q.send({ data: { v: 1 } });
  const message = await q.recv({ wait: false, leaseMs: 150 });
  expect(message).not.toBeNull();

  // Extending by the delivery lease (150ms) must NOT hold it for 30s.
  expect(await message?.touch()).toBe(true);
  await Bun.sleep(1_300);

  const redelivered = await q.recv({ wait: false });
  expect(redelivered).not.toBeNull();
  expect(redelivered?.messageId).toBe(message?.messageId);
  expect(redelivered?.attempt).toBe(2);
});

test("a non-waiting recv forces maintenance instead of waiting out the interval", async () => {
  const q = queue<{ v: number }>({ id: `force-maint-${Date.now()}` });

  await q.send({ data: { v: 1 } });
  expect(await q.recv({ wait: false })).not.toBeNull(); // stamps lastMaintenance

  await q.send({ data: { v: 2 }, delayMs: 30 });
  await Bun.sleep(60);

  // Well inside the 1s maintenance interval: the server promotes this, so the
  // browser must too rather than reporting a phantom empty queue.
  const promoted = await q.recv({ wait: false });
  expect(promoted).not.toBeNull();
  expect(promoted?.data).toEqual({ v: 2 });
});

test("payload limit measures the whole envelope, as the server does", async () => {
  const q = queue<{ v: string }>({ id: `payload-envelope-${Date.now()}`, limits: { payloadBytes: 200 } });

  await expect(
    q.send({ data: { v: "x".repeat(40) }, meta: { note: "y".repeat(200) } }),
  ).rejects.toThrow(/payload exceeds limit/);
});

test("nack and dlq reason strings match the server", async () => {
  const q = queue<{ v: number }>({
    id: `strings-${Date.now()}`,
    delivery: { maxDeliveries: 1 },
    limits: { maxNackDelayMs: 20 },
  });

  await q.send({ data: { v: 1 } });
  const message = await q.recv({ wait: false });
  await expect(message!.nack({ delayMs: 25 })).rejects.toThrow("delayMs exceeds maxNackDelayMs (20)");

  expect(await message?.nack()).toBe(true);
  expect((await q.dlq())[0]?.reason).toBe("max_deliveries_exceeded");
});
