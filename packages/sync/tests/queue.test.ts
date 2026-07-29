import { beforeEach, expect, test } from "bun:test";
import { redis } from "bun";
import { queue } from "../index";

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:q:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("send + recv + ack", async () => {
  const q = queue({
    id: "basic",
    prefix: "test:q",
  });

  await q.send({ data: { msg: "hello" } });

  const message = await q.recv({ wait: false });
  expect(message).not.toBeNull();
  expect(message?.data.msg).toBe("hello");
  expect(await message?.ack()).toBe(true);

  const empty = await q.recv({ wait: false });
  expect(empty).toBeNull();
});

test("nack requeues message and increments attempt", async () => {
  const q = queue({
    id: "nack",
    prefix: "test:q",
  });

  await q.send({ data: { n: 1 } });

  const first = await q.recv({ wait: false });
  expect(first?.attempt).toBe(1);
  expect(await first?.nack()).toBe(true);

  const second = await q.recv({ wait: false });
  expect(second).not.toBeNull();
  expect(second?.attempt).toBe(2);
  expect(await second?.ack()).toBe(true);
});

test("delay sends message to delayed queue", async () => {
  const q = queue({
    id: "delay",
    prefix: "test:q",
  });

  await q.send({ data: { ok: true }, delayMs: 80 });
  expect(await q.recv({ wait: false })).toBeNull();

  await Bun.sleep(100);

  const message = await q.recv({ wait: false });
  expect(message).not.toBeNull();
  expect(message?.data.ok).toBe(true);
  expect(await message?.ack()).toBe(true);
});

test("touch extends lease", async () => {
  const q = queue({
    id: "touch",
    prefix: "test:q",
  });

  await q.send({ data: { id: "a" } });

  const message = await q.recv({ wait: false, leaseMs: 50 });
  expect(message).not.toBeNull();

  expect(await message?.touch({ leaseMs: 250 })).toBe(true);
  await Bun.sleep(100);

  const duplicate = await q.recv({ wait: false });
  expect(duplicate).toBeNull();

  expect(await message?.ack()).toBe(true);
});

test("touch cannot revive a lease after its deadline", async () => {
  const q = queue({ id: "touch-expired", prefix: "test:q" });

  await q.send({ data: { v: 1 } });
  const message = await q.recv({ wait: false, leaseMs: 30 });
  await Bun.sleep(50);

  expect(await message?.touch({ leaseMs: 1_000 })).toBe(false);
  const redelivered = await q.recv({ wait: false });
  expect(redelivered?.messageId).toBe(message?.messageId);
  expect(await redelivered?.ack()).toBe(true);
});

test("expired lease requeues message", async () => {
  const q = queue({
    id: "expire",
    prefix: "test:q",
  });

  await q.send({ data: { id: "b" } });

  const first = await q.recv({ wait: false, leaseMs: 40 });
  expect(first).not.toBeNull();

  await Bun.sleep(70);

  const second = await q.recv({ wait: false });
  expect(second).not.toBeNull();
  expect(second?.messageId).toBe(first?.messageId);
  expect(second?.attempt).toBeGreaterThan(1);
  expect(await second?.ack()).toBe(true);
});

test("idempotency key deduplicates send", async () => {
  const q = queue({
    id: "idempotency",
    prefix: "test:q",
  });

  const a = await q.send({ data: { k: "x" }, idempotencyKey: "same" });
  const b = await q.send({ data: { k: "x" }, idempotencyKey: "same" });

  expect(a.messageId).toBe(b.messageId);

  const message = await q.recv({ wait: false });
  expect(message?.messageId).toBe(a.messageId);
  expect(await message?.ack()).toBe(true);
  expect(await q.recv({ wait: false })).toBeNull();
});

test("reader exposes recv/stream as read-only handle", async () => {
  const q = queue({
    id: "reader",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  await q.send({ data: { v: 2 } });

  const reader = q.reader();
  const values: number[] = [];

  for await (const message of reader.stream({ wait: false })) {
    values.push(message.data.v);
    await message.ack();
  }

  expect(values).toEqual([1, 2]);
});

test("nack delay validation rejects values above maxNackDelayMs", async () => {
  const q = queue({
    id: "nack-limit",
    prefix: "test:q",
    limits: { maxNackDelayMs: 20 },
  });

  await q.send({ data: { id: "x" } });
  const message = await q.recv({ wait: false });
  expect(message).not.toBeNull();

  let thrown: unknown = null;
  try {
    await message?.nack({ delayMs: 25 });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).not.toBeNull();
  expect(await message?.ack()).toBe(true);
});

test("queue timings reject unsafe values before mutating Redis", async () => {
  expect(() =>
    queue({
      id: "invalid-defaults",
      prefix: "test:q",
      delivery: { defaultLeaseMs: Number.NaN },
    }),
  ).toThrow("delivery.defaultLeaseMs must be a safe integer");

  const q = queue({ id: "invalid-timings", prefix: "test:q" });
  await expect(q.send({ data: { v: 1 }, delayMs: Number.NaN })).rejects.toThrow(
    "send.delayMs must be a safe integer",
  );
  await expect(q.send({ data: { v: 1 }, idempotencyTtlMs: 0 })).rejects.toThrow(
    "send.idempotencyTtlMs must be a safe integer",
  );
  expect(await redis.send("GET", ["test:q:default:invalid-timings:seq"])).toBeNull();

  await q.send({ data: { v: 1 } });
  await expect(q.recv({ wait: false, timeoutMs: Number.NaN })).rejects.toThrow(
    "recv.timeoutMs must be a safe integer",
  );
  await expect(q.recv({ wait: false, leaseMs: Number.POSITIVE_INFINITY })).rejects.toThrow(
    "recv.leaseMs must be a safe integer",
  );
  const message = await q.recv({ wait: false });
  await expect(message?.nack({ delayMs: Number.NaN })).rejects.toThrow(
    "nack.delayMs must be a safe integer",
  );
  await expect(message?.touch({ leaseMs: 0 })).rejects.toThrow("touch.leaseMs must be a safe integer");
  expect(await message?.ack()).toBe(true);
  await expect(q.dlq({ limit: Number.NaN })).rejects.toThrow("dlq.limit must be a safe integer");
});

test("tenant isolation keeps queues separated", async () => {
  const q = queue({
    id: "tenant",
    prefix: "test:q",
  });

  await q.send({ tenantId: "t1", data: { value: "a" } });
  await q.send({ tenantId: "t2", data: { value: "b" } });

  const m1 = await q.recv({ tenantId: "t1", wait: false });
  const m2 = await q.recv({ tenantId: "t2", wait: false });

  expect(m1?.data.value).toBe("a");
  expect(m2?.data.value).toBe("b");
  expect(await m1?.ack()).toBe(true);
  expect(await m2?.ack()).toBe(true);
});

test("idempotency key expires after ttl", async () => {
  const q = queue({
    id: "idempotency-ttl",
    prefix: "test:q",
  });

  const a = await q.send({ data: { v: 1 }, idempotencyKey: "k", idempotencyTtlMs: 40 });
  await Bun.sleep(60);
  const b = await q.send({ data: { v: 1 }, idempotencyKey: "k", idempotencyTtlMs: 40 });

  expect(a.messageId).not.toBe(b.messageId);

  const first = await q.recv({ wait: false });
  const second = await q.recv({ wait: false });
  expect(first).not.toBeNull();
  expect(second).not.toBeNull();
  expect(await first?.ack()).toBe(true);
  expect(await second?.ack()).toBe(true);
});

test("parallel recv claims each message exactly once", async () => {
  const q = queue({
    id: "parallel-recv",
    prefix: "test:q",
  });

  const count = 20;
  for (let i = 0; i < count; i++) {
    await q.send({ data: { idx: i } });
  }

  const r1 = q.reader();
  const r2 = q.reader();
  const collected: number[] = [];

  const drain = async (reader: typeof r1): Promise<void> => {
    while (true) {
      const msg = await reader.recv({ wait: false });
      if (!msg) break;
      collected.push(msg.data.idx);
      await msg.ack();
    }
  };

  await Promise.all([drain(r1), drain(r2)]);

  expect(collected.length).toBe(count);
  expect(new Set(collected).size).toBe(count);
});

test("parallel send with same idempotency key deduplicates", async () => {
  const q = queue({
    id: "parallel-idem",
    prefix: "test:q",
  });

  const results = await Promise.all(
    Array.from({ length: 10 }, () =>
      q.send({ data: { v: 1 }, idempotencyKey: "same-key" }),
    ),
  );

  const ids = new Set(results.map((r) => r.messageId));
  expect(ids.size).toBe(1);

  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(await msg?.ack()).toBe(true);
  expect(await q.recv({ wait: false })).toBeNull();
});

test("double ack returns false on second call", async () => {
  const q = queue({
    id: "double-ack",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  expect(await msg?.ack()).toBe(true);
  expect(await msg?.ack()).toBe(false);
});

test("double nack returns false on second call", async () => {
  const q = queue({
    id: "double-nack",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  expect(await msg?.nack()).toBe(true);
  expect(await msg?.nack()).toBe(false);

  // Message was requeued by first nack, consume it
  const requeued = await q.recv({ wait: false });
  expect(requeued).not.toBeNull();
  expect(await requeued?.ack()).toBe(true);
});

test("ack after lease expiry and maintenance returns false", async () => {
  const q = queue({
    id: "ack-expired",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false, leaseMs: 30 });
  expect(msg).not.toBeNull();

  // Wait for lease to expire
  await Bun.sleep(60);

  // Trigger maintenance by calling recv (non-blocking forces maintenance)
  // This cleans up the expired delivery and requeues the message
  const requeued = await q.recv({ wait: false });
  expect(requeued).not.toBeNull();
  expect(requeued?.messageId).toBe(msg?.messageId);

  // Now the original delivery was cleaned up by maintenance — ack fails
  expect(await msg?.ack()).toBe(false);

  expect(await requeued?.ack()).toBe(true);
});

test("nack with delay requeues after delay elapses", async () => {
  const q = queue({
    id: "nack-delay",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  expect(await msg?.nack({ delayMs: 80 })).toBe(true);

  // Not available immediately
  expect(await q.recv({ wait: false })).toBeNull();

  await Bun.sleep(100);

  // Available after delay
  const delayed = await q.recv({ wait: false });
  expect(delayed).not.toBeNull();
  expect(delayed?.messageId).toBe(msg?.messageId);
  expect(await delayed?.ack()).toBe(true);
});

test("payload exceeding size limit is rejected", async () => {
  const q = queue({
    id: "payload-limit",
    prefix: "test:q",
    limits: { payloadBytes: 64 },
  });

  let thrown: unknown = null;
  try {
    await q.send({ data: { data: "x".repeat(200) } });
  } catch (error) {
    thrown = error;
  }

  expect(thrown).toBeInstanceOf(Error);
  expect((thrown as Error).message).toContain("payload exceeds limit");
});

test("FIFO ordering is preserved", async () => {
  const q = queue({
    id: "fifo",
    prefix: "test:q",
  });

  for (let i = 0; i < 10; i++) {
    await q.send({ data: { seq: i } });
  }

  const received: number[] = [];
  for await (const msg of q.stream({ wait: false })) {
    received.push(msg.data.seq);
    await msg.ack();
  }

  expect(received).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
});

test("meta is preserved through send/recv", async () => {
  const q = queue({
    id: "meta",
    prefix: "test:q",
  });

  await q.send({
    data: { v: 1 },
    meta: { source: "test", traceId: "abc-123" },
  });

  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();
  expect(msg?.meta?.source).toBe("test");
  expect(msg?.meta?.traceId).toBe("abc-123");
  expect(await msg?.ack()).toBe(true);
});

test("message moves to dlq after max deliveries", async () => {
  const q = queue({
    id: "dlq",
    prefix: "test:q",
    delivery: { maxDeliveries: 2 },
  });

  await q.send({ data: { value: "x" } });

  const first = await q.recv({ wait: false });
  expect(first).not.toBeNull();
  expect(await first?.nack()).toBe(true);

  const second = await q.recv({ wait: false });
  expect(second).not.toBeNull();
  expect(await second?.nack()).toBe(true);

  expect(await q.recv({ wait: false })).toBeNull();

  const dlqKey = "test:q:default:dlq:dlq";
  const dlqRaw = await redis.hget(dlqKey, first!.messageId);
  expect(typeof dlqRaw).toBe("string");
});

// ==========================
// Race condition tests
// ==========================

test("concurrent ack and nack on same delivery — only one succeeds", async () => {
  const q = queue({
    id: "race-ack-nack",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  const [ackResult, nackResult] = await Promise.all([msg!.ack(), msg!.nack()]);

  // Exactly one should succeed
  expect(ackResult !== nackResult).toBe(true);

  if (nackResult) {
    // Nack won — message should be requeued
    const requeued = await q.recv({ wait: false });
    expect(requeued).not.toBeNull();
    expect(await requeued?.ack()).toBe(true);
  } else {
    // Ack won — queue should be empty
    expect(await q.recv({ wait: false })).toBeNull();
  }
});

test("nack delay exceeding maxMessageAgeMs sends message to DLQ", async () => {
  const q = queue({
    id: "nack-age-dlq",
    prefix: "test:q",
    limits: { maxMessageAgeMs: 80 },
  });

  await q.send({ data: { v: 1 } });
  const msg = await q.recv({ wait: false });
  expect(msg).not.toBeNull();

  // Nack with delay longer than maxMessageAgeMs
  expect(await msg?.nack({ delayMs: 200 })).toBe(true);

  // Wait for message to age past maxMessageAgeMs while in delayed set
  await Bun.sleep(250);

  // Trigger maintenance — message is now older than maxMessageAgeMs
  const result = await q.recv({ wait: false });
  expect(result).toBeNull();

  // Verify message landed in DLQ
  const dlqKey = "test:q:default:nack-age-dlq:dlq";
  const dlqRaw = await redis.hget(dlqKey, msg!.messageId);
  expect(dlqRaw).not.toBeNull();
  const dlqEntry = JSON.parse(dlqRaw!);
  expect(dlqEntry.reason).toBe("expired");
});

test("an expired ready message moves to the DLQ instead of being claimed", async () => {
  const q = queue<{ v: number }>({
    id: "ready-age-dlq",
    prefix: "test:q",
    limits: { maxMessageAgeMs: 50 },
  });

  await q.send({ data: { v: 1 } });
  await Bun.sleep(80);

  expect(await q.recv({ wait: false })).toBeNull();
  const entries = await q.dlq();
  expect(entries.map((entry) => entry.reason)).toEqual(["expired"]);
  expect(await redis.send("LLEN", ["test:q:default:ready-age-dlq:active"])).toBe(0);
  expect(await redis.send("HLEN", ["test:q:default:ready-age-dlq:messages"])).toBe(0);
});

test("recv drains more than one expired claim batch before returning a valid message", async () => {
  const q = queue<{ v: number }>({
    id: "ready-age-batch",
    prefix: "test:q",
    limits: { maxMessageAgeMs: 50 },
  });

  for (let i = 0; i < 33; i++) {
    await q.send({ data: { v: i } });
  }
  await Bun.sleep(80);
  await q.send({ data: { v: 99 } });

  const valid = await q.recv({ wait: false });
  expect(valid?.data).toEqual({ v: 99 });
  expect((await q.dlq()).length).toBe(33);
  expect(await valid?.ack()).toBe(true);
});

test("blocking recv unblocks when message is sent", async () => {
  const q = queue({
    id: "blocking-recv",
    prefix: "test:q",
  });

  const reader = q.reader();
  const recvPromise = reader.recv({ wait: true, timeoutMs: 5_000 });

  // Give the blocking client time to connect and start waiting
  await Bun.sleep(50);

  await q.send({ data: { v: 42 } });

  const msg = await recvPromise;
  expect(msg).not.toBeNull();
  expect(msg?.data.v).toBe(42);
  expect(await msg?.ack()).toBe(true);
});

test("maintenance requeue + second consumer recv vs original ack", async () => {
  const q = queue({
    id: "race-maintenance-ack",
    prefix: "test:q",
  });

  await q.send({ data: { v: 1 } });
  const original = await q.recv({ wait: false, leaseMs: 30 });
  expect(original).not.toBeNull();

  // Wait for lease to expire
  await Bun.sleep(60);

  // Second consumer triggers maintenance and picks up the requeued message
  const reader2 = q.reader();
  const requeued = await reader2.recv({ wait: false });
  expect(requeued).not.toBeNull();
  expect(requeued?.messageId).toBe(original?.messageId);

  // Now both consumers race: original tries late ack, requeued tries ack
  const [originalAck, requeuedAck] = await Promise.all([
    original!.ack(),
    requeued!.ack(),
  ]);

  // Original delivery was cleaned up by maintenance — should fail
  expect(originalAck).toBe(false);
  // Requeued delivery is valid — should succeed
  expect(requeuedAck).toBe(true);

  // Queue should now be empty
  expect(await q.recv({ wait: false })).toBeNull();
});

// ==========================
// Payload fidelity (opaque JSON)
// ==========================

const AWKWARD = {
  empty: [] as number[],
  emptyObj: {},
  unicode: "日本😀",
  big: 9007199254740991,
  nested: [[], {}] as unknown[],
  nul: null,
  deep: { list: [1, [], { inner: [] }] },
};

test("payload round-trips byte-equivalent JSON across claim and redelivery", async () => {
  const q = queue<typeof AWKWARD>({
    id: "opaque-roundtrip",
    prefix: "test:q",
  });

  await q.send({ data: AWKWARD, meta: { tags: [], n: 9007199254740991 } });

  // First claim runs the message through the Lua re-encode path once.
  const first = await q.recv({ wait: false });
  expect(first?.data).toEqual(AWKWARD);
  expect(first?.meta).toEqual({ tags: [], n: 9007199254740991 });
  expect(await first?.nack()).toBe(true);

  // Redelivery runs it through a second time; a lossy encode compounds here.
  const second = await q.recv({ wait: false });
  expect(second?.attempt).toBe(2);
  expect(second?.data).toEqual(AWKWARD);
  expect(second?.meta).toEqual({ tags: [], n: 9007199254740991 });
  expect(Array.isArray(second?.data.empty)).toBe(true);
  expect(await second?.ack()).toBe(true);
});

test("dead-lettered payload keeps the original JSON shape", async () => {
  const q = queue<typeof AWKWARD>({
    id: "opaque-dlq",
    prefix: "test:q",
    delivery: { maxDeliveries: 1 },
  });

  await q.send({ data: AWKWARD });
  const message = await q.recv({ wait: false });
  expect(await message?.nack()).toBe(true); // settled by moving it to the DLQ
  expect(await q.recv({ wait: false })).toBeNull(); // not requeued

  const raw = await redis.send("HGETALL", ["test:q:default:opaque-dlq:dlq"]);
  const entries = Object.values(raw as Record<string, string>);
  expect(entries.length).toBe(1);
  const dlq = JSON.parse(entries[0]!) as { dataJson: string; reason: string };
  expect(dlq.reason).toBe("max_deliveries_exceeded");
  expect(JSON.parse(dlq.dataJson)).toEqual(AWKWARD);
});

test("messages written in the 5.8.0 record format are still delivered", async () => {
  const q = queue<{ tag: string }>({
    id: "legacy-record",
    prefix: "test:q",
  });

  // Exactly what <= 5.8.0 wrote: decoded `data`/`meta`, no `v`, no `dataJson`.
  const legacy = JSON.stringify({
    data: { tag: "from-5.8.0" },
    attempt: 0,
    meta: { source: "legacy" },
    enqueuedAt: Date.now(),
  });
  await redis.send("HSET", ["test:q:default:legacy-record:messages", "9001", legacy]);
  await redis.send("LPUSH", ["test:q:default:legacy-record:ready", "9001"]);

  const message = await q.recv({ wait: false });
  expect(message?.messageId).toBe("9001");
  expect(message?.data).toEqual({ tag: "from-5.8.0" });
  expect(message?.meta).toEqual({ source: "legacy" });
  expect(message?.attempt).toBe(1);

  // The record is upgraded in place, so redelivery reads the new format.
  expect(await message?.nack()).toBe(true);
  const again = await q.recv({ wait: false });
  expect(again?.data).toEqual({ tag: "from-5.8.0" });
  expect(again?.attempt).toBe(2);
  expect(await again?.ack()).toBe(true);
});

test("the unimplemented ordering mode is rejected instead of silently ignored", () => {
  expect(() =>
    queue({ id: "ordering-reject", prefix: "test:q", ordering: { mode: "ordering_key_partitioned" } }),
  ).toThrow(/ordering_key_partitioned/);

  // The implemented mode and a bare orderingKey stay accepted.
  expect(() => queue({ id: "ordering-ok", prefix: "test:q", ordering: { mode: "best_effort" } })).not.toThrow();
});

test("recv records the consumerId on the delivery record", async () => {
  const q = queue<{ v: number }>({ id: "consumer-id", prefix: "test:q" });

  await q.send({ data: { v: 1 } });
  const message = await q.recv({ wait: false, consumerId: "worker-7" });
  expect(message).not.toBeNull();

  const raw = await redis.send("HGET", ["test:q:default:consumer-id:deliveries", message!.deliveryId]);
  expect(JSON.parse(raw as string).consumerId).toBe("worker-7");

  expect(await message?.ack()).toBe(true);
});

test("dlq entries are readable, drainable and bounded per entry", async () => {
  const q = queue<{ v: number }>({
    id: "dlq-api",
    prefix: "test:q",
    delivery: { maxDeliveries: 1 },
    limits: { dlqRetentionMs: 60_000 },
  });

  await q.send({ data: { v: 1 } });
  const first = await q.recv({ wait: false });
  await first?.nack({ error: "boom" });

  await q.send({ data: { v: 2 } });
  const second = await q.recv({ wait: false });
  await second?.nack();

  const entries = await q.dlq();
  expect(entries.length).toBe(2);
  expect(entries.map((e) => e.data.v)).toEqual([1, 2]); // oldest first
  expect(entries[0]?.reason).toBe("max_deliveries_exceeded");
  expect(entries[0]?.lastError).toBe("boom");
  expect(entries[0]?.attempts).toBe(1);

  expect(await q.dlqRemove({ messageId: entries[0]!.messageId })).toBe(true);
  expect(await redis.send("ZSCORE", ["test:q:default:dlq-api:dlq:index", entries[0]!.messageId])).toBeNull();
  expect(await q.dlqRemove({ messageId: entries[0]!.messageId })).toBe(false);
  expect((await q.dlq()).length).toBe(1);
});

test("dlq reads purge expired hash and index entries without a later failure", async () => {
  const q = queue<{ v: number }>({
    id: "dlq-read-retention",
    prefix: "test:q",
    delivery: { maxDeliveries: 1 },
    limits: { dlqRetentionMs: 80 },
  });

  await q.send({ data: { v: 1 } });
  await (await q.recv({ wait: false }))?.nack();
  expect((await q.dlq()).length).toBe(1);

  await Bun.sleep(120);

  expect(await q.dlq()).toEqual([]);
  expect(await redis.send("HLEN", ["test:q:default:dlq-read-retention:dlq"])).toBe(0);
  expect(await redis.send("ZCARD", ["test:q:default:dlq-read-retention:dlq:index"])).toBe(0);
});

test("dlq retention drops entries older than the window without dropping fresh ones", async () => {
  const q = queue<{ v: number }>({
    id: "dlq-retention",
    prefix: "test:q",
    delivery: { maxDeliveries: 1 },
    limits: { dlqRetentionMs: 120 },
  });

  await q.send({ data: { v: 1 } });
  await (await q.recv({ wait: false }))?.nack();
  expect((await q.dlq()).length).toBe(1);

  await Bun.sleep(200);

  // A later failure must not keep the stale entry alive, and must survive itself.
  await q.send({ data: { v: 2 } });
  await (await q.recv({ wait: false }))?.nack();

  const entries = await q.dlq();
  expect(entries.length).toBe(1);
  expect(entries[0]?.data).toEqual({ v: 2 });
});

test("dead letters written in the 5.8.0 format are still listed", async () => {
  const q = queue<{ tag: string }>({ id: "dlq-legacy", prefix: "test:q" });

  await redis.send("HSET", [
    "test:q:default:dlq-legacy:dlq",
    "4242",
    JSON.stringify({
      messageId: "4242",
      data: { tag: "old" },
      meta: { source: "legacy" },
      attempts: 3,
      movedAt: Date.now(),
      reason: "expired",
    }),
  ]);

  const entries = await q.dlq();
  expect(entries.length).toBe(1);
  expect(entries[0]?.data).toEqual({ tag: "old" });
  expect(entries[0]?.meta).toEqual({ source: "legacy" });
  expect(entries[0]?.reason).toBe("expired");
});

test("dlq reads reconcile a stale index without hiding an unindexed legacy entry", async () => {
  const q = queue<{ tag: string }>({ id: "dlq-stale-index", prefix: "test:q" });
  const dlqKey = "test:q:default:dlq-stale-index:dlq";
  const indexKey = `${dlqKey}:index`;

  await redis.send("HSET", [
    dlqKey,
    "valid",
    JSON.stringify({
      messageId: "valid",
      data: { tag: "kept" },
      attempts: 1,
      movedAt: Date.now(),
      reason: "failed",
    }),
  ]);
  await redis.send("ZADD", [indexKey, String(Date.now() - 1), "missing"]);

  const entries = await q.dlq({ limit: 1 });
  expect(entries.map((entry) => entry.messageId)).toEqual(["valid"]);
  expect(await redis.send("ZSCORE", [indexKey, "missing"])).toBeNull();
  expect(await redis.send("ZSCORE", [indexKey, "valid"])).not.toBeNull();
});
