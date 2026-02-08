import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { queue, QueueValidationError } from "../index";

const testSchema = z.object({
  message: z.string(),
});

// Clean up Redis before each test
beforeEach(async () => {
  const keys = await redis.send("KEYS", ["queue:test:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

test("send adds a message to the queue", async () => {
  const q = queue.create({
    name: "test:send",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.send({ message: "hello" });

  const size = await q.size();
  expect(size).toBe(1);
});

test("send validates data against schema", async () => {
  const q = queue.create({
    name: "test:validate",
    schema: testSchema,
    prefix: "queue:test",
  });

  try {
    // @ts-expect-error - intentionally invalid
    await q.send({ invalid: "data" });
    expect(true).toBe(false);
  } catch (e) {
    expect(e).toBeInstanceOf(QueueValidationError);
  }
});

test("sendBatch adds multiple messages atomically", async () => {
  const q = queue.create({
    name: "test:batch",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.sendBatch([{ message: "one" }, { message: "two" }, { message: "three" }]);

  const size = await q.size();
  expect(size).toBe(3);
});

test("sendBatch validates all messages", async () => {
  const q = queue.create({
    name: "test:batch-validate",
    schema: testSchema,
    prefix: "queue:test",
  });

  try {
    // @ts-expect-error - intentionally invalid
    await q.sendBatch([{ message: "valid" }, { invalid: "data" }]);
    expect(true).toBe(false);
  } catch (e) {
    expect(e).toBeInstanceOf(QueueValidationError);
  }

  // Nothing should have been added (validation fails before push)
  const size = await q.size();
  expect(size).toBe(0);
});

test("sendBatch with empty array is a no-op", async () => {
  const q = queue.create({
    name: "test:batch-empty",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.sendBatch([]);

  const size = await q.size();
  expect(size).toBe(0);
});

test("process consumes messages in FIFO order", async () => {
  const q = queue.create({
    name: "test:fifo",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.send({ message: "first" });
  await q.send({ message: "second" });
  await q.send({ message: "third" });

  const processed: string[] = [];

  const stop = q.process(
    async (data) => {
      processed.push(data.message);
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  stop();

  expect(processed).toEqual(["first", "second", "third"]);
});

test("process calls onSuccess on successful handling", async () => {
  const q = queue.create({
    name: "test:onsuccess",
    schema: testSchema,
    prefix: "queue:test",
  });

  let successData: unknown = null;

  await q.send({ message: "success" });

  const stop = q.process(async () => {}, {
    pollInterval: 50,
    onSuccess: (data) => {
      successData = data;
    },
  });

  await Bun.sleep(200);
  stop();

  expect(successData).toEqual({ message: "success" });
});

test("process calls onError when handler throws", async () => {
  const q = queue.create({
    name: "test:onerror",
    schema: testSchema,
    prefix: "queue:test",
  });

  let errorMessage = "";
  let errorData: unknown = null;

  await q.send({ message: "fail" });

  const stop = q.process(
    async () => {
      throw new Error("handler failed");
    },
    {
      pollInterval: 50,
      onError: (data, error) => {
        errorData = data;
        errorMessage = error.message;
      },
    },
  );

  await Bun.sleep(200);
  stop();

  expect(errorData).toEqual({ message: "fail" });
  expect(errorMessage).toBe("handler failed");
});

test("failed messages are consumed (no retry)", async () => {
  const q = queue.create({
    name: "test:no-retry",
    schema: testSchema,
    prefix: "queue:test",
  });

  let attempts = 0;

  await q.send({ message: "fail" });

  const stop = q.process(
    async () => {
      attempts++;
      throw new Error("fail");
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  stop();

  expect(attempts).toBe(1);
  const size = await q.size();
  expect(size).toBe(0);
});

test("concurrent processing with multiple consumers", async () => {
  const q = queue.create({
    name: "test:concurrent",
    schema: testSchema,
    prefix: "queue:test",
  });

  const processed: string[] = [];

  for (let i = 0; i < 5; i++) {
    await q.send({ message: `msg${i}` });
  }

  const stop = q.process(
    async (data) => {
      await Bun.sleep(50);
      processed.push(data.message);
    },
    { concurrency: 3, pollInterval: 50 },
  );

  await Bun.sleep(400);
  stop();

  expect(processed.length).toBe(5);
  // All messages should be unique (no duplicates)
  expect(new Set(processed).size).toBe(5);
});

test("size returns the number of messages", async () => {
  const q = queue.create({
    name: "test:size",
    schema: testSchema,
    prefix: "queue:test",
  });

  expect(await q.size()).toBe(0);

  await q.send({ message: "one" });
  await q.send({ message: "two" });

  expect(await q.size()).toBe(2);
});

test("purge removes all messages", async () => {
  const q = queue.create({
    name: "test:purge",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.send({ message: "one" });
  await q.send({ message: "two" });
  await q.send({ message: "three" });

  expect(await q.size()).toBe(3);

  await q.purge();

  expect(await q.size()).toBe(0);
});

test("sendBatch preserves FIFO order", async () => {
  const q = queue.create({
    name: "test:batch-fifo",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q.sendBatch([{ message: "first" }, { message: "second" }, { message: "third" }]);

  const processed: string[] = [];

  const stop = q.process(
    async (data) => {
      processed.push(data.message);
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(300);
  stop();

  expect(processed).toEqual(["first", "second", "third"]);
});

test("multiple queues are isolated", async () => {
  const q1 = queue.create({
    name: "test:iso-a",
    schema: testSchema,
    prefix: "queue:test",
  });

  const q2 = queue.create({
    name: "test:iso-b",
    schema: testSchema,
    prefix: "queue:test",
  });

  await q1.send({ message: "from-a" });
  await q2.send({ message: "from-b" });

  expect(await q1.size()).toBe(1);
  expect(await q2.size()).toBe(1);

  const processed1: string[] = [];
  const processed2: string[] = [];

  const stop1 = q1.process(
    async (data) => {
      processed1.push(data.message);
    },
    { pollInterval: 50 },
  );
  const stop2 = q2.process(
    async (data) => {
      processed2.push(data.message);
    },
    { pollInterval: 50 },
  );

  await Bun.sleep(200);
  stop1();
  stop2();

  expect(processed1).toEqual(["from-a"]);
  expect(processed2).toEqual(["from-b"]);
});

test("default prefix works", async () => {
  // Clean up default prefix keys
  const existingKeys = await redis.send("KEYS", ["queue:test-default*"]);
  if (Array.isArray(existingKeys) && existingKeys.length > 0) {
    await redis.send("DEL", existingKeys as string[]);
  }

  const q = queue.create({
    name: "test-default",
    schema: testSchema,
    // no prefix — should default to "queue"
  });

  await q.send({ message: "hello" });
  expect(await q.size()).toBe(1);

  await q.purge();
});

test("stop function stops processing", async () => {
  const q = queue.create({
    name: "test:stop",
    schema: testSchema,
    prefix: "queue:test",
  });

  let processCount = 0;

  const stop = q.process(
    async () => {
      processCount++;
    },
    { pollInterval: 50 },
  );

  stop();

  await q.send({ message: "after-stop" });
  await Bun.sleep(200);

  expect(processCount).toBe(0);
});
