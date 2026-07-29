import { expect, test } from "bun:test";
import { ephemeral, mutex, queue, ratelimit, topic } from "../index";
import { createMemoryStore } from "../src/store";

// README.md: "Multiple instances with the same id in the same tab share state
// via module-level maps." On the server every one of these modules is keyed
// purely by `{prefix}:{id}` in Redis, so two factory calls with the same id
// always meet. These tests construct two same-id handles with *default* config,
// which is the case that used to silently not share.

let counter = 0;
const uid = (label: string): string => `${label}-${++counter}-${Date.now()}`;

test("two same-id mutex handles are mutually exclusive", async () => {
  const id = uid("checkout");
  const a = mutex({ id });
  const b = mutex({ id });

  const held = await a.acquire("cart:42", 5_000);
  expect(held).not.toBeNull();

  // The single defining invariant of a mutex. It used to fail open.
  expect(await b.acquire("cart:42", 5_000)).toBeNull();

  await a.release(held!);
  const afterRelease = await b.acquire("cart:42", 5_000);
  expect(afterRelease).not.toBeNull();
  await b.release(afterRelease!);
});

test("two same-id ratelimit handles share one ceiling", async () => {
  const id = uid("sms");
  const a = ratelimit({ id, limit: 2, windowSecs: 60 });
  const b = ratelimit({ id, limit: 2, windowSecs: 60 });

  expect((await a.check("user:1")).limited).toBe(false);
  expect((await b.check("user:1")).limited).toBe(false);

  // N instances used to give N x limit, silently multiplying an abuse ceiling.
  expect((await b.check("user:1")).limited).toBe(true);
});

test("a producer and a consumer meet through two same-id queue handles", async () => {
  const id = uid("emails");
  const producer = queue<{ to: string }>({ id });
  const consumer = queue<{ to: string }>({ id });

  await producer.send({ data: { to: "a@example.com" } });

  const message = await consumer.recv({ wait: false });
  expect(message?.data).toEqual({ to: "a@example.com" });
  expect(await message?.ack()).toBe(true);
});

test("a publisher and a reader meet through two same-id topic handles", async () => {
  const id = uid("events");
  const publisher = topic<{ v: number }>({ id });
  const subscriber = topic<{ v: number }>({ id });

  const reader = subscriber.reader("workers");
  await publisher.pub({ data: { v: 7 } });

  const delivery = await reader.recv({ wait: false });
  expect(delivery?.data).toEqual({ v: 7 });
  await reader.close();
});

test("two same-id ephemeral handles see the same entries", async () => {
  const id = uid("presence");
  const a = ephemeral<{ status: string }>({ id, ttlMs: 60_000 });
  const b = ephemeral<{ status: string }>({ id, ttlMs: 60_000 });

  await a.upsert({ key: "user:1", value: { status: "online" } });

  const snap = await b.snapshot({});
  expect(snap.entries.map((e) => e.key)).toEqual(["user:1"]);
});

test("an explicit store scopes coordination to the handles sharing it", async () => {
  const id = uid("scoped");
  const shared = createMemoryStore();

  const a = mutex({ id, store: shared });
  const b = mutex({ id, store: shared });
  const isolated = mutex({ id, store: createMemoryStore() });

  const held = await a.acquire("r", 5_000);
  expect(held).not.toBeNull();
  expect(await b.acquire("r", 5_000)).toBeNull();

  // A handle given its own Store coordinates only with handles given that Store.
  const other = await isolated.acquire("r", 5_000);
  expect(other).not.toBeNull();

  await a.release(held!);
  await isolated.release(other!);
});
