import { test, expect, beforeEach } from "bun:test";
import { redis } from "bun";
import { mutex } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

beforeEach(async () => {
  const keys = await redis.send("KEYS", ["test:fmx:*"]);
  if (Array.isArray(keys) && keys.length > 0) {
    await redis.send("DEL", keys as string[]);
  }
});

// ==========================
// Extend vs. expiry timing
// ==========================

test("extend succeeds when called just before TTL expiry", async () => {
  const m = mutex({ id: uid("extend-edge"), prefix: "test:fmx", retryCount: 0, defaultTtl: 150 });

  const lock = await m.acquire("res");
  expect(lock).not.toBeNull();

  // Wait until close to expiry but not past it
  await Bun.sleep(120);

  const extended = await m.extend(lock!, 500);
  expect(extended).toBe(true);

  // Lock should still be held after original TTL would have expired
  await Bun.sleep(100);
  const competitor = await m.acquire("res");
  expect(competitor).toBeNull();

  await m.release(lock!);
});

test("extend fails after TTL expires — competitor can acquire", async () => {
  const m = mutex({ id: uid("extend-expired"), prefix: "test:fmx", retryCount: 0, defaultTtl: 80 });

  const lock = await m.acquire("res");
  expect(lock).not.toBeNull();

  await Bun.sleep(120);

  const extended = await m.extend(lock!, 500);
  expect(extended).toBe(false);

  // Competitor should succeed
  const competitor = await m.acquire("res");
  expect(competitor).not.toBeNull();
  await m.release(competitor!);
});

test("extend by original owner fails after competitor takes over expired lock", async () => {
  const m = mutex({ id: uid("extend-takeover"), prefix: "test:fmx", retryCount: 0, defaultTtl: 80 });

  const original = await m.acquire("res");
  expect(original).not.toBeNull();

  await Bun.sleep(120);

  // Competitor acquires
  const competitor = await m.acquire("res");
  expect(competitor).not.toBeNull();

  // Original owner tries to extend — must fail (different value in Redis)
  const extended = await m.extend(original!, 500);
  expect(extended).toBe(false);

  // Competitor's lock is unaffected
  const stillHeld = await m.extend(competitor!, 200);
  expect(stillHeld).toBe(true);

  await m.release(competitor!);
});

// ==========================
// Owner-token correctness
// ==========================

test("spoofed release with wrong token does not affect real lock holder", async () => {
  const m = mutex({ id: uid("spoof-release"), prefix: "test:fmx", retryCount: 0 });

  const real = await m.acquire("res");
  expect(real).not.toBeNull();

  // Attempt release with fabricated token
  const fake = { ...real!, value: "aaaa" + real!.value.slice(4) };
  await m.release(fake);

  // Real lock should still be held
  const attempt = await m.acquire("res");
  expect(attempt).toBeNull();

  await m.release(real!);
});

test("spoofed extend with wrong token does not prolong lock", async () => {
  const m = mutex({ id: uid("spoof-extend"), prefix: "test:fmx", retryCount: 0, defaultTtl: 80 });

  const real = await m.acquire("res");
  expect(real).not.toBeNull();

  const fake = { ...real!, value: "zzzz" + real!.value.slice(4) };
  const fakeExtend = await m.extend(fake, 5_000);
  expect(fakeExtend).toBe(false);

  // Lock should still expire at original TTL
  await Bun.sleep(120);
  const competitor = await m.acquire("res");
  expect(competitor).not.toBeNull();
  await m.release(competitor!);
});

// ==========================
// Rapid contention
// ==========================

test("rapid acquire/release cycles do not leak locks", async () => {
  const m = mutex({ id: uid("rapid-cycle"), prefix: "test:fmx", retryCount: 0 });

  for (let i = 0; i < 50; i++) {
    const lock = await m.acquire("res");
    expect(lock).not.toBeNull();
    await m.release(lock!);
  }

  // Lock should be acquirable at the end
  const final = await m.acquire("res");
  expect(final).not.toBeNull();
  await m.release(final!);
});

test("concurrent contenders — each acquire/release cycle is mutually exclusive", async () => {
  const m = mutex({ id: uid("contention"), prefix: "test:fmx", retryCount: 20, retryDelay: 10, defaultTtl: 2_000 });

  let concurrentCount = 0;
  let maxConcurrent = 0;
  const iterations = 10;

  const work = async (): Promise<void> => {
    for (let i = 0; i < 3; i++) {
      const lock = await m.acquire("critical");
      if (!lock) continue;
      concurrentCount += 1;
      maxConcurrent = Math.max(maxConcurrent, concurrentCount);
      await Bun.sleep(5);
      concurrentCount -= 1;
      await m.release(lock);
    }
  };

  await Promise.all(Array.from({ length: iterations }, () => work()));

  // Mutual exclusion invariant: never more than 1 concurrent holder
  expect(maxConcurrent).toBe(1);
});

test("many concurrent acquirers — exactly one wins each round", async () => {
  const m = mutex({ id: uid("many-contenders"), prefix: "test:fmx", retryCount: 0 });

  for (let round = 0; round < 5; round++) {
    const results = await Promise.all(
      Array.from({ length: 20 }, () => m.acquire("contested")),
    );

    const winners = results.filter((r) => r !== null);
    expect(winners.length).toBe(1);
    await m.release(winners[0]!);
  }
});

// ==========================
// Extend race with competitor
// ==========================

test("extend and competitor acquire race — lock owner is consistent", async () => {
  const m = mutex({ id: uid("extend-race"), prefix: "test:fmx", retryCount: 0, defaultTtl: 60 });

  const holder = await m.acquire("res");
  expect(holder).not.toBeNull();

  // Wait until just past expiry boundary
  await Bun.sleep(70);

  // Race: holder extends vs competitor acquires
  const [extendResult, competitorLock] = await Promise.all([
    m.extend(holder!, 500),
    m.acquire("res"),
  ]);

  // Exactly one should succeed
  if (extendResult) {
    expect(competitorLock).toBeNull();
    await m.release(holder!);
  } else {
    expect(competitorLock).not.toBeNull();
    await m.release(competitorLock!);
  }
});

// ==========================
// withLock under contention
// ==========================

test("withLock serializes increments even under high contention", async () => {
  const m = mutex({ id: uid("serial"), prefix: "test:fmx", retryCount: 30, retryDelay: 10, defaultTtl: 3_000 });

  const key = "test:fmx:counter";
  await redis.set(key, "0");

  const increment = async (): Promise<void> => {
    await m.withLock("counter", async () => {
      const val = Number(await redis.get(key) ?? "0");
      await Bun.sleep(2);
      await redis.set(key, String(val + 1));
    });
  };

  const n = 10;
  await Promise.all(Array.from({ length: n }, () => increment()));

  const final = Number(await redis.get(key) ?? "0");
  expect(final).toBe(n);
});
