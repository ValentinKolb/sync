# API

## Browser

```ts
import { mutex, LockError } from "@valentinkolb/sync/browser";

const m = mutex({
  id: "checkout",
  // store: new MemoryStore(), // default
  // retryCount: 10,
  // retryDelay: 200,
  // defaultTtl: 10_000,
});
```

Same types and API. Additional config: `store?: Store`. Lock state is in-memory, single-tab only. No Lua scripts — JS single-threading provides atomicity.

---

## Factory

```ts
import { mutex, LockError } from "@valentinkolb/sync";

const m = mutex({
  id: "checkout",
  // prefix: "sync:mutex",
  // retryCount: 10,
  // retryDelay: 200,
  // defaultTtl: 10_000,
});
```

## Types

```ts
type MutexConfig = {
  id: string;
  prefix?: string; // default: "sync:mutex"
  retryCount?: number; // default: 10
  retryDelay?: number; // default: 200ms (+0..100ms jitter)
  defaultTtl?: number; // default: 10000ms
};

type Lock = {
  resource: string; // full Redis key
  value: string; // random owner token
  ttl: number;
  expiration: number; // Date.now() + ttl at acquire/extend time
};

type Mutex = {
  id: string;
  acquire(resource: string, ttl?: number): Promise<Lock | null>;
  release(lock: Lock): Promise<void>;
  extend(lock: Lock, ttl?: number): Promise<boolean>;
  withLock<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T | null>;
  withLockOrThrow<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T>;
};
```

## Error

`LockError extends Error`

- thrown by `withLockOrThrow()` when acquire fails after retries.
- contains `resource` (original resource string).

## Config Options

- `id`: required namespace.
- `prefix`: key prefix override.
- `retryCount`: number of retries after first attempt (`attempts = retryCount + 1`).
- `retryDelay`: base wait between retries in ms.
- `defaultTtl`: lease TTL in ms for acquire/extend when not overridden.

## Usage Patterns

### Strict critical section

```ts
await m.withLockOrThrow(`order:${orderId}`, async (lock) => {
  await processOrder(orderId);
  if (needsMoreTime()) await m.extend(lock, 15_000);
});
```

### Optional lock

```ts
const result = await m.withLock(`batch:${key}`, async () => runBatch(key));
if (result === null) {
  // someone else is processing it
}
```

### Manual acquire/release

```ts
const lock = await m.acquire("resource:1", 5000);
if (!lock) return;
try {
  await doWork();
} finally {
  await m.release(lock);
}
```

## Redis Keys

Pattern: `{prefix}:{id}:{resource}`

- resources longer than 128 chars are hashed: `{prefix}:{id}:hash:{sha256}`.

## Operational Notes

- Pick TTL > expected critical section time (or heartbeat with `extend`).
- Treat `extend()` returning `false` as lock loss; stop exclusive work.
