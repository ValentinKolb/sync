# mutex

Distributed lock with retry, TTL auto-expiry, and owner-only release. Same API on server and browser.

## Factory

```ts
import { mutex, LockError } from "@k2b/sync";

const m = mutex({
  id: "checkout",
  defaultTtl: 5_000,
  // retryCount?: number,   // default: 10
  // retryDelay?: number,   // default: 200 (ms between retries)
  // prefix?: string,       // default: "sync:mutex"
  // store?: Store,         // browser only (additive)
});
```

## API

```ts
type Lock = { /* opaque handle — pass to extend/release */ };

type Mutex = {
  id: string;
  acquire(resource: string, ttlMs?: number): Promise<Lock | null>;
  extend(lock: Lock, ttlMs?: number): Promise<boolean>;
  release(lock: Lock): Promise<void>;

  withLock<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttlMs?: number): Promise<T | null>;
  withLockOrThrow<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttlMs?: number): Promise<T>;
};
```

## Usage

```ts
// Auto acquire/release
const result = await m.withLock("order:123", async (lock) => {
  await m.extend(lock, 10_000); // extend if work takes longer
  return await processOrder();
});
// Returns null if lock couldn't be acquired (after retries)

// Throw on failure
await m.withLockOrThrow("order:123", async () => {
  await doExclusiveWork();
});

// Manual acquire/release
const lock = await m.acquire("order:123");
if (lock) {
  try { await work(); } finally { await m.release(lock); }
}
```

## Gotchas

- **Owner-only release**: `release(lock)` only succeeds if the lock hasn't been taken over via TTL expiry by someone else. Safe to call at end of try/finally.
- **TTL auto-expires**: if the holder crashes, TTL expires and the lock is free. Don't rely on `release` alone for cleanup.
- **Extend during long work**: if your critical section might exceed `ttlMs`, periodically call `extend` to prevent premature takeover.
- **`retryCount: 0`** disables retry entirely — useful for leader election loops (see scheduler).
- `withLock` returns `null` on failure; `withLockOrThrow` throws `LockError`.
- During browser rolling upgrades, the current lock also holds the legacy Store
  key. This prevents an old bundle from entering the same critical section; two
  identities that collided under the old layout may therefore serialize until
  the legacy bundle is retired.

## Redis keys (server)

- `{prefix}:{id}:{resource}` — string with owner-token value + TTL
