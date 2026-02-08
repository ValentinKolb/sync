import { redis, sleep } from "bun";
import { createHash, randomBytes } from "crypto";

// ==========================
// Configuration
// ==========================

const DEFAULT_RETRY_COUNT = 10;
const DEFAULT_RETRY_DELAY = 200;
const DEFAULT_TTL = 10000;
const MAX_RESOURCE_LENGTH = 128;

const normalizeResource = (resource: string): string => {
  if (resource.length <= MAX_RESOURCE_LENGTH) return resource;
  const hash = createHash("sha256").update(resource).digest("hex");
  return `hash:${hash}`;
};

// ==========================
// Types
// ==========================

export type Lock = {
  resource: string;
  value: string;
  ttl: number;
  expiration: number;
};

export type MutexConfig = {
  /** Key prefix for Redis keys (default: "mutex") */
  prefix?: string;
  /** Number of retry attempts (default: 10) */
  retryCount?: number;
  /** Delay between retries in ms (default: 200) */
  retryDelay?: number;
  /** Default lock TTL in ms (default: 10000) */
  defaultTtl?: number;
};

export type Mutex = {
  /** Acquire a lock on a resource */
  acquire: (resource: string, ttl?: number) => Promise<Lock | null>;
  /** Release a lock */
  release: (lock: Lock) => Promise<void>;
  /** Acquire a lock and execute a function, releasing the lock afterward */
  withLock: <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number) => Promise<T | null>;
  /** Acquire a lock and execute a function, throwing if lock cannot be acquired */
  withLockOrThrow: <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number) => Promise<T>;
  /** Extend a lock's TTL */
  extend: (lock: Lock, ttl?: number) => Promise<boolean>;
};

// ==========================
// Lock Error
// ==========================

export class LockError extends Error {
  readonly resource: string;

  constructor(resource: string) {
    super(`Failed to acquire lock on resource: ${resource}`);
    this.name = "LockError";
    this.resource = resource;
  }
}

// ==========================
// Mutex Factory
// ==========================

/**
 * Create a mutex with the given configuration.
 * Uses Redis SET NX for distributed locking with automatic expiry.
 *
 * @example
 * ```ts
 * const m = mutex.create({ prefix: "myapp" });
 *
 * // Manual acquire/release
 * const lock = await m.acquire("resource:123");
 * if (lock) {
 *   try {
 *     // do work
 *   } finally {
 *     await m.release(lock);
 *   }
 * }
 *
 * // Or use withLock for automatic release
 * const result = await m.withLock("resource:123", async (lock) => {
 *   return await doSomething();
 * });
 *
 * // Throw if lock cannot be acquired
 * const result = await m.withLockOrThrow("resource:123", async () => {
 *   return await doSomething();
 * });
 * ```
 */
const create = (config: MutexConfig = {}): Mutex => {
  const {
    prefix = "mutex",
    retryCount = DEFAULT_RETRY_COUNT,
    retryDelay = DEFAULT_RETRY_DELAY,
    defaultTtl = DEFAULT_TTL,
  } = config;

  const acquire = async (resource: string, ttl: number = defaultTtl): Promise<Lock | null> => {
    const safeResource = normalizeResource(resource);
    const key = `${prefix}:${safeResource}`;
    const value = randomBytes(16).toString("hex");

    for (let attempt = 0; attempt <= retryCount; attempt++) {
      try {
        // Use SET with NX (only set if not exists) and PX (millisecond expiry)
        const result = await redis.send("SET", [key, value, "NX", "PX", ttl.toString()]);

        if (result === "OK") {
          return {
            resource: key,
            value,
            ttl,
            expiration: Date.now() + ttl,
          };
        }
      } catch (error) {
        console.error(`Lock acquire attempt ${attempt} failed:`, error);
      }

      // Wait before retry with jitter
      if (attempt < retryCount) {
        await sleep(retryDelay + Math.random() * 100);
      }
    }

    return null;
  };

  const release = async (lock: Lock): Promise<void> => {
    try {
      // Use Lua script to ensure we only delete if we own the lock
      const releaseScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("del", KEYS[1])
        else
          return 0
        end
      `;
      await redis.send("EVAL", [releaseScript, "1", lock.resource, lock.value]);
    } catch (error) {
      // Ignore errors during release - lock will expire anyway
      console.error("Error releasing lock:", error);
    }
  };

  const extend = async (lock: Lock, ttl: number = defaultTtl): Promise<boolean> => {
    try {
      // Use Lua script to ensure we only extend if we own the lock
      const extendScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("pexpire", KEYS[1], ARGV[2])
        else
          return 0
        end
      `;
      const result = await redis.send("EVAL", [extendScript, "1", lock.resource, lock.value, ttl.toString()]);
      if (result === 1) {
        lock.ttl = ttl;
        lock.expiration = Date.now() + ttl;
        return true;
      }
      return false;
    } catch (error) {
      console.error("Error extending lock:", error);
      return false;
    }
  };

  const withLock = async <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T | null> => {
    const lock = await acquire(resource, ttl);
    if (!lock) return null;

    try {
      return await fn(lock);
    } finally {
      await release(lock);
    }
  };

  const withLockOrThrow = async <T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T> => {
    const lock = await acquire(resource, ttl);
    if (!lock) {
      throw new LockError(resource);
    }

    try {
      return await fn(lock);
    } finally {
      await release(lock);
    }
  };

  return { acquire, release, withLock, withLockOrThrow, extend };
};

// ==========================
// Export
// ==========================

export const mutex = { create };
