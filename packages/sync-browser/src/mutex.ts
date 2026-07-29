import { type Store } from "./store";
import { resolveStore } from "./internal/shared-state";
import { sleep } from "./internal/sleep";
import { randomHex, simpleHash } from "./internal/id";

const DEFAULT_PREFIX = "sync:mutex";
const DEFAULT_RETRY_COUNT = 10;
const DEFAULT_RETRY_DELAY = 200;
const DEFAULT_TTL = 10_000;
const MAX_RESOURCE_LENGTH = 128;

const normalizeResource = (resource: string): string => {
  if (resource.length <= MAX_RESOURCE_LENGTH) return resource;
  return simpleHash(resource);
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
  id: string;
  prefix?: string;
  retryCount?: number;
  retryDelay?: number;
  defaultTtl?: number;
  store?: Store;
};

export type Mutex = {
  id: string;
  acquire(resource: string, ttl?: number): Promise<Lock | null>;
  release(lock: Lock): Promise<void>;
  withLock<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T | null>;
  withLockOrThrow<T>(resource: string, fn: (lock: Lock) => Promise<T> | T, ttl?: number): Promise<T>;
  extend(lock: Lock, ttl?: number): Promise<boolean>;
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

export const mutex = (config: MutexConfig): Mutex => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const retryCount = config.retryCount ?? DEFAULT_RETRY_COUNT;
  const retryDelay = config.retryDelay ?? DEFAULT_RETRY_DELAY;
  const defaultTtl = config.defaultTtl ?? DEFAULT_TTL;
  const store = resolveStore(config.store);

  const acquire = async (resource: string, ttl: number = defaultTtl): Promise<Lock | null> => {
    if (!Number.isFinite(ttl) || ttl <= 0) return null;
    const safeResource = normalizeResource(resource);
    const key = `${prefix}:${config.id}:${safeResource}`;
    const value = randomHex(16);

    for (let attempt = 0; attempt <= retryCount; attempt++) {
      // SET NX equivalent: only set if key does not exist
      const existing = store.get(key);
      if (existing === undefined) {
        store.set(key, value, ttl);
        return {
          resource: key,
          value,
          ttl,
          expiration: Date.now() + ttl,
        };
      }

      if (attempt < retryCount) {
        await sleep(retryDelay + Math.random() * 100);
      }
    }

    return null;
  };

  const release = async (lock: Lock): Promise<void> => {
    // Compare-and-delete (safe in single-threaded JS)
    const current = store.get(lock.resource);
    if (current === lock.value) {
      store.del(lock.resource);
    }
  };

  const extend = async (lock: Lock, ttl: number = defaultTtl): Promise<boolean> => {
    // Compare-and-extend (safe in single-threaded JS)
    const current = store.get(lock.resource);
    if (current === lock.value) {
      store.set(lock.resource, lock.value, ttl);
      lock.ttl = ttl;
      lock.expiration = Date.now() + ttl;
      return true;
    }
    return false;
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

  return { id: config.id, acquire, release, withLock, withLockOrThrow, extend };
};
