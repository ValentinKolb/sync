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

const assertTtl = (ttl: number, label = "ttl"): void => {
  if (!Number.isSafeInteger(ttl) || ttl <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
};

const assertRetryCount = (value: number): void => {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("retryCount must be a non-negative safe integer");
  }
};

const assertRetryDelay = (value: number): void => {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("retryDelay must be a non-negative safe integer");
  }
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
  assertRetryCount(retryCount);
  assertRetryDelay(retryDelay);
  assertTtl(defaultTtl, "defaultTtl");

  const acquire = async (resource: string, ttl: number = defaultTtl): Promise<Lock | null> => {
    assertTtl(ttl);
    const safeResource = normalizeResource(resource);
    const key =
      `sync:mutex:browser:v2:${encodeURIComponent(JSON.stringify([prefix, config.id, safeResource]))}`;
    const legacyKey = `${prefix}:${config.id}:${safeResource}`;
    const value = randomHex(16);

    for (let attempt = 0; attempt <= retryCount; attempt++) {
      // SET NX equivalent: only set if key does not exist
      const existing = store.get(key);
      const legacyExisting = store.get(legacyKey);
      if (existing === undefined && legacyExisting === undefined) {
        store.set(legacyKey, value, ttl);
        try {
          store.set(key, value, ttl);
        } catch (error) {
          if (store.get(legacyKey) === value) store.del(legacyKey);
          throw error;
        }
        return {
          resource: key,
          value,
          ttl,
          expiration: Date.now() + ttl,
          legacyResource: legacyKey,
        } as Lock & { legacyResource: string };
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
    const legacyResource = (lock as Lock & { legacyResource?: string }).legacyResource;
    if (legacyResource && store.get(legacyResource) === lock.value) {
      store.del(legacyResource);
    }
  };

  const extend = async (lock: Lock, ttl: number = defaultTtl): Promise<boolean> => {
    assertTtl(ttl);
    // Compare-and-extend (safe in single-threaded JS)
    const current = store.get(lock.resource);
    const legacyResource = (lock as Lock & { legacyResource?: string }).legacyResource;
    const legacyCurrent = legacyResource ? store.get(legacyResource) : lock.value;
    if (current === lock.value && legacyCurrent === lock.value) {
      if (legacyResource) store.set(legacyResource, lock.value, ttl);
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
