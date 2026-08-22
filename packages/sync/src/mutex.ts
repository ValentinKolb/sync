import { headers as natsHeaders } from "@nats-io/nats-core";
import { StorageType } from "@nats-io/jetstream";
import type { KV } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import { asError } from "./errors.ts";
import { assertName, kvBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureKv } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import type { JsonValue } from "./codec.ts";

// ==========================
// Types
// ==========================

export type Lock = {
  resource: string;
  ownerToken: string;
  /**
   * Monotonic fencing token: the KV revision of the successful acquisition.
   * A lease alone cannot stop a stale owner from writing to external systems
   * after expiry — persist and compare the fence, or make effects idempotent.
   */
  fence: bigint;
  expiresAt: Date;
};

export type MutexConfig = {
  id: string;
  owner?: string;
  /** Lease TTL. Rounded up to whole seconds (NATS minimum 1s). Default 10_000. */
  ttlMs?: number;
  retry?: { attempts?: number; delayMs?: number };
  replicas?: number;
};

export type Mutex = {
  ready(): Promise<void>;
  acquire(resource: string, options?: { ttlMs?: number; signal?: AbortSignal }): Promise<Lock | null>;
  extend(lock: Lock, options?: { ttlMs?: number }): Promise<boolean>;
  release(lock: Lock): Promise<boolean>;
  withLock<T>(
    resource: string,
    fn: (lock: Lock) => Promise<T>,
    options?: { ttlMs?: number; signal?: AbortSignal },
  ): Promise<T | null>;
};

type LockRecord = {
  ownerToken: string;
  expiresAt: string;
};

// ==========================
// Mutex factory
// ==========================

export const createMutex = (runtime: SyncRuntime, config: MutexConfig): Mutex => {
  const identity = resourceIdentity(runtime.namespace, "mutex", config.id);
  const owner = config.owner ?? runtime.application;
  const defaultTtlMs = config.ttlMs ?? 10_000;
  if (!Number.isSafeInteger(defaultTtlMs) || defaultTtlMs <= 0) {
    throw new RangeError("ttlMs must be a positive integer");
  }
  const retryAttempts = config.retry?.attempts ?? 10;
  const retryDelayMs = config.retry?.delayMs ?? 200;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;
  const bucket = kvBucketName(identity);
  const label = `mutex ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["mutex", config.id, owner, replicas]),
    natsNames: [`KV_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history: 1,
        replicas,
        storage,
        markerTTL: 1_000,
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return { locks: status.values } satisfies Record<string, JsonValue>;
    },
  });

  const getKv = async (): Promise<KV> => {
    await declaration.ready();
    if (kv === null) {
      const ctx = await runtime.context();
      kv = await ctx.kvm.open(bucket);
    }
    return kv;
  };

  const keyOf = (resource: string): string => {
    assertName(resource, "resource");
    return `r.${subjectToken(resource, "resource")}`;
  };

  const ttlSeconds = (ttlMs: number): number => Math.max(1, Math.ceil(ttlMs / 1_000));

  /** CAS put with per-key TTL: expected-last-subject-sequence plus Nats-TTL headers. */
  const casPutWithTtl = async (key: string, record: LockRecord, previousSeq: number, ttlMs: number): Promise<number | null> => {
    const ctx = await runtime.context();
    const hdrs = natsHeaders();
    hdrs.set("Nats-Expected-Last-Subject-Sequence", String(previousSeq));
    hdrs.set("Nats-TTL", `${ttlSeconds(ttlMs)}s`);
    try {
      const ack = await ctx.js.publish(`$KV.${bucket}.${key}`, encodeJson(label, record, 4_096), { headers: hdrs });
      return ack.seq;
    } catch (error) {
      if (/wrong last sequence/i.test(asError(error).message)) return null;
      throw error;
    }
  };

  /** Last KV state of the resource key: live record, tombstone revision, or nothing. */
  const readKey = async (
    resource: string,
  ): Promise<{ record: LockRecord | null; lastRevision: number }> => {
    const store = await getKv();
    const entry = await store.get(keyOf(resource));
    if (entry === null) return { record: null, lastRevision: 0 };
    if (entry.operation !== "PUT") return { record: null, lastRevision: entry.revision };
    try {
      return { record: decodeJson<LockRecord>(entry.value), lastRevision: entry.revision };
    } catch {
      return { record: null, lastRevision: entry.revision };
    }
  };

  const tryAcquire = async (resource: string, ttlMs: number): Promise<Lock | null> => {
    const key = keyOf(resource);
    const { record: existing, lastRevision } = await readKey(resource);
    if (existing !== null) return null;
    const ownerToken = crypto.randomUUID();
    const expiresAt = new Date(Date.now() + ttlSeconds(ttlMs) * 1_000);
    const record: LockRecord = { ownerToken, expiresAt: expiresAt.toISOString() };
    // A tombstone (expired/released lock) counts as the expected last revision.
    const seq = await casPutWithTtl(key, record, lastRevision, ttlMs);
    if (seq === null) return null;
    // The acquisition revision is the fence; it is preserved across extends.
    return { resource, ownerToken, fence: BigInt(seq), expiresAt };
  };

  const acquire: Mutex["acquire"] = async (resource, options = {}) => {
    runtime.assertActive();
    await declaration.ready();
    const ttlMs = options.ttlMs ?? defaultTtlMs;
    for (let attempt = 0; attempt <= retryAttempts; attempt++) {
      if (options.signal?.aborted) return null;
      const lock = await tryAcquire(resource, ttlMs);
      if (lock !== null) return lock;
      if (attempt < retryAttempts) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, retryDelayMs);
          options.signal?.addEventListener(
            "abort",
            () => {
              clearTimeout(timer);
              resolve();
            },
            { once: true },
          );
        });
      }
    }
    return null;
  };

  const extend: Mutex["extend"] = async (lock, options = {}) => {
    runtime.assertActive();
    const ttlMs = options.ttlMs ?? defaultTtlMs;
    const current = await readKey(lock.resource);
    if (current.record === null || current.record.ownerToken !== lock.ownerToken) {
      runtime.events.emit({ type: "lock_lost", resource: config.id, kind: "mutex", detail: { resource: lock.resource } });
      return false;
    }
    const expiresAt = new Date(Date.now() + ttlSeconds(ttlMs) * 1_000);
    const record: LockRecord = { ...current.record, expiresAt: expiresAt.toISOString() };
    const seq = await casPutWithTtl(keyOf(lock.resource), record, current.lastRevision, ttlMs);
    if (seq === null) return false;
    lock.expiresAt = expiresAt;
    return true;
  };

  const release: Mutex["release"] = async (lock) => {
    runtime.assertActive();
    const store = await getKv();
    const current = await readKey(lock.resource);
    if (current.record === null || current.record.ownerToken !== lock.ownerToken) return false;
    try {
      await store.delete(keyOf(lock.resource), { previousSeq: current.lastRevision });
      return true;
    } catch {
      return false;
    }
  };

  const withLock: Mutex["withLock"] = async (resource, fn, options = {}) => {
    const lock = await acquire(resource, options);
    if (lock === null) return null;
    try {
      return await fn(lock);
    } finally {
      await release(lock).catch(() => {});
    }
  };

  return {
    ready: () => declaration.ready(),
    acquire,
    extend,
    release,
    withLock,
  };
};
