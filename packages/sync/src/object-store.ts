import { StorageType } from "@nats-io/jetstream";
import type { ObjectInfo as NatsObjectInfo, ObjectStore as NatsObjectStore } from "@nats-io/obj";
import { InvalidNameError, ObjectTooLargeError, asError } from "./errors.ts";
import { assertName, decodeSubjectToken, objBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureObjectStore } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { DEFAULT_TENANT, nanos } from "./types.ts";
import type { JsonValue } from "./codec.ts";

// ==========================
// Types
// ==========================

export type ObjectStoreConfig = {
  id: string;
  owner?: string;
  storage?: "file" | "memory";
  replicas?: number;
  /** Explicit bucket compression. Default "none". */
  compression?: "none" | "s2";
  retention: {
    /** Bucket-wide maximum object age. Required. */
    maxAgeMs: number;
    /** Bucket-wide maximum stored bytes. Required. */
    maxBytes: number;
  };
  /** Application safety limit for a single object. Required. */
  maxObjectBytes: number;
};

/** A JSON-safe reference to a stored object, suitable for queue/job payloads. */
export type ObjectRef = {
  storeId: string;
  tenantId: string;
  key: string;
  size: number;
  /** Canonical digest as returned by NATS Object Store (`SHA-256=...`). */
  digest: string;
};

export type ObjectMetadata = Record<string, string>;

export type StoredObject = {
  ref: ObjectRef;
  metadata: ObjectMetadata;
  modifiedAt: Date;
  body: ReadableStream<Uint8Array>;
};

export type SyncObjectInfo = Omit<StoredObject, "body">;

export type ObjectStoreEvent =
  | { type: "put"; object: SyncObjectInfo }
  | { type: "delete"; tenantId: string; key: string };

export type ObjectStore = {
  ready(): Promise<void>;
  put(input: {
    tenantId?: string;
    key: string;
    body: ReadableStream<Uint8Array>;
    metadata?: ObjectMetadata;
    signal?: AbortSignal;
  }): Promise<ObjectRef>;
  get(ref: ObjectRef, options?: { signal?: AbortSignal }): Promise<StoredObject | null>;
  info(input: { tenantId?: string; key: string }): Promise<SyncObjectInfo | null>;
  delete(input: { tenantId?: string; key: string }): Promise<boolean>;
  list(input?: { tenantId?: string; prefix?: string }): AsyncIterable<SyncObjectInfo>;
  watch(input?: { tenantId?: string; prefix?: string; signal?: AbortSignal }): AsyncIterable<ObjectStoreEvent>;
};

// ==========================
// Object store factory
// ==========================

export const createObjectStore = (runtime: SyncRuntime, config: ObjectStoreConfig): ObjectStore => {
  const identity = resourceIdentity(runtime.namespace, "object-store", config.id);
  const owner = config.owner ?? runtime.application;
  if (!Number.isSafeInteger(config.maxObjectBytes) || config.maxObjectBytes <= 0) {
    throw new RangeError("maxObjectBytes must be a positive integer");
  }
  if (!Number.isSafeInteger(config.retention.maxAgeMs) || config.retention.maxAgeMs <= 0) {
    throw new RangeError("retention.maxAgeMs must be a positive integer");
  }
  if (!Number.isSafeInteger(config.retention.maxBytes) || config.retention.maxBytes <= 0) {
    throw new RangeError("retention.maxBytes must be a positive integer");
  }
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storageKind = config.storage ?? runtime.defaults.storage;
  const storage = storageKind === "memory" ? StorageType.Memory : StorageType.File;
  const compression = (config.compression ?? "none") === "s2";
  const bucket = objBucketName(identity);
  const label = `object-store ${config.id}`;

  let os: NatsObjectStore | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify([
      "object-store",
      config.id,
      owner,
      storageKind,
      replicas,
      compression,
      config.retention,
      config.maxObjectBytes,
    ]),
    natsNames: [`OBJ_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      os = await ensureObjectStore(ctx, identity, owner, bucket, {
        storage,
        replicas,
        compression,
        ttl: nanos(config.retention.maxAgeMs),
        max_bytes: config.retention.maxBytes,
        metadata: { "sync.max_object_bytes": String(config.maxObjectBytes) },
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (os ?? (await ctx.objm.open(bucket, false))).status();
      return {
        bytes: status.size,
        compression: status.compression,
      } satisfies Record<string, JsonValue>;
    },
  });

  const getStore = async (): Promise<NatsObjectStore> => {
    await declaration.ready();
    if (os === null) {
      // The resource was provisioned by another handle for the same
      // declaration in this process; bind without creating.
      const ctx = await runtime.context();
      os = await ctx.objm.open(bucket, false);
    }
    return os;
  };

  // Physical object names are injective token encodings; the original tenant
  // and key strings travel in object metadata and in every ObjectRef.
  const objectName = (tenantId: string, key: string): string =>
    `${subjectToken(tenantId, "tenantId")}.${subjectToken(key, "key")}`;
  const decodeName = (name: string): { tenantId: string; key: string } | null => {
    const parts = name.split(".");
    if (parts.length !== 2) return null;
    return { tenantId: decodeSubjectToken(parts[0]!), key: decodeSubjectToken(parts[1]!) };
  };

  const toInfo = (info: NatsObjectInfo): SyncObjectInfo | null => {
    const decoded = decodeName(info.name);
    if (decoded === null) return null;
    const metadata = { ...info.metadata };
    delete metadata["sync.tenant"];
    delete metadata["sync.key"];
    return {
      ref: {
        storeId: config.id,
        tenantId: decoded.tenantId,
        key: decoded.key,
        size: info.size,
        digest: info.digest,
      },
      metadata,
      modifiedAt: new Date(info.mtime),
    };
  };

  const put: ObjectStore["put"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getStore();

    // Count streamed bytes and abort explicitly once maxObjectBytes is
    // exceeded — without buffering the object.
    let total = 0;
    const limited = input.body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        transform: (chunk, controller) => {
          total += chunk.byteLength;
          if (total > config.maxObjectBytes) {
            controller.error(new ObjectTooLargeError(input.key, config.maxObjectBytes));
            return;
          }
          controller.enqueue(chunk);
        },
      }),
      { signal: input.signal },
    );

    try {
      const info = await store.put(
        {
          name: objectName(tenantId, input.key),
          metadata: { ...input.metadata, "sync.tenant": tenantId, "sync.key": input.key },
        },
        limited,
      );
      return {
        storeId: config.id,
        tenantId,
        key: input.key,
        size: info.size,
        digest: info.digest,
      };
    } catch (error) {
      const err = asError(error);
      runtime.events.emit({ type: "object_error", resource: config.id, kind: "object-store", error: err.message });
      if (total > config.maxObjectBytes) throw new ObjectTooLargeError(input.key, config.maxObjectBytes);
      throw err;
    }
  };

  const get: ObjectStore["get"] = async (ref, options = {}) => {
    runtime.assertActive();
    if (ref.storeId !== config.id) {
      throw new InvalidNameError(`ObjectRef belongs to store ${ref.storeId}, not ${config.id}`);
    }
    const store = await getStore();
    const result = await store.get(objectName(ref.tenantId, ref.key)).catch(() => null);
    if (result === null || result.info.deleted) return null;
    // The reference pins an exact artifact: a replaced object (different
    // digest or size) is not the referenced object any more.
    if (result.info.digest !== ref.digest || result.info.size !== ref.size) {
      await result.data.cancel().catch(() => {});
      return null;
    }
    const info = toInfo(result.info);
    if (info === null) return null;
    let body = result.data;
    if (options.signal !== undefined) {
      const signal = options.signal;
      body = body.pipeThrough(new TransformStream(), { signal });
    }
    return { ...info, body };
  };

  const info: ObjectStore["info"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const store = await getStore();
    const raw = await store.info(objectName(input.tenantId ?? DEFAULT_TENANT, input.key)).catch(() => null);
    if (raw === null || raw.deleted) return null;
    return toInfo(raw);
  };

  const deleteObject: ObjectStore["delete"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const store = await getStore();
    const name = objectName(input.tenantId ?? DEFAULT_TENANT, input.key);
    const existing = await store.info(name).catch(() => null);
    if (existing === null || existing.deleted) return false;
    try {
      const response = await store.delete(name);
      return response.success;
    } catch {
      return false;
    }
  };

  const list: ObjectStore["list"] = (input = {}) => ({
    async *[Symbol.asyncIterator]() {
      runtime.assertActive();
      const tenantId = input.tenantId ?? DEFAULT_TENANT;
      const store = await getStore();
      for (const raw of await store.list()) {
        if (raw.deleted) continue;
        const entry = toInfo(raw);
        if (entry === null || entry.ref.tenantId !== tenantId) continue;
        if (input.prefix !== undefined && !entry.ref.key.startsWith(input.prefix)) continue;
        yield entry;
      }
    },
  });

  const watch: ObjectStore["watch"] = (input = {}) => ({
    async *[Symbol.asyncIterator]() {
      runtime.assertActive();
      const tenantId = input.tenantId ?? DEFAULT_TENANT;
      const store = await getStore();
      const iterator = await store.watch({ includeHistory: false, ignoreDeletes: false });
      const onAbort = (): void => iterator.stop();
      input.signal?.addEventListener("abort", onAbort, { once: true });
      try {
        for await (const raw of iterator) {
          const decoded = decodeName(raw.name);
          if (decoded === null || decoded.tenantId !== tenantId) continue;
          if (input.prefix !== undefined && !decoded.key.startsWith(input.prefix)) continue;
          if (raw.deleted) {
            yield { type: "delete", tenantId: decoded.tenantId, key: decoded.key } as ObjectStoreEvent;
          } else {
            const entry = toInfo(raw);
            if (entry !== null) yield { type: "put", object: entry } as ObjectStoreEvent;
          }
        }
      } finally {
        input.signal?.removeEventListener("abort", onAbort);
        iterator.stop();
      }
    },
  });

  return {
    ready: () => declaration.ready(),
    put,
    get,
    info,
    delete: deleteObject,
    list,
    watch,
  };
};
