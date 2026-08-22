import { headers as natsHeaders } from "@nats-io/nats-core";
import { StorageType } from "@nats-io/jetstream";
import type { KV, KvWatchEntry } from "@nats-io/kv";
import { decodeJson, encodeJson } from "./codec.ts";
import { SnapshotOverflowError, asError } from "./errors.ts";
import { assertName, decodeSubjectToken, kvBucketName, resourceIdentity, subjectToken } from "./naming.ts";
import { ensureKv } from "./resources.ts";
import type { ProvisionContext } from "./resources.ts";
import type { SyncRuntime } from "./runtime.ts";
import { DEFAULT_EPHEMERAL_PAYLOAD_BYTES, DEFAULT_TENANT } from "./types.ts";
import type { JsonValue } from "./codec.ts";

// ==========================
// Types
// ==========================

export type EphemeralConfig = {
  id: string;
  owner?: string;
  /** Default per-key TTL. Rounded up to whole seconds (NATS minimum 1s). */
  ttlMs: number;
  /** Revisions kept per key. Default 1. */
  history?: number;
  /** Bounded snapshot size. Default 10_000. */
  maxEntries?: number;
  maxValueBytes?: number;
  replicas?: number;
};

export type EphemeralEntry<T> = {
  key: string;
  value: T;
  revision: string;
  createdAt: Date;
  updatedAt: Date;
  expiresAt: Date;
};

export type EphemeralSnapshot<T> = {
  entries: EphemeralEntry<T>[];
  revision: string;
};

export type EphemeralEvent<T> =
  | { type: "upsert"; entry: EphemeralEntry<T>; revision: string }
  | { type: "delete" | "expire"; key: string; revision: string }
  | { type: "resync_required"; requested: string; firstAvailable: string };

export type Ephemeral<T> = {
  ready(): Promise<void>;
  upsert(input: { tenantId?: string; key: string; value: T; ttlMs?: number }): Promise<EphemeralEntry<T>>;
  /** Refresh a key's TTL by republishing its last value. False if the key is absent. */
  touch(input: { tenantId?: string; key: string; ttlMs?: number }): Promise<boolean>;
  remove(input: { tenantId?: string; key: string }): Promise<boolean>;
  snapshot(input?: { tenantId?: string; prefix?: string }): Promise<EphemeralSnapshot<T>>;
  watch(input?: {
    tenantId?: string;
    prefix?: string;
    after?: string;
    signal?: AbortSignal;
  }): AsyncIterable<EphemeralEvent<T>>;
};

// ==========================
// Ephemeral factory
// ==========================

export const createEphemeral = <T>(runtime: SyncRuntime, config: EphemeralConfig): Ephemeral<T> => {
  const identity = resourceIdentity(runtime.namespace, "ephemeral", config.id);
  const owner = config.owner ?? runtime.application;
  if (!Number.isSafeInteger(config.ttlMs) || config.ttlMs <= 0) {
    throw new RangeError("ttlMs must be a positive integer");
  }
  const history = config.history ?? 1;
  const maxEntries = config.maxEntries ?? 10_000;
  const maxValueBytes = config.maxValueBytes ?? DEFAULT_EPHEMERAL_PAYLOAD_BYTES;
  const replicas = config.replicas ?? runtime.defaults.replicas;
  const storage = runtime.defaults.storage === "memory" ? StorageType.Memory : StorageType.File;
  const bucket = kvBucketName(identity);
  const label = `ephemeral ${config.id}`;

  let kv: KV | null = null;

  const declaration = runtime.declare({
    identity,
    owner,
    configKey: JSON.stringify(["ephemeral", config.id, owner, config.ttlMs, history, maxValueBytes, replicas]),
    natsNames: [`KV_${bucket}`],
    provision: async (ctx: ProvisionContext) => {
      kv = await ensureKv(ctx, identity, owner, bucket, {
        history,
        replicas,
        storage,
        // Expiry markers must outlive watch resume windows; tie them to the TTL.
        markerTTL: Math.max(config.ttlMs, 1_000),
      });
    },
    summary: async (ctx: ProvisionContext) => {
      const status = await (kv ?? (await ctx.kvm.open(bucket))).status();
      return {
        entries: status.values,
        bytes: status.size,
        history: status.history,
      } satisfies Record<string, JsonValue>;
    },
  });

  const getKv = async (): Promise<KV> => {
    await declaration.ready();
    return kv!;
  };

  // Keys are token-encoded so any UTF-8 tenant/key is KV-safe and injective.
  const kvKey = (tenantId: string, key: string): string =>
    `t.${subjectToken(tenantId, "tenantId")}.k.${subjectToken(key, "key")}`;
  const tenantFilter = (tenantId: string): string => `t.${subjectToken(tenantId, "tenantId")}.k.>`;
  const decodeKvKey = (raw: string): string | null => {
    const parts = raw.split(".");
    if (parts.length !== 4 || parts[0] !== "t" || parts[2] !== "k") return null;
    return decodeSubjectToken(parts[3]!);
  };

  const ttlSeconds = (ttlMs: number | undefined): number => Math.max(1, Math.ceil((ttlMs ?? config.ttlMs) / 1_000));

  /**
   * KV put with a per-message TTL header. The KV client's put() does not
   * expose per-message TTL, so this publishes to the bucket's documented
   * `$KV.<bucket>.<key>` subject directly.
   */
  const putWithTtl = async (rawKey: string, value: T, ttlMs: number | undefined): Promise<number> => {
    const ctx = await runtime.context();
    const bytes = encodeJson(label, value ?? null, maxValueBytes);
    const hdrs = natsHeaders();
    hdrs.set("Nats-TTL", `${ttlSeconds(ttlMs)}s`);
    const ack = await ctx.js.publish(`$KV.${bucket}.${rawKey}`, bytes, { headers: hdrs });
    return ack.seq;
  };

  const toEntry = (rawKey: string, value: T, revision: number, created: Date, ttlMs: number | undefined): EphemeralEntry<T> => ({
    key: decodeKvKey(rawKey) ?? rawKey,
    value,
    revision: String(revision),
    createdAt: created,
    updatedAt: created,
    expiresAt: new Date(created.getTime() + ttlSeconds(ttlMs) * 1_000),
  });

  const upsert: Ephemeral<T>["upsert"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    await declaration.ready();
    const rawKey = kvKey(tenantId, input.key);
    const now = new Date();
    const seq = await putWithTtl(rawKey, input.value, input.ttlMs);
    return toEntry(rawKey, input.value, seq, now, input.ttlMs);
  };

  const touch: Ephemeral<T>["touch"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const rawKey = kvKey(tenantId, input.key);
    const entry = await store.get(rawKey);
    if (entry === null || entry.operation !== "PUT") return false;
    await putWithTtl(rawKey, decodeJson<T>(entry.value), input.ttlMs);
    return true;
  };

  const remove: Ephemeral<T>["remove"] = async (input) => {
    runtime.assertActive();
    assertName(input.key, "key");
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const rawKey = kvKey(tenantId, input.key);
    const entry = await store.get(rawKey);
    if (entry === null || entry.operation !== "PUT") return false;
    await store.delete(rawKey);
    return true;
  };

  const snapshot: Ephemeral<T>["snapshot"] = async (input = {}) => {
    runtime.assertActive();
    const tenantId = input.tenantId ?? DEFAULT_TENANT;
    const store = await getKv();
    const status = await store.status();
    const revision = String(status.streamInfo.state.last_seq);
    const entries: EphemeralEntry<T>[] = [];
    const keys = await store.keys(tenantFilter(tenantId));
    const rawKeys: string[] = [];
    for await (const rawKey of keys) rawKeys.push(rawKey);
    for (const rawKey of rawKeys) {
      const key = decodeKvKey(rawKey);
      if (key === null) continue;
      if (input.prefix !== undefined && !key.startsWith(input.prefix)) continue;
      const entry = await store.get(rawKey);
      if (entry === null || entry.operation !== "PUT") continue;
      entries.push(toEntry(rawKey, decodeJson<T>(entry.value), entry.revision, entry.created, undefined));
      if (entries.length > maxEntries) throw new SnapshotOverflowError(label, maxEntries);
    }
    return { entries, revision };
  };

  async function* watchIterator(input: {
    tenantId: string;
    prefix?: string;
    after?: string;
    signal?: AbortSignal;
  }): AsyncGenerator<EphemeralEvent<T>> {
    runtime.assertActive();
    const store = await getKv();

    let resumeFrom: number | null = null;
    if (input.after !== undefined) {
      const after = Number(input.after);
      if (!Number.isSafeInteger(after) || after < 0) {
        throw new RangeError(`after is not a valid ephemeral revision: ${input.after}`);
      }
      const status = await store.status();
      const firstSeq = status.streamInfo.state.first_seq;
      if (after + 1 < firstSeq) {
        // History no longer covers the requested revision. The caller must
        // take a fresh snapshot; Sync never silently skips to the head.
        yield { type: "resync_required", requested: input.after, firstAvailable: String(firstSeq) };
        return;
      }
      resumeFrom = after + 1;
    }

    const iterator = await store.watch({
      key: tenantFilter(input.tenantId),
      ...(resumeFrom !== null ? { resumeFromRevision: resumeFrom } : {}),
    });
    const onAbort = (): void => iterator.stop();
    input.signal?.addEventListener("abort", onAbort, { once: true });
    try {
      for await (const entry of iterator as AsyncIterable<KvWatchEntry>) {
        const key = decodeKvKey(entry.key);
        if (key === null) continue;
        if (input.prefix !== undefined && !key.startsWith(input.prefix)) continue;
        const revision = String(entry.revision);
        if (entry.operation === "PUT") {
          let value: T;
          try {
            value = decodeJson<T>(entry.value);
          } catch (error) {
            runtime.events.emit({ type: "handler_error", resource: config.id, kind: "ephemeral", error: asError(error).message });
            continue;
          }
          yield { type: "upsert", entry: toEntry(entry.key, value, entry.revision, entry.created, undefined), revision };
        } else {
          yield { type: entry.operation === "DEL" ? "delete" : "expire", key, revision };
        }
      }
    } finally {
      input.signal?.removeEventListener("abort", onAbort);
      iterator.stop();
    }
  }

  const watch: Ephemeral<T>["watch"] = (input = {}) => ({
    [Symbol.asyncIterator]: () =>
      watchIterator({
        tenantId: input.tenantId ?? DEFAULT_TENANT,
        prefix: input.prefix,
        after: input.after,
        signal: input.signal,
      }),
  });

  return {
    ready: () => declaration.ready(),
    upsert,
    touch,
    remove,
    snapshot,
    watch,
  };
};
