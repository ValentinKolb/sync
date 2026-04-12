import type { z } from "zod";
import { EventLog, type EventLogEntry } from "./internal/event-log";

const DEFAULT_MAX_ENTRIES = 10_000;
const DEFAULT_MAX_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_EVENT_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_EVENT_MAXLEN = 50_000;
const DEFAULT_TOMBSTONE_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_LIST_LIMIT = 1_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_KEY_BYTES = 512;
const MAX_KEY_DEPTH = 8;

const textEncoder = new TextEncoder();

// ==========================
// Error Classes
// ==========================

export class RegistryCapacityError extends Error {
  constructor(message = "registry capacity reached") {
    super(message);
    this.name = "RegistryCapacityError";
  }
}

export class RegistryPayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RegistryPayloadTooLargeError";
  }
}

// ==========================
// Types
// ==========================

export type RegistryConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string;
  limits?: {
    maxEntries?: number;
    maxPayloadBytes?: number;
    eventRetentionMs?: number;
    eventMaxLen?: number;
    tombstoneRetentionMs?: number;
    reconcileBatchSize?: number;
  };
};

export type RegistryUpsertConfig<T> = {
  key: string;
  value: T;
  ttlMs?: number;
  tenantId?: string;
};

export type RegistryTouchConfig = {
  key: string;
  ttlMs?: number;
  tenantId?: string;
};

export type RegistryRemoveConfig = {
  key: string;
  reason?: string;
  tenantId?: string;
};

export type RegistryGetConfig = {
  key: string;
  tenantId?: string;
  includeExpired?: boolean;
};

export type RegistryListConfig = {
  prefix?: string;
  status?: "active" | "expired";
  afterKey?: string;
  limit?: number;
  tenantId?: string;
};

export type RegistryCasConfig<T> = {
  key: string;
  version: string;
  value: T;
  tenantId?: string;
};

export type RegistryEntry<T> = {
  key: string;
  value: T;
  version: string;
  status: "active" | "expired";
  createdAt: number;
  updatedAt: number;
  ttlMs: number | null;
  expiresAt: number | null;
};

export type RegistrySnapshot<T> = {
  entries: RegistryEntry<T>[];
  cursor: string;
  nextKey?: string;
};

export type RegistryRecvConfig = {
  wait?: boolean;
  timeoutMs?: number;
  signal?: AbortSignal;
};

export type RegistryEvent<T> =
  | { type: "upsert"; cursor: string; entry: RegistryEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; updatedAt: number; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; removedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; removedAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };

export type RegistryReader<T> = {
  recv(cfg?: RegistryRecvConfig): Promise<RegistryEvent<T> | null>;
  stream(cfg?: RegistryRecvConfig): AsyncIterable<RegistryEvent<T>>;
};

export type Registry<T> = {
  upsert(cfg: RegistryUpsertConfig<T>): Promise<RegistryEntry<T>>;
  touch(cfg: RegistryTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: RegistryRemoveConfig): Promise<boolean>;
  get(cfg: RegistryGetConfig): Promise<RegistryEntry<T> | null>;
  list(cfg?: RegistryListConfig): Promise<RegistrySnapshot<T>>;
  cas(cfg: RegistryCasConfig<T>): Promise<{ ok: boolean; entry?: RegistryEntry<T> }>;
  reader(cfg?: { key?: string; prefix?: string; after?: string; tenantId?: string }): RegistryReader<T>;
};

// ==========================
// Internal Types
// ==========================

type StoredEntry<T> = {
  key: string;
  data: T;
  version: string;
  createdAt: number;
  updatedAt: number;
  ttlMs: number | null;
  expiresAt: number | null;
};

type TombstoneEntry = {
  key: string;
  version: string;
  removedAt: number;
  reason?: string;
};

type TenantState<T> = {
  seq: number;
  entries: Map<string, StoredEntry<T>>;
  ttlTimers: Map<string, ReturnType<typeof setTimeout>>;
  tombstones: Map<string, TombstoneEntry>;
  tombstoneTimers: Map<string, ReturnType<typeof setTimeout>>;
  prefixRefs: Map<string, number>;
  rootEventLog: EventLog;
  keyEventLogs: Map<string, EventLog>;
  prefixEventLogs: Map<string, EventLog>;
};

// ==========================
// Helpers
// ==========================

const assertLogicalKey = (value: string): void => {
  if (value.length === 0) throw new Error("key must be non-empty");
  if (value.startsWith("/")) throw new Error("key must not start with '/'");
  if (value.endsWith("/")) throw new Error("key must not end with '/'");
  if (value.includes("//")) throw new Error("key must not contain '//'");
  const bytes = textEncoder.encode(value).byteLength;
  if (bytes > MAX_KEY_BYTES) throw new Error(`key exceeds max length (${MAX_KEY_BYTES} bytes)`);
  const segments = value.split("/").filter(Boolean);
  if (segments.length > MAX_KEY_DEPTH) throw new Error(`key depth exceeds max (${MAX_KEY_DEPTH})`);
};

const assertIdentifier = (value: string, label: string): void => {
  if (value.length === 0) throw new Error(`${label} must be non-empty`);
  if (value.length > 256) throw new Error(`${label} too long (max 256 chars)`);
};

/** Get ancestor prefixes for a key, e.g. "a/b/c" -> ["a/", "a/b/"] */
const ancestorPrefixes = (key: string): string[] => {
  const parts = key.split("/").filter(Boolean);
  const prefixes: string[] = [];
  let current = "";
  for (const part of parts) {
    current += part + "/";
    prefixes.push(current);
  }
  // Remove the last one (which includes the key itself as a prefix)
  if (prefixes.length > 0) {
    prefixes.pop();
  }
  return prefixes;
};

// ==========================
// Registry Factory
// ==========================

export const registry = <TSchema extends z.ZodTypeAny>(config: RegistryConfig<TSchema>): Registry<z.infer<TSchema>> => {
  type TData = z.infer<TSchema>;

  assertIdentifier(config.id, "config.id");
  const defaultTenant = config.tenantId ?? "default";
  assertIdentifier(defaultTenant, "tenantId");

  const maxEntries = config.limits?.maxEntries ?? DEFAULT_MAX_ENTRIES;
  const maxPayloadBytes = config.limits?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  const eventRetentionMs = config.limits?.eventRetentionMs ?? DEFAULT_EVENT_RETENTION_MS;
  const eventMaxLen = config.limits?.eventMaxLen ?? DEFAULT_EVENT_MAXLEN;
  const tombstoneRetentionMs = config.limits?.tombstoneRetentionMs ?? DEFAULT_TOMBSTONE_RETENTION_MS;

  const resolveTenant = (tenantId?: string): string => {
    const resolved = tenantId ?? defaultTenant;
    assertIdentifier(resolved, "tenantId");
    return resolved;
  };

  // Per-tenant state
  const tenantStates = new Map<string, TenantState<TData>>();
  const getTenantState = (tenantId: string): TenantState<TData> => {
    let state = tenantStates.get(tenantId);
    if (!state) {
      state = {
        seq: 0,
        entries: new Map(),
        ttlTimers: new Map(),
        tombstones: new Map(),
        tombstoneTimers: new Map(),
        prefixRefs: new Map(),
        rootEventLog: new EventLog({ maxLen: eventMaxLen, retentionMs: eventRetentionMs }),
        keyEventLogs: new Map(),
        prefixEventLogs: new Map(),
      };
      tenantStates.set(tenantId, state);
    }
    return state;
  };

  const getKeyEventLog = (state: TenantState<TData>, key: string): EventLog => {
    let log = state.keyEventLogs.get(key);
    if (!log) {
      log = new EventLog({ maxLen: eventMaxLen, retentionMs: eventRetentionMs });
      state.keyEventLogs.set(key, log);
    }
    return log;
  };

  const getPrefixEventLog = (state: TenantState<TData>, pfx: string): EventLog => {
    let log = state.prefixEventLogs.get(pfx);
    if (!log) {
      log = new EventLog({ maxLen: eventMaxLen, retentionMs: eventRetentionMs });
      state.prefixEventLogs.set(pfx, log);
    }
    return log;
  };

  const prefixRefInc = (state: TenantState<TData>, key: string): void => {
    for (const pfx of ancestorPrefixes(key)) {
      state.prefixRefs.set(pfx, (state.prefixRefs.get(pfx) ?? 0) + 1);
    }
  };

  const prefixRefDec = (state: TenantState<TData>, key: string): void => {
    for (const pfx of ancestorPrefixes(key)) {
      const next = (state.prefixRefs.get(pfx) ?? 1) - 1;
      if (next <= 0) {
        state.prefixRefs.delete(pfx);
      } else {
        state.prefixRefs.set(pfx, next);
      }
    }
  };

  const emitEvent = (state: TenantState<TData>, key: string, fields: Record<string, unknown>): void => {
    state.rootEventLog.append(fields);
    getKeyEventLog(state, key).append(fields);
    for (const pfx of ancestorPrefixes(key)) {
      getPrefixEventLog(state, pfx).append(fields);
    }
  };

  const scheduleExpiry = (state: TenantState<TData>, logicalKey: string, ttlMs: number): void => {
    const existing = state.ttlTimers.get(logicalKey);
    if (existing) clearTimeout(existing);

    state.ttlTimers.set(
      logicalKey,
      setTimeout(() => {
        const entry = state.entries.get(logicalKey);
        if (!entry) return;

        state.entries.delete(logicalKey);
        state.ttlTimers.delete(logicalKey);
        prefixRefDec(state, logicalKey);

        const version = String(++state.seq);

        // Create tombstone
        const tombstone: TombstoneEntry = {
          key: logicalKey,
          version,
          removedAt: Date.now(),
        };
        state.tombstones.set(logicalKey, tombstone);
        state.tombstoneTimers.set(
          logicalKey,
          setTimeout(() => {
            state.tombstones.delete(logicalKey);
            state.tombstoneTimers.delete(logicalKey);
          }, tombstoneRetentionMs),
        );

        emitEvent(state, logicalKey, {
          type: "expire",
          key: logicalKey,
          version,
          removedAt: Date.now(),
        });
      }, ttlMs),
    );
  };

  // ==========================
  // upsert
  // ==========================

  const upsert = async (cfg: RegistryUpsertConfig<TData>): Promise<RegistryEntry<TData>> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    // Capacity check
    const isNew = !state.entries.has(cfg.key);
    if (isNew && state.entries.size >= maxEntries) {
      throw new RegistryCapacityError(`maxEntries (${maxEntries}) reached`);
    }

    const parsed = config.schema.safeParse(cfg.value);
    if (!parsed.success) throw parsed.error;

    const payloadRaw = JSON.stringify(parsed.data);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new RegistryPayloadTooLargeError(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const now = Date.now();
    const version = String(++state.seq);

    const existingEntry = state.entries.get(cfg.key);
    const stored: StoredEntry<TData> = {
      key: cfg.key,
      data: parsed.data,
      version,
      createdAt: existingEntry?.createdAt ?? now,
      updatedAt: now,
      ttlMs: cfg.ttlMs ?? null,
      expiresAt: cfg.ttlMs != null ? now + cfg.ttlMs : null,
    };

    if (isNew) {
      prefixRefInc(state, cfg.key);
    }

    state.entries.set(cfg.key, stored);

    // Clear any existing tombstone
    const tombstoneTimer = state.tombstoneTimers.get(cfg.key);
    if (tombstoneTimer) {
      clearTimeout(tombstoneTimer);
      state.tombstoneTimers.delete(cfg.key);
    }
    state.tombstones.delete(cfg.key);

    // Schedule TTL if set
    if (stored.ttlMs != null) {
      scheduleExpiry(state, cfg.key, stored.ttlMs);
    } else {
      // Clear any existing TTL timer
      const ttlTimer = state.ttlTimers.get(cfg.key);
      if (ttlTimer) {
        clearTimeout(ttlTimer);
        state.ttlTimers.delete(cfg.key);
      }
    }

    const result: RegistryEntry<TData> = {
      key: cfg.key,
      value: parsed.data,
      version,
      status: "active",
      createdAt: stored.createdAt,
      updatedAt: now,
      ttlMs: stored.ttlMs,
      expiresAt: stored.expiresAt,
    };

    emitEvent(state, cfg.key, {
      type: "upsert",
      key: cfg.key,
      version,
      status: "active",
      createdAt: stored.createdAt,
      updatedAt: now,
      ttlMs: stored.ttlMs,
      expiresAt: stored.expiresAt,
      payload: payloadRaw,
    });

    return result;
  };

  // ==========================
  // touch
  // ==========================

  const touch = async (cfg: RegistryTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const existing = state.entries.get(cfg.key);
    if (!existing) return { ok: false };

    // If no ttlMs provided, reuse the existing TTL duration.
    // If the entry has no TTL at all, touch is a no-op (matches server behavior).
    let ttlMs = cfg.ttlMs;
    if (ttlMs == null) {
      if (existing.expiresAt == null) return { ok: false };
      // Compute original TTL from the stored expiresAt and updatedAt
      ttlMs = existing.expiresAt - existing.updatedAt;
      if (ttlMs <= 0) ttlMs = 1;
    }
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) throw new Error("ttlMs must be > 0");

    const now = Date.now();
    const expiresAt = now + ttlMs;

    // touch does NOT bump version — matches server behavior
    existing.updatedAt = now;
    existing.expiresAt = expiresAt;

    scheduleExpiry(state, cfg.key, ttlMs);

    emitEvent(state, cfg.key, {
      type: "touch",
      key: cfg.key,
      version: existing.version,
      updatedAt: now,
      expiresAt,
    });

    return { ok: true, version: existing.version, expiresAt };
  };

  // ==========================
  // remove
  // ==========================

  const remove = async (cfg: RegistryRemoveConfig): Promise<boolean> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const existing = state.entries.get(cfg.key);
    if (!existing) return false;

    state.entries.delete(cfg.key);
    prefixRefDec(state, cfg.key);

    // Clear TTL timer
    const ttlTimer = state.ttlTimers.get(cfg.key);
    if (ttlTimer) {
      clearTimeout(ttlTimer);
      state.ttlTimers.delete(cfg.key);
    }

    const version = String(++state.seq);
    const now = Date.now();

    // Create tombstone
    const tombstone: TombstoneEntry = {
      key: cfg.key,
      version,
      removedAt: now,
      reason: cfg.reason,
    };
    state.tombstones.set(cfg.key, tombstone);
    state.tombstoneTimers.set(
      cfg.key,
      setTimeout(() => {
        state.tombstones.delete(cfg.key);
        state.tombstoneTimers.delete(cfg.key);
      }, tombstoneRetentionMs),
    );

    const fields: Record<string, unknown> = {
      type: "delete",
      key: cfg.key,
      version,
      removedAt: now,
    };
    if (cfg.reason) fields.reason = cfg.reason;

    emitEvent(state, cfg.key, fields);

    return true;
  };

  // ==========================
  // get
  // ==========================

  const get = async (cfg: RegistryGetConfig): Promise<RegistryEntry<TData> | null> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const stored = state.entries.get(cfg.key);
    if (stored) {
      const parsed = config.schema.safeParse(stored.data);
      if (!parsed.success) return null;
      return {
        key: stored.key,
        value: parsed.data,
        version: stored.version,
        status: "active",
        createdAt: stored.createdAt,
        updatedAt: stored.updatedAt,
        ttlMs: stored.ttlMs,
        expiresAt: stored.expiresAt,
      };
    }

    // Check tombstones if includeExpired
    if (cfg.includeExpired) {
      const tombstone = state.tombstones.get(cfg.key);
      if (tombstone) {
        return {
          key: tombstone.key,
          value: null as unknown as z.infer<TSchema>,
          version: tombstone.version,
          status: "expired",
          createdAt: tombstone.removedAt,
          updatedAt: tombstone.removedAt,
          ttlMs: null,
          expiresAt: null,
        };
      }
    }

    return null;
  };

  // ==========================
  // list
  // ==========================

  const list = async (cfg: RegistryListConfig = {}): Promise<RegistrySnapshot<TData>> => {
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);
    const limit = cfg.limit ?? DEFAULT_LIST_LIMIT;
    const status = cfg.status ?? "active";

    const entries: RegistryEntry<TData>[] = [];

    if (status === "active") {
      for (const stored of state.entries.values()) {
        if (cfg.prefix && !stored.key.startsWith(cfg.prefix)) continue;
        const parsed = config.schema.safeParse(stored.data);
        if (!parsed.success) continue;
        entries.push({
          key: stored.key,
          value: parsed.data,
          version: stored.version,
          status: "active",
          createdAt: stored.createdAt,
          updatedAt: stored.updatedAt,
          ttlMs: stored.ttlMs,
          expiresAt: stored.expiresAt,
        });
      }
    } else {
      // status === "expired" — list tombstones
      for (const tombstone of state.tombstones.values()) {
        if (cfg.prefix && !tombstone.key.startsWith(cfg.prefix)) continue;
        entries.push({
          key: tombstone.key,
          value: null as unknown as TData,
          version: tombstone.version,
          status: "expired",
          createdAt: tombstone.removedAt,
          updatedAt: tombstone.removedAt,
          ttlMs: null,
          expiresAt: null,
        });
      }
    }

    entries.sort((a, b) => a.key.localeCompare(b.key));

    let start = 0;
    if (cfg.afterKey) {
      const idx = entries.findIndex((e) => e.key > cfg.afterKey!);
      start = idx >= 0 ? idx : entries.length;
    }

    const paginated = entries.slice(start, start + limit);
    const hasMore = start + limit < entries.length;

    return {
      entries: paginated,
      cursor: state.rootEventLog.latest(),
      nextKey: hasMore ? entries[start + limit]!.key : undefined,
    };
  };

  // ==========================
  // cas (compare-and-swap)
  // ==========================

  const cas = async (cfg: RegistryCasConfig<TData>): Promise<{ ok: boolean; entry?: RegistryEntry<TData> }> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const existing = state.entries.get(cfg.key);
    if (!existing) return { ok: false };
    if (existing.version !== cfg.version) return { ok: false };

    // Preserve existing TTL: compute remaining ttlMs from current expiresAt
    let preservedTtlMs: number | undefined;
    if (existing.expiresAt != null) {
      preservedTtlMs = Math.max(1, existing.expiresAt - Date.now());
    }

    // Perform upsert with same key, preserving TTL
    const entry = await upsert({
      key: cfg.key,
      value: cfg.value,
      ttlMs: preservedTtlMs,
      tenantId: cfg.tenantId,
    });

    return { ok: true, entry };
  };

  // ==========================
  // reader
  // ==========================

  const reader = (readerCfg: { key?: string; prefix?: string; after?: string; tenantId?: string } = {}): RegistryReader<TData> => {
    const tenantId = resolveTenant(readerCfg.tenantId);
    const state = getTenantState(tenantId);

    // Select the appropriate EventLog
    let log: EventLog;
    if (readerCfg.key) {
      log = getKeyEventLog(state, readerCfg.key);
    } else if (readerCfg.prefix) {
      log = getPrefixEventLog(state, readerCfg.prefix);
    } else {
      log = state.rootEventLog;
    }

    let cursor = readerCfg.after ?? log.latest();

    const parseEvent = (entry: EventLogEntry): RegistryEvent<TData> | null => {
      const type = entry.fields.type as string;

      if (type === "upsert") {
        const rawPayload = entry.fields.payload as string | undefined;
        if (!rawPayload) return null;

        try {
          const payload = JSON.parse(rawPayload) as unknown;
          const parsed = config.schema.safeParse(payload);
          if (!parsed.success) return null;

          return {
            type: "upsert",
            cursor: entry.id,
            entry: {
              key: (entry.fields.key as string) ?? "",
              value: parsed.data,
              version: String(entry.fields.version ?? ""),
              status: "active" as const,
              createdAt: Number(entry.fields.createdAt ?? entry.fields.updatedAt),
              updatedAt: Number(entry.fields.updatedAt),
              ttlMs: entry.fields.ttlMs != null ? Number(entry.fields.ttlMs) : null,
              expiresAt: entry.fields.expiresAt != null ? Number(entry.fields.expiresAt) : null,
            },
          };
        } catch {
          return null;
        }
      }

      if (type === "touch") {
        return {
          type: "touch",
          cursor: entry.id,
          key: (entry.fields.key as string) ?? "",
          version: String(entry.fields.version ?? ""),
          updatedAt: Number(entry.fields.updatedAt ?? entry.fields.expiresAt),
          expiresAt: Number(entry.fields.expiresAt),
        };
      }

      if (type === "delete") {
        return {
          type: "delete",
          cursor: entry.id,
          key: (entry.fields.key as string) ?? "",
          version: String(entry.fields.version ?? ""),
          removedAt: Number(entry.fields.removedAt ?? entry.fields.deletedAt),
          reason: entry.fields.reason as string | undefined,
        };
      }

      if (type === "expire") {
        return {
          type: "expire",
          cursor: entry.id,
          key: (entry.fields.key as string) ?? "",
          version: String(entry.fields.version ?? ""),
          removedAt: Number(entry.fields.removedAt ?? entry.fields.expiredAt),
        };
      }

      return null;
    };

    const recv = async (cfg: RegistryRecvConfig = {}): Promise<RegistryEvent<TData> | null> => {
      const wait = cfg.wait ?? true;
      const timeoutMs = cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      // Try buffered entries
      const entries = log.range(cursor, 1);
      if (entries.length > 0) {
        cursor = entries[0]!.id;
        return parseEvent(entries[0]!);
      }

      if (!wait) return null;

      // Wait with timeout
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), timeoutMs);

      try {
        for await (const entry of log.subscribe(cursor, cfg.signal ?? ac.signal)) {
          clearTimeout(timeout);
          cursor = entry.id;
          const parsed = parseEvent(entry);
          if (parsed) return parsed;
        }
      } catch {
        // Timeout or abort
      } finally {
        clearTimeout(timeout);
      }

      return null;
    };

    const stream = async function* (cfg: RegistryRecvConfig = {}): AsyncIterable<RegistryEvent<TData>> {
      const wait = cfg.wait ?? true;

      while (!cfg.signal?.aborted) {
        const event = await recv(cfg);
        if (event) {
          yield event;
          continue;
        }
        if (!wait) break;
      }
    };

    return { recv, stream };
  };

  return { upsert, touch, remove, get, list, cas, reader };
};
