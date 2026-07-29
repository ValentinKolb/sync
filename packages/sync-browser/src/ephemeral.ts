import { EventLog, type EventLogEntry } from "./internal/event-log";
import { sharedState } from "./internal/shared-state";

const DEFAULT_MAX_ENTRIES = 10_000;
const DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024;
const DEFAULT_EVENT_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_EVENT_MAXLEN = 50_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_KEY_BYTES = 512;

const textEncoder = new TextEncoder();

// ==========================
// Error Classes
// ==========================

export class EphemeralCapacityError extends Error {
  constructor(message = "ephemeral store capacity reached") {
    super(message);
    this.name = "EphemeralCapacityError";
  }
}

export class EphemeralPayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EphemeralPayloadTooLargeError";
  }
}

// ==========================
// Types
// ==========================

export type EphemeralConfig<T = unknown> = {
  id: string;
  ttlMs: number;
  tenantId?: string;
  limits?: {
    maxEntries?: number;
    maxPayloadBytes?: number;
    eventRetentionMs?: number;
    eventMaxLen?: number;
  };
};

export type EphemeralUpsertConfig<T> = {
  key: string;
  value: T;
  ttlMs?: number;
  tenantId?: string;
};

export type EphemeralTouchConfig = {
  key: string;
  ttlMs?: number;
  tenantId?: string;
};

export type EphemeralRemoveConfig = {
  key: string;
  reason?: string;
  tenantId?: string;
};

export type EphemeralEntry<T> = {
  key: string;
  value: T;
  version: string;
  createdAt: number;
  updatedAt: number;
  expiresAt: number;
};

export type EphemeralSnapshot<T> = {
  entries: EphemeralEntry<T>[];
  cursor: string;
};

export type EphemeralRecvConfig = {
  wait?: boolean;
  timeoutMs?: number;
  signal?: AbortSignal;
};

export type EphemeralEvent<T> =
  | { type: "upsert"; cursor: string; entry: EphemeralEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; deletedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; expiredAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };

export type EphemeralReader<T> = {
  recv(cfg?: EphemeralRecvConfig): Promise<EphemeralEvent<T> | null>;
  stream(cfg?: EphemeralRecvConfig): AsyncIterable<EphemeralEvent<T>>;
  /** Release reader resources. Idempotent. In-memory readers hold no connection. */
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

export type EphemeralStore<T> = {
  upsert(cfg: EphemeralUpsertConfig<T>): Promise<EphemeralEntry<T>>;
  touch(cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: EphemeralRemoveConfig): Promise<boolean>;
  snapshot(cfg?: { tenantId?: string; prefix?: string }): Promise<EphemeralSnapshot<T>>;
  reader(cfg?: { after?: string; tenantId?: string; prefix?: string }): EphemeralReader<T>;
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
  expiresAt: number;
};

type TenantState<T> = {
  seq: number;
  entries: Map<string, StoredEntry<T>>;
  timers: Map<string, ReturnType<typeof setTimeout>>;
  eventLog: EventLog;
};

// ==========================
// Helpers
// ==========================

const assertLogicalKey = (value: string): void => {
  if (value.length === 0) throw new Error("key must be non-empty");
  const bytes = textEncoder.encode(value).byteLength;
  if (bytes > MAX_KEY_BYTES) throw new Error(`key exceeds max length (${MAX_KEY_BYTES} bytes)`);
};

const assertIdentifier = (value: string, label: string): void => {
  if (value.length === 0) throw new Error(`${label} must be non-empty`);
  if (value.length > 256) throw new Error(`${label} too long (max 256 chars)`);
};

// ==========================
// Ephemeral Factory
// ==========================

export const ephemeral = <T>(config: EphemeralConfig<T>): EphemeralStore<T> => {
  type TData = T;

  if (!Number.isFinite(config.ttlMs) || config.ttlMs <= 0) {
    throw new Error("ttlMs must be > 0");
  }
  assertIdentifier(config.id, "config.id");

  const defaultTenant = config.tenantId ?? "default";
  assertIdentifier(defaultTenant, "tenantId");
  const maxEntries = config.limits?.maxEntries ?? DEFAULT_MAX_ENTRIES;
  const maxPayloadBytes = config.limits?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  const eventRetentionMs = config.limits?.eventRetentionMs ?? DEFAULT_EVENT_RETENTION_MS;
  const eventMaxLen = config.limits?.eventMaxLen ?? DEFAULT_EVENT_MAXLEN;

  const resolveTenant = (tenantId?: string): string => {
    const resolved = tenantId ?? defaultTenant;
    assertIdentifier(resolved, "tenantId");
    return resolved;
  };

  // Per-tenant state, shared by every handle with this id.
  const tenantStates = sharedState(
    `ephemeral:${config.id}`,
    undefined,
    () => new Map<string, TenantState<TData>>(),
  );
  const getTenantState = (tenantId: string): TenantState<TData> => {
    let state = tenantStates.get(tenantId);
    if (!state) {
      state = {
        seq: 0,
        entries: new Map(),
        timers: new Map(),
        eventLog: new EventLog({ maxLen: eventMaxLen, retentionMs: eventRetentionMs }),
      };
      tenantStates.set(tenantId, state);
    }
    return state;
  };

  const scheduleExpiry = (state: TenantState<TData>, logicalKey: string, ttlMs: number): void => {
    // Clear existing timer
    const existing = state.timers.get(logicalKey);
    if (existing) clearTimeout(existing);

    state.timers.set(
      logicalKey,
      setTimeout(() => {
        const entry = state.entries.get(logicalKey);
        if (!entry) return;

        state.entries.delete(logicalKey);
        state.timers.delete(logicalKey);

        const version = String(++state.seq);
        state.eventLog.append({
          type: "expire",
          key: logicalKey,
          version,
          expiredAt: Date.now(),
        });
      }, ttlMs),
    );
  };

  // ==========================
  // upsert
  // ==========================

  const upsert = async (cfg: EphemeralUpsertConfig<TData>): Promise<EphemeralEntry<TData>> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const ttlMs = cfg.ttlMs ?? config.ttlMs;
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error("ttlMs must be > 0");
    }

    // Capacity check
    if (!state.entries.has(cfg.key) && state.entries.size >= maxEntries) {
      throw new EphemeralCapacityError(`maxEntries (${maxEntries}) reached`);
    }

    const payloadRaw = JSON.stringify(cfg.value);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new EphemeralPayloadTooLargeError(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const now = Date.now();
    const version = String(++state.seq);
    const expiresAt = now + ttlMs;

    // Preserve createdAt across upserts on the same key; reset only on fresh-create.
    const previous = state.entries.get(cfg.key);
    const createdAt = previous?.createdAt ?? now;

    const stored: StoredEntry<TData> = {
      key: cfg.key,
      data: cfg.value,
      version,
      createdAt,
      updatedAt: now,
      expiresAt,
    };

    state.entries.set(cfg.key, stored);
    scheduleExpiry(state, cfg.key, ttlMs);

    state.eventLog.append({
      type: "upsert",
      key: cfg.key,
      version,
      createdAt,
      updatedAt: now,
      expiresAt,
      payload: payloadRaw,
    });

    return {
      key: cfg.key,
      value: cfg.value,
      version,
      createdAt,
      updatedAt: now,
      expiresAt,
    };
  };

  // ==========================
  // touch
  // ==========================

  const touch = async (cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const existing = state.entries.get(cfg.key);
    if (!existing) return { ok: false };

    const ttlMs = cfg.ttlMs ?? config.ttlMs;
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error("ttlMs must be > 0");
    }

    const now = Date.now();
    const version = String(++state.seq);
    const expiresAt = now + ttlMs;

    existing.version = version;
    existing.updatedAt = now;
    existing.expiresAt = expiresAt;

    scheduleExpiry(state, cfg.key, ttlMs);

    state.eventLog.append({
      type: "touch",
      key: cfg.key,
      version,
      expiresAt,
    });

    return { ok: true, version, expiresAt };
  };

  // ==========================
  // remove
  // ==========================

  const remove = async (cfg: EphemeralRemoveConfig): Promise<boolean> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);

    const existing = state.entries.get(cfg.key);
    if (!existing) return false;

    state.entries.delete(cfg.key);

    // Clear timer
    const timer = state.timers.get(cfg.key);
    if (timer) {
      clearTimeout(timer);
      state.timers.delete(cfg.key);
    }

    const version = String(++state.seq);
    const fields: Record<string, unknown> = {
      type: "delete",
      key: cfg.key,
      version,
      deletedAt: Date.now(),
    };
    if (cfg.reason) fields.reason = cfg.reason;

    state.eventLog.append(fields);

    return true;
  };

  // ==========================
  // snapshot
  // ==========================

  const snapshot = async (cfg: { tenantId?: string; prefix?: string } = {}): Promise<EphemeralSnapshot<TData>> => {
    const tenantId = resolveTenant(cfg.tenantId);
    const state = getTenantState(tenantId);
    const prefix = cfg.prefix;

    const entries: EphemeralEntry<TData>[] = [];
    for (const stored of state.entries.values()) {
      if (prefix && !stored.key.startsWith(prefix)) continue;
      entries.push({
        key: stored.key,
        value: stored.data,
        version: stored.version,
        createdAt: stored.createdAt,
        updatedAt: stored.updatedAt,
        expiresAt: stored.expiresAt,
      });
    }

    entries.sort((a, b) => a.key.localeCompare(b.key));

    return {
      entries,
      cursor: state.eventLog.latest(),
    };
  };

  // ==========================
  // reader
  // ==========================

  const reader = (readerCfg: { after?: string; tenantId?: string; prefix?: string } = {}): EphemeralReader<TData> => {
    const tenantId = resolveTenant(readerCfg.tenantId);
    const state = getTenantState(tenantId);
    const prefix = readerCfg.prefix;

    const matchesPrefix = (event: EphemeralEvent<TData>): boolean => {
      if (!prefix) return true;
      if (event.type === "overflow") return true;
      if (event.type === "upsert") return event.entry.key.startsWith(prefix);
      return event.key.startsWith(prefix);
    };

    let cursor = readerCfg.after ?? state.eventLog.latest();
    let overflowPending: EphemeralEvent<TData> | null = null;
    let replayChecked = false;

    const checkReplayGap = (): void => {
      if (replayChecked) return;
      replayChecked = true;

      const after = readerCfg.after;
      if (!after || after === "0") return;

      const earliest = state.eventLog.earliest();
      if (!earliest) return;

      if (!state.eventLog.has(after) && Number(after) < Number(earliest)) {
        const liveCursor = state.eventLog.latest();
        overflowPending = {
          type: "overflow",
          cursor: liveCursor,
          after,
          firstAvailable: earliest,
        };
        cursor = liveCursor;
      }
    };

    const parseEvent = (entry: EventLogEntry): EphemeralEvent<TData> | null => {
      const type = entry.fields.type as string;

      if (type === "upsert") {
        const rawPayload = entry.fields.payload as string | undefined;
        if (!rawPayload) return null;

        try {
          const payload = JSON.parse(rawPayload) as TData;
          const updatedAt = Number(entry.fields.updatedAt);
          const createdAtField = entry.fields.createdAt;
          const createdAt =
            createdAtField !== undefined && Number.isFinite(Number(createdAtField))
              ? Number(createdAtField)
              : updatedAt;

          return {
            type: "upsert",
            cursor: entry.id,
            entry: {
              key: (entry.fields.key as string) ?? "",
              value: payload,
              version: String(entry.fields.version ?? ""),
              createdAt,
              updatedAt,
              expiresAt: Number(entry.fields.expiresAt),
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
          expiresAt: Number(entry.fields.expiresAt),
        };
      }

      if (type === "delete") {
        return {
          type: "delete",
          cursor: entry.id,
          key: (entry.fields.key as string) ?? "",
          version: String(entry.fields.version ?? ""),
          deletedAt: Number(entry.fields.deletedAt),
          reason: entry.fields.reason as string | undefined,
        };
      }

      if (type === "expire") {
        return {
          type: "expire",
          cursor: entry.id,
          key: (entry.fields.key as string) ?? "",
          version: String(entry.fields.version ?? ""),
          expiredAt: Number(entry.fields.expiredAt),
        };
      }

      return null;
    };

    const recv = async (cfg: EphemeralRecvConfig = {}): Promise<EphemeralEvent<TData> | null> => {
      checkReplayGap();

      if (overflowPending) {
        const event = overflowPending;
        overflowPending = null;
        return event;
      }

      const wait = cfg.wait ?? true;
      const timeoutMs = cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      // Loop to skip prefix-mismatched events. One pass if no prefix configured.
      while (true) {
        // Try buffered entries first
        const entries = state.eventLog.range(cursor, 1);
        if (entries.length > 0) {
          cursor = entries[0]!.id;
          const parsed = parseEvent(entries[0]!);
          if (parsed && matchesPrefix(parsed)) return parsed;
          continue; // skip and try next buffered entry
        }

        if (!wait) return null;

        // Wait with timeout for a matching event
        const ac = new AbortController();
        const timeout = setTimeout(() => ac.abort(), timeoutMs);
        const onUserAbort = (): void => ac.abort();
        if (cfg.signal) cfg.signal.addEventListener("abort", onUserAbort, { once: true });

        let got: EphemeralEvent<TData> | null = null;
        try {
          for await (const entry of state.eventLog.subscribe(cursor, ac.signal)) {
            cursor = entry.id;
            const parsed = parseEvent(entry);
            if (!parsed) continue;
            if (!matchesPrefix(parsed)) continue;
            got = parsed;
            break;
          }
        } catch {
          // Timeout or abort
        } finally {
          clearTimeout(timeout);
          if (cfg.signal) cfg.signal.removeEventListener("abort", onUserAbort);
        }

        return got;
      }
    };

    const stream = async function* (cfg: EphemeralRecvConfig = {}): AsyncIterable<EphemeralEvent<TData>> {
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

    const close = async (): Promise<void> => {
      // No connection to release in memory; present so the same teardown code
      // works on both runtimes.
    };

    return { recv, stream, close, [Symbol.asyncDispose]: close };
  };

  return { upsert, touch, remove, snapshot, reader };
};
