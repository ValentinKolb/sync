import { type Store } from "./store";
import { resolveStore, sharedState } from "./internal/shared-state";
import { EventLog, type EventLogEntry } from "./internal/event-log";
import { randomId } from "./internal/id";

const DEFAULT_PREFIX = "sync:topic";
const DEFAULT_TENANT = "default";
const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_MAX_LOG_ENTRIES = 256;
const DEFAULT_TIMEOUT_MS = 30_000;

const textEncoder = new TextEncoder();

const assertTtlMs = (ttlMs: number): void => {
  if (!Number.isInteger(ttlMs) || ttlMs <= 0 || ttlMs > Number.MAX_SAFE_INTEGER) {
    throw new Error("idempotencyTtlMs must be a positive integer number of milliseconds");
  }
};

const assertPositiveSafeInteger = (name: string, value: number): void => {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer`);
  }
};

const assertNonNegativeSafeInteger = (name: string, value: number): void => {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer`);
  }
};

// ==========================
// Types
// ==========================

export type TopicConfig<T = unknown> = {
  id: string;
  tenantId?: string;
  prefix?: string;
  limits?: {
    payloadBytes?: number;
  };
  retentionMs?: number;
  store?: Store;
};

export type TopicPubConfig<T> = {
  tenantId?: string;
  data: T;
  orderingKey?: string;
  idempotencyKey?: string;
  idempotencyTtlMs?: number;
  meta?: Record<string, unknown>;
};

export type TopicCursorConfig = {
  tenantId?: string;
};

export type TopicRecvConfig = {
  tenantId?: string;
  timeoutMs?: number;
  wait?: boolean;
  signal?: AbortSignal;
  invalidPayload?: "ack" | "throw";
};

export type TopicReclaimConfig = {
  tenantId?: string;
  minIdleMs?: number;
  cursor?: string;
  count?: number;
};

export type TopicDelivery<T> = {
  data: T;
  eventId: string;
  deliveryId: string;
  cursor: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
  commit(): Promise<boolean>;
};

export type TopicInvalidDelivery = {
  kind: "invalid";
  eventId: string;
  deliveryId: string;
  cursor: string;
  error: string;
  rawPayload: string | null;
  commit(): Promise<boolean>;
};

export type TopicReclaimedDelivery<T> =
  | { kind: "delivery"; delivery: TopicDelivery<T> }
  | TopicInvalidDelivery;

export type TopicReclaimResult<T> = {
  nextCursor: string;
  entries: TopicReclaimedDelivery<T>[];
};

export class TopicPayloadError extends Error {
  readonly eventId: string;
  readonly rawPayload: string | null;

  constructor(eventId: string, reason: string, rawPayload: string | null) {
    super(`Invalid topic payload at ${eventId}: ${reason}`);
    this.name = "TopicPayloadError";
    this.eventId = eventId;
    this.rawPayload = rawPayload;
  }
}

export type TopicLiveConfig = {
  tenantId?: string;
  after?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
};

export type TopicLiveEvent<T> = {
  data: T;
  eventId: string;
  cursor: string;
  orderingKey?: string;
  publishedAt: number;
  meta?: Record<string, unknown>;
};

export type TopicReader<T> = {
  group: string;
  recv(cfg?: TopicRecvConfig): Promise<TopicDelivery<T> | null>;
  reclaim?(cfg?: TopicReclaimConfig): Promise<TopicReclaimResult<T>>;
  stream(cfg?: TopicRecvConfig): AsyncIterable<TopicDelivery<T>>;
  /** Permanently close the reader and release its resources. Idempotent. */
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

export type TopicReaderConfig = {
  /** Stable consumer name. Accepted for parity; in-memory readers keep no registry. */
  consumerId?: string;
};

export type RecoverableTopicReader<T> = TopicReader<T> & {
  reclaim(cfg?: TopicReclaimConfig): Promise<TopicReclaimResult<T>>;
};

export type Topic<T> = {
  pub(cfg: TopicPubConfig<T>): Promise<{ eventId: string; cursor: string }>;
  latestCursor(cfg?: TopicCursorConfig): Promise<string | null>;
  reader(group?: string, cfg?: TopicReaderConfig): TopicReader<T>;
  live(cfg?: TopicLiveConfig): AsyncIterable<TopicLiveEvent<T>>;
};

export type RecoverableTopic<T> = Omit<Topic<T>, "reader"> & {
  reader(group?: string, cfg?: TopicReaderConfig): RecoverableTopicReader<T>;
};

// ==========================
// Internal Types
// ==========================

type StoredEvent<T> = {
  data: T;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  publishedAt: number;
};

type IdempotencyFence = {
  eventId: string;
  payloadHash?: string;
};

type GroupState = {
  committed: string;
  delivered: string;
  inFlight: Map<string, { at: number; consumerId: string }>;
};

type PersistedGroupState = Omit<GroupState, "inFlight"> & {
  inFlight: Array<[string, { at: number; consumerId: string }]>;
};

const persistedEntries = (value: unknown): EventLogEntry[] => {
  if (!value || typeof value !== "object") return [];
  const entries = (value as { entries?: unknown }).entries;
  if (!Array.isArray(entries)) return [];

  return entries.flatMap((entry): EventLogEntry[] => {
    if (!entry || typeof entry !== "object") return [];
    const candidate = entry as Partial<EventLogEntry>;
    if (
      typeof candidate.id !== "string"
      || typeof candidate.ts !== "number"
      || !Number.isFinite(candidate.ts)
      || !candidate.fields
      || typeof candidate.fields !== "object"
      || Array.isArray(candidate.fields)
    ) {
      return [];
    }
    return [{ id: candidate.id, ts: candidate.ts, fields: { ...candidate.fields } }];
  });
};

const parseIdempotencyFence = (value: unknown): IdempotencyFence | null => {
  if (typeof value === "string") return { eventId: value };
  if (
    !value
    || typeof value !== "object"
    || typeof (value as { eventId?: unknown }).eventId !== "string"
  ) {
    return null;
  }
  const candidate = value as { eventId: string; payloadHash?: unknown };
  return {
    eventId: candidate.eventId,
    ...(typeof candidate.payloadHash === "string" ? { payloadHash: candidate.payloadHash } : {}),
  };
};

const sha256 = async (value: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", textEncoder.encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

const persistedGroup = (value: unknown): GroupState | null => {
  if (!value || typeof value !== "object") return null;
  const candidate = value as Partial<PersistedGroupState>;
  if (
    typeof candidate.committed !== "string"
    || typeof candidate.delivered !== "string"
    || !Array.isArray(candidate.inFlight)
  ) {
    return null;
  }
  const validCursor = (cursor: string, allowZero: boolean): boolean =>
    (allowZero && cursor === "0")
    || (/^[1-9]\d*$/.test(cursor) && Number.isSafeInteger(Number(cursor)));
  if (
    !validCursor(candidate.committed, true)
    || !validCursor(candidate.delivered, true)
    || Number(candidate.committed) > Number(candidate.delivered)
  ) {
    return null;
  }

  const inFlight = new Map<string, { at: number; consumerId: string }>();
  for (const item of candidate.inFlight) {
    if (
      !Array.isArray(item)
      || item.length !== 2
      || typeof item[0] !== "string"
      || !validCursor(item[0], false)
      || Number(item[0]) > Number(candidate.delivered)
    ) {
      continue;
    }
    const held = item[1];
    if (
      !held
      || typeof held !== "object"
      || typeof held.at !== "number"
      || !Number.isFinite(held.at)
      || typeof held.consumerId !== "string"
    ) {
      continue;
    }
    inFlight.set(item[0], { at: held.at, consumerId: held.consumerId });
  }
  return { committed: candidate.committed, delivered: candidate.delivered, inFlight };
};

// ==========================
// Topic Factory
// ==========================

export const topic = <T>(config: TopicConfig<T>): RecoverableTopic<T> => {
  type TData = T;

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const retentionMs = config.retentionMs ?? DEFAULT_RETENTION_MS;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;
  assertPositiveSafeInteger("retentionMs", retentionMs);
  assertPositiveSafeInteger("limits.payloadBytes", maxPayloadBytes);
  const store = resolveStore(config.store);

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;
  const encodedKey = (kind: string, ...segments: string[]): string =>
    `sync:topic:browser:v2:${encodeURIComponent(JSON.stringify([prefix, config.id, kind, ...segments]))}`;
  const eventLogMapKey = (tenantId: string): string =>
    JSON.stringify([prefix, config.id, tenantId]);
  const eventLogStoreKey = (tenantId: string): string => encodedKey("event-log", tenantId);
  const highWaterStoreKey = (tenantId: string): string => encodedKey("high-water", tenantId);
  const groupStoreKey = (tenantId: string, group: string): string => encodedKey("group", tenantId, group);

  // One EventLog per tenant, shared by every handle in this scope.
  const eventLogs = sharedState(
    JSON.stringify(["topic:logs", prefix, config.id]),
    config.store,
    () => new Map<string, EventLog>(),
  );
  const getEventLog = (tenantId: string): EventLog => {
    const key = eventLogMapKey(tenantId);
    let log = eventLogs.get(key);
    if (!log) {
      const initialEntries = config.store
        ? persistedEntries(store.get(eventLogStoreKey(tenantId)))
        : [];
      log = new EventLog({ retentionMs, maxLen: DEFAULT_MAX_LOG_ENTRIES, initialEntries });
      const highWater = store.get(highWaterStoreKey(tenantId));
      if (typeof highWater === "string") log.advanceTo(highWater);
      eventLogs.set(key, log);
    }
    return log;
  };
  const persistEventLog = (tenantId: string, log: EventLog): void => {
    if (config.store) store.set(eventLogStoreKey(tenantId), { entries: log.snapshot() });
  };
  const persistHighWater = (tenantId: string, eventId: string): void => {
    if (config.store) store.set(highWaterStoreKey(tenantId), eventId);
  };
  const restorePersistedEventLog = (tenantId: string, eventId: string): EventLog | null => {
    if (!config.store) return null;
    const log = getEventLog(tenantId);
    log.restore(persistedEntries(store.get(eventLogStoreKey(tenantId))));
    return log.has(eventId) ? log : null;
  };

  const idempotencyKey = (tenantId: string, key: string): string => encodedKey("idempotency", tenantId, key);
  const parsePayload = (entry: EventLogEntry): StoredEvent<unknown> | null => {
    const rawPayload = entry.fields.payload;
    if (typeof rawPayload !== "string") return null;

    try {
      return JSON.parse(rawPayload) as StoredEvent<unknown>;
    } catch {
      return null;
    }
  };

  // ==========================
  // pub
  // ==========================

  const pub = async (pubCfg: TopicPubConfig<TData>): Promise<{ eventId: string; cursor: string }> => {
    const tenantId = resolveTenant(pubCfg.tenantId);
    const idempotencyTtlMs = pubCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS;
    if (pubCfg.idempotencyKey || pubCfg.idempotencyTtlMs !== undefined) {
      assertTtlMs(idempotencyTtlMs);
    }
    let log = getEventLog(tenantId);

    const payload: StoredEvent<TData> = {
      data: pubCfg.data,
      orderingKey: pubCfg.orderingKey,
      meta: pubCfg.meta,
      publishedAt: Date.now(),
    };

    const payloadRaw = JSON.stringify(payload);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new Error(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    // Idempotency check
    if (pubCfg.idempotencyKey) {
      const idemKey = idempotencyKey(tenantId, pubCfg.idempotencyKey);
      const payloadHash = await sha256(payloadRaw);
      const existing = parseIdempotencyFence(store.get(idemKey));
      if (existing && !log.has(existing.eventId)) {
        log = restorePersistedEventLog(tenantId, existing.eventId) ?? log;
      }
      if (existing) {
        const storedPayload = log.get(existing.eventId)?.fields.payload;
        const matches = typeof storedPayload === "string"
          && (existing.payloadHash === undefined || await sha256(storedPayload) === existing.payloadHash);
        if (matches) {
          return { eventId: existing.eventId, cursor: existing.eventId };
        }
        log.advanceTo(existing.eventId);
        store.del(idemKey);
      }

      const eventId = log.append(
        { payload: payloadRaw },
        {
          beforeEmit: (entry) => {
            persistHighWater(tenantId, entry.id);
            store.set(idemKey, { eventId: entry.id, payloadHash }, idempotencyTtlMs);
            persistEventLog(tenantId, log);
          },
          rollback: () => {
            try {
              persistEventLog(tenantId, log);
            } catch {
              // The persisted snapshot is uncertain, so the event ID must
              // remain fenced until a later handle can resolve it.
              return;
            }
            store.del(idemKey);
          },
        },
      );
      return { eventId, cursor: eventId };
    }

    const eventId = log.append(
      { payload: payloadRaw },
      {
        beforeEmit: (entry) => {
          persistHighWater(tenantId, entry.id);
          persistEventLog(tenantId, log);
        },
        rollback: () => persistEventLog(tenantId, log),
      },
    );
    return { eventId, cursor: eventId };
  };

  const latestCursor = async (cursorCfg: TopicCursorConfig = {}): Promise<string | null> => {
    const tenantId = resolveTenant(cursorCfg.tenantId);
    const existing = eventLogs.get(eventLogMapKey(tenantId));
    const cursor = existing
      ? existing.latest()
      : config.store
          ? new EventLog({
              retentionMs,
              maxLen: DEFAULT_MAX_LOG_ENTRIES,
              initialEntries: persistedEntries(store.get(eventLogStoreKey(tenantId))),
            }).latest()
        : "0";
    return cursor === "0" ? null : cursor;
  };

  // ==========================
  // reader
  // ==========================

  /**
   * Per-(topic, tenant, group) delivery state, shared by every reader of that
   * group. `group` used to be a display string: each reader() allocated a
   * private cursor starting at "0", so same-group readers broadcast instead of
   * distributing, every side effect ran once per worker, and a reader recreated
   * after an SPA remount or a React StrictMode double-mount replayed the entire
   * retained log.
   */
  const groupState = (tenantId: string, group: string): GroupState =>
    sharedState(JSON.stringify(["topic:group", prefix, config.id, tenantId, group]), config.store, () => {
      const restored = config.store
        ? persistedGroup(store.get(groupStoreKey(tenantId, group)))
        : null;
      return restored ?? { committed: "0", delivered: "0", inFlight: new Map() };
    });
  const cloneGroup = (state: GroupState): GroupState => ({
    committed: state.committed,
    delivered: state.delivered,
    inFlight: new Map(state.inFlight),
  });
  const persistGroup = (tenantId: string, group: string, state: GroupState): void => {
    if (!config.store) return;
    const persisted: PersistedGroupState = {
      committed: state.committed,
      delivered: state.delivered,
      inFlight: [...state.inFlight.entries()],
    };
    store.set(groupStoreKey(tenantId, group), persisted);
  };
  const publishGroup = (tenantId: string, group: string, current: GroupState, next: GroupState): void => {
    persistGroup(tenantId, group, next);
    current.committed = next.committed;
    current.delivered = next.delivered;
    current.inFlight = next.inFlight;
  };

  const reader = (group = "default", readerCfg: TopicReaderConfig = {}): RecoverableTopicReader<TData> => {
    const consumerId = readerCfg.consumerId ?? `consumer:${randomId()}`;
    const closeController = new AbortController();
    let closed = false;

    const assertOpen = (): void => {
      if (closed) throw new Error("topic reader is closed");
    };

    const deliverEntry = (
      entry: EventLogEntry,
      tenantId: string,
      state: GroupState,
      invalidPayload: TopicRecvConfig["invalidPayload"],
    ): TopicDelivery<TData> | null => {
      const stored = parsePayload(entry);
      if (!stored) {
        // Malformed transport envelope. The server honours invalidPayload:
        // "throw" here; the browser accepted the option and never read it, so a
        // catch written against the documented contract was dead code.
        if (invalidPayload === "throw") {
          const raw = entry.fields.payload;
          throw new TopicPayloadError(
            entry.id,
            "envelope is not a valid topic payload",
            typeof raw === "string" ? raw : null,
          );
        }
        const next = cloneGroup(state);
        next.committed = entry.id;
        next.delivered = entry.id;
        publishGroup(tenantId, group, state, next);
        return null;
      }

      const next = cloneGroup(state);
      next.delivered = entry.id;
      next.inFlight.set(entry.id, { at: Date.now(), consumerId });
      publishGroup(tenantId, group, state, next);

      const commit = async (): Promise<boolean> => {
        const held = state.inFlight.get(entry.id);
        // Mirror the server's fenced XACK: only the current owner may commit.
        if (!held || held.consumerId !== consumerId) return false;
        const committed = cloneGroup(state);
        committed.inFlight.delete(entry.id);
        if (Number(entry.id) > Number(committed.committed)) committed.committed = entry.id;
        publishGroup(tenantId, group, state, committed);
        return true;
      };

      return {
        data: stored.data as TData,
        eventId: entry.id,
        cursor: entry.id,
        deliveryId: `${group}:${entry.id}:${consumerId}`,
        orderingKey: stored.orderingKey,
        publishedAt: stored.publishedAt,
        meta: stored.meta,
        commit,
      };
    };

    const nextFromLog = (
      log: EventLog,
      tenantId: string,
      state: GroupState,
      invalidPayload: TopicRecvConfig["invalidPayload"],
    ): TopicDelivery<TData> | null => {
      // Keep draining past malformed entries rather than reporting end-of-log.
      while (true) {
        const entries = log.range(state.delivered, 1);
        if (entries.length === 0) return null;
        const delivery = deliverEntry(entries[0]!, tenantId, state, invalidPayload);
        if (delivery) return delivery;
      }
    };

    const recv = async (recvCfg: TopicRecvConfig = {}): Promise<TopicDelivery<TData> | null> => {
      assertOpen();
      if (recvCfg.signal?.aborted) return null;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;
      assertNonNegativeSafeInteger("timeoutMs", timeoutMs);
      const tenantId = resolveTenant(recvCfg.tenantId);
      const log = getEventLog(tenantId);
      const state = groupState(tenantId, group);
      const wait = recvCfg.wait ?? true;

      const immediate = nextFromLog(log, tenantId, state, recvCfg.invalidPayload);
      if (immediate) return immediate;

      if (!wait) return null;

      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), timeoutMs);
      const onUserAbort = (): void => ac.abort();
      const onReaderClose = (): void => ac.abort();
      if (recvCfg.signal) recvCfg.signal.addEventListener("abort", onUserAbort, { once: true });
      closeController.signal.addEventListener("abort", onReaderClose, { once: true });

      try {
        for await (const _entry of log.subscribe(state.delivered, ac.signal)) {
          const delivery = nextFromLog(log, tenantId, state, recvCfg.invalidPayload);
          if (delivery) return delivery;
        }
      } catch {
        // Timeout or abort.
      } finally {
        clearTimeout(timeout);
        if (recvCfg.signal) recvCfg.signal.removeEventListener("abort", onUserAbort);
        closeController.signal.removeEventListener("abort", onReaderClose);
      }

      return null;
    };

    const stream = async function* (streamCfg: TopicRecvConfig = {}): AsyncIterable<TopicDelivery<TData>> {
      const wait = streamCfg.wait ?? true;

      while (!closed && !streamCfg.signal?.aborted) {
        const message = await recv(streamCfg);
        if (message) {
          yield message;
          continue;
        }
        if (!wait) break;
      }
    };

    /**
     * Recover deliveries this group handed out and never committed. Previously a
     * validating stub returning nothing, which satisfied the type but left
     * uncommitted work unrecoverable — the cursor had already advanced past it.
     */
    const reclaim = async (reclaimCfg: TopicReclaimConfig = {}): Promise<TopicReclaimResult<TData>> => {
      assertOpen();
      const minIdleMs = reclaimCfg.minIdleMs ?? 60_000;
      assertNonNegativeSafeInteger("minIdleMs", minIdleMs);
      const count = reclaimCfg.count ?? 25;
      if (!Number.isInteger(count) || count < 1 || count > 1_000) {
        throw new Error("count must be an integer between 1 and 1000");
      }

      const tenantId = resolveTenant(reclaimCfg.tenantId);
      const log = getEventLog(tenantId);
      const state = groupState(tenantId, group);
      const nextState = cloneGroup(state);
      const now = Date.now();
      const cursor = Number(reclaimCfg.cursor ?? "0-0") || 0;

      const candidates = [...state.inFlight.entries()]
        .filter(([eventId, held]) => Number(eventId) > cursor && now - held.at >= minIdleMs)
        .sort(([a], [b]) => Number(a) - Number(b));
      const stale = candidates.slice(0, count);

      const entries: Array<TopicReclaimedDelivery<TData>> = [];
      let lastId = "0-0";
      let groupChanged = false;
      for (const [eventId] of stale) {
        lastId = eventId;
        const found = log.range(String(Number(eventId) - 1), 1).find((e) => e.id === eventId);
        if (!found) {
          nextState.inFlight.delete(eventId);
          groupChanged = true;
          continue;
        }
        // Take ownership, then hand it out again.
        nextState.inFlight.set(eventId, { at: now, consumerId });
        groupChanged = true;
        const commit = async (): Promise<boolean> => {
          const held = state.inFlight.get(found.id);
          if (!held || held.consumerId !== consumerId) return false;
          const committed = cloneGroup(state);
          committed.inFlight.delete(found.id);
          if (Number(found.id) > Number(committed.committed)) committed.committed = found.id;
          publishGroup(tenantId, group, state, committed);
          return true;
        };

        const stored = parsePayload(found);
        if (!stored) {
          const raw = found.fields.payload;
          entries.push({
            kind: "invalid",
            eventId: found.id,
            deliveryId: `${group}:${found.id}:${consumerId}`,
            cursor: found.id,
            error: "envelope is not a valid topic payload",
            rawPayload: typeof raw === "string" ? raw : null,
            commit,
          });
          continue;
        }

        entries.push({
          kind: "delivery",
          delivery: {
            data: stored.data as TData,
            eventId: found.id,
            cursor: found.id,
            deliveryId: `${group}:${found.id}:${consumerId}`,
            orderingKey: stored.orderingKey,
            publishedAt: stored.publishedAt,
            meta: stored.meta,
            commit,
          },
        });
      }
      if (groupChanged) publishGroup(tenantId, group, state, nextState);

      return { nextCursor: candidates.length > stale.length ? lastId : "0-0", entries };
    };

    const close = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      closeController.abort();
    };

    return { group, recv, reclaim, stream, close, [Symbol.asyncDispose]: close };
  };

  // ==========================
  // live
  // ==========================

  const live = async function* (liveCfg: TopicLiveConfig = {}): AsyncIterable<TopicLiveEvent<TData>> {
    if (liveCfg.timeoutMs !== undefined) assertNonNegativeSafeInteger("timeoutMs", liveCfg.timeoutMs);
    const tenantId = resolveTenant(liveCfg.tenantId);
    const log = getEventLog(tenantId);

    let cursor = liveCfg.after ?? log.latest();

    for await (const entry of log.subscribe(cursor, liveCfg.signal)) {
      const stored = parsePayload(entry);
      if (!stored) continue;

      cursor = entry.id;

      yield {
        data: stored.data as TData,
        eventId: entry.id,
        cursor: entry.id,
        orderingKey: stored.orderingKey,
        publishedAt: stored.publishedAt,
        meta: stored.meta,
      };
    }
  };

  return { pub, latestCursor, reader, live };
};
