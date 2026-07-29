import { type Store, createMemoryStore } from "./store";
import { EventLog, type EventLogEntry } from "./internal/event-log";
import { randomId } from "./internal/id";

const DEFAULT_PREFIX = "sync:topic";
const DEFAULT_TENANT = "default";
const DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

const textEncoder = new TextEncoder();

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
  /** Release reader resources. Idempotent. In-memory readers hold no connection. */
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

// ==========================
// Topic Factory
// ==========================

export const topic = <T>(config: TopicConfig<T>): RecoverableTopic<T> => {
  type TData = T;

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const retentionMs = config.retentionMs ?? DEFAULT_RETENTION_MS;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;
  const store = config.store ?? createMemoryStore();

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  // One EventLog per tenant
  const eventLogs = new Map<string, EventLog>();
  const getEventLog = (tenantId: string): EventLog => {
    const key = `${prefix}:${tenantId}:${config.id}`;
    let log = eventLogs.get(key);
    if (!log) {
      log = new EventLog({ retentionMs });
      eventLogs.set(key, log);
    }
    return log;
  };

  const idempotencyKey = (tenantId: string, key: string): string =>
    `${prefix}:${tenantId}:${config.id}:idempotency:${key}`;

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
    const log = getEventLog(tenantId);

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
      const existing = store.get(idemKey) as string | undefined;
      if (existing) {
        return { eventId: existing, cursor: existing };
      }

      const eventId = log.append({ payload: payloadRaw });
      store.set(idemKey, eventId, pubCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS);
      return { eventId, cursor: eventId };
    }

    const eventId = log.append({ payload: payloadRaw });
    return { eventId, cursor: eventId };
  };

  const latestCursor = async (cursorCfg: TopicCursorConfig = {}): Promise<string | null> => {
    const tenantId = resolveTenant(cursorCfg.tenantId);
    const cursor = getEventLog(tenantId).latest();
    return cursor === "0" ? null : cursor;
  };

  // ==========================
  // reader
  // ==========================

  const reader = (group = "default", _readerCfg: TopicReaderConfig = {}): RecoverableTopicReader<TData> => {
    const consumerId = `consumer:${randomId()}`;
    const cursors = new Map<string, string>();
    const getCursor = (tenantId: string): string => cursors.get(tenantId) ?? "0";
    const setCursor = (tenantId: string, c: string): void => { cursors.set(tenantId, c); };

    const recv = async (recvCfg: TopicRecvConfig = {}): Promise<TopicDelivery<TData> | null> => {
      const tenantId = resolveTenant(recvCfg.tenantId);
      const log = getEventLog(tenantId);
      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      // Try to get the next entry after cursor
      const entries = log.range(getCursor(tenantId), 1);
      if (entries.length > 0) {
        return deliverEntry(entries[0]!, log, tenantId);
      }

      if (!wait) return null;

      // Wait for the next entry with timeout
      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), timeoutMs);

      // Combine user signal and timeout signal
      const onUserAbort = (): void => ac.abort();
      if (recvCfg.signal) recvCfg.signal.addEventListener("abort", onUserAbort, { once: true });

      try {
        for await (const entry of log.subscribe(getCursor(tenantId), ac.signal)) {
          clearTimeout(timeout);
          if (recvCfg.signal) recvCfg.signal.removeEventListener("abort", onUserAbort);
          return deliverEntry(entry, log, tenantId);
        }
      } catch {
        // Timeout or abort
      } finally {
        clearTimeout(timeout);
        if (recvCfg.signal) recvCfg.signal.removeEventListener("abort", onUserAbort);
      }

      return null;
    };

    const deliverEntry = (entry: EventLogEntry, _log: EventLog, tenantId: string): TopicDelivery<TData> | null => {
      const stored = parsePayload(entry);
      if (!stored) {
        setCursor(tenantId, entry.id);
        return null;
      }

      setCursor(tenantId, entry.id);

      const commit = async (): Promise<boolean> => {
        // In-memory: commit is a no-op (cursor already advanced)
        return true;
      };

      return {
        data: stored.data as TData,
        eventId: entry.id,
        cursor: entry.id,
        deliveryId: `${group}:${entry.id}`,
        orderingKey: stored.orderingKey,
        publishedAt: stored.publishedAt,
        meta: stored.meta,
        commit,
      };
    };

    const stream = async function* (streamCfg: TopicRecvConfig = {}): AsyncIterable<TopicDelivery<TData>> {
      const wait = streamCfg.wait ?? true;

      while (!streamCfg.signal?.aborted) {
        const message = await recv(streamCfg);
        if (message) {
          yield message;
          continue;
        }
        if (!wait) break;
      }
    };

    const reclaim = async (reclaimCfg: TopicReclaimConfig = {}): Promise<TopicReclaimResult<TData>> => {
      const minIdleMs = reclaimCfg.minIdleMs ?? 60_000;
      if (!Number.isFinite(minIdleMs) || minIdleMs < 0) throw new Error("minIdleMs must be a non-negative number");
      const count = reclaimCfg.count ?? 25;
      if (!Number.isInteger(count) || count < 1 || count > 1_000) {
        throw new Error("count must be an integer between 1 and 1000");
      }
      return { nextCursor: "0-0", entries: [] };
    };

    const close = async (): Promise<void> => {
      // No connection to release in memory; present so the same teardown code
      // works on both runtimes.
    };

    return { group, recv, reclaim, stream, close, [Symbol.asyncDispose]: close };
  };

  // ==========================
  // live
  // ==========================

  const live = async function* (liveCfg: TopicLiveConfig = {}): AsyncIterable<TopicLiveEvent<TData>> {
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
