import { type Store } from "./store";
import { resolveStore, sharedState } from "./internal/shared-state";
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
  const store = resolveStore(config.store);

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  // One EventLog per tenant, shared by every handle in this scope.
  const eventLogs = sharedState(`topic:logs:${prefix}:${config.id}`, config.store, () => new Map<string, EventLog>());
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

  /**
   * Per-(topic, tenant, group) delivery state, shared by every reader of that
   * group. `group` used to be a display string: each reader() allocated a
   * private cursor starting at "0", so same-group readers broadcast instead of
   * distributing, every side effect ran once per worker, and a reader recreated
   * after an SPA remount or a React StrictMode double-mount replayed the entire
   * retained log.
   */
  type GroupState = {
    /** Last committed id. Delivery resumes from here, not from what was handed out. */
    committed: string;
    /** Highest id handed out but not yet committed. */
    delivered: string;
    /** eventId -> when it was delivered, for reclaim after minIdleMs. */
    inFlight: Map<string, { at: number; consumerId: string }>;
  };

  const groupState = (tenantId: string, group: string): GroupState =>
    sharedState(`topic:group:${prefix}:${tenantId}:${config.id}:${group}`, config.store, () => ({
      committed: "0",
      delivered: "0",
      inFlight: new Map<string, { at: number; consumerId: string }>(),
    }));

  const reader = (group = "default", readerCfg: TopicReaderConfig = {}): RecoverableTopicReader<TData> => {
    const consumerId = readerCfg.consumerId ?? `consumer:${randomId()}`;

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
        state.committed = entry.id;
        state.delivered = entry.id;
        return null;
      }

      state.delivered = entry.id;
      state.inFlight.set(entry.id, { at: Date.now(), consumerId });

      const commit = async (): Promise<boolean> => {
        const held = state.inFlight.get(entry.id);
        // Mirror the server's fenced XACK: only the current owner may commit.
        if (!held || held.consumerId !== consumerId) return false;
        state.inFlight.delete(entry.id);
        if (Number(entry.id) > Number(state.committed)) state.committed = entry.id;
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
      const tenantId = resolveTenant(recvCfg.tenantId);
      const log = getEventLog(tenantId);
      const state = groupState(tenantId, group);
      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      const immediate = nextFromLog(log, tenantId, state, recvCfg.invalidPayload);
      if (immediate) return immediate;

      if (!wait) return null;

      const ac = new AbortController();
      const timeout = setTimeout(() => ac.abort(), timeoutMs);
      const onUserAbort = (): void => ac.abort();
      if (recvCfg.signal) recvCfg.signal.addEventListener("abort", onUserAbort, { once: true });

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
      }

      return null;
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

    /**
     * Recover deliveries this group handed out and never committed. Previously a
     * validating stub returning nothing, which satisfied the type but left
     * uncommitted work unrecoverable — the cursor had already advanced past it.
     */
    const reclaim = async (reclaimCfg: TopicReclaimConfig = {}): Promise<TopicReclaimResult<TData>> => {
      const minIdleMs = reclaimCfg.minIdleMs ?? 60_000;
      if (!Number.isFinite(minIdleMs) || minIdleMs < 0) throw new Error("minIdleMs must be a non-negative number");
      const count = reclaimCfg.count ?? 25;
      if (!Number.isInteger(count) || count < 1 || count > 1_000) {
        throw new Error("count must be an integer between 1 and 1000");
      }

      const tenantId = resolveTenant(reclaimCfg.tenantId);
      const log = getEventLog(tenantId);
      const state = groupState(tenantId, group);
      const now = Date.now();

      const stale = [...state.inFlight.entries()]
        .filter(([, held]) => now - held.at >= minIdleMs)
        .sort(([a], [b]) => Number(a) - Number(b))
        .slice(0, count);

      const entries: Array<TopicReclaimedDelivery<TData>> = [];
      let lastId = "0-0";
      for (const [eventId] of stale) {
        const found = log.range(String(Number(eventId) - 1), 1).find((e) => e.id === eventId);
        if (!found) {
          state.inFlight.delete(eventId);
          continue;
        }
        // Take ownership, then hand it out again.
        state.inFlight.set(eventId, { at: now, consumerId });
        const commit = async (): Promise<boolean> => {
          const held = state.inFlight.get(found.id);
          if (!held || held.consumerId !== consumerId) return false;
          state.inFlight.delete(found.id);
          if (Number(found.id) > Number(state.committed)) state.committed = found.id;
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
          lastId = found.id;
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
        lastId = found.id;
      }

      return { nextCursor: lastId, entries };
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
