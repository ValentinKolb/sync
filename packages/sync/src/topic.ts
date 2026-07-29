import { redis, RedisClient } from "bun";
import { randomUUID } from "crypto";
import { fieldArrayToObject, parseFirstRangeEntry, parseFirstStreamEntry, type ParsedEntry } from "./internal/topic-utils";
import { isRetryableTransportError, retry } from "./retry";

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PREFIX = "sync:topic";
const DEFAULT_TENANT = "default";
const DEFAULT_RETENTION_MS = 7 * DAY_MS;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * DAY_MS;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_RECLAIM_COUNT = 25;
const MAX_RECLAIM_COUNT = 1_000;

const PUB_SCRIPT = `
  local payload = ARGV[1]
  local idemKey = ARGV[2]
  local idemTtlMs = tonumber(ARGV[3])
  local trimMinId = ARGV[4]

  if idemKey ~= "" then
    local existing = redis.call("GET", idemKey)
    if existing then
      local retained = redis.call("XRANGE", KEYS[1], existing, existing, "COUNT", 1)
      if retained and #retained > 0 then
        return existing
      end
      redis.call("DEL", idemKey)
    end
  end

  local eventId = redis.call("XADD", KEYS[1], "*", "payload", payload)

  if idemKey ~= "" then
    redis.call("SET", idemKey, eventId, "PX", tostring(idemTtlMs))
  end

  if trimMinId ~= "" then
    redis.call("XTRIM", KEYS[1], "MINID", "~", trimMinId)
  end

  return eventId
`;

// XACK only if the entry is still pending for this consumer. XPENDING with an
// explicit consumer returns that consumer's pending entries in the range, so an
// empty reply means the claim was lost.
const COMMIT_SCRIPT = `
  local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[3])
  if not pending or #pending == 0 then return 0 end
  return redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
`;

// Check and remove an idle consumer atomically. A separate XPENDING followed by
// DELCONSUMER could delete a pending entry delivered to a replacement reader
// with the same stable consumer id between those two commands.
const CLOSE_CONSUMER_SCRIPT = `
  local pending = redis.call("XPENDING", KEYS[1], ARGV[1], "-", "+", 1, ARGV[2])
  if pending and #pending > 0 then return 0 end
  redis.call("XGROUP", "DELCONSUMER", KEYS[1], ARGV[1], ARGV[2])
  return 1
`;

const ENSURED_GROUPS_MAX = 10_000;

const textEncoder = new TextEncoder();

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const assertTtlMs = (ttlMs: number): void => {
  if (!Number.isInteger(ttlMs) || ttlMs <= 0 || ttlMs > Number.MAX_SAFE_INTEGER) {
    throw new Error("idempotencyTtlMs must be a positive integer number of milliseconds");
  }
};

const safeClose = (client: RedisClient): void => {
  if (!client.connected) return;
  try {
    client.close();
  } catch {
    // ignore close races
  }
};

const raceWithAbort = async <T>(operation: Promise<T>, signal: AbortSignal): Promise<T | null> => {
  if (signal.aborted) return null;

  return await new Promise<T | null>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener("abort", onAbort);
      resolve(null);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
};

const evalScript = async (script: string, keys: string[], args: Array<string | number>): Promise<unknown> => {
  return await redis.send("EVAL", [script, keys.length.toString(), ...keys, ...args.map((v) => String(v))]);
};

const blockingReadWithClient = async (
  client: RedisClient,
  command: string,
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  if (signal?.aborted) return null;

  const onAbort = (): void => {
    safeClose(client);
  };

  if (signal) {
    signal.addEventListener("abort", onAbort, { once: true });
  }

  try {
    if (!client.connected) {
      const connect = client.connect();
      const connected = signal ? await raceWithAbort(connect, signal) : await connect;
      if (connected === null) {
        void connect.then(() => safeClose(client), () => {});
        return null;
      }
    }
    if (signal?.aborted) return null;

    const read = client.send(command, args);
    if (!signal) return await read;
    return await raceWithAbort(read, signal);
  } catch (error) {
    if (signal?.aborted) return null;
    throw asError(error);
  } finally {
    if (signal) signal.removeEventListener("abort", onAbort);
  }
};

const blockingReadWithTemporaryClient = async (
  command: string,
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  const client = new RedisClient();
  try {
    return await blockingReadWithClient(client, command, args, signal);
  } finally {
    safeClose(client);
  }
};

export type TopicConfig<T = unknown> = {
  id: string;
  tenantId?: string;
  prefix?: string;
  limits?: {
    payloadBytes?: number;
  };
  retentionMs?: number;
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
  /**
   * Stable consumer name. Defaults to a fresh per-reader name, which Redis
   * retains in the group registry until the consumer is deleted.
   */
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

type StoredEvent<T> = {
  data: T;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  publishedAt: number;
};

type StoredEventParseResult =
  | { ok: true; value: StoredEvent<unknown> }
  | { ok: false; error: string; rawPayload: string | null };

const parseAutoClaimResult = (raw: unknown): { nextCursor: string; entries: ParsedEntry[] } => {
  if (!Array.isArray(raw)) return { nextCursor: "0-0", entries: [] };
  const nextCursor = typeof raw[0] === "string" ? raw[0] : "0-0";
  const rawEntries = Array.isArray(raw[1]) ? raw[1] : [];
  const entries: ParsedEntry[] = [];
  for (const rawEntry of rawEntries) {
    if (!Array.isArray(rawEntry) || typeof rawEntry[0] !== "string") continue;
    entries.push({ id: rawEntry[0], fields: fieldArrayToObject(rawEntry[1]) });
  }
  return { nextCursor, entries };
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

export const topic = <T>(config: TopicConfig<T>): RecoverableTopic<T> => {
  type TData = T;

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const retentionMs = config.retentionMs ?? DEFAULT_RETENTION_MS;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  const streamKey = (tenantId: string): string => `${prefix}:${tenantId}:${config.id}:stream`;
  const idempotencyKey = (tenantId: string, key: string): string => `${prefix}:${tenantId}:${config.id}:idempotency:${key}`;
  const ensuredGroups = new Set<string>();

  const forgetGroup = (key: string, group: string): void => {
    ensuredGroups.delete(`${key}:${group}`);
  };

  /**
   * A NOGROUP means the group vanished under us — a Redis restart without
   * persistence, an eviction, a FLUSHDB. The cache still claimed it existed, and
   * NOGROUP is not a retryable transport error, so every later read failed and
   * the reader stayed broken until the process restarted.
   */
  const isNoGroupError = (error: unknown): boolean => asError(error).message.includes("NOGROUP");

  const ensureGroup = async (key: string, group: string): Promise<void> => {
    const ensuredKey = `${key}:${group}`;
    if (ensuredGroups.has(ensuredKey)) return;

    try {
      await redis.send("XGROUP", ["CREATE", key, group, "0", "MKSTREAM"]);
    } catch (error) {
      const message = asError(error).message;
      if (!message.includes("BUSYGROUP")) throw error;
    }

    if (ensuredGroups.size >= ENSURED_GROUPS_MAX) {
      const first = ensuredGroups.values().next().value;
      if (first) ensuredGroups.delete(first);
    }
    ensuredGroups.add(ensuredKey);
  };

  const parsePayload = (entry: ParsedEntry): StoredEventParseResult => {
    const rawPayload = entry.fields.payload;
    if (!rawPayload) return { ok: false, error: "missing payload field", rawPayload: null };

    try {
      const value = JSON.parse(rawPayload) as unknown;
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        return { ok: false, error: "envelope must be an object", rawPayload };
      }
      const envelope = value as Record<string, unknown>;
      if (typeof envelope.publishedAt !== "number" || !Number.isFinite(envelope.publishedAt)) {
        return { ok: false, error: "envelope has invalid publishedAt", rawPayload };
      }
      if (envelope.orderingKey !== undefined && typeof envelope.orderingKey !== "string") {
        return { ok: false, error: "envelope has invalid orderingKey", rawPayload };
      }
      if (envelope.meta !== undefined && (!envelope.meta || typeof envelope.meta !== "object" || Array.isArray(envelope.meta))) {
        return { ok: false, error: "envelope has invalid meta", rawPayload };
      }
      return { ok: true, value: envelope as StoredEvent<unknown> };
    } catch {
      return { ok: false, error: "payload is not valid JSON", rawPayload };
    }
  };

  const pub = async (pubCfg: TopicPubConfig<TData>): Promise<{ eventId: string; cursor: string }> => {
    const tenantId = resolveTenant(pubCfg.tenantId);
    const key = streamKey(tenantId);
    const idempotencyTtlMs = pubCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS;
    if (pubCfg.idempotencyKey || pubCfg.idempotencyTtlMs !== undefined) {
      assertTtlMs(idempotencyTtlMs);
    }

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

    // Clamp: a retention larger than the current epoch produced a negative
    // stream id, which Redis rejects — and the trim runs after the XADD in the
    // same script, so the event was written but pub() threw and the caller's
    // retry duplicated it. At 0 the trim is skipped entirely.
    const trimFrom = Math.floor(Date.now() - retentionMs);
    const trimMinId = trimFrom > 0 ? `${trimFrom}-0` : "";

    const rawId = await evalScript(
      PUB_SCRIPT,
      [key],
      [payloadRaw, pubCfg.idempotencyKey ? idempotencyKey(tenantId, pubCfg.idempotencyKey) : "", idempotencyTtlMs, trimMinId],
    );

    const eventId = typeof rawId === "string" ? rawId : String(rawId);
    return { eventId, cursor: eventId };
  };

  const latestCursor = async (cursorCfg: TopicCursorConfig = {}): Promise<string | null> => {
    const tenantId = resolveTenant(cursorCfg.tenantId);
    const key = streamKey(tenantId);
    const raw = await redis.send("XREVRANGE", [key, "+", "-", "COUNT", "1"]);
    return parseFirstRangeEntry(raw)?.id ?? null;
  };

  const reader = (group = "default", readerCfg: TopicReaderConfig = {}): RecoverableTopicReader<TData> => {
    // A fresh name per reader() call leaves a consumer record behind in the
    // group registry forever, because nothing ever issues XGROUP DELCONSUMER.
    // Callers with a stable identity can supply one; close() cleans up the rest.
    const consumer = readerCfg.consumerId ?? `consumer:${process.pid}:${randomUUID()}`;
    const blockingClients = new Set<RedisClient>();
    const usedKeys = new Set<string>();
    const closeController = new AbortController();
    let closed = false;
    let closePromise: Promise<void> | null = null;
    let activeOperations = 0;
    let operationsIdle = Promise.resolve();
    let resolveOperationsIdle: (() => void) | null = null;

    const assertOpen = (): void => {
      if (closed) throw new Error("topic reader is closed");
    };

    const runOperation = async <R>(operation: () => Promise<R>): Promise<R> => {
      assertOpen();
      if (activeOperations === 0) {
        operationsIdle = new Promise<void>((resolve) => {
          resolveOperationsIdle = resolve;
        });
      }
      activeOperations += 1;
      try {
        return await operation();
      } finally {
        activeOperations -= 1;
        if (activeOperations === 0) {
          resolveOperationsIdle?.();
          resolveOperationsIdle = null;
        }
      }
    };

    /**
     * Release this reader's dedicated blocking connection, and drop its consumer
     * record when it holds nothing. Without it, `const m = await topic
     * .reader("g").recv({ timeoutMs })` in a request handler leaked one Redis
     * connection per request until the process hit its fd or maxclients limit.
     *
     * A consumer with pending entries is deliberately left in place: deleting it
     * would remove those entries from the group PEL, where they can never be
     * redelivered or reclaimed again.
     */
    const close = (): Promise<void> => {
      if (closePromise) return closePromise;
      closed = true;
      closeController.abort();
      for (const client of blockingClients) safeClose(client);
      closePromise = (async () => {
        await operationsIdle;
        for (const key of usedKeys) {
          try {
            await evalScript(CLOSE_CONSUMER_SCRIPT, [key], [group, consumer]);
          } catch {
            // Best effort: the group or stream may be gone already.
          }
        }
      })();
      return closePromise;
    };

    const blockingReadGroup = async (args: string[], signal?: AbortSignal): Promise<unknown> => {
      const combinedSignal = signal
        ? AbortSignal.any([signal, closeController.signal])
        : closeController.signal;
      return await blockingReadWithTemporaryClient("XREADGROUP", args, combinedSignal);
    };

    // A bare XACK let a stalled consumer acknowledge an entry another consumer
    // had already reclaimed: the entry left the group PEL entirely, so if the
    // new owner then died it was in nobody's PEL, could never be redelivered and
    // could never be reclaimed. The commit is refused unless this consumer still
    // owns the pending entry.
    const createCommit = (key: string, eventId: string, owner: string) => async (): Promise<boolean> => {
      const acked = await redis.send("EVAL", [
        COMMIT_SCRIPT,
        "1",
        key,
        group,
        eventId,
        owner,
      ]);
      return Number(acked) > 0;
    };

    const createDelivery = (entry: ParsedEntry, stored: StoredEvent<unknown>, key: string): TopicDelivery<TData> => ({
      data: stored.data as TData,
      eventId: entry.id,
      cursor: entry.id,
      deliveryId: `${group}:${entry.id}`,
      orderingKey: stored.orderingKey,
      publishedAt: stored.publishedAt,
      meta: stored.meta,
      commit: createCommit(key, entry.id, consumer),
    });

    const recvInternal = async (
      recvCfg: TopicRecvConfig,
      retriedAfterNoGroup = false,
      readGroup = blockingReadGroup,
    ): Promise<TopicDelivery<TData> | null> => {
      if (closed) return null;
      const tenantId = resolveTenant(recvCfg.tenantId);
      const key = streamKey(tenantId);
      usedKeys.add(key);
      await ensureGroup(key, group);
      if (closed) return null;

      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      let result: unknown;
      try {
        result = wait
          ? await readGroup(
              [
                "GROUP",
                group,
                consumer,
                "COUNT",
                "1",
                "BLOCK",
                timeoutMs.toString(),
                "STREAMS",
                key,
                ">",
              ],
              recvCfg.signal,
            )
          : await redis.send("XREADGROUP", ["GROUP", group, consumer, "COUNT", "1", "STREAMS", key, ">"]);
      } catch (error) {
        if (retriedAfterNoGroup || !isNoGroupError(error)) throw error;
        forgetGroup(key, group);
        return await recvInternal(recvCfg, true, readGroup);
      }

      const entry = parseFirstStreamEntry(result);
      if (!entry) return null;

      const stored = parsePayload(entry);
      if (!stored.ok) {
        if (recvCfg.invalidPayload === "throw") throw new TopicPayloadError(entry.id, stored.error, stored.rawPayload);
        // Acknowledge and keep draining. Returning null here conflated "poison,
        // skipped" with "nothing available", so a non-waiting stream() treated
        // one bad envelope as end-of-drain and yielded nothing at all — making
        // exactly one message of progress per run on a 500-message backlog.
        await createCommit(key, entry.id, consumer)();
        return await recvInternal(recvCfg, retriedAfterNoGroup, readGroup);
      }
      return createDelivery(entry, stored.value, key);
    };

    const recv = async (recvCfg: TopicRecvConfig = {}): Promise<TopicDelivery<TData> | null> =>
      await runOperation(() => recvInternal(recvCfg));

    const reclaimInternal = async (
      reclaimCfg: TopicReclaimConfig,
      retriedAfterNoGroup = false,
    ): Promise<TopicReclaimResult<TData>> => {
      if (closed) return { nextCursor: "0-0", entries: [] };
      const tenantId = resolveTenant(reclaimCfg.tenantId);
      const key = streamKey(tenantId);
      usedKeys.add(key);
      await ensureGroup(key, group);
      if (closed) return { nextCursor: "0-0", entries: [] };

      const minIdleMs = reclaimCfg.minIdleMs ?? 60_000;
      if (!Number.isFinite(minIdleMs) || minIdleMs < 0) throw new Error("minIdleMs must be a non-negative number");
      const count = reclaimCfg.count ?? DEFAULT_RECLAIM_COUNT;
      if (!Number.isInteger(count) || count < 1 || count > MAX_RECLAIM_COUNT) {
        throw new Error(`count must be an integer between 1 and ${MAX_RECLAIM_COUNT}`);
      }

      let raw: unknown;
      try {
        raw = await redis.send("XAUTOCLAIM", [
          key,
          group,
          consumer,
          String(minIdleMs),
          reclaimCfg.cursor ?? "0-0",
          "COUNT",
          String(count),
        ]);
      } catch (error) {
        if (retriedAfterNoGroup || !isNoGroupError(error)) throw error;
        forgetGroup(key, group);
        return await reclaimInternal(reclaimCfg, true);
      }
      const claimed = parseAutoClaimResult(raw);
      const entries: TopicReclaimedDelivery<TData>[] = claimed.entries.map((entry) => {
        const stored = parsePayload(entry);
        if (!stored.ok) {
          return {
            kind: "invalid",
            eventId: entry.id,
            cursor: entry.id,
            deliveryId: `${group}:${entry.id}`,
            error: stored.error,
            rawPayload: stored.rawPayload,
            commit: createCommit(key, entry.id, consumer),
          };
        }
        return {
          kind: "delivery",
          delivery: createDelivery(entry, stored.value, key),
        };
      });
      return { nextCursor: claimed.nextCursor, entries };
    };

    const reclaim = async (reclaimCfg: TopicReclaimConfig = {}): Promise<TopicReclaimResult<TData>> =>
      await runOperation(() => reclaimInternal(reclaimCfg));

    const stream = async function* (streamCfg: TopicRecvConfig = {}): AsyncIterable<TopicDelivery<TData>> {
      const wait = streamCfg.wait ?? true;
      let streamClient = new RedisClient();
      blockingClients.add(streamClient);
      const readGroup = async (args: string[], signal?: AbortSignal): Promise<unknown> => {
        const combinedSignal = signal
          ? AbortSignal.any([signal, closeController.signal])
          : closeController.signal;
        try {
          return await blockingReadWithClient(streamClient, "XREADGROUP", args, combinedSignal);
        } catch (error) {
          safeClose(streamClient);
          blockingClients.delete(streamClient);
          streamClient = new RedisClient();
          blockingClients.add(streamClient);
          throw error;
        }
      };
      try {
        while (!closed && !streamCfg.signal?.aborted) {
          let message: TopicDelivery<TData> | null;
          try {
            message = wait
              ? await retry({
                  run: () => runOperation(() => recvInternal(streamCfg, false, readGroup)),
                  after: ({ ctx }) => {
                    if (ctx.error && isRetryableTransportError(ctx.error)) {
                      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
                    }
                  },
                  signal: streamCfg.signal,
                })
              : await recv(streamCfg);
          } catch (error) {
            if (closed || streamCfg.signal?.aborted) break;
            throw error;
          }
          if (message) {
            yield message;
            continue;
          }
          if (!wait) break;
        }
      } finally {
        safeClose(streamClient);
        blockingClients.delete(streamClient);
      }
    };

    return {
      group,
      recv,
      reclaim,
      stream,
      close,
      [Symbol.asyncDispose]: close,
    };
  };

  const live = async function* (liveCfg: TopicLiveConfig = {}): AsyncIterable<TopicLiveEvent<TData>> {
    const tenantId = resolveTenant(liveCfg.tenantId);
    const key = streamKey(tenantId);
    const timeoutMs = liveCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

    let blockingClient: RedisClient | null = null;
    const resetBlockingClient = (): void => {
      if (!blockingClient) return;
      safeClose(blockingClient);
      blockingClient = null;
    };
    const ensureBlockingClient = async (): Promise<RedisClient> => {
      if (blockingClient?.connected) return blockingClient;
      resetBlockingClient();
      blockingClient = new RedisClient();
      await blockingClient.connect();
      return blockingClient;
    };

    let cursor = liveCfg.after ?? "$";

    try {
      while (!liveCfg.signal?.aborted) {
        let result: unknown;
        try {
          result = await retry<unknown>({
            run: () =>
              liveCfg.signal
                ? blockingReadWithTemporaryClient(
                    "XREAD",
                    ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", key, cursor],
                    liveCfg.signal,
                  )
                : (async (): Promise<unknown> => {
                    try {
                      const client = await ensureBlockingClient();
                      return await client.send("XREAD", ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", key, cursor]);
                    } catch (error) {
                      resetBlockingClient();
                      throw asError(error);
                    }
                  })(),
            after: ({ ctx }) => {
              if (ctx.error && isRetryableTransportError(ctx.error)) {
                ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
              }
            },
            signal: liveCfg.signal,
          });
        } catch (error) {
          if (liveCfg.signal?.aborted) break;
          throw error;
        }

        const entry = parseFirstStreamEntry(result);
        if (!entry) continue;

        cursor = entry.id;

        const stored = parsePayload(entry);
        if (!stored.ok) continue;

        yield {
          data: stored.value.data as TData,
          eventId: entry.id,
          cursor: entry.id,
          orderingKey: stored.value.orderingKey,
          publishedAt: stored.value.publishedAt,
          meta: stored.value.meta,
        };
      }
    } finally {
      resetBlockingClient();
    }
  };

  return {
    pub,
    latestCursor,
    reader,
    live,
  };
};
