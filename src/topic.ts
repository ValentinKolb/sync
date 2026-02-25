import { redis, RedisClient } from "bun";
import { randomUUID } from "crypto";
import type { z } from "zod";
import { parseFirstStreamEntry, type ParsedEntry } from "./internal/topic-utils";

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PREFIX = "sync:topic";
const DEFAULT_TENANT = "default";
const DEFAULT_RETENTION_MS = 7 * DAY_MS;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * DAY_MS;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_TIMEOUT_MS = 30_000;

const PUB_SCRIPT = `
  local payload = ARGV[1]
  local idemKey = ARGV[2]
  local idemTtlMs = tonumber(ARGV[3])
  local trimMinId = ARGV[4]

  if idemKey ~= "" then
    local existing = redis.call("GET", idemKey)
    if existing then
      return existing
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

const ENSURED_GROUPS_MAX = 10_000;

const textEncoder = new TextEncoder();

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const safeClose = (client: RedisClient): void => {
  if (!client.connected) return;
  try {
    client.close();
  } catch {
    // ignore close races
  }
};

const evalScript = async (script: string, keys: string[], args: Array<string | number>): Promise<unknown> => {
  return await redis.send("EVAL", [script, keys.length.toString(), ...keys, ...args.map((v) => String(v))]);
};

const blockingReadWithTemporaryClient = async (
  command: string,
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  if (signal?.aborted) return null;

  const client = new RedisClient();
  const onAbort = (): void => {
    safeClose(client);
  };

  if (signal) {
    signal.addEventListener("abort", onAbort, { once: true });
  }

  try {
    if (!client.connected) await client.connect();
    return await client.send(command, args);
  } catch (error) {
    if (signal?.aborted) return null;
    throw asError(error);
  } finally {
    if (signal) signal.removeEventListener("abort", onAbort);
    safeClose(client);
  }
};

export type TopicConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
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

export type TopicRecvConfig = {
  tenantId?: string;
  timeoutMs?: number;
  wait?: boolean;
  signal?: AbortSignal;
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
  stream(cfg?: TopicRecvConfig): AsyncIterable<TopicDelivery<T>>;
};

export type Topic<T> = {
  pub(cfg: TopicPubConfig<T>): Promise<{ eventId: string; cursor: string }>;
  reader(group?: string): TopicReader<T>;
  live(cfg?: TopicLiveConfig): AsyncIterable<TopicLiveEvent<T>>;
};

type StoredEvent<T> = {
  data: T;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  publishedAt: number;
};

export const topic = <TSchema extends z.ZodTypeAny>(config: TopicConfig<TSchema>): Topic<z.infer<TSchema>> => {
  type TData = z.infer<TSchema>;

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const retentionMs = config.retentionMs ?? DEFAULT_RETENTION_MS;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  const streamKey = (tenantId: string): string => `${prefix}:${tenantId}:${config.id}:stream`;
  const idempotencyKey = (tenantId: string, key: string): string => `${prefix}:${tenantId}:${config.id}:idempotency:${key}`;
  const ensuredGroups = new Set<string>();

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

  const parsePayload = (entry: ParsedEntry): StoredEvent<unknown> | null => {
    const rawPayload = entry.fields.payload;
    if (!rawPayload) return null;

    try {
      return JSON.parse(rawPayload) as StoredEvent<unknown>;
    } catch {
      return null;
    }
  };

  const pub = async (pubCfg: TopicPubConfig<TData>): Promise<{ eventId: string; cursor: string }> => {
    const tenantId = resolveTenant(pubCfg.tenantId);
    const key = streamKey(tenantId);

    const parsed = config.schema.safeParse(pubCfg.data);
    if (!parsed.success) throw parsed.error;

    const payload: StoredEvent<TData> = {
      data: parsed.data,
      orderingKey: pubCfg.orderingKey,
      meta: pubCfg.meta,
      publishedAt: Date.now(),
    };

    const payloadRaw = JSON.stringify(payload);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new Error(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const trimMinId = `${Date.now() - retentionMs}-0`;

    const rawId = await evalScript(
      PUB_SCRIPT,
      [key],
      [payloadRaw, pubCfg.idempotencyKey ? idempotencyKey(tenantId, pubCfg.idempotencyKey) : "", pubCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS, trimMinId],
    );

    const eventId = typeof rawId === "string" ? rawId : String(rawId);
    return { eventId, cursor: eventId };
  };

  const reader = (group = "default"): TopicReader<TData> => {
    const consumer = `consumer:${process.pid}:${randomUUID()}`;
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

    const blockingReadGroup = async (args: string[], signal?: AbortSignal): Promise<unknown> => {
      if (signal?.aborted) return null;

      const onAbort = (): void => {
        resetBlockingClient();
      };
      if (signal) signal.addEventListener("abort", onAbort, { once: true });

      try {
        const client = await ensureBlockingClient();
        return await client.send("XREADGROUP", args);
      } catch (error) {
        if (signal?.aborted) return null;
        resetBlockingClient();
        throw asError(error);
      } finally {
        if (signal) signal.removeEventListener("abort", onAbort);
      }
    };

    const recv = async (recvCfg: TopicRecvConfig = {}): Promise<TopicDelivery<TData> | null> => {
      const tenantId = resolveTenant(recvCfg.tenantId);
      const key = streamKey(tenantId);
      await ensureGroup(key, group);

      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      const result = wait
        ? await blockingReadGroup(
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

      const entry = parseFirstStreamEntry(result);
      if (!entry) return null;

      const stored = parsePayload(entry);
      if (!stored) {
        await redis.send("XACK", [key, group, entry.id]);
        return null;
      }

      const parsed = config.schema.safeParse(stored.data);
      if (!parsed.success) {
        await redis.send("XACK", [key, group, entry.id]);
        return null;
      }

      const commit = async (): Promise<boolean> => {
        const acked = await redis.send("XACK", [key, group, entry.id]);
        return Number(acked) > 0;
      };

      return {
        data: parsed.data,
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
      try {
        while (!streamCfg.signal?.aborted) {
          const message = await recv(streamCfg);
          if (message) {
            yield message;
            continue;
          }
          if (!wait) break;
        }
      } finally {
        resetBlockingClient();
      }
    };

    return {
      group,
      recv,
      stream,
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
        const result = liveCfg.signal
          ? await blockingReadWithTemporaryClient(
              "XREAD",
              ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", key, cursor],
              liveCfg.signal,
            )
          : await (async (): Promise<unknown> => {
              try {
                const client = await ensureBlockingClient();
                return await client.send("XREAD", ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", key, cursor]);
              } catch (error) {
                resetBlockingClient();
                throw asError(error);
              }
            })();

        const entry = parseFirstStreamEntry(result);
        if (!entry) continue;

        cursor = entry.id;

        const stored = parsePayload(entry);
        if (!stored) continue;

        const parsed = config.schema.safeParse(stored.data);
        if (!parsed.success) continue;

        yield {
          data: parsed.data,
          eventId: entry.id,
          cursor: entry.id,
          orderingKey: stored.orderingKey,
          publishedAt: stored.publishedAt,
          meta: stored.meta,
        };
      }
    } finally {
      resetBlockingClient();
    }
  };

  return {
    pub,
    reader,
    live,
  };
};
