import { redis, RedisClient } from "bun";
import { randomUUID } from "crypto";
import type { z } from "zod";
import { isRetryableTransportError, retry } from "./retry";

const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_PREFIX = "sync:queue";
const DEFAULT_TENANT = "default";
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WAIT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_DELIVERIES = 10;
const DEFAULT_MAX_NACK_DELAY_MS = 7 * DAY_MS;
const DEFAULT_MAX_MESSAGE_AGE_MS = 7 * DAY_MS;
const DEFAULT_DLQ_RETENTION_MS = 7 * DAY_MS;
const DEFAULT_IDEMPOTENCY_TTL_MS = 7 * DAY_MS;
const DEFAULT_PAYLOAD_BYTES = 128 * 1024;
const MAINTENANCE_BATCH_SIZE = 200;
const DEFAULT_MAINTENANCE_INTERVAL_MS = 1_000;

const SEND_SCRIPT = `
  local now = tonumber(ARGV[1])
  local delayMs = tonumber(ARGV[2])
  local payload = ARGV[3]
  local idemKey = ARGV[4]
  local idemTtlMs = tonumber(ARGV[5])

  if idemKey ~= "" then
    local existing = redis.call("GET", idemKey)
    if existing then
      return { existing, "duplicate" }
    end
  end

  local messageId = tostring(redis.call("INCR", KEYS[1]))
  redis.call("HSET", KEYS[2], messageId, payload)

  if delayMs > 0 then
    redis.call("ZADD", KEYS[4], tostring(now + delayMs), messageId)
  else
    redis.call("LPUSH", KEYS[3], messageId)
  end

  if idemKey ~= "" then
    redis.call("SET", idemKey, messageId, "PX", tostring(idemTtlMs))
  end

  return { messageId, "new" }
`;

const MAINTENANCE_SCRIPT = `
  local now = tonumber(ARGV[1])
  local maxDeliveries = tonumber(ARGV[2])
  local maxMessageAgeMs = tonumber(ARGV[3])
  local batch = tonumber(ARGV[4])
  local dlqTtlMs = tonumber(ARGV[5])

  local delayedIds = redis.call("ZRANGEBYSCORE", KEYS[2], "0", tostring(now), "LIMIT", "0", tostring(batch))
  for _, messageId in ipairs(delayedIds) do
    local removed = redis.call("ZREM", KEYS[2], messageId)
    if removed > 0 then
      local messageRaw = redis.call("HGET", KEYS[5], messageId)
      if messageRaw then
        local ok, message = pcall(cjson.decode, messageRaw)
        if ok then
          local enqueuedAt = tonumber(message.enqueuedAt) or now
          if (now - enqueuedAt) > maxMessageAgeMs then
            local dlq = {
              messageId = messageId,
              data = message.data,
              orderingKey = message.orderingKey,
              meta = message.meta,
              attempts = message.attempt or 0,
              movedAt = now,
              reason = "expired"
            }
            redis.call("HSET", KEYS[7], messageId, cjson.encode(dlq))
            if dlqTtlMs > 0 then
              redis.call("PEXPIRE", KEYS[7], tostring(dlqTtlMs))
            end
            redis.call("HDEL", KEYS[5], messageId)
          else
            redis.call("LPUSH", KEYS[1], messageId)
          end
        else
          redis.call("HDEL", KEYS[5], messageId)
        end
      end
    end
  end

  local expiredDeliveryIds = redis.call("ZRANGEBYSCORE", KEYS[3], "0", tostring(now), "LIMIT", "0", tostring(batch))
  for _, deliveryId in ipairs(expiredDeliveryIds) do
    local deliveryRaw = redis.call("HGET", KEYS[4], deliveryId)
    redis.call("ZREM", KEYS[3], deliveryId)

    if deliveryRaw then
      redis.call("HDEL", KEYS[4], deliveryId)
      local ok, delivery = pcall(cjson.decode, deliveryRaw)
      if ok then
        local messageId = delivery.messageId
        redis.call("LREM", KEYS[6], 1, messageId)

        local messageRaw = redis.call("HGET", KEYS[5], messageId)
        if messageRaw then
          local mOk, message = pcall(cjson.decode, messageRaw)
          if mOk then
            local enqueuedAt = tonumber(message.enqueuedAt) or now
            if (now - enqueuedAt) > maxMessageAgeMs then
              local dlqExpired = {
                messageId = messageId,
                data = message.data,
                orderingKey = message.orderingKey,
                meta = message.meta,
                attempts = message.attempt or 0,
                movedAt = now,
                reason = "expired"
              }
              redis.call("HSET", KEYS[7], messageId, cjson.encode(dlqExpired))
              if dlqTtlMs > 0 then
                redis.call("PEXPIRE", KEYS[7], tostring(dlqTtlMs))
              end
              redis.call("HDEL", KEYS[5], messageId)
            elseif (tonumber(message.attempt) or 0) >= maxDeliveries then
              local dlqMax = {
                messageId = messageId,
                data = message.data,
                orderingKey = message.orderingKey,
                meta = message.meta,
                attempts = message.attempt or 0,
                movedAt = now,
                reason = "max_deliveries_exceeded"
              }
              redis.call("HSET", KEYS[7], messageId, cjson.encode(dlqMax))
              if dlqTtlMs > 0 then
                redis.call("PEXPIRE", KEYS[7], tostring(dlqTtlMs))
              end
              redis.call("HDEL", KEYS[5], messageId)
            else
              redis.call("LPUSH", KEYS[1], messageId)
            end
          else
            redis.call("HDEL", KEYS[5], messageId)
          end
        end
      end
    end
  end

  return 1
`;

const CLAIM_SCRIPT = `
  local messageId = ARGV[1]
  local deliveryId = ARGV[2]
  local leaseUntil = tonumber(ARGV[3])

  local messageRaw = redis.call("HGET", KEYS[1], messageId)
  if not messageRaw then
    redis.call("LREM", KEYS[4], 1, messageId)
    return nil
  end

  local ok, message = pcall(cjson.decode, messageRaw)
  if not ok then
    redis.call("LREM", KEYS[4], 1, messageId)
    redis.call("HDEL", KEYS[1], messageId)
    return nil
  end

  message.attempt = (tonumber(message.attempt) or 0) + 1
  redis.call("HSET", KEYS[1], messageId, cjson.encode(message))

  local delivery = {
    messageId = messageId,
    leaseUntil = leaseUntil,
    attempt = message.attempt
  }

  redis.call("HSET", KEYS[2], deliveryId, cjson.encode(delivery))
  redis.call("ZADD", KEYS[3], tostring(leaseUntil), deliveryId)

  local result = {
    messageId = messageId,
    deliveryId = deliveryId,
    leaseUntil = leaseUntil,
    attempt = message.attempt,
    orderingKey = message.orderingKey,
    enqueuedAt = message.enqueuedAt,
    meta = message.meta,
    data = message.data
  }

  return cjson.encode(result)
`;

const ACK_SCRIPT = `
  local deliveryId = ARGV[1]

  local deliveryRaw = redis.call("HGET", KEYS[1], deliveryId)
  if not deliveryRaw then
    return 0
  end

  local ok, delivery = pcall(cjson.decode, deliveryRaw)
  if not ok then
    redis.call("HDEL", KEYS[1], deliveryId)
    redis.call("ZREM", KEYS[2], deliveryId)
    return 0
  end

  redis.call("HDEL", KEYS[1], deliveryId)
  redis.call("ZREM", KEYS[2], deliveryId)
  redis.call("LREM", KEYS[4], 1, delivery.messageId)
  redis.call("HDEL", KEYS[3], delivery.messageId)

  return 1
`;

const NACK_SCRIPT = `
  local deliveryId = ARGV[1]
  local now = tonumber(ARGV[2])
  local delayMs = tonumber(ARGV[3])
  local maxDeliveries = tonumber(ARGV[4])
  local reason = ARGV[5]
  local error = ARGV[6]
  local dlqTtlMs = tonumber(ARGV[7])

  local deliveryRaw = redis.call("HGET", KEYS[1], deliveryId)
  if not deliveryRaw then
    return 0
  end

  local dOk, delivery = pcall(cjson.decode, deliveryRaw)
  if not dOk then
    redis.call("HDEL", KEYS[1], deliveryId)
    redis.call("ZREM", KEYS[2], deliveryId)
    return 0
  end

  local messageId = delivery.messageId

  redis.call("HDEL", KEYS[1], deliveryId)
  redis.call("ZREM", KEYS[2], deliveryId)
  redis.call("LREM", KEYS[4], 1, messageId)

  local messageRaw = redis.call("HGET", KEYS[3], messageId)
  if not messageRaw then
    return 0
  end

  local mOk, message = pcall(cjson.decode, messageRaw)
  if not mOk then
    redis.call("HDEL", KEYS[3], messageId)
    return 0
  end

  if (tonumber(message.attempt) or 0) >= maxDeliveries then
    local dlq = {
      messageId = messageId,
      data = message.data,
      orderingKey = message.orderingKey,
      meta = message.meta,
      attempts = message.attempt or 0,
      movedAt = now,
      reason = reason ~= "" and reason or "max_deliveries_exceeded",
      lastError = error ~= "" and error or nil
    }
    redis.call("HSET", KEYS[7], messageId, cjson.encode(dlq))
    if dlqTtlMs > 0 then
      redis.call("PEXPIRE", KEYS[7], tostring(dlqTtlMs))
    end
    redis.call("HDEL", KEYS[3], messageId)
    return 2
  end

  if delayMs > 0 then
    redis.call("ZADD", KEYS[6], tostring(now + delayMs), messageId)
  else
    redis.call("LPUSH", KEYS[5], messageId)
  end

  return 1
`;

const TOUCH_SCRIPT = `
  local deliveryId = ARGV[1]
  local leaseUntil = tonumber(ARGV[2])

  local deliveryRaw = redis.call("HGET", KEYS[1], deliveryId)
  if not deliveryRaw then
    return 0
  end

  local ok, delivery = pcall(cjson.decode, deliveryRaw)
  if not ok then
    redis.call("HDEL", KEYS[1], deliveryId)
    redis.call("ZREM", KEYS[2], deliveryId)
    return 0
  end

  delivery.leaseUntil = leaseUntil
  redis.call("HSET", KEYS[1], deliveryId, cjson.encode(delivery))
  redis.call("ZADD", KEYS[2], tostring(leaseUntil), deliveryId)

  return 1
`;

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

const asObject = <T>(value: unknown): T | null => {
  if (!value || typeof value !== "object") return null;
  return value as T;
};

const evalScript = async (script: string, keys: string[], args: Array<string | number>): Promise<unknown> => {
  return await redis.send("EVAL", [script, keys.length.toString(), ...keys, ...args.map((v) => String(v))]);
};

type QueueKeys = {
  seq: string;
  ready: string;
  delayed: string;
  leases: string;
  deliveries: string;
  messages: string;
  active: string;
  dlq: string;
  idempotencyPrefix: string;
};

export type QueueConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string;
  ordering?: {
    mode?: "best_effort" | "ordering_key_partitioned";
    partitions?: number;
  };
  limits?: {
    payloadBytes?: number;
    maxMessageAgeMs?: number;
    maxNackDelayMs?: number;
    dlqRetentionMs?: number;
  };
  delivery?: {
    defaultLeaseMs?: number;
    maxDeliveries?: number;
  };
};

export type QueueSendConfig<T> = {
  tenantId?: string;
  data: T;
  delayMs?: number;
  orderingKey?: string;
  idempotencyKey?: string;
  idempotencyTtlMs?: number;
  meta?: Record<string, unknown>;
};

export type QueueRecvConfig = {
  tenantId?: string;
  wait?: boolean;
  timeoutMs?: number;
  leaseMs?: number;
  consumerId?: string;
  signal?: AbortSignal;
};

export type QueueReceived<T> = {
  data: T;
  messageId: string;
  deliveryId: string;
  attempt: number;
  leaseUntil: number;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  ack(): Promise<boolean>;
  nack(cfg?: { delayMs?: number; reason?: string; error?: string }): Promise<boolean>;
  touch(cfg?: { leaseMs?: number }): Promise<boolean>;
};

export type QueueReader<T> = {
  recv(cfg?: QueueRecvConfig): Promise<QueueReceived<T> | null>;
  stream(cfg?: QueueRecvConfig): AsyncIterable<QueueReceived<T>>;
};

export type Queue<T> = QueueReader<T> & {
  send(cfg: QueueSendConfig<T>): Promise<{ messageId: string }>;
  reader(): QueueReader<T>;
};

type StoredMessage<T> = {
  data: T;
  attempt: number;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  enqueuedAt: number;
};

type ClaimedMessage<T> = {
  data: T;
  messageId: string;
  deliveryId: string;
  attempt: number;
  leaseUntil: number;
  orderingKey?: string;
  enqueuedAt: number;
  meta?: Record<string, unknown>;
};

export const queue = <TSchema extends z.ZodTypeAny>(config: QueueConfig<TSchema>): Queue<z.infer<TSchema>> => {
  type TData = z.infer<TSchema>;

  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  const maxPayloadBytes = config.limits?.payloadBytes ?? DEFAULT_PAYLOAD_BYTES;
  const maxMessageAgeMs = config.limits?.maxMessageAgeMs ?? DEFAULT_MAX_MESSAGE_AGE_MS;
  const maxNackDelayMs = config.limits?.maxNackDelayMs ?? DEFAULT_MAX_NACK_DELAY_MS;
  const dlqRetentionMs = config.limits?.dlqRetentionMs ?? DEFAULT_DLQ_RETENTION_MS;
  const defaultLeaseMs = config.delivery?.defaultLeaseMs ?? DEFAULT_LEASE_MS;
  const maxDeliveries = config.delivery?.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;

  const resolveTenant = (tenantId?: string): string => tenantId ?? defaultTenant;

  const keysForTenant = (tenantId: string): QueueKeys => {
    const base = `${prefix}:${tenantId}:${config.id}`;
    return {
      seq: `${base}:seq`,
      ready: `${base}:ready`,
      delayed: `${base}:delayed`,
      leases: `${base}:leases`,
      deliveries: `${base}:deliveries`,
      messages: `${base}:messages`,
      active: `${base}:active`,
      dlq: `${base}:dlq`,
      idempotencyPrefix: `${base}:idempotency`,
    };
  };

  const lastMaintenanceByTenant = new Map<string, number>();

  const runMaintenance = async (keys: QueueKeys, now: number): Promise<void> => {
    await evalScript(
      MAINTENANCE_SCRIPT,
      [keys.ready, keys.delayed, keys.leases, keys.deliveries, keys.messages, keys.active, keys.dlq],
      [now, maxDeliveries, maxMessageAgeMs, MAINTENANCE_BATCH_SIZE, dlqRetentionMs],
    );
  };

  const maybeRunMaintenance = async (tenantId: string, keys: QueueKeys, currentTs: number, force = false): Promise<void> => {
    if (!force) {
      const lastTs = lastMaintenanceByTenant.get(tenantId) ?? 0;
      if (currentTs - lastTs < DEFAULT_MAINTENANCE_INTERVAL_MS) return;
    }
    lastMaintenanceByTenant.set(tenantId, currentTs);
    await runMaintenance(keys, currentTs);
  };

  const createReader = (): QueueReader<TData> => {
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

    const popMessageId = async (
      keys: QueueKeys,
      cfg: Required<Pick<QueueRecvConfig, "wait" | "timeoutMs">> & Pick<QueueRecvConfig, "signal">,
    ): Promise<string | null> => {
      if (!cfg.wait) {
        const popped = await redis.send("LMOVE", [keys.ready, keys.active, "RIGHT", "LEFT"]);
        return typeof popped === "string" ? popped : null;
      }

      const timeoutSecs = Math.max(1, Math.ceil(cfg.timeoutMs / 1000));

      if (cfg.signal) {
        if (cfg.signal.aborted) return null;

        const client = new RedisClient();
        const onAbort = (): void => {
          safeClose(client);
        };
        cfg.signal.addEventListener("abort", onAbort, { once: true });

        try {
          if (!client.connected) await client.connect();
          const popped = await client.send("BLMOVE", [keys.ready, keys.active, "RIGHT", "LEFT", timeoutSecs.toString()]);
          return typeof popped === "string" ? popped : null;
        } catch (error) {
          if (cfg.signal.aborted) return null;
          throw asError(error);
        } finally {
          cfg.signal.removeEventListener("abort", onAbort);
          safeClose(client);
        }
      }

      try {
        const client = await ensureBlockingClient();
        const popped = await client.send("BLMOVE", [keys.ready, keys.active, "RIGHT", "LEFT", timeoutSecs.toString()]);
        return typeof popped === "string" ? popped : null;
      } catch (error) {
        resetBlockingClient();
        throw asError(error);
      }
    };

    const recv = async (recvCfg: QueueRecvConfig = {}): Promise<QueueReceived<TData> | null> => {
      const tenantId = resolveTenant(recvCfg.tenantId);
      const keys = keysForTenant(tenantId);
      const ts = Date.now();
      const wait = recvCfg.wait ?? true;

      await maybeRunMaintenance(tenantId, keys, ts, !wait);


      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
      const leaseMs = recvCfg.leaseMs ?? defaultLeaseMs;

      for (let i = 0; i < 4; i++) {
        if (recvCfg.signal?.aborted) return null;

        let messageId: string | null = null;
        try {
          messageId = await popMessageId(keys, { wait, timeoutMs, signal: recvCfg.signal });
        } catch (error) {
          if (!wait) throw asError(error);
          continue;
        }
        if (!messageId) return null;

        const deliveryId = randomUUID();
        const leaseUntil = Date.now() + leaseMs;

        const claimRaw = await evalScript(
          CLAIM_SCRIPT,
          [keys.messages, keys.deliveries, keys.leases, keys.active],
          [messageId, deliveryId, leaseUntil],
        );

        if (typeof claimRaw !== "string") {
          if (!wait) return null;
          continue;
        }

        let claimed: ClaimedMessage<unknown> | null = null;
        try {
          claimed = JSON.parse(claimRaw) as ClaimedMessage<unknown>;
        } catch {
          claimed = null;
        }

        if (!claimed) {
          if (!wait) return null;
          continue;
        }

        const parsed = config.schema.safeParse(claimed.data);
        if (!parsed.success) {
          await evalScript(ACK_SCRIPT, [keys.deliveries, keys.leases, keys.messages, keys.active], [claimed.deliveryId]);
          if (!wait) return null;
          continue;
        }

        const ack = async (): Promise<boolean> => {
          const result = await evalScript(
            ACK_SCRIPT,
            [keys.deliveries, keys.leases, keys.messages, keys.active],
            [claimed.deliveryId],
          );
          return Number(result) > 0;
        };

        const nack = async (nackCfg: { delayMs?: number; reason?: string; error?: string } = {}): Promise<boolean> => {
          const delayMs = Math.max(0, nackCfg.delayMs ?? 0);
          if (delayMs > maxNackDelayMs) {
            throw new Error(`delayMs exceeds maxNackDelayMs (${maxNackDelayMs})`);
          }

          const result = await evalScript(
            NACK_SCRIPT,
            [keys.deliveries, keys.leases, keys.messages, keys.active, keys.ready, keys.delayed, keys.dlq],
            [
              claimed.deliveryId,
              Date.now(),
              delayMs,
            maxDeliveries,
            nackCfg.reason ?? "",
            nackCfg.error ?? "",
            dlqRetentionMs,
          ],
        );

          return Number(result) > 0;
        };

        const touch = async (touchCfg: { leaseMs?: number } = {}): Promise<boolean> => {
          const nextLeaseMs = touchCfg.leaseMs ?? leaseMs;
          const nextLeaseUntil = Date.now() + nextLeaseMs;
          const result = await evalScript(TOUCH_SCRIPT, [keys.deliveries, keys.leases], [claimed.deliveryId, nextLeaseUntil]);
          return Number(result) > 0;
        };

        return {
          data: parsed.data,
          messageId: claimed.messageId,
          deliveryId: claimed.deliveryId,
          attempt: claimed.attempt,
          leaseUntil: claimed.leaseUntil,
          orderingKey: claimed.orderingKey,
          meta: asObject<Record<string, unknown>>(claimed.meta) ?? undefined,
          ack,
          nack,
          touch,
        };
      }

      return null;
    };

    const stream = async function* (streamCfg: QueueRecvConfig = {}): AsyncIterable<QueueReceived<TData>> {
      const wait = streamCfg.wait ?? true;
      try {
        while (!streamCfg.signal?.aborted) {
          const message = wait
            ? await retry(
                async () => await recv(streamCfg),
                {
                  attempts: Number.POSITIVE_INFINITY,
                  signal: streamCfg.signal,
                  retryIf: isRetryableTransportError,
                },
              )
            : await recv(streamCfg);
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

    return { recv, stream };
  };

  const send = async (sendCfg: QueueSendConfig<TData>): Promise<{ messageId: string }> => {
    const tenantId = resolveTenant(sendCfg.tenantId);
    const keys = keysForTenant(tenantId);

    const parsed = config.schema.safeParse(sendCfg.data);
    if (!parsed.success) {
      throw parsed.error;
    }

    const now = Date.now();
    const delayMs = Math.max(0, sendCfg.delayMs ?? 0);
    const idempotencyTtlMs = sendCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS;

    const message: StoredMessage<TData> = {
      data: parsed.data,
      attempt: 0,
      orderingKey: sendCfg.orderingKey,
      meta: sendCfg.meta,
      enqueuedAt: now,
    };

    const payload = JSON.stringify(message);
    const payloadBytes = textEncoder.encode(payload).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new Error(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const idempotencyKey = sendCfg.idempotencyKey
      ? `${keys.idempotencyPrefix}:${sendCfg.idempotencyKey}`
      : "";

    const result = await evalScript(
      SEND_SCRIPT,
      [keys.seq, keys.messages, keys.ready, keys.delayed],
      [now, delayMs, payload, idempotencyKey, idempotencyTtlMs],
    );

    const messageId = Array.isArray(result) && typeof result[0] === "string" ? result[0] : String(result);
    return { messageId };
  };

  const defaultReader = createReader();
  const reader = (): QueueReader<TData> => createReader();

  return {
    send,
    recv: defaultReader.recv,
    stream: defaultReader.stream,
    reader,
  };
};
