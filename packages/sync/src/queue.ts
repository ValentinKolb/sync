import { redis, RedisClient, sleep } from "bun";
import { randomUUID } from "crypto";
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
// Longest single block on the notify list. Bounding it keeps a parked consumer
// running maintenance and re-checking the ready list roughly once a second, so
// delayed sends and expired leases can no longer wait out a long recv timeout.
const NOTIFY_BLOCK_MS = 1_000;
// How many dead ids one claim may drain before giving up for this round.
const CLAIM_MAX_SKIPS = 32;

// Stored message records are envelopes whose user-controlled parts (`dataJson`,
// `metaJson`) are opaque pre-serialized JSON strings. Lua may copy them but never
// decodes and re-encodes them, because Redis' bundled cjson is lossy: an empty
// array round-trips to an empty object and integers past 14 significant digits
// lose precision. Records written by <= 5.8.0 carry the decoded values under
// `data`/`meta` instead; `readMessage` upgrades those in place on first touch,
// which preserves exactly the fidelity 5.8.0 itself had for them.
const LUA_MESSAGE_HELPERS = `
  local function readMessage(raw)
    local ok, message = pcall(cjson.decode, raw)
    if not ok or type(message) ~= "table" then return nil end
    if message.dataJson ~= nil or message.v == 2 then return message end
    return {
      v = 2,
      attempt = tonumber(message.attempt) or 0,
      enqueuedAt = tonumber(message.enqueuedAt) or 0,
      orderingKey = message.orderingKey,
      dataJson = message.data ~= nil and cjson.encode(message.data) or nil,
      metaJson = message.meta ~= nil and cjson.encode(message.meta) or nil,
    }
  end

  -- Dead letters live in one hash with a movedAt-scored index. Retention is
  -- enforced per entry against that index; the whole-key TTL is only a
  -- last-resort sweep for a queue that goes permanently idle. Previously the
  -- single whole-key PEXPIRE was refreshed by every insert, so a steady trickle
  -- of failures kept the hash alive forever while a pause dropped fresh entries.
  local function writeDeadLetter(dlqKey, indexKey, messageId, message, movedAt, reason, lastError, ttlMs)
    redis.call("HSET", dlqKey, messageId, cjson.encode({
      v = 2,
      messageId = messageId,
      dataJson = message.dataJson,
      metaJson = message.metaJson,
      orderingKey = message.orderingKey,
      attempts = tonumber(message.attempt) or 0,
      movedAt = movedAt,
      reason = reason,
      lastError = lastError,
    }))
    redis.call("ZADD", indexKey, tostring(movedAt), messageId)

    if ttlMs > 0 then
      local stale = redis.call("ZRANGEBYSCORE", indexKey, "-inf", tostring(movedAt - ttlMs))
      for _, staleId in ipairs(stale) do
        redis.call("HDEL", dlqKey, staleId)
        redis.call("ZREM", indexKey, staleId)
      end
      redis.call("PEXPIRE", dlqKey, tostring(ttlMs))
      redis.call("PEXPIRE", indexKey, tostring(ttlMs))
    end
  end

`;

// Wake one parked consumer. The notify list is a wake-up signal only: it is
// trimmed, it may lag the ready list, and every consumer re-checks by running
// CLAIM_SCRIPT, so a lost or surplus token costs latency, never correctness.
const LUA_NOTIFY_HELPER = `
  local function notifyReady(notifyKey)
    redis.call("LPUSH", notifyKey, "1")
    redis.call("LTRIM", notifyKey, 0, 255)
  end
`;

const SEND_SCRIPT = `
  ${LUA_NOTIFY_HELPER}

  local now = tonumber(ARGV[1])
  local delayMs = tonumber(ARGV[2])
  local payload = ARGV[3]
  -- Declared as a key, not an argument: a script must declare every key it
  -- touches for key-routing topologies to route it correctly.
  local idemKey = KEYS[6]
  local idemTtlMs = tonumber(ARGV[4])

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
    notifyReady(KEYS[5])
  end

  if idemKey ~= "" then
    redis.call("SET", idemKey, messageId, "PX", tostring(idemTtlMs))
  end

  return { messageId, "new" }
`;

const MAINTENANCE_SCRIPT = `
  ${LUA_MESSAGE_HELPERS}
  ${LUA_NOTIFY_HELPER}

  local now = tonumber(ARGV[1])
  local maxDeliveries = tonumber(ARGV[2])
  local maxMessageAgeMs = tonumber(ARGV[3])
  local batch = tonumber(ARGV[4])
  local dlqTtlMs = tonumber(ARGV[5])

  local function toDlq(messageId, message, reason)
    writeDeadLetter(KEYS[7], KEYS[9], messageId, message, now, reason, nil, dlqTtlMs)
    redis.call("HDEL", KEYS[5], messageId)
    redis.call("HDEL", KEYS[10], messageId)
    redis.call("HDEL", KEYS[11], messageId)
  end

  local delayedIds = redis.call("ZRANGEBYSCORE", KEYS[2], "0", tostring(now), "LIMIT", "0", tostring(batch))
  for _, messageId in ipairs(delayedIds) do
    local removed = redis.call("ZREM", KEYS[2], messageId)
    if removed > 0 then
      local messageRaw = redis.call("HGET", KEYS[5], messageId)
      if messageRaw then
        local message = readMessage(messageRaw)
        if message then
          local enqueuedAt = tonumber(message.enqueuedAt) or now
          if (now - enqueuedAt) > maxMessageAgeMs then
            toDlq(messageId, message, "expired")
          else
            redis.call("LPUSH", KEYS[1], messageId)
            notifyReady(KEYS[8])
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
        if redis.call("HGET", KEYS[10], messageId) == deliveryId then
          redis.call("HDEL", KEYS[10], messageId)
        end
        redis.call("HDEL", KEYS[11], messageId)
        redis.call("LREM", KEYS[6], 1, messageId)

        local messageRaw = redis.call("HGET", KEYS[5], messageId)
        if messageRaw then
          local message = readMessage(messageRaw)
          if message then
            local enqueuedAt = tonumber(message.enqueuedAt) or now
            if (now - enqueuedAt) > maxMessageAgeMs then
              toDlq(messageId, message, "expired")
            elseif (tonumber(message.attempt) or 0) >= maxDeliveries then
              toDlq(messageId, message, "max_deliveries_exceeded")
            else
              redis.call("LPUSH", KEYS[1], messageId)
              notifyReady(KEYS[8])
            end
          else
            redis.call("HDEL", KEYS[5], messageId)
          end
        end
      end
    end
  end

  -- New claims maintain message -> delivery directly. During rolling upgrades,
  -- old workers do not, so incrementally backfill their live deliveries first.
  -- A candidate is only reaped after a complete delivery scan since it was
  -- observed, which prevents a genuine legacy delivery from being requeued.
  local deliveryCursor = redis.call("HGET", KEYS[12], "deliveryCursor") or "0"
  local generation = tonumber(redis.call("HGET", KEYS[12], "deliveryGeneration")) or 0
  local deliveryScan = redis.call("HSCAN", KEYS[4], deliveryCursor, "COUNT", tostring(batch))
  local nextDeliveryCursor = deliveryScan[1]
  local deliveryEntries = deliveryScan[2]
  for i = 1, #deliveryEntries, 2 do
    local deliveryId = deliveryEntries[i]
    local ok, delivery = pcall(cjson.decode, deliveryEntries[i + 1])
    if ok and type(delivery) == "table" and delivery.messageId then
      redis.call("HSET", KEYS[10], delivery.messageId, deliveryId)
      redis.call("HDEL", KEYS[11], delivery.messageId)
    end
  end
  if nextDeliveryCursor == "0" then generation = generation + 1 end
  redis.call("HSET", KEYS[12], "deliveryCursor", nextDeliveryCursor, "deliveryGeneration", tostring(generation))

  -- Old workers can settle a delivery without cleaning the new reverse index.
  -- Sweep stale index entries incrementally as long as mixed versions coexist.
  local ownerCursor = redis.call("HGET", KEYS[12], "ownerCursor") or "0"
  local ownerScan = redis.call("HSCAN", KEYS[10], ownerCursor, "COUNT", tostring(batch))
  redis.call("HSET", KEYS[12], "ownerCursor", ownerScan[1])
  local ownerEntries = ownerScan[2]
  for i = 1, #ownerEntries, 2 do
    if redis.call("HEXISTS", KEYS[4], ownerEntries[i + 1]) == 0 then
      redis.call("HDEL", KEYS[10], ownerEntries[i])
    end
  end

  local activeLength = tonumber(redis.call("LLEN", KEYS[6])) or 0
  local inspectCount = math.min(activeLength, batch)
  for _ = 1, inspectCount do
    -- Claims enter at the head. Rotating from the tail guarantees that old
    -- entries are inspected even while new claims arrive continuously.
    local messageId = redis.call("RPOPLPUSH", KEYS[6], KEYS[6])
    if not messageId then break end

    local deliveryId = redis.call("HGET", KEYS[10], messageId)
    if deliveryId and redis.call("HEXISTS", KEYS[4], deliveryId) == 1 then
      redis.call("HDEL", KEYS[11], messageId)
    else
      redis.call("HDEL", KEYS[10], messageId)
      local candidate = redis.call("HGET", KEYS[11], messageId)
      local observedGeneration = candidate and tonumber(string.match(candidate, "^([^:]+)"))
      local observedAttempt = candidate and tonumber(string.match(candidate, ":(.+)$"))
      local messageRaw = redis.call("HGET", KEYS[5], messageId)
      local message = messageRaw and readMessage(messageRaw)
      if not message then
        redis.call("LPOP", KEYS[6])
        redis.call("HDEL", KEYS[11], messageId)
      elseif not observedGeneration or observedAttempt ~= (tonumber(message.attempt) or 0) then
        -- A legacy nack + claim increments the attempt without touching this
        -- hash. Treat that as a new active incarnation and observe it afresh.
        redis.call(
          "HSET",
          KEYS[11],
          messageId,
          tostring(generation) .. ":" .. tostring(tonumber(message.attempt) or 0)
        )
      elseif observedGeneration + 2 <= generation then
        -- RPOPLPUSH put this entry at the head, so removal is O(1).
        redis.call("LPOP", KEYS[6])
        redis.call("HDEL", KEYS[11], messageId)
        if redis.call("HEXISTS", KEYS[5], messageId) == 1 then
          redis.call("LPUSH", KEYS[1], messageId)
          notifyReady(KEYS[8])
        end
      end
    end
  end

  -- A legacy worker can ack after a candidate was recorded. Clean terminal
  -- candidates incrementally; nacked candidates are cleared by the next claim.
  local candidateCursor = redis.call("HGET", KEYS[12], "candidateCursor") or "0"
  local candidateScan = redis.call("HSCAN", KEYS[11], candidateCursor, "COUNT", tostring(batch))
  redis.call("HSET", KEYS[12], "candidateCursor", candidateScan[1])
  local candidateEntries = candidateScan[2]
  for i = 1, #candidateEntries, 2 do
    local messageId = candidateEntries[i]
    if redis.call("HEXISTS", KEYS[5], messageId) == 0 then
      redis.call("HDEL", KEYS[11], messageId)
    end
  end

  return 1
`;

// Moving ready -> active and creating the delivery record must be one atomic
// step. When they were two round-trips, a consumer that died in between left the
// id parked in the active list with no delivery and no lease, where nothing ever
// looked for it again: the message was lost permanently. Blocking consumers now
// wait on a notify list purely as a wake-up signal and always claim through this
// script.
const CLAIM_SCRIPT = `
  ${LUA_MESSAGE_HELPERS}

  local deliveryId = ARGV[1]
  local leaseUntil = tonumber(ARGV[2])
  local maxSkips = tonumber(ARGV[3])
  local consumerId = ARGV[4]

  local messageId = nil
  local message = nil

  for _ = 1, maxSkips do
    local candidate = redis.call("RPOPLPUSH", KEYS[5], KEYS[4])
    if not candidate then return nil end

    local messageRaw = redis.call("HGET", KEYS[1], candidate)
    if messageRaw then
      local parsed = readMessage(messageRaw)
      if parsed then
        messageId = candidate
        message = parsed
        break
      end
      redis.call("HDEL", KEYS[1], candidate)
    end
    -- Dead id: drop it from active and keep draining.
    redis.call("LREM", KEYS[4], 1, candidate)
  end

  if not messageId then return nil end

  message.attempt = (tonumber(message.attempt) or 0) + 1
  redis.call("HSET", KEYS[1], messageId, cjson.encode(message))

  local delivery = {
    messageId = messageId,
    leaseUntil = leaseUntil,
    attempt = message.attempt,
    consumerId = consumerId ~= "" and consumerId or nil
  }

  redis.call("HSET", KEYS[2], deliveryId, cjson.encode(delivery))
  redis.call("ZADD", KEYS[3], tostring(leaseUntil), deliveryId)
  redis.call("HSET", KEYS[6], messageId, deliveryId)
  redis.call("HDEL", KEYS[7], messageId)

  return cjson.encode({
    messageId = messageId,
    deliveryId = deliveryId,
    leaseUntil = leaseUntil,
    attempt = message.attempt,
    orderingKey = message.orderingKey,
    enqueuedAt = message.enqueuedAt,
    metaJson = message.metaJson,
    dataJson = message.dataJson
  })
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
  if redis.call("HGET", KEYS[5], delivery.messageId) == deliveryId then
    redis.call("HDEL", KEYS[5], delivery.messageId)
  end
  redis.call("HDEL", KEYS[6], delivery.messageId)

  return 1
`;

const NACK_SCRIPT = `
  ${LUA_MESSAGE_HELPERS}
  ${LUA_NOTIFY_HELPER}

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
  if redis.call("HGET", KEYS[10], messageId) == deliveryId then
    redis.call("HDEL", KEYS[10], messageId)
  end
  redis.call("HDEL", KEYS[11], messageId)

  local messageRaw = redis.call("HGET", KEYS[3], messageId)
  if not messageRaw then
    return 0
  end

  local message = readMessage(messageRaw)
  if not message then
    redis.call("HDEL", KEYS[3], messageId)
    return 0
  end

  if (tonumber(message.attempt) or 0) >= maxDeliveries then
    writeDeadLetter(
      KEYS[7],
      KEYS[9],
      messageId,
      message,
      now,
      reason ~= "" and reason or "max_deliveries_exceeded",
      error ~= "" and error or nil,
      dlqTtlMs
    )
    redis.call("HDEL", KEYS[3], messageId)
    return 2
  end

  if delayMs > 0 then
    redis.call("ZADD", KEYS[6], tostring(now + delayMs), messageId)
  else
    redis.call("LPUSH", KEYS[5], messageId)
    notifyReady(KEYS[8])
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
  notify: string;
  delayed: string;
  leases: string;
  deliveries: string;
  messages: string;
  active: string;
  deliveryOwners: string;
  orphanCandidates: string;
  maintenance: string;
  dlq: string;
  dlqIndex: string;
  idempotencyPrefix: string;
};

export type QueueConfig<T = unknown> = {
  id: string;
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

export type QueueDeadLetter<T> = {
  messageId: string;
  data: T;
  attempts: number;
  movedAt: number;
  reason: string;
  orderingKey?: string;
  meta?: Record<string, unknown>;
  lastError?: string;
};

export type Queue<T> = QueueReader<T> & {
  send(cfg: QueueSendConfig<T>): Promise<{ messageId: string }>;
  reader(): QueueReader<T>;
  /** Oldest dead letters first. Read-only; use `dlqRemove` to drain. */
  dlq(cfg?: { tenantId?: string; limit?: number }): Promise<Array<QueueDeadLetter<T>>>;
  dlqRemove(cfg: { messageId: string; tenantId?: string }): Promise<boolean>;
};

/**
 * Stored message envelope, version 2. `dataJson` and `metaJson` hold the
 * caller's values as opaque pre-serialized JSON so that Lua only ever copies
 * the strings. Records written by <= 5.8.0 have no `v` and carry decoded
 * `data`/`meta` instead; Lua upgrades those on first touch.
 */
type StoredMessage = {
  v: 2;
  attempt: number;
  enqueuedAt: number;
  orderingKey?: string;
  dataJson?: string;
  metaJson?: string;
};

type ClaimResult = {
  messageId: string;
  deliveryId: string;
  attempt: number;
  leaseUntil: number;
  orderingKey?: string;
  enqueuedAt: number;
  dataJson?: string;
  metaJson?: string;
};

const parseOpaque = <T>(json: string | undefined): T | undefined => {
  if (json === undefined) return undefined;
  try {
    return JSON.parse(json) as T;
  } catch {
    return undefined;
  }
};

export const queue = <T>(config: QueueConfig<T>): Queue<T> => {
  type TData = T;

  if (config.ordering?.mode === "ordering_key_partitioned") {
    // Nothing in this module partitions or serialises by ordering key, so
    // accepting the option would silently break the per-key order it promises.
    throw new Error("ordering.mode 'ordering_key_partitioned' is not implemented; use 'best_effort'");
  }

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
      notify: `${base}:notify`,
      delayed: `${base}:delayed`,
      leases: `${base}:leases`,
      deliveries: `${base}:deliveries`,
      messages: `${base}:messages`,
      active: `${base}:active`,
      deliveryOwners: `${base}:delivery-owners`,
      orphanCandidates: `${base}:orphan-candidates`,
      maintenance: `${base}:maintenance`,
      dlq: `${base}:dlq`,
      dlqIndex: `${base}:dlq:index`,
      idempotencyPrefix: `${base}:idempotency`,
    };
  };

  const lastMaintenanceByTenant = new Map<string, number>();

  const runMaintenance = async (keys: QueueKeys, now: number): Promise<void> => {
    await evalScript(
      MAINTENANCE_SCRIPT,
      [
        keys.ready,
        keys.delayed,
        keys.leases,
        keys.deliveries,
        keys.messages,
        keys.active,
        keys.dlq,
        keys.notify,
        keys.dlqIndex,
        keys.deliveryOwners,
        keys.orphanCandidates,
        keys.maintenance,
      ],
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
      const client = new RedisClient();
      blockingClient = client;
      await client.connect();
      // Return the local handle: a concurrent reset may already have cleared the
      // slot while this connect was in flight.
      return client;
    };

    // Block until someone signals that the ready list may be non-empty. The
    // result is deliberately discarded — the caller re-checks by claiming.
    const awaitNotification = async (keys: QueueKeys, waitMs: number): Promise<void> => {
      const timeoutSecs = Math.max(0.001, waitMs / 1000).toFixed(3);
      try {
        const client = await ensureBlockingClient();
        await client.send("BRPOP", [keys.notify, timeoutSecs]);
      } catch {
        // A broken blocking connection must not spin: drop it, pause briefly and
        // let the next claim surface a genuine Redis outage.
        resetBlockingClient();
        await sleep(25);
      }
    };

    const recv = async (recvCfg: QueueRecvConfig = {}): Promise<QueueReceived<TData> | null> => {
      const tenantId = resolveTenant(recvCfg.tenantId);
      const keys = keysForTenant(tenantId);
      const wait = recvCfg.wait ?? true;
      const timeoutMs = recvCfg.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
      const leaseMs = recvCfg.leaseMs ?? defaultLeaseMs;
      const deadline = Date.now() + timeoutMs;

      const onAbort = (): void => {
        resetBlockingClient();
      };
      recvCfg.signal?.addEventListener("abort", onAbort, { once: true });

      try {
        while (true) {
          if (recvCfg.signal?.aborted) return null;

          await maybeRunMaintenance(tenantId, keys, Date.now(), !wait);

          const deliveryId = randomUUID();
          const claimRaw = await evalScript(
            CLAIM_SCRIPT,
            [
              keys.messages,
              keys.deliveries,
              keys.leases,
              keys.active,
              keys.ready,
              keys.deliveryOwners,
              keys.orphanCandidates,
            ],
            [deliveryId, Date.now() + leaseMs, CLAIM_MAX_SKIPS, recvCfg.consumerId ?? ""],
          );

          let claimed: ClaimResult | null = null;
          if (typeof claimRaw === "string") {
            try {
              claimed = JSON.parse(claimRaw) as ClaimResult;
            } catch {
              claimed = null;
            }
          }

          if (!claimed) {
            if (!wait) return null;
            const remainingMs = deadline - Date.now();
            if (remainingMs <= 0) return null;
            await awaitNotification(keys, Math.min(NOTIFY_BLOCK_MS, remainingMs));
            continue;
          }

        const ack = async (): Promise<boolean> => {
          const result = await evalScript(
            ACK_SCRIPT,
            [
              keys.deliveries,
              keys.leases,
              keys.messages,
              keys.active,
              keys.deliveryOwners,
              keys.orphanCandidates,
            ],
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
            [
              keys.deliveries,
              keys.leases,
              keys.messages,
              keys.active,
              keys.ready,
              keys.delayed,
              keys.dlq,
              keys.notify,
              keys.dlqIndex,
              keys.deliveryOwners,
              keys.orphanCandidates,
            ],
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
          data: parseOpaque<TData>(claimed.dataJson) as TData,
          messageId: claimed.messageId,
          deliveryId: claimed.deliveryId,
          attempt: claimed.attempt,
          leaseUntil: claimed.leaseUntil,
          orderingKey: claimed.orderingKey,
          meta: asObject<Record<string, unknown>>(parseOpaque(claimed.metaJson)) ?? undefined,
          ack,
          nack,
          touch,
        };
        }
      } finally {
        recvCfg.signal?.removeEventListener("abort", onAbort);
      }
    };

    const stream = async function* (streamCfg: QueueRecvConfig = {}): AsyncIterable<QueueReceived<TData>> {
      const wait = streamCfg.wait ?? true;
      try {
        while (!streamCfg.signal?.aborted) {
          const message = wait
            ? await retry({
                run: () => recv(streamCfg),
                after: ({ ctx }) => {
                  if (ctx.error && isRetryableTransportError(ctx.error)) {
                    ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
                  }
                },
                signal: streamCfg.signal,
              })
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

    const now = Date.now();
    const delayMs = Math.max(0, sendCfg.delayMs ?? 0);
    const idempotencyTtlMs = sendCfg.idempotencyTtlMs ?? DEFAULT_IDEMPOTENCY_TTL_MS;

    const dataJson = sendCfg.data === undefined ? undefined : JSON.stringify(sendCfg.data);
    const metaJson = sendCfg.meta === undefined ? undefined : JSON.stringify(sendCfg.meta);

    // The limit is measured against the logical envelope the caller sees, not
    // against the stored representation, so that escaping `dataJson` into the
    // envelope does not silently shrink the accepted payload size.
    const logical: string[] = [];
    if (dataJson !== undefined) logical.push(`"data":${dataJson}`);
    logical.push(`"attempt":0`);
    if (sendCfg.orderingKey !== undefined) logical.push(`"orderingKey":${JSON.stringify(sendCfg.orderingKey)}`);
    if (metaJson !== undefined) logical.push(`"meta":${metaJson}`);
    logical.push(`"enqueuedAt":${now}`);
    const payloadBytes = textEncoder.encode(`{${logical.join(",")}}`).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new Error(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const message: StoredMessage = {
      v: 2,
      attempt: 0,
      enqueuedAt: now,
      orderingKey: sendCfg.orderingKey,
      dataJson,
      metaJson,
    };
    const payload = JSON.stringify(message);

    const idempotencyKey = sendCfg.idempotencyKey
      ? `${keys.idempotencyPrefix}:${sendCfg.idempotencyKey}`
      : "";

    const result = await evalScript(
      SEND_SCRIPT,
      [keys.seq, keys.messages, keys.ready, keys.delayed, keys.notify, idempotencyKey],
      [now, delayMs, payload, idempotencyTtlMs],
    );

    const messageId = Array.isArray(result) && typeof result[0] === "string" ? result[0] : String(result);
    return { messageId };
  };

  const parseDeadLetter = (raw: unknown): QueueDeadLetter<TData> | null => {
    if (typeof raw !== "string") return null;
    let record: {
      messageId?: string;
      dataJson?: string;
      metaJson?: string;
      orderingKey?: string;
      attempts?: number;
      movedAt?: number;
      reason?: string;
      lastError?: string;
      data?: unknown;
      meta?: unknown;
    };
    try {
      record = JSON.parse(raw) as typeof record;
    } catch {
      return null;
    }
    if (!record.messageId) return null;
    return {
      messageId: record.messageId,
      // `data`/`meta` are how <= 5.8.0 wrote dead letters.
      data: (record.dataJson !== undefined ? parseOpaque<TData>(record.dataJson) : (record.data as TData)) as TData,
      meta:
        asObject<Record<string, unknown>>(
          record.metaJson !== undefined ? parseOpaque(record.metaJson) : record.meta,
        ) ?? undefined,
      orderingKey: record.orderingKey,
      attempts: record.attempts ?? 0,
      movedAt: record.movedAt ?? 0,
      reason: record.reason ?? "unknown",
      lastError: record.lastError,
    };
  };

  const dlq = async (cfg: { tenantId?: string; limit?: number } = {}): Promise<Array<QueueDeadLetter<TData>>> => {
    const keys = keysForTenant(resolveTenant(cfg.tenantId));
    const limit = Math.max(1, cfg.limit ?? 100);

    // Entries written by <= 5.8.0 have no index member. Backfill once so the
    // index stays the single ordering source afterwards.
    const [indexed, stored] = await Promise.all([
      redis.send("ZCARD", [keys.dlqIndex]),
      redis.send("HLEN", [keys.dlq]),
    ]);
    if (Number(stored) > Number(indexed)) {
      const all = (await redis.send("HGETALL", [keys.dlq])) as Record<string, string>;
      const members: string[] = [];
      for (const [messageId, raw] of Object.entries(all ?? {})) {
        members.push(String(parseDeadLetter(raw)?.movedAt ?? 0), messageId);
      }
      if (members.length > 0) await redis.send("ZADD", [keys.dlqIndex, ...members]);
    }

    const ids = (await redis.send("ZRANGE", [keys.dlqIndex, "0", String(limit - 1)])) as string[];
    if (!Array.isArray(ids) || ids.length === 0) return [];

    const raws = (await redis.send("HMGET", [keys.dlq, ...ids])) as unknown[];
    const entries: Array<QueueDeadLetter<TData>> = [];
    for (const raw of raws) {
      const entry = parseDeadLetter(raw);
      if (entry) entries.push(entry);
    }
    return entries;
  };

  const dlqRemove = async (cfg: { messageId: string; tenantId?: string }): Promise<boolean> => {
    const keys = keysForTenant(resolveTenant(cfg.tenantId));
    const removed = await redis.send("HDEL", [keys.dlq, cfg.messageId]);
    await redis.send("ZREM", [keys.dlqIndex, cfg.messageId]);
    return Number(removed) > 0;
  };

  const defaultReader = createReader();
  const reader = (): QueueReader<TData> => createReader();

  return {
    send,
    recv: defaultReader.recv,
    stream: defaultReader.stream,
    reader,
    dlq,
    dlqRemove,
  };
};
