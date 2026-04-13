import { redis, RedisClient } from "bun";
import type { z } from "zod";
import { fieldArrayToObject, parseFirstStreamEntry, type ParsedEntry } from "./internal/topic-utils";
import { isRetryableTransportError, retry } from "./retry";

const DEFAULT_PREFIX = "sync:e";
const DEFAULT_TENANT = "default";
const DEFAULT_MAX_ENTRIES = 10_000;
const DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024;
const DEFAULT_EVENT_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_EVENT_MAXLEN = 50_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_RECONCILE_BATCH_SIZE = 200;
const DEFAULT_RECONCILE_INTERVAL_MS = 250;
const MAX_RECONCILE_TENANTS = 1_000;
const MAX_KEY_BYTES = 512;

const textEncoder = new TextEncoder();

const UPSERT_SCRIPT = `
  local now = tonumber(ARGV[1])
  local ttlMs = tonumber(ARGV[2])
  local dataRaw = ARGV[3]
  local logicalKey = ARGV[4]
  local maxEntries = tonumber(ARGV[5])
  local trimMinId = ARGV[6]
  local maxEventLen = tonumber(ARGV[7])

  local ttlKey = KEYS[4] .. string.len(logicalKey) .. ":" .. logicalKey

  local exists = redis.call("HEXISTS", KEYS[2], logicalKey)
  if exists == 0 then
    local count = tonumber(redis.call("HLEN", KEYS[2]))
    if count >= maxEntries then
      return "__ERR_CAPACITY__"
    end
  end

  local decodeOk, data = pcall(cjson.decode, dataRaw)
  if not decodeOk then
    return "__ERR_PAYLOAD__"
  end

  local version = tostring(redis.call("INCR", KEYS[1]))
  local updatedAt = now
  local expiresAt = now + ttlMs

  local entry = {
    key = logicalKey,
    data = data,
    version = version,
    updatedAt = updatedAt,
    expiresAt = expiresAt,
  }

  local encoded = cjson.encode(entry)
  redis.call("HSET", KEYS[2], logicalKey, encoded)
  redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
  redis.call("SET", ttlKey, "1", "PX", tostring(ttlMs))

  redis.call(
    "XADD",
    KEYS[5],
    "*",
    "type",
    "upsert",
    "key",
    logicalKey,
    "version",
    version,
    "updatedAt",
    tostring(updatedAt),
    "expiresAt",
    tostring(expiresAt),
    "payload",
    dataRaw
  )

  if trimMinId ~= "" then
    redis.call("XTRIM", KEYS[5], "MINID", "~", trimMinId)
  end
  if maxEventLen > 0 then
    redis.call("XTRIM", KEYS[5], "MAXLEN", "~", tostring(maxEventLen))
  end

  return encoded
`;

const TOUCH_SCRIPT = `
  local now = tonumber(ARGV[1])
  local ttlMs = tonumber(ARGV[2])
  local logicalKey = ARGV[3]
  local trimMinId = ARGV[4]
  local maxEventLen = tonumber(ARGV[5])

  local ttlKey = KEYS[4] .. string.len(logicalKey) .. ":" .. logicalKey

  local existingRaw = redis.call("HGET", KEYS[2], logicalKey)
  if not existingRaw then
    return nil
  end

  local decodeOk, existing = pcall(cjson.decode, existingRaw)
  if not decodeOk then
    redis.call("HDEL", KEYS[2], logicalKey)
    redis.call("ZREM", KEYS[3], logicalKey)
    redis.call("DEL", ttlKey)
    return nil
  end

  local version = tostring(redis.call("INCR", KEYS[1]))
  local expiresAt = now + ttlMs

  existing.version = version
  existing.updatedAt = now
  existing.expiresAt = expiresAt

  redis.call("HSET", KEYS[2], logicalKey, cjson.encode(existing))
  redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
  redis.call("SET", ttlKey, "1", "PX", tostring(ttlMs))

  redis.call(
    "XADD",
    KEYS[5],
    "*",
    "type",
    "touch",
    "key",
    logicalKey,
    "version",
    version,
    "expiresAt",
    tostring(expiresAt)
  )

  if trimMinId ~= "" then
    redis.call("XTRIM", KEYS[5], "MINID", "~", trimMinId)
  end
  if maxEventLen > 0 then
    redis.call("XTRIM", KEYS[5], "MAXLEN", "~", tostring(maxEventLen))
  end

  return cjson.encode({ version = version, expiresAt = expiresAt })
`;

const REMOVE_SCRIPT = `
  local now = tonumber(ARGV[1])
  local logicalKey = ARGV[2]
  local reason = ARGV[3]
  local trimMinId = ARGV[4]
  local maxEventLen = tonumber(ARGV[5])

  local ttlKey = KEYS[4] .. string.len(logicalKey) .. ":" .. logicalKey

  local existingRaw = redis.call("HGET", KEYS[2], logicalKey)
  if not existingRaw then
    return 0
  end

  redis.call("HDEL", KEYS[2], logicalKey)
  redis.call("ZREM", KEYS[3], logicalKey)
  redis.call("DEL", ttlKey)

  local version = tostring(redis.call("INCR", KEYS[1]))
  if reason ~= "" then
    redis.call(
      "XADD",
      KEYS[5],
      "*",
      "type",
      "delete",
      "key",
      logicalKey,
      "version",
      version,
      "reason",
      reason,
      "deletedAt",
      tostring(now)
    )
  else
    redis.call(
      "XADD",
      KEYS[5],
      "*",
      "type",
      "delete",
      "key",
      logicalKey,
      "version",
      version,
      "deletedAt",
      tostring(now)
    )
  end

  if trimMinId ~= "" then
    redis.call("XTRIM", KEYS[5], "MINID", "~", trimMinId)
  end
  if maxEventLen > 0 then
    redis.call("XTRIM", KEYS[5], "MAXLEN", "~", tostring(maxEventLen))
  end

  return 1
`;

const RECONCILE_SCRIPT = `
  local now = tonumber(ARGV[1])
  local batch = tonumber(ARGV[2])
  local trimMinId = ARGV[3]
  local maxEventLen = tonumber(ARGV[4])

  local due = redis.call("ZRANGEBYSCORE", KEYS[3], "-inf", tostring(now), "LIMIT", "0", tostring(batch))
  local expired = 0

  for _, logicalKey in ipairs(due) do
    local ttlKey = KEYS[4] .. string.len(logicalKey) .. ":" .. logicalKey
    local ttlExists = redis.call("EXISTS", ttlKey)
    if ttlExists == 0 then
      redis.call("ZREM", KEYS[3], logicalKey)

      local existingRaw = redis.call("HGET", KEYS[2], logicalKey)
      if existingRaw then
        redis.call("HDEL", KEYS[2], logicalKey)
        local version = tostring(redis.call("INCR", KEYS[1]))
        redis.call(
          "XADD",
          KEYS[5],
          "*",
          "type",
          "expire",
          "key",
          logicalKey,
          "version",
          version,
          "expiredAt",
          tostring(now)
        )
        expired = expired + 1
      end
    end
  end

  if expired > 0 and trimMinId ~= "" then
    redis.call("XTRIM", KEYS[5], "MINID", "~", trimMinId)
  end
  if expired > 0 and maxEventLen > 0 then
    redis.call("XTRIM", KEYS[5], "MAXLEN", "~", tostring(maxEventLen))
  end

  return expired
`;

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
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  if (signal?.aborted) return null;

  const client = new RedisClient();
  const onAbort = (): void => {
    safeClose(client);
  };

  if (signal) signal.addEventListener("abort", onAbort, { once: true });

  try {
    if (!client.connected) await client.connect();
    return await client.send("XREAD", args);
  } catch (error) {
    if (signal?.aborted) return null;
    throw asError(error);
  } finally {
    if (signal) signal.removeEventListener("abort", onAbort);
    safeClose(client);
  }
};

const parseFirstRangeEntry = (raw: unknown): ParsedEntry | null => {
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const first = raw[0];
  if (!Array.isArray(first) || first.length < 2) return null;
  const id = first[0];
  if (typeof id !== "string") return null;
  return {
    id,
    fields: fieldArrayToObject(first[1]),
  };
};

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

export type EphemeralConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
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
};

export type EphemeralStore<T> = {
  upsert(cfg: EphemeralUpsertConfig<T>): Promise<EphemeralEntry<T>>;
  touch(cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: EphemeralRemoveConfig): Promise<boolean>;
  snapshot(cfg?: { tenantId?: string }): Promise<EphemeralSnapshot<T>>;
  reader(cfg?: { after?: string; tenantId?: string }): EphemeralReader<T>;
};

type EphemeralKeys = {
  seq: string;
  state: string;
  expirations: string;
  ttlPrefix: string;
  events: string;
};

type StoredEntry<T> = {
  key: string;
  data: T;
  version: string;
  updatedAt: number;
  expiresAt: number;
};

const parseOptionalNumber = (value: unknown): number | null => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }
  return null;
};

const encodeSegment = (value: string): string => encodeURIComponent(value);

const assertIdentifier = (value: string, label: string): void => {
  if (value.length === 0) throw new Error(`${label} must be non-empty`);
  if (value.length > 256) throw new Error(`${label} too long (max 256 chars)`);
};

const assertLogicalKey = (value: string): void => {
  if (value.length === 0) throw new Error("key must be non-empty");
  const bytes = textEncoder.encode(value).byteLength;
  if (bytes > MAX_KEY_BYTES) throw new Error(`key exceeds max length (${MAX_KEY_BYTES} bytes)`);
};

export const ephemeral = <TSchema extends z.ZodTypeAny>(config: EphemeralConfig<TSchema>): EphemeralStore<z.infer<TSchema>> => {
  type TData = z.infer<TSchema>;

  if (!Number.isFinite(config.ttlMs) || config.ttlMs <= 0) {
    throw new Error("ttlMs must be > 0");
  }
  assertIdentifier(config.id, "config.id");

  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
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

  const keysForTenant = (tenantId: string): EphemeralKeys => {
    const base = `${DEFAULT_PREFIX}:${encodeSegment(tenantId)}:${encodeSegment(config.id)}`;
    return {
      seq: `${base}:seq`,
      state: `${base}:state`,
      expirations: `${base}:exp`,
      ttlPrefix: `${base}:ttl:`,
      events: `${base}:events`,
    };
  };

  const trimMinId = (): string => `${Date.now() - eventRetentionMs}-0`;

  const parseStoredEntry = (raw: string): EphemeralEntry<TData> | null => {
    try {
      const parsed = JSON.parse(raw) as StoredEntry<unknown>;
      const validated = config.schema.safeParse(parsed.data);
      if (!validated.success) return null;

      return {
        key: parsed.key,
        value: validated.data,
        version: String(parsed.version),
        updatedAt: Number(parsed.updatedAt),
        expiresAt: Number(parsed.expiresAt),
      };
    } catch {
      return null;
    }
  };

  const parseUpsertEvent = (entry: ParsedEntry): EphemeralEvent<TData> | null => {
    const rawPayload = entry.fields.payload;
    if (!rawPayload) return null;

    try {
      const payload = JSON.parse(rawPayload) as unknown;
      const parsed = config.schema.safeParse(payload);
      if (!parsed.success) return null;

      const updatedAt = parseOptionalNumber(entry.fields.updatedAt);
      const expiresAt = parseOptionalNumber(entry.fields.expiresAt);
      if (updatedAt === null || expiresAt === null) return null;

      return {
        type: "upsert",
        cursor: entry.id,
        entry: {
          key: entry.fields.key ?? "",
          value: parsed.data,
          version: entry.fields.version ?? "",
          updatedAt,
          expiresAt,
        },
      };
    } catch {
      return null;
    }
  };

  const parseEvent = (entry: ParsedEntry): EphemeralEvent<TData> | null => {
    const type = entry.fields.type;
    if (type === "upsert") return parseUpsertEvent(entry);

    if (type === "touch") {
      const expiresAt = parseOptionalNumber(entry.fields.expiresAt);
      if (expiresAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        expiresAt,
      };
    }

    if (type === "delete") {
      const deletedAt = parseOptionalNumber(entry.fields.deletedAt);
      if (deletedAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        deletedAt,
        reason: entry.fields.reason,
      };
    }

    if (type === "expire") {
      const expiredAt = parseOptionalNumber(entry.fields.expiredAt);
      if (expiredAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        expiredAt,
      };
    }

    return null;
  };

  const lastReconcileByTenant = new Map<string, number>();

  const runReconcile = async (keys: EphemeralKeys, now: number): Promise<number> => {
    const raw = await evalScript(
      RECONCILE_SCRIPT,
      [keys.seq, keys.state, keys.expirations, keys.ttlPrefix, keys.events],
      [now, DEFAULT_RECONCILE_BATCH_SIZE, trimMinId(), eventMaxLen],
    );
    return Number(raw ?? 0);
  };

  const maybeRunReconcile = async (tenantId: string, keys: EphemeralKeys, force = false): Promise<void> => {
    const now = Date.now();
    if (!force) {
      const last = lastReconcileByTenant.get(tenantId) ?? 0;
      if (now - last < DEFAULT_RECONCILE_INTERVAL_MS) return;
    }

    if (!lastReconcileByTenant.has(tenantId) && lastReconcileByTenant.size >= MAX_RECONCILE_TENANTS) {
      const first = lastReconcileByTenant.keys().next().value;
      if (first) lastReconcileByTenant.delete(first);
    }
    lastReconcileByTenant.set(tenantId, now);
    await runReconcile(keys, now);
  };

  const runFullReconcile = async (keys: EphemeralKeys): Promise<void> => {
    let loops = 0;
    while (loops < 50) {
      const count = await runReconcile(keys, Date.now());
      if (count < DEFAULT_RECONCILE_BATCH_SIZE) break;
      loops += 1;
      await Bun.sleep(1);
    }
  };

  const latestCursor = async (eventsKey: string): Promise<string> => {
    const raw = await redis.send("XREVRANGE", [eventsKey, "+", "-", "COUNT", "1"]);
    const parsed = parseFirstRangeEntry(raw);
    return parsed?.id ?? "0-0";
  };

  const firstAtOrAfterCursor = async (eventsKey: string, cursor: string): Promise<string | null> => {
    const raw = await redis.send("XRANGE", [eventsKey, cursor, "+", "COUNT", "1"]);
    return parseFirstRangeEntry(raw)?.id ?? null;
  };

  const upsert = async (cfg: EphemeralUpsertConfig<TData>): Promise<EphemeralEntry<TData>> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);
    await maybeRunReconcile(tenantId, keys);

    const ttlMs = cfg.ttlMs ?? config.ttlMs;
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error("ttlMs must be > 0");
    }

    const parsed = config.schema.safeParse(cfg.value);
    if (!parsed.success) throw parsed.error;

    const payloadRaw = JSON.stringify(parsed.data);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new EphemeralPayloadTooLargeError(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const raw = await evalScript(
      UPSERT_SCRIPT,
      [keys.seq, keys.state, keys.expirations, keys.ttlPrefix, keys.events],
      [Date.now(), ttlMs, payloadRaw, cfg.key, maxEntries, trimMinId(), eventMaxLen],
    );

    if (raw === "__ERR_CAPACITY__") {
      throw new EphemeralCapacityError(`maxEntries (${maxEntries}) reached`);
    }
    if (raw === "__ERR_PAYLOAD__") {
      throw new Error("invalid payload encoding");
    }

    const storedRaw = typeof raw === "string" ? raw : String(raw ?? "");
    const entry = parseStoredEntry(storedRaw);
    if (!entry) throw new Error("failed to parse stored entry");
    return entry;
  };

  const touch = async (cfg: EphemeralTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);
    await maybeRunReconcile(tenantId, keys);

    const ttlMs = cfg.ttlMs ?? config.ttlMs;
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error("ttlMs must be > 0");
    }

    const raw = await evalScript(
      TOUCH_SCRIPT,
      [keys.seq, keys.state, keys.expirations, keys.ttlPrefix, keys.events],
      [Date.now(), ttlMs, cfg.key, trimMinId(), eventMaxLen],
    );

    if (!raw) return { ok: false };

    try {
      const parsed = JSON.parse(typeof raw === "string" ? raw : String(raw)) as {
        version: string;
        expiresAt: number;
      };
      return { ok: true, version: String(parsed.version), expiresAt: Number(parsed.expiresAt) };
    } catch {
      return { ok: false };
    }
  };

  const remove = async (cfg: EphemeralRemoveConfig): Promise<boolean> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);
    await maybeRunReconcile(tenantId, keys);

    const raw = await evalScript(
      REMOVE_SCRIPT,
      [keys.seq, keys.state, keys.expirations, keys.ttlPrefix, keys.events],
      [Date.now(), cfg.key, cfg.reason ?? "", trimMinId(), eventMaxLen],
    );

    return Number(raw) > 0;
  };

  const snapshot = async (cfg: { tenantId?: string } = {}): Promise<EphemeralSnapshot<TData>> => {
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    await runFullReconcile(keys);

    const rawEntries = await redis.hvals(keys.state);
    const entries: EphemeralEntry<TData>[] = [];

    for (const raw of rawEntries) {
      const parsed = parseStoredEntry(String(raw));
      if (!parsed) continue;
      entries.push(parsed);
    }

    entries.sort((a, b) => a.key.localeCompare(b.key));

    return {
      entries,
      cursor: await latestCursor(keys.events),
    };
  };

  const reader = (readerCfg: { after?: string; tenantId?: string } = {}): EphemeralReader<TData> => {
    const tenantId = resolveTenant(readerCfg.tenantId);
    const keys = keysForTenant(tenantId);

    let cursor = readerCfg.after ?? "$";
    let overflowPending: EphemeralEvent<TData> | null = null;
    let replayChecked = false;
    let anchored = false;
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

    const checkReplayGap = async (): Promise<void> => {
      if (replayChecked) return;
      replayChecked = true;

      const after = readerCfg.after;
      if (!after || after === "$") return;

      const firstAtOrAfter = await firstAtOrAfterCursor(keys.events, after);
      if (!firstAtOrAfter) return;

      if (after === "0-0" || firstAtOrAfter !== after) {
        const liveCursor = await latestCursor(keys.events);
        overflowPending = {
          type: "overflow",
          cursor: liveCursor,
          after,
          firstAvailable: firstAtOrAfter,
        };
        cursor = liveCursor;
      }
    };

    const anchorLiveCursor = async (): Promise<void> => {
      if (anchored) return;
      anchored = true;
      if (cursor !== "$") return;
      cursor = await latestCursor(keys.events);
    };

    const recv = async (cfg: EphemeralRecvConfig = {}): Promise<EphemeralEvent<TData> | null> => {
      await anchorLiveCursor();
      await maybeRunReconcile(tenantId, keys);
      await checkReplayGap();

      if (overflowPending) {
        const event = overflowPending;
        overflowPending = null;
        return event;
      }

      const wait = cfg.wait ?? true;
      const timeoutMs = cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      const args = wait
        ? ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", keys.events, cursor]
        : ["COUNT", "1", "STREAMS", keys.events, cursor];

      const result = cfg.signal
        ? await blockingReadWithTemporaryClient(args, cfg.signal)
        : wait
          ? await (async (): Promise<unknown> => {
              const client = await ensureBlockingClient();

              try {
                return await client.send("XREAD", args);
              } catch (error) {
                resetBlockingClient();
                throw asError(error);
              }
            })()
          : await redis.send("XREAD", args);

      const entry = parseFirstStreamEntry(result);
      if (!entry) return null;

      cursor = entry.id;
      const parsed = parseEvent(entry);
      if (!parsed) return null;
      return parsed;
    };

    const stream = async function* (cfg: EphemeralRecvConfig = {}): AsyncIterable<EphemeralEvent<TData>> {
      const wait = cfg.wait ?? true;
      try {
        while (!cfg.signal?.aborted) {
          const event = wait
            ? await retry(
                async () => await recv(cfg),
                {
                  attempts: Number.POSITIVE_INFINITY,
                  signal: cfg.signal,
                  retryIf: isRetryableTransportError,
                },
              )
            : await recv(cfg);
          if (event) {
            yield event;
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

  return {
    upsert,
    touch,
    remove,
    snapshot,
    reader,
  };
};
