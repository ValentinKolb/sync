import { redis, RedisClient } from "bun";
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
  local ttlMsRaw = ARGV[2]
  local ttlMs = tonumber(ttlMsRaw)
  local dataRaw = ARGV[3]
  local logicalKey = ARGV[4]
  local maxEntries = tonumber(ARGV[5])
  local trimMinId = ARGV[6]
  local maxEventLen = tonumber(ARGV[7])

  local ttlKey = KEYS[4] .. string.len(logicalKey) .. ":" .. logicalKey

  local existingRaw = redis.call("HGET", KEYS[2], logicalKey)
  if not existingRaw then
    local count = tonumber(redis.call("HLEN", KEYS[2]))
    if count >= maxEntries then
      return "__ERR_CAPACITY__"
    end
  end

  -- dataJson remains authoritative because cjson changes empty arrays and
  -- precision-sensitive numbers. data is an N-1 compatibility shadow for 5.8
  -- readers and intentionally retains their existing cjson semantics.
  local decodeOk, legacyData = pcall(cjson.decode, dataRaw)
  if not decodeOk then
    return "__ERR_PAYLOAD__"
  end

  -- Preserve createdAt across upserts on the same key; reset only on fresh-create
  local createdAt = now
  if existingRaw then
    local okExisting, existing = pcall(cjson.decode, existingRaw)
    if okExisting and type(existing.createdAt) == "number" then
      createdAt = existing.createdAt
    end
  end

  local version = tostring(redis.call("INCR", KEYS[1]))
  local updatedAt = now
  local expiresAt = now + ttlMs

  local entry = {
    v = 2,
    key = logicalKey,
    dataJson = dataRaw,
    data = legacyData,
    version = version,
    createdAt = createdAt,
    updatedAt = updatedAt,
    expiresAt = expiresAt,
  }

  local encoded = cjson.encode(entry)
  redis.call("HSET", KEYS[2], logicalKey, encoded)
  redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
  redis.call("SET", ttlKey, "1", "PX", ttlMsRaw)

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
    "createdAt",
    tostring(createdAt),
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
  local ttlMsRaw = ARGV[2]
  local ttlMs = tonumber(ttlMsRaw)
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

  -- Keep both representations during the 5.8 compatibility window. Existing
  -- records from either side are expanded on touch without changing dataJson.
  if existing.dataJson == nil and existing.data ~= nil then
    existing.dataJson = cjson.encode(existing.data)
  end
  if existing.data == nil and existing.dataJson ~= nil then
    local legacyOk, legacyData = pcall(cjson.decode, existing.dataJson)
    if legacyOk then
      existing.data = legacyData
    end
  end
  existing.v = 2
  existing.version = version
  existing.updatedAt = now
  existing.expiresAt = expiresAt

  redis.call("HSET", KEYS[2], logicalKey, cjson.encode(existing))
  redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
  redis.call("SET", ttlKey, "1", "PX", ttlMsRaw)

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

const blockingReadWithClient = async (
  client: RedisClient,
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  if (signal?.aborted) return null;

  const onAbort = (): void => {
    safeClose(client);
  };
  if (signal) signal.addEventListener("abort", onAbort, { once: true });

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

    const read = client.send("XREAD", args);
    return signal ? await raceWithAbort(read, signal) : await read;
  } catch (error) {
    if (signal?.aborted) return null;
    throw asError(error);
  } finally {
    if (signal) signal.removeEventListener("abort", onAbort);
  }
};

const blockingReadWithTemporaryClient = async (
  args: string[],
  signal?: AbortSignal,
): Promise<unknown> => {
  const client = new RedisClient();
  try {
    return await blockingReadWithClient(client, args, signal);
  } finally {
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
  /** Release the reader's blocking connection. Idempotent. */
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

type EphemeralKeys = {
  seq: string;
  state: string;
  expirations: string;
  ttlPrefix: string;
  events: string;
};

/**
 * Stored entry, version 2. `dataJson` is the authoritative caller JSON. `data`
 * remains as a decoded compatibility shadow for 5.8 readers until the next
 * major storage boundary. Records written by <= 5.8.0 only carry `data`.
 */
type StoredEntry<T> = {
  v?: 2;
  key: string;
  data?: T;
  dataJson?: string;
  version: string;
  createdAt: number;
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

/**
 * Redis takes PX as an integer. A fractional value, or one large enough that a
 * round-trip through Lua renders it in exponential form, is rejected at the
 * command level and surfaces as an opaque argument error.
 */
const assertTtlMs = (ttlMs: number): void => {
  if (!Number.isInteger(ttlMs) || ttlMs <= 0 || ttlMs > Number.MAX_SAFE_INTEGER) {
    throw new Error("ttlMs must be a positive integer number of milliseconds");
  }
};

const positiveSafeIntegerLimit = (value: number | undefined, fallback: number, label: string): number => {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return resolved;
};

export const ephemeral = <T>(config: EphemeralConfig<T>): EphemeralStore<T> => {
  type TData = T;

  assertTtlMs(config.ttlMs);
  assertIdentifier(config.id, "config.id");

  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  assertIdentifier(defaultTenant, "tenantId");
  const maxEntries = positiveSafeIntegerLimit(
    config.limits?.maxEntries,
    DEFAULT_MAX_ENTRIES,
    "limits.maxEntries",
  );
  const maxPayloadBytes = positiveSafeIntegerLimit(
    config.limits?.maxPayloadBytes,
    DEFAULT_MAX_PAYLOAD_BYTES,
    "limits.maxPayloadBytes",
  );
  const eventRetentionMs = positiveSafeIntegerLimit(
    config.limits?.eventRetentionMs,
    DEFAULT_EVENT_RETENTION_MS,
    "limits.eventRetentionMs",
  );
  const eventMaxLen = positiveSafeIntegerLimit(
    config.limits?.eventMaxLen,
    DEFAULT_EVENT_MAXLEN,
    "limits.eventMaxLen",
  );

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

  // Clamped and floored: a retention larger than the current epoch yields a
  // negative stream id that Redis rejects, and the trim runs after the XADD in
  // the same script, so the event landed but the call threw. Empty skips it.
  const trimMinId = (): string => {
    const trimFrom = Math.floor(Date.now() - eventRetentionMs);
    return trimFrom > 0 ? `${trimFrom}-0` : "";
  };

  const parseStoredEntry = (raw: string): EphemeralEntry<TData> | null => {
    try {
      const parsed = JSON.parse(raw) as StoredEntry<TData>;
      const updatedAt = Number(parsed.updatedAt);
      // Fallback for legacy records that may not have createdAt: use updatedAt.
      const createdAt = Number.isFinite(Number(parsed.createdAt)) ? Number(parsed.createdAt) : updatedAt;
      return {
        key: parsed.key,
        value: (parsed.dataJson !== undefined ? (JSON.parse(parsed.dataJson) as TData) : parsed.data) as TData,
        version: String(parsed.version),
        createdAt,
        updatedAt,
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
      const payload = JSON.parse(rawPayload) as TData;

      const updatedAt = parseOptionalNumber(entry.fields.updatedAt);
      const expiresAt = parseOptionalNumber(entry.fields.expiresAt);
      if (updatedAt === null || expiresAt === null) return null;

      // Legacy events may not have createdAt: fall back to updatedAt.
      const createdAt = parseOptionalNumber(entry.fields.createdAt) ?? updatedAt;

      return {
        type: "upsert",
        cursor: entry.id,
        entry: {
          key: entry.fields.key ?? "",
          value: payload,
          version: entry.fields.version ?? "",
          createdAt,
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
    assertTtlMs(ttlMs);

    const payloadRaw = JSON.stringify(cfg.value);
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
    assertTtlMs(ttlMs);

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

  const snapshot = async (cfg: { tenantId?: string; prefix?: string } = {}): Promise<EphemeralSnapshot<TData>> => {
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    await runFullReconcile(keys);

    // Capture the replay boundary first. A write between this read and HVALS may
    // then appear in both snapshot and replay, but can never be absent from both.
    const cursor = await latestCursor(keys.events);
    const rawEntries = await redis.hvals(keys.state);
    const entries: EphemeralEntry<TData>[] = [];
    const prefix = cfg.prefix;
    const now = Date.now();

    for (const raw of rawEntries) {
      const parsed = parseStoredEntry(String(raw));
      if (!parsed) continue;
      // Reconciliation is deliberately batched. A very large expired backlog,
      // or a legacy row missing its expiration index, must still never leak into
      // a point-in-time snapshot.
      if (parsed.expiresAt <= now) continue;
      if (prefix && !parsed.key.startsWith(prefix)) continue;
      entries.push(parsed);
    }

    entries.sort((a, b) => a.key.localeCompare(b.key));

    return {
      entries,
      cursor,
    };
  };

  const reader = (readerCfg: { after?: string; tenantId?: string; prefix?: string } = {}): EphemeralReader<TData> => {
    const tenantId = resolveTenant(readerCfg.tenantId);
    const keys = keysForTenant(tenantId);
    const prefix = readerCfg.prefix;

    const eventKey = (event: EphemeralEvent<TData>): string | null => {
      if (event.type === "upsert") return event.entry.key;
      if (event.type === "overflow") return null;
      return event.key;
    };

    const matchesPrefix = (event: EphemeralEvent<TData>): boolean => {
      if (!prefix) return true;
      const key = eventKey(event);
      if (key === null) return true; // overflow always passes
      return key.startsWith(prefix);
    };

    let cursor = readerCfg.after ?? "$";
    let overflowPending: EphemeralEvent<TData> | null = null;
    let replayChecked = false;
    let anchored = false;
    const blockingClients = new Set<RedisClient>();
    const closeController = new AbortController();
    let closed = false;
    let closePromise: Promise<void> | null = null;
    let activeOperations = 0;
    let operationsIdle = Promise.resolve();
    let resolveOperationsIdle: (() => void) | null = null;
    let operationTail: Promise<void> | null = null;

    const assertOpen = (): void => {
      if (closed) throw new Error("ephemeral reader is closed");
    };

    const runOperation = async <R>(operation: () => Promise<R>): Promise<R> => {
      assertOpen();
      if (activeOperations === 0) {
        operationsIdle = new Promise<void>((resolve) => {
          resolveOperationsIdle = resolve;
        });
      }
      activeOperations += 1;
      const previous = operationTail;
      let releaseTurn!: () => void;
      const turn = new Promise<void>((resolve) => {
        releaseTurn = resolve;
      });
      operationTail = turn;
      try {
        if (previous) await previous;
        return await operation();
      } finally {
        releaseTurn();
        if (operationTail === turn) operationTail = null;
        activeOperations -= 1;
        if (activeOperations === 0) {
          resolveOperationsIdle?.();
          resolveOperationsIdle = null;
        }
      }
    };

    /**
     * Release this reader's blocking connections. Streams keep separate clients
     * for cancellation, while their reads are serialized around the shared cursor.
     */
    const close = (): Promise<void> => {
      if (closePromise) return closePromise;
      closed = true;
      closeController.abort();
      for (const client of blockingClients) safeClose(client);
      closePromise = operationsIdle;
      return closePromise;
    };

    /**
     * Overflow is a property of the reader, not of its first read. This used to
     * be a one-shot latch over the constructor-supplied cursor, so a reader that
     * started healthy and then fell behind — a GC pause, a slow handler, a long
     * redeploy — silently resumed at the oldest surviving entry with no signal.
     * Every event in the trimmed range was dropped from the consumer's
     * materialised view permanently, because the corrective event was gone too.
     *
     * It now runs against the live cursor before every read.
     */
    const checkReplayGap = async (from = replayChecked ? cursor : (readerCfg.after ?? cursor)): Promise<void> => {
      replayChecked = true;

      if (!from || from === "$") return;

      const firstAtOrAfter = await firstAtOrAfterCursor(keys.events, from);
      if (!firstAtOrAfter) return;

      // `0-0` means "replay everything", which is an overflow by definition
      // whenever the stream has already been trimmed at all.
      if (from === "0-0" || firstAtOrAfter !== from) {
        const liveCursor = await latestCursor(keys.events);
        overflowPending = {
          type: "overflow",
          cursor: liveCursor,
          after: from,
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

    const recvInternal = async (
      cfg: EphemeralRecvConfig = {},
      blockingRead = blockingReadWithTemporaryClient,
    ): Promise<EphemeralEvent<TData> | null> => {
      if (closed) return null;
      await anchorLiveCursor();
      if (closed) return null;
      await maybeRunReconcile(tenantId, keys);
      if (closed) return null;
      await checkReplayGap();
      if (closed) return null;

      if (overflowPending) {
        const event = overflowPending;
        overflowPending = null;
        return event;
      }

      const wait = cfg.wait ?? true;
      const timeoutMs = cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      // Loop internally to skip prefix-mismatched events without returning null
      // prematurely. Only meaningful when `prefix` is set; otherwise one pass.
      while (true) {
        if (cfg.signal?.aborted || closeController.signal.aborted) return null;

        const args = wait
          ? ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", keys.events, cursor]
          : ["COUNT", "1", "STREAMS", keys.events, cursor];

        const previousCursor = cursor;
        let result: unknown;
        if (wait) {
          const readController = new AbortController();
          const abortRead = (): void => readController.abort();
          cfg.signal?.addEventListener("abort", abortRead, { once: true });
          closeController.signal.addEventListener("abort", abortRead, { once: true });
          if (cfg.signal?.aborted || closeController.signal.aborted) readController.abort();
          try {
            result = await blockingRead(args, readController.signal);
          } finally {
            cfg.signal?.removeEventListener("abort", abortRead);
            closeController.signal.removeEventListener("abort", abortRead);
          }
        } else {
          result = await redis.send("XREAD", args);
        }

        if (closed) return null;
        const entry = parseFirstStreamEntry(result);
        if (!entry) return null;

        if (previousCursor !== "0-0" && previousCursor !== "$") {
          await checkReplayGap(previousCursor);
          if (overflowPending) {
            const event = overflowPending;
            overflowPending = null;
            return event;
          }
        }

        cursor = entry.id;
        const parsed = parseEvent(entry);
        if (!parsed) continue;
        if (!matchesPrefix(parsed)) continue;
        return parsed;
      }
    };

    const recv = async (cfg: EphemeralRecvConfig = {}): Promise<EphemeralEvent<TData> | null> =>
      await runOperation(() => recvInternal(cfg));

    const stream = async function* (cfg: EphemeralRecvConfig = {}): AsyncIterable<EphemeralEvent<TData>> {
      const wait = cfg.wait ?? true;
      const streamClient = new RedisClient();
      blockingClients.add(streamClient);
      try {
        while (!closed && !cfg.signal?.aborted) {
          let event: EphemeralEvent<TData> | null;
          try {
            event = wait
              ? await retry({
                  run: () =>
                    runOperation(() =>
                      recvInternal(
                        cfg,
                        (args, signal) => blockingReadWithClient(streamClient, args, signal),
                      ),
                    ),
                  after: ({ ctx }) => {
                    if (ctx.error && isRetryableTransportError(ctx.error)) {
                      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
                    }
                  },
                  signal: cfg.signal,
                })
              : await recv(cfg);
          } catch (error) {
            if (closed || cfg.signal?.aborted) break;
            throw error;
          }
          if (event) {
            yield event;
            continue;
          }
          if (!wait) break;
        }
      } finally {
        safeClose(streamClient);
        blockingClients.delete(streamClient);
      }
    };

    return { recv, stream, close, [Symbol.asyncDispose]: close };
  };

  return {
    upsert,
    touch,
    remove,
    snapshot,
    reader,
  };
};
