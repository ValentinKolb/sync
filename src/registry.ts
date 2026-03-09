import { redis, RedisClient } from "bun";
import type { z } from "zod";
import { fieldArrayToObject, parseFirstStreamEntry, type ParsedEntry } from "./internal/topic-utils";
import { isRetryableTransportError, retry } from "./retry";

const DEFAULT_PREFIX = "sync:registry";
const DEFAULT_TENANT = "default";
const DEFAULT_MAX_ENTRIES = 10_000;
const DEFAULT_MAX_PAYLOAD_BYTES = 128 * 1024;
const DEFAULT_EVENT_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_EVENT_MAXLEN = 50_000;
const DEFAULT_TOMBSTONE_RETENTION_MS = 5 * 60 * 1000;
const DEFAULT_RECONCILE_BATCH_SIZE = 200;
const DEFAULT_LIST_LIMIT = 1_000;
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_RECONCILE_LOOPS = 50;
const MAX_KEY_BYTES = 512;
const MAX_IDENTIFIER_LENGTH = 256;
const MAX_KEY_DEPTH = 8;

const textEncoder = new TextEncoder();

const LUA_HELPERS = `
  local function ttl_key(ttlPrefix, logicalKey)
    return ttlPrefix .. string.len(logicalKey) .. ":" .. logicalKey
  end

  local function key_stream(keyPrefix, logicalKey)
    return keyPrefix .. logicalKey
  end

  local function prefix_stream(prefixPrefix, prefix)
    return prefixPrefix .. prefix
  end

  local function xadd_bounded(streamKey, maxEventLen, fields)
    if maxEventLen > 0 then
      redis.call("XADD", streamKey, "MAXLEN", "~", tostring(maxEventLen), "*", unpack(fields))
      return
    end
    redis.call("XADD", streamKey, "*", unpack(fields))
  end

  local function trim_root_stream(streamKey, trimMinId)
    if trimMinId ~= "" then
      redis.call("XTRIM", streamKey, "MINID", "~", trimMinId)
    end
  end

  local function latest_cursor(streamKey)
    local raw = redis.call("XREVRANGE", streamKey, "+", "-", "COUNT", "1")
    if type(raw) ~= "table" or #raw == 0 then
      return "0-0"
    end
    local first = raw[1]
    if type(first) ~= "table" or #first == 0 then
      return "0-0"
    end
    local id = first[1]
    if type(id) ~= "string" then
      return "0-0"
    end
    return id
  end

  local function ancestor_prefixes(logicalKey)
    local prefixes = {}
    local current = ""
    for segment in string.gmatch(logicalKey, "[^/]+") do
      current = current .. segment .. "/"
      table.insert(prefixes, current)
    end
    if #prefixes > 0 then
      table.remove(prefixes, #prefixes)
    end
    return prefixes
  end

  local function prefix_ref_inc(prefixRefsKey, logicalKey)
    local prefixes = ancestor_prefixes(logicalKey)
    for _, prefix in ipairs(prefixes) do
      redis.call("HINCRBY", prefixRefsKey, prefix, 1)
    end
  end

  local function prefix_ref_dec(prefixRefsKey, prefixStreamPrefix, logicalKey)
    local prefixes = ancestor_prefixes(logicalKey)
    for _, prefix in ipairs(prefixes) do
      local nextValue = tonumber(redis.call("HINCRBY", prefixRefsKey, prefix, -1))
      if nextValue <= 0 then
        redis.call("HDEL", prefixRefsKey, prefix)
      end
    end
  end

  local function emit_registry_event(rootStream, keyStreamPrefix, prefixStreamPrefix, logicalKey, trimMinId, maxEventLen, ...)
    local fields = { ... }
    xadd_bounded(rootStream, maxEventLen, fields)
    trim_root_stream(rootStream, trimMinId)

    local exactStream = key_stream(keyStreamPrefix, logicalKey)
    xadd_bounded(exactStream, maxEventLen, fields)

    local prefixes = ancestor_prefixes(logicalKey)
    for _, prefix in ipairs(prefixes) do
      local streamKey = prefix_stream(prefixStreamPrefix, prefix)
      xadd_bounded(streamKey, maxEventLen, fields)
    end
  end

  local function parse_json(raw)
    if not raw then return nil end
    local ok, decoded = pcall(cjson.decode, raw)
    if not ok then return nil end
    return decoded
  end

  local function store_tombstone(deadKey, deadKeysKey, deadExpKey, logicalKey, tombstone, tombstoneRetentionMs)
    redis.call("HSET", deadKey, logicalKey, cjson.encode(tombstone))
    redis.call("ZADD", deadKeysKey, "0", logicalKey)
    redis.call("ZADD", deadExpKey, tostring(tombstone.removedAt + tombstoneRetentionMs), logicalKey)
  end

  local function clear_stale_tombstone(deadKey, deadKeysKey, deadExpKey, logicalKey)
    redis.call("HDEL", deadKey, logicalKey)
    redis.call("ZREM", deadKeysKey, logicalKey)
    redis.call("ZREM", deadExpKey, logicalKey)
  end

  local function expire_loaded_entry(
    logicalKey,
    now,
    existing,
    stateKey,
    activeKeysKey,
    expKey,
    ttlPrefix,
    deadKey,
    deadKeysKey,
    deadExpKey,
    tombstoneRetentionMs,
    rootStream,
    keyStreamPrefix,
    prefixStreamPrefix,
    trimMinId,
    maxEventLen
  )
    redis.call("HDEL", stateKey, logicalKey)
    redis.call("ZREM", activeKeysKey, logicalKey)
    redis.call("ZREM", expKey, logicalKey)

    local tombstone = {
      key = existing.key,
      value = existing.value,
      version = tostring(existing.version),
      status = "expired",
      createdAt = tonumber(existing.createdAt) or now,
      updatedAt = tonumber(existing.updatedAt) or now,
      ttlMs = tonumber(existing.ttlMs),
      expiresAt = tonumber(existing.expiresAt),
      removedAt = now,
    }
    store_tombstone(deadKey, deadKeysKey, deadExpKey, logicalKey, tombstone, tombstoneRetentionMs)

    emit_registry_event(
      rootStream,
      keyStreamPrefix,
      prefixStreamPrefix,
      logicalKey,
      trimMinId,
      maxEventLen,
      "type",
      "expire",
      "key",
      logicalKey,
      "version",
      tostring(existing.version),
      "removedAt",
      tostring(now)
    )

    return 1
  end

  local function reconcile_exact(
    logicalKey,
    now,
    stateKey,
    activeKeysKey,
    expKey,
    ttlPrefix,
    deadKey,
    deadKeysKey,
    deadExpKey,
    tombstoneRetentionMs,
    rootStream,
    keyStreamPrefix,
    prefixStreamPrefix,
    trimMinId,
    maxEventLen
  )
    local existingRaw = redis.call("HGET", stateKey, logicalKey)
    if not existingRaw then return nil end

    local existing = parse_json(existingRaw)
    if not existing then
      redis.call("HDEL", stateKey, logicalKey)
      redis.call("ZREM", activeKeysKey, logicalKey)
      redis.call("ZREM", expKey, logicalKey)
      return nil
    end

    local expiresAt = tonumber(existing.expiresAt)
    local entryTtlMs = tonumber(existing.ttlMs)
    if not entryTtlMs or not expiresAt then
      redis.call("ZREM", expKey, logicalKey)
      return existing
    end

    if expiresAt > now then
      local ttlKey = ttl_key(ttlPrefix, logicalKey)
      if redis.call("EXISTS", ttlKey) == 1 then
        return existing
      end
    end

    expire_loaded_entry(
      logicalKey,
      now,
      existing,
      stateKey,
      activeKeysKey,
      expKey,
      ttlPrefix,
      deadKey,
      deadKeysKey,
      deadExpKey,
      tombstoneRetentionMs,
      rootStream,
      keyStreamPrefix,
      prefixStreamPrefix,
      trimMinId,
      maxEventLen
    )
    return nil
  end

  local function cleanup_tombstone_entry(deadKey, deadKeysKey, deadExpKey, stateKey, prefixRefsKey, prefixStreamPrefix, logicalKey)
    redis.call("HDEL", deadKey, logicalKey)
    redis.call("ZREM", deadKeysKey, logicalKey)
    redis.call("ZREM", deadExpKey, logicalKey)

    local activeExists = redis.call("HEXISTS", stateKey, logicalKey)
    if activeExists == 0 then
      prefix_ref_dec(prefixRefsKey, prefixStreamPrefix, logicalKey)
    end

    return 1
  end

  local function cleanup_tombstone(deadKey, deadKeysKey, deadExpKey, stateKey, prefixRefsKey, prefixStreamPrefix, logicalKey)
    local tombstoneRaw = redis.call("HGET", deadKey, logicalKey)
    if not tombstoneRaw then
      redis.call("ZREM", deadExpKey, logicalKey)
      return 0
    end

    return cleanup_tombstone_entry(deadKey, deadKeysKey, deadExpKey, stateKey, prefixRefsKey, prefixStreamPrefix, logicalKey)
  end

  local function reconcile_batch(
    now,
    batchSize,
    stateKey,
    activeKeysKey,
    expKey,
    ttlPrefix,
    deadKey,
    deadKeysKey,
    deadExpKey,
    prefixRefsKey,
    tombstoneRetentionMs,
    rootStream,
    keyStreamPrefix,
    prefixStreamPrefix,
    trimMinId,
    maxEventLen
  )
    local expired = 0
    local cleaned = 0

    local due = redis.call("ZRANGEBYSCORE", expKey, "-inf", tostring(now), "LIMIT", "0", tostring(batchSize))
    for _, logicalKey in ipairs(due) do
      local existingRaw = redis.call("HGET", stateKey, logicalKey)
      if not existingRaw then
        redis.call("ZREM", expKey, logicalKey)
      else
        local existing = parse_json(existingRaw)
        if not existing then
          redis.call("HDEL", stateKey, logicalKey)
          redis.call("ZREM", activeKeysKey, logicalKey)
          redis.call("ZREM", expKey, logicalKey)
        else
          local entryTtlMs = tonumber(existing.ttlMs)
          local expiresAt = tonumber(existing.expiresAt)
          if not entryTtlMs or not expiresAt then
            redis.call("ZREM", expKey, logicalKey)
          else
            local ttlKey = ttl_key(ttlPrefix, logicalKey)
            if redis.call("EXISTS", ttlKey) == 0 then
              expired = expired + expire_loaded_entry(
                logicalKey,
                now,
                existing,
                stateKey,
                activeKeysKey,
                expKey,
                ttlPrefix,
                deadKey,
                deadKeysKey,
                deadExpKey,
                tombstoneRetentionMs,
                rootStream,
                keyStreamPrefix,
                prefixStreamPrefix,
                trimMinId,
                maxEventLen
              )
            end
          end
        end
      end
    end

    local stale = redis.call("ZRANGEBYSCORE", deadExpKey, "-inf", tostring(now), "LIMIT", "0", tostring(batchSize))
    for _, logicalKey in ipairs(stale) do
      cleaned = cleaned + cleanup_tombstone(
        deadKey,
        deadKeysKey,
        deadExpKey,
        stateKey,
        prefixRefsKey,
        prefixStreamPrefix,
        logicalKey
      )
    end

    return {
      expired = expired,
      cleaned = cleaned,
      dueCount = #due,
      staleCount = #stale,
    }
  end
`;

const UPSERT_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local ttlMsRaw = ARGV[2]
  local payloadRaw = ARGV[3]
  local logicalKey = ARGV[4]
  local maxEntries = tonumber(ARGV[5])
  local tombstoneRetentionMs = tonumber(ARGV[6])
  local trimMinId = ARGV[7]
  local maxEventLen = tonumber(ARGV[8])

  local existing = reconcile_exact(
    logicalKey,
    now,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  )

  local payload = parse_json(payloadRaw)
  if not payload then
    return "__ERR_PAYLOAD__"
  end

  local createdAt = now
  local version

  if existing then
    createdAt = tonumber(existing.createdAt) or now
    version = tostring(redis.call("INCR", KEYS[9]))
  else
    local count = tonumber(redis.call("HLEN", KEYS[1]))
    if count >= maxEntries then
      return "__ERR_CAPACITY__"
    end
    version = tostring(redis.call("INCR", KEYS[9]))
    if redis.call("HEXISTS", KEYS[5], logicalKey) == 0 then
      prefix_ref_inc(KEYS[8], logicalKey)
    end
  end

  local ttlMs = cjson.null
  local expiresAt = cjson.null
  if ttlMsRaw ~= "" then
    ttlMs = tonumber(ttlMsRaw)
    expiresAt = now + ttlMs
  end
  local hasTtl = ttlMs ~= cjson.null

  local entry = {
    key = logicalKey,
    value = payload,
    version = version,
    status = "active",
    createdAt = createdAt,
    updatedAt = now,
    ttlMs = ttlMs,
    expiresAt = expiresAt,
  }

  redis.call("HSET", KEYS[1], logicalKey, cjson.encode(entry))
  redis.call("ZADD", KEYS[2], "0", logicalKey)

  if hasTtl then
    redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
    redis.call("SET", ttl_key(KEYS[4], logicalKey), "1", "PX", tostring(ttlMs))
  else
    redis.call("ZREM", KEYS[3], logicalKey)
    redis.call("DEL", ttl_key(KEYS[4], logicalKey))
  end

  clear_stale_tombstone(KEYS[5], KEYS[6], KEYS[7], logicalKey)

  emit_registry_event(
    KEYS[10],
    KEYS[11],
    KEYS[12],
    logicalKey,
    trimMinId,
    maxEventLen,
    "type",
    "upsert",
    "key",
    logicalKey,
    "version",
    version,
    "createdAt",
    tostring(createdAt),
    "updatedAt",
    tostring(now),
    "ttlMs",
    hasTtl and tostring(ttlMs) or "",
    "expiresAt",
    hasTtl and tostring(expiresAt) or "",
    "payload",
    payloadRaw
  )

  return cjson.encode(entry)
`;

const TOUCH_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local logicalKey = ARGV[2]
  local tombstoneRetentionMs = tonumber(ARGV[3])
  local trimMinId = ARGV[4]
  local maxEventLen = tonumber(ARGV[5])

  local existing = reconcile_exact(
    logicalKey,
    now,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  )

  if not existing then
    return nil
  end

  local ttlMs = tonumber(existing.ttlMs)
  if not ttlMs or ttlMs <= 0 then
    return nil
  end

  local expiresAt = now + ttlMs
  existing.updatedAt = now
  existing.expiresAt = expiresAt

  redis.call("HSET", KEYS[1], logicalKey, cjson.encode(existing))
  redis.call("ZADD", KEYS[3], tostring(expiresAt), logicalKey)
  redis.call("SET", ttl_key(KEYS[4], logicalKey), "1", "PX", tostring(ttlMs))

  emit_registry_event(
    KEYS[10],
    KEYS[11],
    KEYS[12],
    logicalKey,
    trimMinId,
    maxEventLen,
    "type",
    "touch",
    "key",
    logicalKey,
    "version",
    tostring(existing.version),
    "updatedAt",
    tostring(now),
    "expiresAt",
    tostring(expiresAt)
  )

  return cjson.encode({
    version = tostring(existing.version),
    expiresAt = expiresAt,
  })
`;

const REMOVE_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local logicalKey = ARGV[2]
  local reason = ARGV[3]
  local tombstoneRetentionMs = tonumber(ARGV[4])
  local trimMinId = ARGV[5]
  local maxEventLen = tonumber(ARGV[6])

  local existing = reconcile_exact(
    logicalKey,
    now,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  )

  if not existing then
    return 0
  end

  redis.call("HDEL", KEYS[1], logicalKey)
  redis.call("ZREM", KEYS[2], logicalKey)
  redis.call("ZREM", KEYS[3], logicalKey)
  redis.call("DEL", ttl_key(KEYS[4], logicalKey))

  local tombstone = {
    key = logicalKey,
    value = existing.value,
    version = tostring(existing.version),
    status = "deleted",
    createdAt = tonumber(existing.createdAt) or now,
    updatedAt = tonumber(existing.updatedAt) or now,
    ttlMs = tonumber(existing.ttlMs),
    expiresAt = tonumber(existing.expiresAt),
    removedAt = now,
    reason = reason ~= "" and reason or cjson.null,
  }
  store_tombstone(KEYS[5], KEYS[6], KEYS[7], logicalKey, tombstone, tombstoneRetentionMs)

  if reason ~= "" then
    emit_registry_event(
      KEYS[10],
      KEYS[11],
      KEYS[12],
      logicalKey,
      trimMinId,
      maxEventLen,
      "type",
      "delete",
      "key",
      logicalKey,
      "version",
      tostring(existing.version),
      "removedAt",
      tostring(now),
      "reason",
      reason
    )
  else
    emit_registry_event(
      KEYS[10],
      KEYS[11],
      KEYS[12],
      logicalKey,
      trimMinId,
      maxEventLen,
      "type",
      "delete",
      "key",
      logicalKey,
      "version",
      tostring(existing.version),
      "removedAt",
      tostring(now)
    )
  end

  return 1
`;

const CAS_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local logicalKey = ARGV[2]
  local expectedVersion = ARGV[3]
  local payloadRaw = ARGV[4]
  local tombstoneRetentionMs = tonumber(ARGV[5])
  local trimMinId = ARGV[6]
  local maxEventLen = tonumber(ARGV[7])

  local existing = reconcile_exact(
    logicalKey,
    now,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  )

  if not existing then
    return cjson.encode({ ok = false })
  end

  if tostring(existing.version) ~= expectedVersion then
    return cjson.encode({ ok = false })
  end

  local payload = parse_json(payloadRaw)
  if not payload then
    return "__ERR_PAYLOAD__"
  end

  local version = tostring(redis.call("INCR", KEYS[9]))
  local entry = {
    key = logicalKey,
    value = payload,
    version = version,
    status = "active",
    createdAt = tonumber(existing.createdAt) or now,
    updatedAt = now,
    ttlMs = tonumber(existing.ttlMs) or cjson.null,
    expiresAt = cjson.null,
  }

  if tonumber(existing.ttlMs) then
    entry.expiresAt = now + tonumber(existing.ttlMs)
  end
  local hasTtl = entry.ttlMs ~= cjson.null and entry.expiresAt ~= cjson.null

  redis.call("HSET", KEYS[1], logicalKey, cjson.encode(entry))
  if hasTtl then
    redis.call("ZADD", KEYS[3], tostring(entry.expiresAt), logicalKey)
    redis.call("SET", ttl_key(KEYS[4], logicalKey), "1", "PX", tostring(entry.ttlMs))
  else
    redis.call("ZREM", KEYS[3], logicalKey)
    redis.call("DEL", ttl_key(KEYS[4], logicalKey))
  end

  emit_registry_event(
    KEYS[10],
    KEYS[11],
    KEYS[12],
    logicalKey,
    trimMinId,
    maxEventLen,
    "type",
    "upsert",
    "key",
    logicalKey,
    "version",
    version,
    "createdAt",
    tostring(entry.createdAt),
    "updatedAt",
    tostring(now),
    "ttlMs",
    hasTtl and tostring(entry.ttlMs) or "",
    "expiresAt",
    hasTtl and tostring(entry.expiresAt) or "",
    "payload",
    payloadRaw
  )

  return cjson.encode({
    ok = true,
    entry = entry,
  })
`;

const GET_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local logicalKey = ARGV[2]
  local includeExpired = ARGV[3] == "1"
  local tombstoneRetentionMs = tonumber(ARGV[4])
  local trimMinId = ARGV[5]
  local maxEventLen = tonumber(ARGV[6])

  local existing = reconcile_exact(
    logicalKey,
    now,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  )

  if existing then
    return cjson.encode(existing)
  end

  if includeExpired then
    local tombstoneRaw = redis.call("HGET", KEYS[5], logicalKey)
    if tombstoneRaw then
      local tomb = parse_json(tombstoneRaw)
      if tomb then
        local removedAt = tonumber(tomb.removedAt) or 0
        if removedAt + tombstoneRetentionMs <= now then
          cleanup_tombstone_entry(KEYS[5], KEYS[6], KEYS[7], KEYS[1], KEYS[8], KEYS[12], logicalKey)
          return nil
        end
      end
      if tomb and tomb.status == "expired" then
        return tombstoneRaw
      end
    end
  end

  return nil
`;

const RECONCILE_BATCH_SCRIPT = `
${LUA_HELPERS}

  local now = tonumber(ARGV[1])
  local tombstoneRetentionMs = tonumber(ARGV[2])
  local batchSize = tonumber(ARGV[3])
  local trimMinId = ARGV[4]
  local maxEventLen = tonumber(ARGV[5])

  return cjson.encode(reconcile_batch(
    now,
    batchSize,
    KEYS[1],
    KEYS[2],
    KEYS[3],
    KEYS[4],
    KEYS[5],
    KEYS[6],
    KEYS[7],
    KEYS[8],
    tombstoneRetentionMs,
    KEYS[10],
    KEYS[11],
    KEYS[12],
    trimMinId,
    maxEventLen
  ))
`;

const LIST_PAGE_SCRIPT = `
${LUA_HELPERS}

  local rawPrefix = ARGV[1]
  local status = ARGV[2]
  local limit = tonumber(ARGV[3])
  local afterKey = ARGV[4]

  local sourceHash = KEYS[1]
  local sourceIndex = KEYS[2]
  if status == "expired" then
    sourceHash = KEYS[5]
    sourceIndex = KEYS[6]
  end

  local lower = "-"
  local upper = "+"
  if rawPrefix ~= "" then
    lower = "[" .. rawPrefix
    upper = "[" .. rawPrefix .. "\\255"
  end
  if afterKey ~= "" then
    lower = "(" .. afterKey
  end

  local collected = {}
  local nextKey = cjson.null
  local scanLower = lower
  local chunkSize = limit > 0 and math.max(limit * 2, 32) or 0

  while true do
    local range
    if limit > 0 then
      range = redis.call("ZRANGEBYLEX", sourceIndex, scanLower, upper, "LIMIT", "0", tostring(chunkSize))
    else
      range = redis.call("ZRANGEBYLEX", sourceIndex, scanLower, upper)
    end

    if #range == 0 then
      break
    end

    local stop = false
    for _, logicalKey in ipairs(range) do
      local raw = redis.call("HGET", sourceHash, logicalKey)
      if raw then
        local entry = parse_json(raw)
        if entry then
          if status ~= "expired" or entry.status == "expired" then
            table.insert(collected, entry)
            if limit > 0 and #collected > limit then
              nextKey = collected[limit].key
              local trimmed = {}
              for i = 1, limit do
                trimmed[i] = collected[i]
              end
              collected = trimmed
              stop = true
              break
            end
          end
        end
      end
      scanLower = "(" .. logicalKey
    end

    if stop or limit == 0 or #range < chunkSize then
      break
    end
  end

  local streamKey = KEYS[10]
  if rawPrefix ~= "" then
    streamKey = prefix_stream(KEYS[12], rawPrefix)
  end

  return cjson.encode({
    entries = collected,
    cursor = latest_cursor(streamKey),
    nextKey = nextKey,
  })
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

const parseOptionalNumber = (value: unknown): number | null => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string" && value !== "") {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }
  return null;
};

const parseOptionalString = (value: unknown): string | null => {
  if (typeof value === "string" && value.length > 0) return value;
  return null;
};

const encodeSegment = (value: string): string => encodeURIComponent(value);

const assertIdentifier = (value: string, label: string): void => {
  if (value.length === 0) throw new Error(`${label} must be non-empty`);
  if (value.length > MAX_IDENTIFIER_LENGTH) throw new Error(`${label} too long (max ${MAX_IDENTIFIER_LENGTH} chars)`);
};

const assertNoReservedBraces = (value: string, label: string): void => {
  if (value.includes("{") || value.includes("}")) {
    throw new Error(`${label} must not contain '{' or '}'`);
  }
};

const assertKeyStructure = (value: string, label: string, allowTrailingSlash: boolean): void => {
  if (value.length === 0) throw new Error(`${label} must be non-empty`);
  if (value.includes("\0")) throw new Error(`${label} must not contain null bytes`);
  assertNoReservedBraces(value, label);

  const bytes = textEncoder.encode(value).byteLength;
  if (bytes > MAX_KEY_BYTES) throw new Error(`${label} exceeds max length (${MAX_KEY_BYTES} bytes)`);
  if (value.startsWith("/")) throw new Error(`${label} must not start with '/'`);
  if (!allowTrailingSlash && value.endsWith("/")) throw new Error(`${label} must not end with '/'`);
  if (value.includes("//")) throw new Error(`${label} must not contain empty path segments`);

  const trimmed = allowTrailingSlash && value.endsWith("/") ? value.slice(0, -1) : value;
  const segments = trimmed.split("/").filter(Boolean);
  if (segments.length === 0) throw new Error(`${label} must contain at least one path segment`);
  if (segments.length > MAX_KEY_DEPTH) throw new Error(`${label} exceeds max depth (${MAX_KEY_DEPTH})`);
};

const assertLogicalKey = (value: string): void => {
  assertKeyStructure(value, "key", false);
};

const normalizePrefix = (value?: string): string => {
  if (!value) return "";
  assertKeyStructure(value, "prefix", true);
  if (!value.endsWith("/")) {
    throw new Error("prefix must end with '/'");
  }
  return value;
};

export class RegistryCapacityError extends Error {
  constructor(message = "registry capacity reached") {
    super(message);
    this.name = "RegistryCapacityError";
  }
}

export class RegistryPayloadTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RegistryPayloadTooLargeError";
  }
}

export type RegistryConfig<TSchema extends z.ZodTypeAny> = {
  id: string;
  schema: TSchema;
  tenantId?: string;
  prefix?: string;
  limits?: {
    maxEntries?: number;
    maxPayloadBytes?: number;
    eventRetentionMs?: number;
    eventMaxLen?: number;
    tombstoneRetentionMs?: number;
    reconcileBatchSize?: number;
  };
};

export type RegistryUpsertConfig<T> = {
  key: string;
  value: T;
  ttlMs?: number;
  tenantId?: string;
};

export type RegistryTouchConfig = {
  key: string;
  tenantId?: string;
};

export type RegistryRemoveConfig = {
  key: string;
  reason?: string;
  tenantId?: string;
};

export type RegistryGetConfig = {
  key: string;
  tenantId?: string;
  includeExpired?: boolean;
};

export type RegistryListConfig = {
  prefix?: string;
  status?: "active" | "expired";
  tenantId?: string;
  limit?: number;
  afterKey?: string;
};

export type RegistryCasConfig<T> = {
  key: string;
  version: string;
  value: T;
  tenantId?: string;
};

export type RegistryEntry<T> = {
  key: string;
  value: T;
  version: string;
  status: "active" | "expired";
  createdAt: number;
  updatedAt: number;
  ttlMs: number | null;
  expiresAt: number | null;
};

export type RegistrySnapshot<T> = {
  entries: RegistryEntry<T>[];
  cursor: string;
  nextKey?: string;
};

export type RegistryRecvConfig = {
  wait?: boolean;
  timeoutMs?: number;
  signal?: AbortSignal;
};

export type RegistryEvent<T> =
  | { type: "upsert"; cursor: string; entry: RegistryEntry<T> }
  | { type: "touch"; cursor: string; key: string; version: string; updatedAt: number; expiresAt: number }
  | { type: "delete"; cursor: string; key: string; version: string; removedAt: number; reason?: string }
  | { type: "expire"; cursor: string; key: string; version: string; removedAt: number }
  | { type: "overflow"; cursor: string; after: string; firstAvailable: string };

export type RegistryReader<T> = {
  recv(cfg?: RegistryRecvConfig): Promise<RegistryEvent<T> | null>;
  stream(cfg?: RegistryRecvConfig): AsyncIterable<RegistryEvent<T>>;
};

export type Registry<T> = {
  upsert(cfg: RegistryUpsertConfig<T>): Promise<RegistryEntry<T>>;
  touch(cfg: RegistryTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }>;
  remove(cfg: RegistryRemoveConfig): Promise<boolean>;
  get(cfg: RegistryGetConfig): Promise<RegistryEntry<T> | null>;
  list(cfg?: RegistryListConfig): Promise<RegistrySnapshot<T>>;
  cas(cfg: RegistryCasConfig<T>): Promise<{ ok: boolean; entry?: RegistryEntry<T> }>;
  reader(cfg?: { key?: string; prefix?: string; after?: string; tenantId?: string }): RegistryReader<T>;
};

type RegistryKeys = {
  state: string;
  activeKeys: string;
  expirations: string;
  ttlPrefix: string;
  tombstones: string;
  tombstoneKeys: string;
  tombstoneExpirations: string;
  prefixRefs: string;
  seq: string;
  rootStream: string;
  keyStreamPrefix: string;
  prefixStreamPrefix: string;
};

type StoredEntry<T> = {
  key: string;
  value: T;
  version: string;
  status: "active" | "expired";
  createdAt: number;
  updatedAt: number;
  ttlMs: number | null;
  expiresAt: number | null;
};

type ListResult<T> = {
  entries: StoredEntry<T>[];
  cursor: string;
  nextKey?: string;
};

export const registry = <TSchema extends z.ZodTypeAny>(config: RegistryConfig<TSchema>): Registry<z.infer<TSchema>> => {
  type TData = z.infer<TSchema>;

  assertIdentifier(config.id, "config.id");
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultTenant = config.tenantId ?? DEFAULT_TENANT;
  assertIdentifier(defaultTenant, "tenantId");

  const maxEntries = config.limits?.maxEntries ?? DEFAULT_MAX_ENTRIES;
  const maxPayloadBytes = config.limits?.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  const eventRetentionMs = config.limits?.eventRetentionMs ?? DEFAULT_EVENT_RETENTION_MS;
  const eventMaxLen = config.limits?.eventMaxLen ?? DEFAULT_EVENT_MAXLEN;
  const tombstoneRetentionMs = config.limits?.tombstoneRetentionMs ?? DEFAULT_TOMBSTONE_RETENTION_MS;
  const reconcileBatchSize = config.limits?.reconcileBatchSize ?? DEFAULT_RECONCILE_BATCH_SIZE;

  if (!Number.isInteger(maxEntries) || maxEntries <= 0) throw new Error("limits.maxEntries must be > 0");
  if (!Number.isInteger(maxPayloadBytes) || maxPayloadBytes <= 0) throw new Error("limits.maxPayloadBytes must be > 0");
  if (!Number.isInteger(eventRetentionMs) || eventRetentionMs <= 0) throw new Error("limits.eventRetentionMs must be > 0");
  if (!Number.isInteger(eventMaxLen) || eventMaxLen <= 0) throw new Error("limits.eventMaxLen must be > 0");
  if (!Number.isInteger(tombstoneRetentionMs) || tombstoneRetentionMs <= 0) throw new Error("limits.tombstoneRetentionMs must be > 0");
  if (!Number.isInteger(reconcileBatchSize) || reconcileBatchSize <= 0) throw new Error("limits.reconcileBatchSize must be > 0");

  const resolveTenant = (tenantId?: string): string => {
    const resolved = tenantId ?? defaultTenant;
    assertIdentifier(resolved, "tenantId");
    return resolved;
  };

  const keysForTenant = (tenantId: string): RegistryKeys => {
    const base = `${prefix}:${encodeSegment(tenantId)}:${encodeSegment(config.id)}`;
    return {
      state: `${base}:state`,
      activeKeys: `${base}:keys`,
      expirations: `${base}:exp`,
      ttlPrefix: `${base}:ttl:`,
      tombstones: `${base}:dead`,
      tombstoneKeys: `${base}:deadkeys`,
      tombstoneExpirations: `${base}:deadexp`,
      prefixRefs: `${base}:pref`,
      seq: `${base}:seq`,
      rootStream: `${base}:ev:root`,
      keyStreamPrefix: `${base}:ev:key:`,
      prefixStreamPrefix: `${base}:ev:px:`,
    };
  };

  const trimMinId = (): string => `${Date.now() - eventRetentionMs}-0`;

  const parseStoredEntry = (raw: string): RegistryEntry<TData> | null => {
    try {
      const parsed = JSON.parse(raw) as StoredEntry<unknown>;
      const validated = config.schema.safeParse(parsed.value);
      if (!validated.success) return null;

      const status = parsed.status === "expired" ? "expired" : "active";
      return {
        key: String(parsed.key),
        value: validated.data,
        version: String(parsed.version),
        status,
        createdAt: Number(parsed.createdAt),
        updatedAt: Number(parsed.updatedAt),
        ttlMs: parsed.ttlMs === null || parsed.ttlMs === undefined ? null : Number(parsed.ttlMs),
        expiresAt: parsed.expiresAt === null || parsed.expiresAt === undefined ? null : Number(parsed.expiresAt),
      };
    } catch {
      return null;
    }
  };

  const parseUpsertEvent = (entry: ParsedEntry): RegistryEvent<TData> | null => {
    const rawPayload = entry.fields.payload;
    if (!rawPayload) return null;

    try {
      const payload = JSON.parse(rawPayload) as unknown;
      const validated = config.schema.safeParse(payload);
      if (!validated.success) return null;

      const createdAt = parseOptionalNumber(entry.fields.createdAt);
      const updatedAt = parseOptionalNumber(entry.fields.updatedAt);
      if (createdAt === null || updatedAt === null) return null;

      return {
        type: "upsert",
        cursor: entry.id,
        entry: {
          key: entry.fields.key ?? "",
          value: validated.data,
          version: entry.fields.version ?? "",
          status: "active",
          createdAt,
          updatedAt,
          ttlMs: parseOptionalNumber(entry.fields.ttlMs),
          expiresAt: parseOptionalNumber(entry.fields.expiresAt),
        },
      };
    } catch {
      return null;
    }
  };

  const parseEvent = (entry: ParsedEntry): RegistryEvent<TData> | null => {
    const type = entry.fields.type;
    if (type === "upsert") return parseUpsertEvent(entry);

    if (type === "touch") {
      const updatedAt = parseOptionalNumber(entry.fields.updatedAt);
      const expiresAt = parseOptionalNumber(entry.fields.expiresAt);
      if (updatedAt === null || expiresAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        updatedAt,
        expiresAt,
      };
    }

    if (type === "delete") {
      const removedAt = parseOptionalNumber(entry.fields.removedAt);
      if (removedAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        removedAt,
        reason: parseOptionalString(entry.fields.reason) ?? undefined,
      };
    }

    if (type === "expire") {
      const removedAt = parseOptionalNumber(entry.fields.removedAt);
      if (removedAt === null) return null;
      return {
        type,
        cursor: entry.id,
        key: entry.fields.key ?? "",
        version: entry.fields.version ?? "",
        removedAt,
      };
    }

    return null;
  };

  const latestCursor = async (streamKey: string): Promise<string> => {
    const raw = await redis.send("XREVRANGE", [streamKey, "+", "-", "COUNT", "1"]);
    const parsed = parseFirstRangeEntry(raw);
    return parsed?.id ?? "0-0";
  };

  const firstAtOrAfterCursor = async (streamKey: string, cursor: string): Promise<string | null> => {
    const raw = await redis.send("XRANGE", [streamKey, cursor, "+", "COUNT", "1"]);
    return parseFirstRangeEntry(raw)?.id ?? null;
  };

  const selectionStreamKey = (keys: RegistryKeys, selection: { key?: string; prefix?: string }): string => {
    if (selection.key) return `${keys.keyStreamPrefix}${selection.key}`;
    if (selection.prefix) return `${keys.prefixStreamPrefix}${selection.prefix}`;
    return keys.rootStream;
  };

  const parseTouchResult = (raw: unknown): { ok: boolean; version?: string; expiresAt?: number } => {
    if (!raw) return { ok: false };
    try {
      const parsed = JSON.parse(typeof raw === "string" ? raw : String(raw)) as {
        version: string;
        expiresAt: number;
      };
      return {
        ok: true,
        version: String(parsed.version),
        expiresAt: Number(parsed.expiresAt),
      };
    } catch {
      return { ok: false };
    }
  };

  const parseListResult = (raw: unknown): ListResult<TData> => {
    const parsed = JSON.parse(typeof raw === "string" ? raw : String(raw)) as {
      entries?: unknown[];
      cursor?: string;
      nextKey?: string | null;
    };
    const entries: RegistryEntry<TData>[] = [];
    const rawEntries = Array.isArray(parsed.entries) ? parsed.entries : [];
    for (const item of rawEntries) {
      const entry = parseStoredEntry(JSON.stringify(item));
      if (entry) entries.push(entry);
    }
    entries.sort((a, b) => a.key.localeCompare(b.key));
    return {
      entries,
      cursor: typeof parsed.cursor === "string" ? parsed.cursor : "0-0",
      nextKey: typeof parsed.nextKey === "string" && parsed.nextKey.length > 0 ? parsed.nextKey : undefined,
    };
  };

  const runReconcileBatch = async (keys: RegistryKeys, now: number): Promise<{
    expired: number;
    cleaned: number;
    dueCount: number;
    staleCount: number;
  }> => {
    const raw = await evalScript(
      RECONCILE_BATCH_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [now, tombstoneRetentionMs, reconcileBatchSize, trimMinId(), eventMaxLen],
    );

    const parsed = JSON.parse(typeof raw === "string" ? raw : String(raw)) as {
      expired?: number;
      cleaned?: number;
      dueCount?: number;
      staleCount?: number;
    };
    return {
      expired: Number(parsed.expired ?? 0),
      cleaned: Number(parsed.cleaned ?? 0),
      dueCount: Number(parsed.dueCount ?? 0),
      staleCount: Number(parsed.staleCount ?? 0),
    };
  };

  const runFullReconcile = async (keys: RegistryKeys, now: number): Promise<void> => {
    let loops = 0;
    while (loops < MAX_RECONCILE_LOOPS) {
      const batch = await runReconcileBatch(keys, now);
      if (batch.dueCount < reconcileBatchSize && batch.staleCount < reconcileBatchSize) {
        break;
      }
      loops += 1;
      await Bun.sleep(1);
    }
  };

  const upsert = async (cfg: RegistryUpsertConfig<TData>): Promise<RegistryEntry<TData>> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    const parsed = config.schema.safeParse(cfg.value);
    if (!parsed.success) throw parsed.error;

    if (cfg.ttlMs !== undefined) {
      if (!Number.isFinite(cfg.ttlMs) || cfg.ttlMs <= 0) {
        throw new Error("ttlMs must be > 0 when provided");
      }
    }

    const payloadRaw = JSON.stringify(parsed.data);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new RegistryPayloadTooLargeError(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const raw = await evalScript(
      UPSERT_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [
        Date.now(),
        cfg.ttlMs ?? "",
        payloadRaw,
        cfg.key,
        maxEntries,
        tombstoneRetentionMs,
        trimMinId(),
        eventMaxLen,
      ],
    );

    if (raw === "__ERR_CAPACITY__") {
      throw new RegistryCapacityError(`maxEntries (${maxEntries}) reached`);
    }
    if (raw === "__ERR_PAYLOAD__") {
      throw new Error("invalid payload encoding");
    }

    const entry = parseStoredEntry(typeof raw === "string" ? raw : String(raw ?? ""));
    if (!entry) throw new Error("failed to parse stored registry entry");
    return entry;
  };

  const touch = async (cfg: RegistryTouchConfig): Promise<{ ok: boolean; version?: string; expiresAt?: number }> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    const raw = await evalScript(
      TOUCH_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [Date.now(), cfg.key, tombstoneRetentionMs, trimMinId(), eventMaxLen],
    );

    return parseTouchResult(raw);
  };

  const remove = async (cfg: RegistryRemoveConfig): Promise<boolean> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    const raw = await evalScript(
      REMOVE_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [Date.now(), cfg.key, cfg.reason ?? "", tombstoneRetentionMs, trimMinId(), eventMaxLen],
    );

    return Number(raw) > 0;
  };

  const get = async (cfg: RegistryGetConfig): Promise<RegistryEntry<TData> | null> => {
    assertLogicalKey(cfg.key);
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    const raw = await evalScript(
      GET_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [Date.now(), cfg.key, cfg.includeExpired ? 1 : 0, tombstoneRetentionMs, trimMinId(), eventMaxLen],
    );

    if (!raw) return null;
    return parseStoredEntry(typeof raw === "string" ? raw : String(raw ?? ""));
  };

  const list = async (cfg: RegistryListConfig = {}): Promise<RegistrySnapshot<TData>> => {
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);
    const prefixValue = normalizePrefix(cfg.prefix);
    const status = cfg.status ?? "active";
    if (status !== "active" && status !== "expired") {
      throw new Error(`unsupported status: ${status}`);
    }

    let limit = cfg.limit ?? 0;
    if (limit !== 0) {
      if (!Number.isInteger(limit) || limit <= 0) {
        throw new Error("limit must be a positive integer when provided");
      }
      limit = Math.min(limit, DEFAULT_LIST_LIMIT);
    }

    if (cfg.afterKey !== undefined) {
      assertLogicalKey(cfg.afterKey);
      if (prefixValue && !cfg.afterKey.startsWith(prefixValue)) {
        throw new Error("afterKey must start with prefix");
      }
    }

    const snapshotNow = Date.now();
    await runFullReconcile(keys, snapshotNow);

    const raw = await evalScript(
      LIST_PAGE_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [
        prefixValue,
        status,
        limit,
        cfg.afterKey ?? "",
      ],
    );

    return parseListResult(raw);
  };

  const cas = async (cfg: RegistryCasConfig<TData>): Promise<{ ok: boolean; entry?: RegistryEntry<TData> }> => {
    assertLogicalKey(cfg.key);
    if (cfg.version.length === 0) throw new Error("version must be non-empty");
    const tenantId = resolveTenant(cfg.tenantId);
    const keys = keysForTenant(tenantId);

    const parsed = config.schema.safeParse(cfg.value);
    if (!parsed.success) throw parsed.error;

    const payloadRaw = JSON.stringify(parsed.data);
    const payloadBytes = textEncoder.encode(payloadRaw).byteLength;
    if (payloadBytes > maxPayloadBytes) {
      throw new RegistryPayloadTooLargeError(`payload exceeds limit (${maxPayloadBytes} bytes)`);
    }

    const raw = await evalScript(
      CAS_SCRIPT,
      [
        keys.state,
        keys.activeKeys,
        keys.expirations,
        keys.ttlPrefix,
        keys.tombstones,
        keys.tombstoneKeys,
        keys.tombstoneExpirations,
        keys.prefixRefs,
        keys.seq,
        keys.rootStream,
        keys.keyStreamPrefix,
        keys.prefixStreamPrefix,
      ],
      [Date.now(), cfg.key, cfg.version, payloadRaw, tombstoneRetentionMs, trimMinId(), eventMaxLen],
    );

    if (raw === "__ERR_PAYLOAD__") {
      throw new Error("invalid payload encoding");
    }

    const parsedRaw = JSON.parse(typeof raw === "string" ? raw : String(raw)) as {
      ok?: boolean;
      entry?: unknown;
    };
    if (!parsedRaw.ok) return { ok: false };

    const entry = parsedRaw.entry ? parseStoredEntry(JSON.stringify(parsedRaw.entry)) : null;
    if (!entry) return { ok: false };
    return { ok: true, entry };
  };

  const reader = (readerCfg: { key?: string; prefix?: string; after?: string; tenantId?: string } = {}): RegistryReader<TData> => {
    if (readerCfg.key && readerCfg.prefix) {
      throw new Error("reader accepts either key or prefix, not both");
    }
    if (readerCfg.key) assertLogicalKey(readerCfg.key);
    const prefixValue = readerCfg.prefix ? normalizePrefix(readerCfg.prefix) : "";
    const tenantId = resolveTenant(readerCfg.tenantId);
    const keys = keysForTenant(tenantId);
    const streamKey = selectionStreamKey(keys, { key: readerCfg.key, prefix: prefixValue || undefined });

    let cursor = readerCfg.after ?? "$";
    let overflowPending: RegistryEvent<TData> | null = null;
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

      const firstAvailable = await firstAtOrAfterCursor(streamKey, after);
      if (!firstAvailable) return;

      if (after === "0-0" || firstAvailable !== after) {
        const liveCursor = await latestCursor(streamKey);
        overflowPending = {
          type: "overflow",
          cursor: liveCursor,
          after,
          firstAvailable,
        };
        cursor = liveCursor;
      }
    };

    const anchorLiveCursor = async (): Promise<void> => {
      if (anchored) return;
      anchored = true;
      if (cursor !== "$") return;
      cursor = await latestCursor(streamKey);
    };

    const recv = async (cfg: RegistryRecvConfig = {}): Promise<RegistryEvent<TData> | null> => {
      await anchorLiveCursor();
      await checkReplayGap();
      if (overflowPending) {
        const pending = overflowPending;
        overflowPending = null;
        return pending;
      }

      const wait = cfg.wait ?? true;
      const timeoutMs = cfg.timeoutMs ?? DEFAULT_TIMEOUT_MS;

      const args = wait
        ? ["COUNT", "1", "BLOCK", timeoutMs.toString(), "STREAMS", streamKey, cursor]
        : ["COUNT", "1", "STREAMS", streamKey, cursor];

      const raw = cfg.signal
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

      const entry = parseFirstStreamEntry(raw);
      if (!entry) return null;

      cursor = entry.id;
      return parseEvent(entry);
    };

    const stream = async function* (cfg: RegistryRecvConfig = {}): AsyncIterable<RegistryEvent<TData>> {
      const wait = cfg.wait ?? true;
      try {
        while (!cfg.signal?.aborted) {
          const next = wait
            ? await retry(
                async () => await recv(cfg),
                {
                  attempts: Number.POSITIVE_INFINITY,
                  signal: cfg.signal,
                  retryIf: isRetryableTransportError,
                },
              )
            : await recv(cfg);
          if (next) {
            yield next;
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
    get,
    list,
    cas,
    reader,
  };
};
