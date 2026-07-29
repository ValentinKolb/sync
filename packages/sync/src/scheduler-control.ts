import { redis, sleep } from "bun";
import {
  INTERNAL_QUEUE_IGNORE_LEGACY_NAMESPACE,
  queue,
  type Queue,
  type QueueConfig,
} from "./queue";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_HANDLER_TTL_MS = 15_000;
const DEFAULT_RESPONSE_TTL_MS = 5 * 60_000;

const BIND_REQUEST_SCRIPT = `
  local current = redis.call("GET", KEYS[1])
  if not current then
    redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
    redis.call("DEL", KEYS[2])
    return 1
  end
  if current == ARGV[1] then
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
    return 1
  end
  return 0
`;

const REFRESH_REQUEST_SCRIPT = `
  if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  if redis.call("EXISTS", KEYS[2]) == 1 then
    redis.call("PEXPIRE", KEYS[2], ARGV[2])
  end
  return 1
`;

const WRITE_PENDING_RESPONSE_SCRIPT = `
  if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  local raw = redis.call("GET", KEYS[2])
  if raw then
    local ok, response = pcall(cjson.decode, raw)
    if ok and response.status == "accepted" then
      redis.call("PEXPIRE", KEYS[2], ARGV[2])
      return 2
    end
    if ok and (response.status == "not_found" or response.status == "unavailable") then
      redis.call("PEXPIRE", KEYS[2], ARGV[2])
      return 3
    end
  end
  redis.call("SET", KEYS[2], ARGV[3], "PX", ARGV[2])
  return 1
`;

const WRITE_BOUND_RESPONSE_SCRIPT = `
  if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
  local incoming = cjson.decode(ARGV[3])
  local raw = redis.call("GET", KEYS[2])
  if raw then
    local ok, existing = pcall(cjson.decode, raw)
    if ok and (
      existing.status == "accepted"
      or existing.status == "not_found"
      or existing.status == "unavailable"
    ) then
      redis.call("PEXPIRE", KEYS[2], ARGV[4])
      if existing.status == incoming.status then return 1 end
      return 0
    end
  end
  redis.call("SET", KEYS[2], ARGV[3], "PX", ARGV[4])
  return 1
`;

const READ_SCHEDULE_SCRIPT = `
  local scheduleId = ARGV[1]

  local function keyType(key)
    local value = redis.call("TYPE", key)
    if type(value) == "table" then return value.ok end
    return value
  end

  local function readRecord(key)
    if keyType(key) ~= "string" then return nil end
    local raw = redis.call("GET", key)
    if not raw then return nil end
    local ok, value = pcall(cjson.decode, raw)
    if not ok or type(value) ~= "table" or tostring(value.id or "") ~= scheduleId then return nil end
    return { raw = raw, value = value }
  end

  local tombstone = nil
  if keyType(KEYS[4]) == "string" then
    local raw = redis.call("GET", KEYS[4])
    local ok, value = pcall(cjson.decode, raw)
    if ok and type(value) == "table" then tombstone = value end
  end

  local function isDeleted(record)
    if not tombstone then return false end
    local revision = tostring(record.value.revision or "")
    if revision == "" then return true end
    local deletedRevisions = tombstone.revisions
    if revision ~= "" and type(deletedRevisions) == "table" then
      for _, deletedRevision in ipairs(deletedRevisions) do
        if revision == tostring(deletedRevision) then return true end
      end
    end
    return false
  end

  local function isNewer(candidate, current)
    if not current then return true end
    local candidateRun = tonumber(candidate.value.runNumber) or 0
    local currentRun = tonumber(current.value.runNumber) or 0
    if candidateRun ~= currentRun then return candidateRun > currentRun end
    return (tonumber(candidate.value.updatedAt) or 0) > (tonumber(current.value.updatedAt) or 0)
  end

  local function hasRevision(record)
    return tostring(record.value.revision or "") ~= ""
  end

  local function runtimeCompatible(candidate, canonical)
    return tostring(candidate.value.cron or "") == tostring(canonical.value.cron or "")
      and tostring(candidate.value.tz or "") == tostring(canonical.value.tz or "")
  end

  local function mergeRuntime(canonical, runtime)
    canonical.value.runNumber = tonumber(runtime.value.runNumber) or canonical.value.runNumber
    canonical.value.nextRunAt = tonumber(runtime.value.nextRunAt) or canonical.value.nextRunAt
    canonical.value.failureCount = tonumber(runtime.value.failureCount) or canonical.value.failureCount
    canonical.value.updatedAt = tonumber(runtime.value.updatedAt) or canonical.value.updatedAt
    if runtime.value.lastError ~= nil and runtime.value.lastError ~= cjson.null then
      canonical.value.lastError = runtime.value.lastError
    else
      canonical.value.lastError = nil
    end
    canonical.raw = cjson.encode(canonical.value)
    return canonical
  end

  local revisioned = nil
  local revisionless = nil
  for _, key in ipairs({ KEYS[1], KEYS[2], KEYS[3] }) do
    local candidate = readRecord(key)
    if candidate and isDeleted(candidate) then
      redis.call("DEL", key)
    elseif candidate and hasRevision(candidate) and isNewer(candidate, revisioned) then
      revisioned = candidate
    elseif candidate and not hasRevision(candidate) and isNewer(candidate, revisionless) then
      revisionless = candidate
    end
  end
  local best = revisioned
  if best
    and revisionless
    and runtimeCompatible(revisionless, best)
    and isNewer(revisionless, best)
  then
    best = mergeRuntime(best, revisionless)
  elseif not best then
    best = revisionless
  end
  if not best then return false end
  if keyType(KEYS[5]) ~= "none" and keyType(KEYS[5]) ~= "zset" then
    return redis.error_reply("scheduler due key has wrong type")
  end
  if keyType(KEYS[7]) ~= "none" and keyType(KEYS[7]) ~= "set" then
    return redis.error_reply("scheduler index key has wrong type")
  end

  local function mirrorRecord(key)
    local kind = keyType(key)
    if kind ~= "none" and kind ~= "string" then return end
    local existing = readRecord(key)
    if kind == "none" or existing then redis.call("SET", key, best.raw) end
  end

  redis.call("SET", KEYS[1], best.raw)
  mirrorRecord(KEYS[2])
  mirrorRecord(KEYS[3])
  local nextRunAt = tostring(tonumber(best.value.nextRunAt) or 0)
  redis.call("ZADD", KEYS[5], nextRunAt, scheduleId)
  redis.call("SADD", KEYS[7], scheduleId)
  if keyType(KEYS[6]) == "none" or keyType(KEYS[6]) == "zset" then
    redis.call("ZADD", KEYS[6], nextRunAt, scheduleId)
  end
  if keyType(KEYS[8]) == "none" or keyType(KEYS[8]) == "set" then
    redis.call("SADD", KEYS[8], scheduleId)
  end
  return best.raw
`;

type StoredSchedule = {
  id: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  lastError?: string;
  meta?: Record<string, unknown>;
};

export type SchedulerControlState = "available" | "unavailable";

export type SchedulerControlInfo = {
  schedulerId: string;
  scheduleId: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  state: SchedulerControlState;
  lastError?: string;
  meta?: Record<string, unknown>;
};

export type SchedulerControlConfig = {
  prefix?: string;
  timeoutMs?: number;
};

export type SchedulerControlRunNowConfig = {
  schedulerId: string;
  scheduleId: string;
  requestId?: string;
  timeoutMs?: number;
};

export type SchedulerControl = {
  list(): Promise<SchedulerControlInfo[]>;
  runNow(cfg: SchedulerControlRunNowConfig): Promise<void>;
};

export class SchedulerControlNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SchedulerControlNotFoundError";
  }
}

export class SchedulerControlUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SchedulerControlUnavailableError";
  }
}

export class SchedulerControlTimeoutError extends SchedulerControlUnavailableError {
  constructor(message: string) {
    super(message);
    this.name = "SchedulerControlTimeoutError";
  }
}

export type SchedulerControlRequest = {
  requestId: string;
  schedulerId: string;
  scheduleId: string;
  requestedAt: number;
};

type SchedulerControlResponse =
  | { status: "pending"; pendingAt: number }
  | { status: "accepted"; acceptedAt: number }
  | { status: "not_found"; error: string }
  | { status: "unavailable"; error: string };

export const encodeSchedulerKeyPart = (value: string): string =>
  value.replaceAll("%", "%25").replaceAll(":", "%3A");
const schedulerIndexKey = (prefix: string): string => `${prefix}:index`;
const schedulerBaseKey = (prefix: string, schedulerId: string): string =>
  `sync:scheduler:namespace:v4:${encodeURIComponent(JSON.stringify([prefix, schedulerId]))}`;
export const schedulerRegistrationKey = (prefix: string, schedulerId: string): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:registered`;
const schedulerLegacyOwnerKey = (prefix: string, schedulerId: string): string =>
  `${prefix}:${schedulerId}:namespace-owner`;
export const schedulerScheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:schedule:${encodeSchedulerKeyPart(scheduleId)}:record`;
export const schedulerV2ScheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:v2:${encodeSchedulerKeyPart(schedulerId)}:schedule:${scheduleId}`;
export const legacySchedulerScheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${schedulerId}:schedule:${scheduleId}`;
export const schedulerScheduleTombstoneKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:schedule:${encodeSchedulerKeyPart(scheduleId)}:deleted`;
export const schedulerDueKey = (prefix: string, schedulerId: string): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:due`;
export const legacySchedulerDueKey = (prefix: string, schedulerId: string): string =>
  `${prefix}:${schedulerId}:due`;
export const schedulerScheduleIndexKey = (prefix: string, schedulerId: string): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:index`;
export const legacySchedulerScheduleIndexKey = (prefix: string, schedulerId: string): string =>
  `${prefix}:${schedulerId}:index`;

export const hasCompatibleLegacySchedule = async (
  prefix: string,
  schedulerId: string,
  scheduleId: string,
): Promise<boolean> => {
  const raw = await redis.get(legacySchedulerScheduleKey(prefix, schedulerId, scheduleId));
  if (raw === null) return true;
  try {
    const legacy = JSON.parse(raw) as { id?: unknown };
    return legacy.id === scheduleId;
  } catch {
    return true;
  }
};

export const resolveLegacySchedulerAccess = async (
  prefix: string,
  schedulerId: string,
): Promise<boolean> => {
  const identity = JSON.stringify([prefix, schedulerId]);
  const ownerKey = schedulerLegacyOwnerKey(prefix, schedulerId);
  const existingOwner = await redis.get(ownerKey);

  const joined = `${prefix}:${schedulerId}`;
  const candidates: Array<[string, string]> = [];
  for (let index = joined.indexOf(":"); index >= 0; index = joined.indexOf(":", index + 1)) {
    const candidatePrefix = joined.slice(0, index);
    const candidateSchedulerId = joined.slice(index + 1);
    if (candidatePrefix && candidateSchedulerId) candidates.push([candidatePrefix, candidateSchedulerId]);
  }

  const registrations = await Promise.all(
    candidates.map(async ([candidatePrefix, candidateSchedulerId]) => {
      const index = schedulerIndexKey(candidatePrefix);
      const typeRaw = await redis.send("TYPE", [index]);
      const type = typeof typeRaw === "string"
        ? typeRaw
        : (typeRaw as { ok?: unknown } | null)?.ok;
      return type === "set"
        && Number(await redis.send("SISMEMBER", [index, candidateSchedulerId])) > 0;
    }),
  );
  const owners = candidates.filter((_, index) => registrations[index]);
  if (owners.length > 1) {
    const conflictingOwners = owners.filter(
      ([candidatePrefix, candidateSchedulerId]) =>
        candidatePrefix !== prefix || candidateSchedulerId !== schedulerId,
    );
    const currentRegistrations = await Promise.all(
      conflictingOwners.map(async ([candidatePrefix, candidateSchedulerId]) => {
        if (
          Number(
            await redis.send("EXISTS", [
              schedulerRegistrationKey(candidatePrefix, candidateSchedulerId),
            ]),
          ) > 0
        ) {
          return true;
        }
        const index = schedulerScheduleIndexKey(candidatePrefix, candidateSchedulerId);
        const typeRaw = await redis.send("TYPE", [index]);
        const type = typeof typeRaw === "string"
          ? typeRaw
          : (typeRaw as { ok?: unknown } | null)?.ok;
        return type === "set" && Number(await redis.send("SCARD", [index])) > 0;
      }),
    );
    if (existingOwner === null || currentRegistrations.some((registered) => !registered)) {
      throw new Error(
        "scheduler namespace migration required for ambiguous legacy identity; drain old workers and migrate or remove legacy keys",
      );
    }
  }
  if (existingOwner !== null) {
    if (existingOwner !== identity) return false;
    if (owners.length === 1) {
      const [ownerPrefix, ownerSchedulerId] = owners[0]!;
      if (ownerPrefix !== prefix || ownerSchedulerId !== schedulerId) {
        throw new Error(
          "scheduler namespace migration required for a late conflicting legacy registration; drain old workers and migrate or remove legacy keys",
        );
      }
    }
    return true;
  }
  if (owners.length === 1) {
    const [ownerPrefix, ownerSchedulerId] = owners[0]!;
    if (ownerPrefix !== prefix || ownerSchedulerId !== schedulerId) return false;
  } else {
    const legacyState = Number(
      await redis.send("EXISTS", [
        legacySchedulerDueKey(prefix, schedulerId),
        legacySchedulerScheduleIndexKey(prefix, schedulerId),
      ]),
    );
    if (legacyState > 0) {
      throw new Error(
        "scheduler namespace migration required for unowned legacy state; drain old workers and migrate or remove legacy keys",
      );
    }
  }

  await redis.send("SET", [ownerKey, identity, "NX"]);
  return (await redis.get(ownerKey)) === identity;
};

const disabledCompatibilityKey = (
  prefix: string,
  schedulerId: string,
  kind: string,
  scheduleId?: string,
): string =>
  `${schedulerBaseKey(prefix, schedulerId)}:compat-disabled:${kind}${
    scheduleId === undefined ? "" : `:${encodeSchedulerKeyPart(scheduleId)}`
  }`;
const handlerIndexKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${encodeSchedulerKeyPart(schedulerId)}:control:${encodeSchedulerKeyPart(scheduleId)}:handlers`;
const handlerInstanceKey = (prefix: string, schedulerId: string, scheduleId: string, instanceId: string): string =>
  `${prefix}:${encodeSchedulerKeyPart(schedulerId)}:control:${encodeSchedulerKeyPart(scheduleId)}:handler:${encodeSchedulerKeyPart(instanceId)}`;
const responseKey = (prefix: string, requestId: string): string =>
  `${prefix}:control:response:${encodeSchedulerKeyPart(requestId)}`;
const requestKey = (prefix: string, requestId: string): string =>
  `${prefix}:control:request:${encodeSchedulerKeyPart(requestId)}`;

const assertId = (name: string, value: string): void => {
  if (!value) throw new Error(`${name} is required`);
};

const assertPositiveMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${name} must be a finite number greater than 0`);
  }
};

const parseSchedule = (raw: string | null): StoredSchedule | null => {
  if (!raw) return null;
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (typeof value.id !== "string" || !value.id) return null;
    if (typeof value.cron !== "string" || !value.cron) return null;
    if (typeof value.tz !== "string" || !value.tz) return null;
    if (typeof value.nextRunAt !== "number" || !Number.isFinite(value.nextRunAt)) return null;
    return {
      id: value.id,
      cron: value.cron,
      tz: value.tz,
      createdAt: Number(value.createdAt) || Date.now(),
      updatedAt: Number(value.updatedAt) || Date.now(),
      nextRunAt: Number(value.nextRunAt),
      runNumber: Number(value.runNumber) || 0,
      failureCount: Number(value.failureCount) || 0,
      ...(typeof value.lastError === "string" ? { lastError: value.lastError } : {}),
      // `metaJson` is the opaque form the scheduler stores; `meta` is how
      // <= 5.8.0 wrote it.
      ...(typeof value.metaJson === "string"
        ? { meta: parseMeta(value.metaJson) }
        : value.meta && typeof value.meta === "object"
          ? { meta: value.meta as Record<string, unknown> }
          : {}),
    };
  } catch {
    return null;
  }
};

const parseMeta = (metaJson: string): Record<string, unknown> | undefined => {
  try {
    const value = JSON.parse(metaJson) as unknown;
    return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
};

export const readSchedulerScheduleRaw = async (
  prefix: string,
  schedulerId: string,
  scheduleId: string,
): Promise<string | null> => {
  const legacyAccess = await resolveLegacySchedulerAccess(prefix, schedulerId);
  const legacyScheduleAccess = legacyAccess
    && await hasCompatibleLegacySchedule(prefix, schedulerId, scheduleId);
  const result = await redis.send("EVAL", [
    READ_SCHEDULE_SCRIPT,
    "8",
    schedulerScheduleKey(prefix, schedulerId, scheduleId),
    legacyAccess
      ? schedulerV2ScheduleKey(prefix, schedulerId, scheduleId)
      : disabledCompatibilityKey(prefix, schedulerId, "v2-record", scheduleId),
    legacyScheduleAccess
      ? legacySchedulerScheduleKey(prefix, schedulerId, scheduleId)
      : disabledCompatibilityKey(prefix, schedulerId, "legacy-record", scheduleId),
    schedulerScheduleTombstoneKey(prefix, schedulerId, scheduleId),
    schedulerDueKey(prefix, schedulerId),
    legacyAccess
      ? legacySchedulerDueKey(prefix, schedulerId)
      : disabledCompatibilityKey(prefix, schedulerId, "due"),
    schedulerScheduleIndexKey(prefix, schedulerId),
    legacyAccess
      ? legacySchedulerScheduleIndexKey(prefix, schedulerId)
      : disabledCompatibilityKey(prefix, schedulerId, "index"),
    scheduleId,
  ]);
  return typeof result === "string" ? result : null;
};

const toInfo = (
  schedulerId: string,
  schedule: StoredSchedule,
  state: SchedulerControlState,
): SchedulerControlInfo => ({
  schedulerId,
  scheduleId: schedule.id,
  cron: schedule.cron,
  tz: schedule.tz,
  createdAt: schedule.createdAt,
  updatedAt: schedule.updatedAt,
  nextRunAt: schedule.nextRunAt,
  runNumber: schedule.runNumber,
  failureCount: schedule.failureCount,
  state,
  ...(schedule.lastError ? { lastError: schedule.lastError } : {}),
  ...(schedule.meta ? { meta: schedule.meta } : {}),
});

const readResponse = async (prefix: string, requestId: string): Promise<SchedulerControlResponse | null> => {
  const raw = await redis.get(responseKey(prefix, requestId));
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as SchedulerControlResponse;
    if (
      parsed.status === "pending"
      || parsed.status === "accepted"
      || parsed.status === "not_found"
      || parsed.status === "unavailable"
    ) {
      return parsed;
    }
  } catch {
    return null;
  }
  return null;
};

const requestTarget = (request: Pick<SchedulerControlRequest, "schedulerId" | "scheduleId">): string =>
  JSON.stringify([request.schedulerId, request.scheduleId]);

const writeBoundResponse = async (
  prefix: string,
  request: SchedulerControlRequest,
  response: SchedulerControlResponse,
  ttlMs = DEFAULT_RESPONSE_TTL_MS,
): Promise<boolean> =>
  Number(
    await redis.send("EVAL", [
      WRITE_BOUND_RESPONSE_SCRIPT,
      "2",
      requestKey(prefix, request.requestId),
      responseKey(prefix, request.requestId),
      requestTarget(request),
      String(ttlMs),
      JSON.stringify(response),
      String(ttlMs),
    ]),
  ) > 0;

const bindRequest = async (
  prefix: string,
  requestId: string,
  schedulerId: string,
  scheduleId: string,
  ttlMs: number,
): Promise<void> => {
  const target = requestTarget({ schedulerId, scheduleId });
  const bound = Number(
    await redis.send("EVAL", [
      BIND_REQUEST_SCRIPT,
      "2",
      requestKey(prefix, requestId),
      responseKey(prefix, requestId),
      target,
      String(ttlMs),
    ]),
  );
  if (bound === 0) {
    throw new Error(`schedulerControl.runNow: requestId ${requestId} is already bound to another schedule`);
  }
};

const liveHandlerKeys = async (prefix: string, schedulerId: string, scheduleId: string): Promise<string[]> => {
  const members = await redis.send("SMEMBERS", [handlerIndexKey(prefix, schedulerId, scheduleId)]);
  if (!Array.isArray(members) || members.length === 0) return [];

  const live: string[] = [];
  for (const raw of members) {
    const key = String(raw);
    const exists = Number(await redis.send("EXISTS", [key]));
    if (exists > 0) {
      live.push(key);
    } else {
      await redis.send("SREM", [handlerIndexKey(prefix, schedulerId, scheduleId), key]);
    }
  }
  return live;
};

export const registerSchedulerControlIndex = async (prefix: string, schedulerId: string): Promise<void> => {
  await redis.send("SADD", [schedulerIndexKey(prefix), schedulerId]);
};

export const refreshSchedulerControlHandler = async (cfg: {
  prefix: string;
  schedulerId: string;
  scheduleId: string;
  instanceId: string;
  ttlMs?: number;
}): Promise<void> => {
  assertPositiveMs("ttlMs", cfg.ttlMs);
  const ttlMs = Math.max(1_000, cfg.ttlMs ?? DEFAULT_HANDLER_TTL_MS);
  const key = handlerInstanceKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId, cfg.instanceId);
  await redis.send("SADD", [handlerIndexKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId), key]);
  await redis.send("SET", [key, "1", "PX", String(ttlMs)]);
};

export const removeSchedulerControlHandler = async (cfg: {
  prefix: string;
  schedulerId: string;
  scheduleId: string;
  instanceId: string;
}): Promise<void> => {
  const key = handlerInstanceKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId, cfg.instanceId);
  await redis.send("DEL", [key]);
  await redis.send("SREM", [handlerIndexKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId), key]);
};

export const hasLiveSchedulerControlHandler = async (
  prefix: string,
  schedulerId: string,
  scheduleId: string,
): Promise<boolean> => (await liveHandlerKeys(prefix, schedulerId, scheduleId)).length > 0;

export const schedulerControlQueue = (
  prefix: string,
  schedulerId: string,
  scheduleId: string,
): Queue<SchedulerControlRequest> =>
  queue<SchedulerControlRequest>({
    id: `${encodeSchedulerKeyPart(schedulerId)}:${encodeSchedulerKeyPart(scheduleId)}:manual`,
    prefix: `${prefix}:control`,
    delivery: { defaultLeaseMs: 30_000, maxDeliveries: Number.MAX_SAFE_INTEGER },
    limits: { maxMessageAgeMs: 5 * 60_000, dlqRetentionMs: 60_000 },
    [INTERNAL_QUEUE_IGNORE_LEGACY_NAMESPACE]: true,
  } as QueueConfig<SchedulerControlRequest> & {
    [INTERNAL_QUEUE_IGNORE_LEGACY_NAMESPACE]: true;
  });

export const markSchedulerControlAccepted = async (
  prefix: string,
  request: SchedulerControlRequest,
): Promise<boolean> =>
  await writeBoundResponse(prefix, request, { status: "accepted", acceptedAt: Date.now() });

export const markSchedulerControlPending = async (
  prefix: string,
  request: SchedulerControlRequest,
  ttlMs = DEFAULT_RESPONSE_TTL_MS,
): Promise<"pending" | "accepted" | "terminal" | null> => {
  assertPositiveMs("ttlMs", ttlMs);
  const result = Number(
    await redis.send("EVAL", [
      WRITE_PENDING_RESPONSE_SCRIPT,
      "2",
      requestKey(prefix, request.requestId),
      responseKey(prefix, request.requestId),
      requestTarget(request),
      String(ttlMs),
      JSON.stringify({ status: "pending", pendingAt: Date.now() }),
    ]),
  );
  if (result === 3) return "terminal";
  if (result === 2) return "accepted";
  if (result === 1) return "pending";
  return null;
};

export const markSchedulerControlNotFound = async (
  prefix: string,
  request: SchedulerControlRequest,
  error: string,
): Promise<boolean> =>
  await writeBoundResponse(prefix, request, { status: "not_found", error });

export const markSchedulerControlUnavailable = async (
  prefix: string,
  request: SchedulerControlRequest,
  error: string,
): Promise<boolean> =>
  await writeBoundResponse(prefix, request, { status: "unavailable", error });

export const refreshSchedulerControlRequestBinding = async (
  prefix: string,
  request: SchedulerControlRequest,
  ttlMs = DEFAULT_RESPONSE_TTL_MS,
): Promise<boolean> => {
  assertPositiveMs("ttlMs", ttlMs);
  return Number(
    await redis.send("EVAL", [
      REFRESH_REQUEST_SCRIPT,
      "2",
      requestKey(prefix, request.requestId),
      responseKey(prefix, request.requestId),
      requestTarget(request),
      String(ttlMs),
    ]),
  ) > 0;
};

export const schedulerControl = (config: SchedulerControlConfig = {}): SchedulerControl => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  assertPositiveMs("timeoutMs", config.timeoutMs);
  const defaultTimeoutMs = Math.max(1, config.timeoutMs ?? DEFAULT_TIMEOUT_MS);

  const list = async (): Promise<SchedulerControlInfo[]> => {
    const schedulerIdsRaw = await redis.send("SMEMBERS", [schedulerIndexKey(prefix)]);
    if (!Array.isArray(schedulerIdsRaw) || schedulerIdsRaw.length === 0) return [];

    const out: SchedulerControlInfo[] = [];
    for (const rawSchedulerId of schedulerIdsRaw) {
      const schedulerId = String(rawSchedulerId);
      const legacyAccess = await resolveLegacySchedulerAccess(prefix, schedulerId);
      const currentIdsRaw = await redis.send("SMEMBERS", [schedulerScheduleIndexKey(prefix, schedulerId)]);
      const legacyTypeRaw = legacyAccess
        ? await redis.send("TYPE", [legacySchedulerScheduleIndexKey(prefix, schedulerId)])
        : "none";
      const legacyType = typeof legacyTypeRaw === "string"
        ? legacyTypeRaw
        : (legacyTypeRaw as { ok?: unknown } | null)?.ok;
      const legacyIdsRaw = legacyAccess && legacyType === "set"
        ? await redis.send("SMEMBERS", [legacySchedulerScheduleIndexKey(prefix, schedulerId)])
        : [];
      const scheduleIds = Array.from(
        new Set(
          [
            ...(Array.isArray(currentIdsRaw) ? currentIdsRaw : []),
            ...(Array.isArray(legacyIdsRaw) ? legacyIdsRaw : []),
          ].map((value) => String(value)),
        ),
      );
      if (scheduleIds.length === 0) continue;
      const values = await Promise.all(
        scheduleIds.map((scheduleId) => readSchedulerScheduleRaw(prefix, schedulerId, scheduleId)),
      );

      for (let i = 0; i < scheduleIds.length; i += 1) {
        const scheduleId = scheduleIds[i];
        if (!scheduleId) continue;
        const raw = values[i];
        const schedule = parseSchedule(typeof raw === "string" ? raw : null);
        if (!schedule) continue;
        const available = await hasLiveSchedulerControlHandler(prefix, schedulerId, scheduleId);
        out.push(toInfo(schedulerId, schedule, available ? "available" : "unavailable"));
      }
    }

    out.sort((a, b) => a.schedulerId.localeCompare(b.schedulerId) || a.scheduleId.localeCompare(b.scheduleId));
    return out;
  };

  const runNow = async (cfg: SchedulerControlRunNowConfig): Promise<void> => {
    assertId("schedulerId", cfg.schedulerId);
    assertId("scheduleId", cfg.scheduleId);

    assertPositiveMs("timeoutMs", cfg.timeoutMs);
    const timeoutMs = Math.max(1, cfg.timeoutMs ?? defaultTimeoutMs);
    const requestId = cfg.requestId ?? crypto.randomUUID();
    assertId("requestId", requestId);
    const requestTtlMs = Math.max(DEFAULT_RESPONSE_TTL_MS, timeoutMs * 2);
    await bindRequest(prefix, requestId, cfg.schedulerId, cfg.scheduleId, requestTtlMs);

    // An accepted retry is the original request even if the handler went away
    // after accepting it. Binding first still rejects attempts to retarget the
    // same request id.
    const existing = await readResponse(prefix, requestId);
    if (existing?.status === "accepted") return;
    if (existing?.status === "not_found") throw new SchedulerControlNotFoundError(existing.error);
    if (existing?.status === "unavailable") throw new SchedulerControlUnavailableError(existing.error);

    if (existing?.status !== "pending") {
      const schedule = parseSchedule(await readSchedulerScheduleRaw(prefix, cfg.schedulerId, cfg.scheduleId));
      if (!schedule) {
        throw new SchedulerControlNotFoundError(
          `schedulerControl.runNow: schedule ${cfg.schedulerId}/${cfg.scheduleId} not found`,
        );
      }

      if (!(await hasLiveSchedulerControlHandler(prefix, cfg.schedulerId, cfg.scheduleId))) {
        throw new SchedulerControlUnavailableError(
          `schedulerControl.runNow: no live handler for schedule ${cfg.schedulerId}/${cfg.scheduleId}`,
        );
      }

      await schedulerControlQueue(prefix, cfg.schedulerId, cfg.scheduleId).send({
        data: {
          requestId,
          schedulerId: cfg.schedulerId,
          scheduleId: cfg.scheduleId,
          requestedAt: Date.now(),
        },
        idempotencyKey: requestId,
        idempotencyTtlMs: requestTtlMs,
      });
    }

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const response = await readResponse(prefix, requestId);
      if (response?.status === "accepted") return;
      if (response?.status === "not_found") throw new SchedulerControlNotFoundError(response.error);
      if (response?.status === "unavailable") throw new SchedulerControlUnavailableError(response.error);
      await sleep(Math.min(50, Math.max(1, deadline - Date.now())));
    }

    throw new SchedulerControlTimeoutError(
      `schedulerControl.runNow: timed out waiting for live handler to accept ${cfg.schedulerId}/${cfg.scheduleId}`,
    );
  };

  return { list, runNow };
};
