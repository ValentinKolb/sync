import { redis, sleep } from "bun";
import { mutex, type Lock } from "./mutex";
import { expBackoff, type BackoffOptions } from "./retry";
import { assertValidTimeZone, nextCronTimestamp } from "./internal/cron";
import { emitTrace, type TraceHandler } from "./trace";
import {
  markSchedulerControlAccepted,
  markSchedulerControlNotFound,
  markSchedulerControlPending,
  markSchedulerControlUnavailable,
  readSchedulerScheduleRaw,
  hasCompatibleLegacySchedule,
  resolveLegacySchedulerAccess,
  refreshSchedulerControlRequestBinding,
  refreshSchedulerControlHandler,
  removeSchedulerControlHandler,
  encodeSchedulerKeyPart,
  legacySchedulerDueKey,
  legacySchedulerScheduleIndexKey,
  legacySchedulerScheduleKey,
  schedulerDueKey,
  schedulerScheduleIndexKey,
  schedulerRegistrationKey,
  schedulerScheduleKey,
  schedulerScheduleTombstoneKey,
  schedulerV2ScheduleKey,
  schedulerControlQueue,
  type SchedulerControlRequest,
} from "./scheduler-control";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_LEASE_MS = 5_000;
const DEFAULT_HEARTBEAT_MS = 500;
const DEFAULT_TICK_MS = 500;
const DEFAULT_BATCH_SIZE = 200;
// How many times one manual-run request may be retried before it is reported as
// failed instead of replayed indefinitely.
const CONTROL_MAX_ATTEMPTS = 3;
const MIN_LEASE_MS = 500;
const MIN_HEARTBEAT_MS = 100;

const normalizeMs = (value: number | undefined, fallback: number, minimum: number): number =>
  Math.max(minimum, Math.floor(value !== undefined && Number.isFinite(value) ? value : fallback));

const assertPositiveMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${name} must be a finite number greater than 0`);
  }
};

const assertNonNegativeMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
    throw new Error(`${name} must be a finite non-negative number`);
  }
};

const LUA_KEY_TYPE_HELPER = `
  local function keyType(key)
    local value = redis.call("TYPE", key)
    if type(value) == "table" then return value.ok end
    return value
  end
`;

const LUA_SCHEDULE_RECORD_HELPERS = `
  ${LUA_KEY_TYPE_HELPER}

  local function readRecord(key)
    if keyType(key) ~= "string" then return nil end
    local raw = redis.call("GET", key)
    if not raw then return nil end
    local ok, value = pcall(cjson.decode, raw)
    if not ok or type(value) ~= "table" or tostring(value.id or "") ~= scheduleId then return nil end
    return { raw = raw, value = value }
  end

  local function readTombstone(key)
    if keyType(key) ~= "string" then return nil end
    local raw = redis.call("GET", key)
    local ok, value = pcall(cjson.decode, raw)
    if ok and type(value) == "table" then return value end
    return nil
  end

  local function isDeleted(record, tombstone)
    if not tombstone then return false end
    local revision = tostring(record.value.revision or "")
    if revision == "" then return true end
    if type(tombstone.revisions) == "table" then
      for _, deletedRevision in ipairs(tombstone.revisions) do
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

  local function selectRecord(keys, tombstone, deleteTombstoned)
    local revisioned = nil
    local revisionless = nil
    for _, key in ipairs(keys) do
      local candidate = readRecord(key)
      if candidate and isDeleted(candidate, tombstone) then
        if deleteTombstoned then redis.call("DEL", key) end
      elseif candidate and hasRevision(candidate) and isNewer(candidate, revisioned) then
        revisioned = candidate
      elseif candidate and not hasRevision(candidate) and isNewer(candidate, revisionless) then
        revisionless = candidate
      end
    end

    if revisioned then
      if revisionless
        and runtimeCompatible(revisionless, revisioned)
        and isNewer(revisionless, revisioned)
      then
        return mergeRuntime(revisioned, revisionless)
      end
      return revisioned
    end
    return revisionless
  end
`;

// Upsert: creates or updates the schedule record. Preserves runNumber always;
// preserves nextRunAt/failureCount iff cron/tz unchanged.
// Returns [1 = created | 2 = updated, stored nextRunAt].
const UPSERT_SCRIPT = `
  local incomingRaw = ARGV[1]
  local firstRunAt = tonumber(ARGV[2])
  local scheduleId = ARGV[3]
  local now = tonumber(ARGV[4])

  ${LUA_SCHEDULE_RECORD_HELPERS}
  local tombstone = readTombstone(KEYS[4])

  local existingRecord = selectRecord({ KEYS[1], KEYS[2], KEYS[3] }, tombstone, true)

  local created = 1
  local incoming = cjson.decode(incomingRaw)

  if existingRecord then
    created = 0
    local existing = existingRecord.value
    incoming.createdAt = tonumber(existing.createdAt) or incoming.createdAt
    incoming.runNumber = tonumber(existing.runNumber) or 0

    local shouldResetNext = tostring(existing.cron) ~= tostring(incoming.cron)
      or tostring(existing.tz) ~= tostring(incoming.tz)

    if shouldResetNext then
      incoming.nextRunAt = firstRunAt
      incoming.failureCount = 0
    else
      incoming.nextRunAt = tonumber(existing.nextRunAt) or firstRunAt
      incoming.failureCount = tonumber(existing.failureCount) or 0
    end
  else
    incoming.runNumber = 0
    incoming.nextRunAt = firstRunAt
    incoming.failureCount = 0
  end

  incoming.updatedAt = now
  local storedRaw = cjson.encode(incoming)
  if keyType(KEYS[5]) ~= "none" and keyType(KEYS[5]) ~= "zset" then
    return redis.error_reply("scheduler due key has wrong type")
  end
  if keyType(KEYS[7]) ~= "none" and keyType(KEYS[7]) ~= "set" then
    return redis.error_reply("scheduler index key has wrong type")
  end
  if keyType(KEYS[9]) ~= "none" and keyType(KEYS[9]) ~= "set" then
    return redis.error_reply("scheduler global index key has wrong type")
  end
  if keyType(KEYS[10]) ~= "none" and keyType(KEYS[10]) ~= "string" then
    return redis.error_reply("scheduler registration key has wrong type")
  end

  local function mirrorRecord(key)
    local kind = keyType(key)
    if kind ~= "none" and kind ~= "string" then return end
    local existing = readRecord(key)
    if kind == "none" or existing then redis.call("SET", key, storedRaw) end
  end

  redis.call("SET", KEYS[1], storedRaw)
  mirrorRecord(KEYS[2])
  mirrorRecord(KEYS[3])
  redis.call("ZADD", KEYS[5], tostring(incoming.nextRunAt), scheduleId)
  redis.call("SADD", KEYS[7], scheduleId)
  redis.call("SADD", KEYS[9], ARGV[5])
  redis.call("SET", KEYS[10], "1")
  if keyType(KEYS[6]) == "none" or keyType(KEYS[6]) == "zset" then
    redis.call("ZADD", KEYS[6], tostring(incoming.nextRunAt), scheduleId)
  end
  if keyType(KEYS[8]) == "none" or keyType(KEYS[8]) == "set" then
    redis.call("SADD", KEYS[8], scheduleId)
  end

  if created == 1 then return {1, tostring(incoming.nextRunAt)} end
  return {2, tostring(incoming.nextRunAt)}
`;

const DELETE_SCRIPT = `
  local scheduleId = ARGV[1]
  local deletedAt = tonumber(ARGV[2])

  ${LUA_KEY_TYPE_HELPER}

  local revisions = {}
  local seenRevisions = {}
  local function rememberRevision(revision)
    if revision == nil then return end
    local value = tostring(revision)
    if seenRevisions[value] then return end
    seenRevisions[value] = true
    table.insert(revisions, value)
  end
  local existingDeletedAt = 0
  if keyType(KEYS[4]) == "string" then
    local raw = redis.call("GET", KEYS[4])
    local ok, existing = pcall(cjson.decode, raw)
    if ok and type(existing) == "table" then
      existingDeletedAt = tonumber(existing.deletedAt) or 0
      if type(existing.revisions) == "table" then
        for _, revision in ipairs(existing.revisions) do
          rememberRevision(revision)
        end
      end
    end
  end
  local function deleteRecord(key, collisionFree)
    local kind = keyType(key)
    if collisionFree and kind ~= "none" and kind ~= "string" then
      redis.call("DEL", key)
      return
    end
    if kind ~= "string" then return end
    local raw = redis.call("GET", key)
    local ok, value = pcall(cjson.decode, raw)
    if collisionFree or (ok and type(value) == "table" and tostring(value.id or "") == scheduleId) then
      if ok and type(value) == "table" and value.revision ~= nil then
        rememberRevision(value.revision)
      end
      redis.call("DEL", key)
    end
  end

  deleteRecord(KEYS[1], true)
  deleteRecord(KEYS[2], false)
  deleteRecord(KEYS[3], false)
  redis.call("SET", KEYS[4], cjson.encode({
    deletedAt = math.max(deletedAt, existingDeletedAt),
    revisions = revisions,
  }))
  redis.call("ZREM", KEYS[5], scheduleId)
  redis.call("SREM", KEYS[7], scheduleId)
  if keyType(KEYS[6]) == "zset" then
    redis.call("ZREM", KEYS[6], scheduleId)
  end
  if keyType(KEYS[8]) == "set" then
    redis.call("SREM", KEYS[8], scheduleId)
  end
  return 1
`;

// Persist after a dispatch. Every durable scheduler mutation goes through here,
// and it refuses to write unless all four fences hold:
//
//  1. the schedule record still exists — a delete during an in-flight run must
//     not resurrect it as a record the index set no longer lists;
//  2. the leader key still holds our token — a pod whose lease lapsed during a
//     long callback must not clobber the new leader's state;
//  3. the stored runNumber is still the one this run was derived from — a
//     concurrent manual and cron dispatch must not collide or rewind nextRunAt.
//  4. the per-schedule dispatch lease still belongs to this run.
//
// A refusal returns 0 and the run's state change is dropped, which is the safe
// direction: the surviving record belongs to whoever still holds the fence.
//
// Only the counters this run owns are patched onto the stored record, so a
// field written by someone else in the meantime is preserved, and `metaJson`
// stays an opaque string.
const PERSIST_SCRIPT = `
  if ARGV[3] ~= "" and redis.call("GET", KEYS[3]) ~= ARGV[3] then return 0 end
  if redis.call("GET", KEYS[4]) ~= ARGV[5] then return 0 end
  if ARGV[7] ~= "" and redis.call("GET", KEYS[8]) ~= ARGV[7] then return 0 end
  if ARGV[8] ~= "" and redis.call("GET", KEYS[9]) ~= ARGV[8] then return 0 end

  local scheduleId = ARGV[1]
  ${LUA_SCHEDULE_RECORD_HELPERS}
  local tombstone = readTombstone(KEYS[10])

  local record = selectRecord({ KEYS[1], KEYS[5], KEYS[6] }, tombstone, true)
  if not record then return 0 end

  local existing = record.value
  if tostring(tonumber(existing.runNumber) or 0) ~= ARGV[2] then return 0 end
  if tostring(existing.revision or "") ~= ARGV[6] then return 0 end

  local patch = cjson.decode(ARGV[4])
  existing.runNumber = patch.runNumber
  existing.nextRunAt = patch.nextRunAt
  existing.failureCount = patch.failureCount
  existing.updatedAt = patch.updatedAt
  if patch.lastError ~= nil and patch.lastError ~= cjson.null then
    existing.lastError = patch.lastError
  else
    existing.lastError = nil
  end

  local storedRaw = cjson.encode(existing)
  if keyType(KEYS[2]) ~= "none" and keyType(KEYS[2]) ~= "zset" then
    return redis.error_reply("scheduler due key has wrong type")
  end
  if keyType(KEYS[11]) ~= "none" and keyType(KEYS[11]) ~= "set" then
    return redis.error_reply("scheduler index key has wrong type")
  end
  local function mirrorRecord(key)
    local kind = keyType(key)
    if kind ~= "none" and kind ~= "string" then return end
    local current = readRecord(key)
    if kind == "none" or current then redis.call("SET", key, storedRaw) end
  end

  redis.call("SET", KEYS[1], storedRaw)
  mirrorRecord(KEYS[5])
  mirrorRecord(KEYS[6])
  redis.call("ZADD", KEYS[2], tostring(patch.nextRunAt), ARGV[1])
  redis.call("SADD", KEYS[11], ARGV[1])
  if keyType(KEYS[7]) == "none" or keyType(KEYS[7]) == "zset" then
    redis.call("ZADD", KEYS[7], tostring(patch.nextRunAt), ARGV[1])
  end
  if keyType(KEYS[12]) == "none" or keyType(KEYS[12]) == "set" then
    redis.call("SADD", KEYS[12], ARGV[1])
  end
  return 1
`;

// ==========================
// Types
// ==========================

type StoredSchedule = {
  id: string;
  revision: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  lastError?: string;
  /**
   * Caller `meta` as an opaque pre-serialized JSON string. Lua copies it and
   * never decodes it: Redis' cjson turns an empty array into an empty object and
   * loses precision past 14 significant digits. Records written by <= 5.8.0
   * carry a decoded `meta` instead and are normalized on read.
   */
  metaJson?: string;
};

type PairedLock = {
  current: Lock;
  legacy: Lock | null;
};

export type SchedulerMetrics = {
  isLeader: boolean;
  leaderChanges: number;
  dispatches: number;
  failures: number;
  reschedules: number;
  tickErrors: number;
  /** Durable writes a fence refused: lost lease, deleted schedule, or a concurrent dispatch. */
  staleWrites: number;
  /** Due slots this instance was leader for but had no handler to serve. */
  unservedSlots: number;
  lastTickAt: number | null;
};

export type SchedulerTraceEvent<Result = unknown> =
  | { type: "scheduled"; scheduleId: string; cron: string; tz: string; nextRunAt: number; meta?: Record<string, unknown> }
  | { type: "started"; scheduleId: string; runNumber: number; trigger: "cron" | "manual"; slotTs: number }
  | { type: "succeeded"; scheduleId: string; runNumber: number; data: Result; durationMs: number }
  | { type: "failed"; scheduleId: string; runNumber: number; error: Error; durationMs: number }
  | { type: "rescheduled"; scheduleId: string; runNumber: number; delayMs: number };

export type ScheduleCtx = {
  scheduleId: string;
  slotTs: number;
  runNumber: number;
  failureCount: number;
  /** What caused this run: "cron" for tick dispatch, "manual" for `runNow`. */
  trigger: "cron" | "manual";
  readonly duration: number;
  signal: AbortSignal;
};

export type ScheduleAfterCtx<Result = unknown> = ScheduleCtx & {
  data?: Result;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: SchedulerMetrics;
};

export type ScheduleConfig<Result = unknown> = {
  id: string;
  cron: string;
  tz?: string;
  meta?: Record<string, unknown>;
  trace?: TraceHandler<SchedulerTraceEvent<Result>>;
  process: (cfg: { ctx: ScheduleCtx }) => Promise<Result> | Result;
  after?: (cfg: { ctx: ScheduleAfterCtx<Result> }) => Promise<void> | void;
};

export type SchedulerConfig = {
  id: string;
  prefix?: string;
  leader?: {
    leaseMs?: number;
    heartbeatMs?: number;
  };
  dispatch?: {
    tickMs?: number;
    batchSize?: number;
  };
};

export type SchedulerInfo = {
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

export type Scheduler = {
  id: string;
  start(): void;
  stop(): Promise<void>;
  create<R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }>;
  delete(cfg: { id: string }): Promise<void>;
  runNow(cfg: { id: string }): Promise<void>;
  get(cfg: { id: string }): Promise<SchedulerInfo | null>;
  list(): Promise<SchedulerInfo[]>;
  metric(): SchedulerMetrics;
};

// ==========================
// Internal
// ==========================

type HandlerEntry = {
  process: (cfg: { ctx: ScheduleCtx }) => Promise<unknown> | unknown;
  after?: (cfg: { ctx: ScheduleAfterCtx<unknown> }) => Promise<void> | void;
  trace?: TraceHandler<SchedulerTraceEvent<unknown>>;
};

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

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
      revision: typeof value.revision === "string" ? value.revision : "",
      cron: value.cron,
      tz: value.tz,
      createdAt: Number(value.createdAt) || Date.now(),
      updatedAt: Number(value.updatedAt) || Date.now(),
      nextRunAt: Number(value.nextRunAt),
      runNumber: Number(value.runNumber) || 0,
      failureCount: Number(value.failureCount) || 0,
      ...(typeof value.lastError === "string" ? { lastError: value.lastError } : {}),
      ...(typeof value.metaJson === "string"
        ? { metaJson: value.metaJson }
        : value.meta && typeof value.meta === "object"
          ? { metaJson: JSON.stringify(value.meta) }
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

const asInfo = (schedule: StoredSchedule): SchedulerInfo => ({
  id: schedule.id,
  cron: schedule.cron,
  tz: schedule.tz,
  createdAt: schedule.createdAt,
  updatedAt: schedule.updatedAt,
  nextRunAt: schedule.nextRunAt,
  runNumber: schedule.runNumber,
  failureCount: schedule.failureCount,
  ...(schedule.lastError ? { lastError: schedule.lastError } : {}),
  ...(schedule.metaJson !== undefined ? { meta: parseMeta(schedule.metaJson) } : {}),
});

// ==========================
// Factory
// ==========================

export const scheduler = (config: SchedulerConfig): Scheduler => {
  assertPositiveMs("leader.leaseMs", config.leader?.leaseMs);
  assertPositiveMs("leader.heartbeatMs", config.leader?.heartbeatMs);
  assertPositiveMs("dispatch.tickMs", config.dispatch?.tickMs);
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const leaseMs = normalizeMs(config.leader?.leaseMs, DEFAULT_LEASE_MS, MIN_LEASE_MS);
  const heartbeatMs = Math.min(
    normalizeMs(config.leader?.heartbeatMs, DEFAULT_HEARTBEAT_MS, MIN_HEARTBEAT_MS),
    Math.floor(leaseMs / 3),
  );
  const tickMs = normalizeMs(config.dispatch?.tickMs, DEFAULT_TICK_MS, 50);
  const batchSize = Math.max(
    1,
    Math.floor(Number.isFinite(config.dispatch?.batchSize) ? config.dispatch!.batchSize! : DEFAULT_BATCH_SIZE),
  );
  const controlHandlerTtlMs = Math.max(5_000, tickMs * 4);
  const controlHeartbeatMs = Math.max(500, Math.floor(controlHandlerTtlMs / 3));

  const scheduleKey = (id: string): string => schedulerScheduleKey(prefix, config.id, id);
  const v2ScheduleKey = (id: string): string => schedulerV2ScheduleKey(prefix, config.id, id);
  const legacyScheduleKey = (id: string): string => legacySchedulerScheduleKey(prefix, config.id, id);
  const tombstoneKey = (id: string): string => schedulerScheduleTombstoneKey(prefix, config.id, id);
  const dueKey = schedulerDueKey(prefix, config.id);
  const legacyDueKey = legacySchedulerDueKey(prefix, config.id);
  const indexKey = schedulerScheduleIndexKey(prefix, config.id);
  const legacyIndexKey = legacySchedulerScheduleIndexKey(prefix, config.id);
  const instanceId = crypto.randomUUID();
  const hasLegacyAccess = (): Promise<boolean> =>
    resolveLegacySchedulerAccess(prefix, config.id);
  const compatibilityKeys = async (scheduleId: string) => {
    const legacyAccess = await hasLegacyAccess();
    if (legacyAccess) {
      const legacyScheduleAccess = await hasCompatibleLegacySchedule(prefix, config.id, scheduleId);
      return {
        v2Schedule: v2ScheduleKey(scheduleId),
        legacySchedule: legacyScheduleAccess
          ? legacyScheduleKey(scheduleId)
          : `${scheduleKey(scheduleId)}:compat-disabled:legacy`,
        legacyDue: legacyDueKey,
        legacyIndex: legacyIndexKey,
      };
    }
    const base = `${scheduleKey(scheduleId)}:compat-disabled`;
    return {
      v2Schedule: `${base}:v2`,
      legacySchedule: `${base}:legacy`,
      legacyDue: `${dueKey}:compat-disabled`,
      legacyIndex: `${indexKey}:compat-disabled`,
    };
  };

  const leaderMutex = mutex({
    id: "scheduler",
    prefix: `${prefix}:v3:leader:${encodeSchedulerKeyPart(config.id)}`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });
  const legacyLeaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });
  const dispatchMutex = mutex({
    id: "scheduler",
    prefix: `${prefix}:v3:dispatch:${encodeSchedulerKeyPart(config.id)}`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });
  const legacyDispatchMutex = mutex({
    id: `${config.id}:dispatch`,
    prefix: `${prefix}:dispatch`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });

  const handlers = new Map<string, HandlerEntry>();
  const controlQueues = new Map<string, ReturnType<typeof schedulerControlQueue>>();
  const metrics: SchedulerMetrics = {
    isLeader: false,
    leaderChanges: 0,
    dispatches: 0,
    failures: 0,
    reschedules: 0,
    tickErrors: 0,
    staleWrites: 0,
    unservedSlots: 0,
    lastTickAt: null,
  };

  let running = false;
  let loopPromise: Promise<void> | null = null;
  let heartbeatPromise: Promise<void> | null = null;
  let controlHeartbeatPromise: Promise<void> | null = null;
  let controlRefreshPromise: Promise<void> | null = null;
  let stopPromise: Promise<void> | null = null;
  let restartRequested = false;
  let dispatchGeneration = 0;
  // Controllers of the callbacks currently running, so stop() can cancel them.
  const activeRuns = new Set<AbortController>();
  let currentLeaderLock: PairedLock | null = null;
  let lastHeartbeatAt = 0;
  let nextLeadershipAttemptAt = 0;

  const controlQueueForSchedule = (scheduleId: string): ReturnType<typeof schedulerControlQueue> => {
    const existing = controlQueues.get(scheduleId);
    if (existing) return existing;
    const created = schedulerControlQueue(prefix, config.id, scheduleId);
    controlQueues.set(scheduleId, created);
    return created;
  };

  const setLeader = (next: boolean): void => {
    if (metrics.isLeader === next) return;
    metrics.isLeader = next;
    metrics.leaderChanges += 1;
  };

  const tryAcquireLeadership = async (): Promise<void> => {
    if (currentLeaderLock) return;
    if (Date.now() < nextLeadershipAttemptAt) return;
    const current = await leaderMutex.acquire("active", leaseMs);
    if (!current) return;
    let legacy: Lock | null = null;
    if (await hasLegacyAccess()) {
      legacy = await legacyLeaderMutex.acquire("active", leaseMs);
      if (!legacy) {
        await leaderMutex.release(current);
        return;
      }
    }
    currentLeaderLock = { current, legacy };
    lastHeartbeatAt = Date.now();
    setLeader(true);
  };

  const maintainLeadership = async (): Promise<void> => {
    const lock = currentLeaderLock;
    if (!lock) return;
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt < heartbeatMs) return;
    lastHeartbeatAt = nowMs;
    const [currentOk, legacyOk] = await Promise.all([
      leaderMutex.extend(lock.current, leaseMs),
      lock.legacy ? legacyLeaderMutex.extend(lock.legacy, leaseMs) : true,
    ]);
    const ok = currentOk && legacyOk;
    if (!ok && currentLeaderLock === lock) {
      currentLeaderLock = null;
      setLeader(false);
      await Promise.allSettled([
        leaderMutex.release(lock.current),
        ...(lock.legacy ? [legacyLeaderMutex.release(lock.legacy)] : []),
      ]);
    }
  };

  const relinquishLeadership = async (): Promise<void> => {
    if (!currentLeaderLock) return;
    const lock = currentLeaderLock;
    currentLeaderLock = null;
    setLeader(false);
    try {
      await Promise.all([
        leaderMutex.release(lock.current),
        ...(lock.legacy ? [legacyLeaderMutex.release(lock.legacy)] : []),
      ]);
    } catch {
      // best effort
    }
  };

  const acquireDispatchLock = async (scheduleId: string): Promise<PairedLock | null> => {
    const current = await dispatchMutex.acquire(encodeSchedulerKeyPart(scheduleId), leaseMs);
    if (!current) return null;
    let legacy: Lock | null = null;
    if (await hasLegacyAccess()) {
      // The namespace owner keeps the old fence throughout a rolling upgrade.
      legacy = await legacyDispatchMutex.acquire(scheduleId, leaseMs);
      if (!legacy) {
        await dispatchMutex.release(current);
        return null;
      }
    }
    return { current, legacy };
  };

  const extendDispatchLock = async (lock: PairedLock): Promise<boolean> => {
    const [currentOk, legacyOk] = await Promise.all([
      dispatchMutex.extend(lock.current, leaseMs),
      lock.legacy ? legacyDispatchMutex.extend(lock.legacy, leaseMs) : true,
    ]);
    return currentOk && legacyOk;
  };

  const releaseDispatchLock = async (lock: PairedLock): Promise<void> => {
    await Promise.allSettled([
      dispatchMutex.release(lock.current),
      ...(lock.legacy ? [legacyDispatchMutex.release(lock.legacy)] : []),
    ]);
  };

  /**
   * @param expectedRunNumber the runNumber this run was derived from
   * @param requireLeadership cron dispatch must own the lease; a manual run,
   *   which legitimately happens on any pod holding the handler, must not
   * @returns false when a fence refused the write
   */
  const persist = async (
    schedule: StoredSchedule,
    expectedRunNumber: number,
    requireLeadership: boolean,
    dispatchLock: PairedLock,
  ): Promise<boolean> => {
    const compatibility = await compatibilityKeys(schedule.id);
    const currentLeaderToken = requireLeadership ? (currentLeaderLock?.current.value ?? null) : "";
    const legacyLeaderToken = requireLeadership ? (currentLeaderLock?.legacy?.value ?? null) : "";
    if (currentLeaderToken === null || legacyLeaderToken === null) return false;

    const result = await redis.send("EVAL", [
      PERSIST_SCRIPT,
      "12",
      scheduleKey(schedule.id),
      dueKey,
      currentLeaderLock?.current.resource ?? `${prefix}:v3:unused:leader`,
      dispatchLock.current.resource,
      compatibility.v2Schedule,
      compatibility.legacySchedule,
      compatibility.legacyDue,
      currentLeaderLock?.legacy?.resource ?? `${prefix}:legacy:unused:leader`,
      dispatchLock.legacy?.resource ?? `${prefix}:legacy:unused:dispatch`,
      tombstoneKey(schedule.id),
      indexKey,
      compatibility.legacyIndex,
      schedule.id,
      String(expectedRunNumber),
      currentLeaderToken,
      JSON.stringify({
        runNumber: schedule.runNumber,
        nextRunAt: schedule.nextRunAt,
        failureCount: schedule.failureCount,
        updatedAt: schedule.updatedAt,
        lastError: schedule.lastError ?? null,
      }),
      dispatchLock.current.value,
      schedule.revision,
      legacyLeaderToken,
      dispatchLock.legacy?.value ?? "",
    ]);
    const persisted = Number(result) > 0;
    if (!persisted) metrics.staleWrites += 1;
    return persisted;
  };

  // One dispatch per schedule at a time in this process, so a manual run and a
  // cron run of the same schedule cannot interleave their read-modify-writes.
  // One entry per schedule id, replaced rather than appended, so this is bounded
  // by the number of schedules this instance knows about.
  const dispatchChains = new Map<string, Promise<unknown>>();
  const serializeDispatch = (scheduleId: string, run: () => Promise<void>): Promise<void> => {
    const previous = dispatchChains.get(scheduleId) ?? Promise.resolve();
    const next = previous.then(run, run);
    const settled = next.catch(() => {});
    dispatchChains.set(scheduleId, settled);
    void settled.then(() => {
      if (dispatchChains.get(scheduleId) === settled) {
        dispatchChains.delete(scheduleId);
      }
    });
    return next;
  };

  // Run a single schedule: increment runNumber, invoke process + after, update state, persist.
  // `trigger` records what caused this run. It also controls cron advancement:
  // "cron" advances nextRunAt to the next cron slot when user does not reschedule;
  // "manual" leaves nextRunAt unchanged (regular cron continues unaffected by runNow).
  const dispatchOne = async (
    schedule: StoredSchedule,
    handler: HandlerEntry,
    trigger: "cron" | "manual",
    dispatchLock: PairedLock,
  ): Promise<void> => {
    const advanceCron = trigger === "cron";
    const slotTs = schedule.nextRunAt;
    // The value the terminal write is fenced against.
    const expectedRunNumber = schedule.runNumber;
    schedule.runNumber += 1;
    const runNumber = schedule.runNumber;
    const failureCountBefore = schedule.failureCount;
    const startedAt = Date.now();
    const jobAc = new AbortController();
    activeRuns.add(jobAc);
    let dispatchLeaseLost = false;
    let heartbeatTail = Promise.resolve();
    const dispatchHeartbeat = setInterval(() => {
      heartbeatTail = heartbeatTail.then(async () => {
        if (dispatchLeaseLost || jobAc.signal.aborted) return;
        try {
          if (await extendDispatchLock(dispatchLock)) return;
        } catch {
          // A transport failure makes ownership uncertain. Fail closed.
        }
        dispatchLeaseLost = true;
        jobAc.abort();
      });
    }, Math.max(50, Math.floor(heartbeatMs / 2)));

    try {
      const makeCtx = (): ScheduleCtx => {
        const ctx = {
          scheduleId: schedule.id,
          slotTs,
          runNumber,
          failureCount: failureCountBefore,
          trigger,
          signal: jobAc.signal,
        } as ScheduleCtx;
        Object.defineProperty(ctx, "duration", {
          get: () => Date.now() - startedAt,
          enumerable: true,
        });
        return ctx;
      };

      const ctx = makeCtx();

      await emitTrace(handler.trace, {
        type: "started",
        scheduleId: schedule.id,
        runNumber,
        trigger,
        slotTs,
      });

      let result: unknown;
      let error: Error | undefined;
      try {
        result = await Promise.resolve(handler.process({ ctx }));
      } catch (err) {
        error = asError(err);
      }

      if (dispatchLeaseLost) {
        throw new Error(`scheduler dispatch lease lost for ${schedule.id}`);
      }
      if (jobAc.signal.aborted) {
        throw new Error(`scheduler dispatch stopped for ${schedule.id}`);
      }

      if (error) {
        await emitTrace(handler.trace, {
          type: "failed",
          scheduleId: schedule.id,
          runNumber,
          error,
          durationMs: Date.now() - startedAt,
        });
      } else {
        await emitTrace(handler.trace, {
          type: "succeeded",
          scheduleId: schedule.id,
          runNumber,
          data: result,
          durationMs: Date.now() - startedAt,
        });
      }

      let rescheduleRequested: { delayMs?: number } | null = null;
      const afterCtx: ScheduleAfterCtx<unknown> = Object.create(ctx) as ScheduleAfterCtx<unknown>;
      if (error) afterCtx.error = error;
      if (!error) afterCtx.data = result;
      afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
        assertNonNegativeMs("reschedule.delayMs", rcfg?.delayMs);
        rescheduleRequested = { delayMs: rcfg?.delayMs };
      };
      afterCtx.expBackoff = (bcfg?: BackoffOptions): number => expBackoff(failureCountBefore + 1, bcfg);
      afterCtx.metric = metrics;

      if (handler.after) {
        try {
          await Promise.resolve(handler.after({ ctx: afterCtx }));
        } catch {
          // after errors swallowed
        }
      }

      if (dispatchLeaseLost) {
        throw new Error(`scheduler dispatch lease lost for ${schedule.id}`);
      }
      if (jobAc.signal.aborted) {
        throw new Error(`scheduler dispatch stopped for ${schedule.id}`);
      }

      if (error) {
        schedule.failureCount += 1;
        schedule.lastError = error.message;
      } else {
        schedule.failureCount = 0;
        delete schedule.lastError;
      }

      let traceRescheduled: { delayMs: number } | null = null;
      if (rescheduleRequested) {
        const delayMs = (rescheduleRequested as { delayMs?: number }).delayMs ?? 0;
        schedule.nextRunAt = Date.now() + delayMs;
        traceRescheduled = { delayMs };
      } else if (advanceCron) {
        schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, Date.now());
      }
      // else: nextRunAt unchanged (runNow without reschedule — cron schedule continues as before)

      schedule.updatedAt = Date.now();

      const persisted = await persist(schedule, expectedRunNumber, advanceCron, dispatchLock);
      if (!persisted) {
        throw new Error(`scheduler dispatch state changed for ${schedule.id}`);
      }

      if (error) metrics.failures += 1;
      else metrics.dispatches += 1;
      if (traceRescheduled) metrics.reschedules += 1;

      if (traceRescheduled) {
        await emitTrace(handler.trace, {
          type: "rescheduled",
          scheduleId: schedule.id,
          runNumber,
          delayMs: traceRescheduled.delayMs,
        });
      }
    } finally {
      clearInterval(dispatchHeartbeat);
      await heartbeatTail;
      jobAc.abort();
      activeRuns.delete(jobAc);
    }
  };

  const dispatchSchedule = async (
    scheduleId: string,
    trigger: "cron" | "manual",
    options?: {
      onAcquired?: () => Promise<void> | void;
      shouldContinue?: () => boolean;
      generation?: number;
    },
  ): Promise<void> => {
    const generation = options?.generation ?? dispatchGeneration;
    await serializeDispatch(scheduleId, async () => {
      let dispatchLock = await acquireDispatchLock(scheduleId);
      if (trigger === "cron" && !dispatchLock) return;
      while (!dispatchLock) {
        if (generation !== dispatchGeneration || (options?.shouldContinue && !options.shouldContinue())) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
        }
        await sleep(Math.max(25, Math.min(100, Math.floor(heartbeatMs / 2))));
        dispatchLock = await acquireDispatchLock(scheduleId);
      }

      let preparationLeaseLost = false;
      let preparationHeartbeatTail = Promise.resolve();
      const preparationHeartbeat = setInterval(() => {
        preparationHeartbeatTail = preparationHeartbeatTail.then(async () => {
          if (preparationLeaseLost) return;
          try {
            if (await extendDispatchLock(dispatchLock)) return;
          } catch {
            // Ownership is uncertain after a transport error.
          }
          preparationLeaseLost = true;
        });
      }, Math.max(50, Math.floor(heartbeatMs / 2)));

      try {
        const schedule = parseSchedule(await readSchedulerScheduleRaw(prefix, config.id, scheduleId));
        if (!schedule) throw new Error(`runNow: schedule ${scheduleId} not found`);
        const handler = handlers.get(scheduleId);
        if (!handler) throw new Error(`runNow: no current handler registered for schedule ${scheduleId} on this pod`);
        if (trigger === "cron" && (!currentLeaderLock || schedule.nextRunAt > Date.now())) return;
        if (
          generation !== dispatchGeneration
          || (options?.shouldContinue && !options.shouldContinue())
        ) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
        }
        if (preparationLeaseLost || !(await extendDispatchLock(dispatchLock))) {
          throw new Error(`scheduler dispatch lease lost for ${scheduleId}`);
        }
        await options?.onAcquired?.();
        clearInterval(preparationHeartbeat);
        await preparationHeartbeatTail;
        if (preparationLeaseLost || !(await extendDispatchLock(dispatchLock))) {
          throw new Error(`scheduler dispatch lease lost for ${scheduleId}`);
        }
        if (
          generation !== dispatchGeneration
          || (options?.shouldContinue && !options.shouldContinue())
        ) {
          throw new Error(`scheduler dispatch stopped for ${scheduleId}`);
        }
        await dispatchOne(schedule, handler, trigger, dispatchLock);
      } finally {
        clearInterval(preparationHeartbeat);
        await preparationHeartbeatTail;
        await releaseDispatchLock(dispatchLock);
      }
    });
  };

  const dispatchDue = async (generation: number): Promise<void> => {
    const nowMs = Date.now();
    const dueArgs = ["-inf", String(nowMs), "LIMIT", "0", String(batchSize)];
    const legacyAccess = await hasLegacyAccess();
    const dueIdsRaw = await redis.send("ZRANGEBYSCORE", [dueKey, ...dueArgs]);
    const legacyDueTypeRaw = legacyAccess ? await redis.send("TYPE", [legacyDueKey]) : "none";
    const legacyDueType = typeof legacyDueTypeRaw === "string"
      ? legacyDueTypeRaw
      : (legacyDueTypeRaw as { ok?: unknown } | null)?.ok;
    const legacyDueIdsRaw = legacyAccess && legacyDueType === "zset"
      ? await redis.send("ZRANGEBYSCORE", [legacyDueKey, ...dueArgs])
      : [];
    if (!running || generation !== dispatchGeneration) return;
    const dueIds = Array.from(
      new Set(
        [
          ...(Array.isArray(dueIdsRaw) ? dueIdsRaw : []),
          ...(Array.isArray(legacyDueIdsRaw) ? legacyDueIdsRaw : []),
        ].map((value) => String(value)),
      ),
    ).slice(0, batchSize);
    if (dueIds.length === 0) return;

    for (const scheduleId of dueIds) {
      if (!currentLeaderLock) break;

      const raw = await readSchedulerScheduleRaw(prefix, config.id, scheduleId);
      if (!running || generation !== dispatchGeneration) return;
      const schedule = parseSchedule(raw);
      if (!schedule) {
        // Broken record: clean up
        const compatibility = await compatibilityKeys(scheduleId);
        await redis.send("EVAL", [
          DELETE_SCRIPT,
          "8",
          scheduleKey(scheduleId),
          compatibility.v2Schedule,
          compatibility.legacySchedule,
          tombstoneKey(scheduleId),
          dueKey,
          compatibility.legacyDue,
          indexKey,
          compatibility.legacyIndex,
          scheduleId,
          String(Date.now()),
        ]);
        continue;
      }

      if (schedule.nextRunAt > nowMs) continue;

      const handler = handlers.get(scheduleId);
      if (!handler) {
        // Do not advance a slot this pod cannot serve. Advancing made leadership
        // a black hole: leases are sticky, so the same handler-less leader kept
        // winning and skipping slot after slot while `get()`/`list()` showed a
        // healthy schedule with a frozen runNumber. Step down instead, so a pod
        // that did register the handler can take over and actually run it. A
        // local cooldown prevents this same tick loop from immediately winning
        // the released lock again and starving the capable pod.
        metrics.unservedSlots += 1;
        nextLeadershipAttemptAt = Date.now() + leaseMs;
        await relinquishLeadership();
        return;
      }

      await dispatchSchedule(scheduleId, "cron", {
        generation,
        shouldContinue: () => running && generation === dispatchGeneration,
      });
    }
  };

  const refreshControlHandlers = async (): Promise<void> => {
    const results = await Promise.allSettled(
      Array.from(handlers.keys()).map((scheduleId) =>
        refreshSchedulerControlHandler({
          prefix,
          schedulerId: config.id,
          scheduleId,
          instanceId,
          ttlMs: controlHandlerTtlMs,
        }),
      ),
    );
    const failed = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
    if (failed) throw failed.reason;
  };

  const removeControlHandlers = async (): Promise<void> => {
    await Promise.all(
      Array.from(handlers.keys()).map((scheduleId) =>
        removeSchedulerControlHandler({
          prefix,
          schedulerId: config.id,
          scheduleId,
          instanceId,
        }),
      ),
    );
  };

  const dispatchControlRequests = async (generation: number): Promise<void> => {
    let dispatched = 0;
    const controlLeaseMs = 30_000;
    for (const scheduleId of handlers.keys()) {
      if (!running || generation !== dispatchGeneration) return;
      if (dispatched >= batchSize) break;
      const message = await controlQueueForSchedule(scheduleId).recv({
        wait: false,
        leaseMs: controlLeaseMs,
        consumerId: instanceId,
      });
      if (!message) continue;

      const request: SchedulerControlRequest = message.data;
      if (request.schedulerId !== config.id || request.scheduleId !== scheduleId) {
        await markSchedulerControlUnavailable(
          prefix,
          request,
          `schedulerControl.runNow: request target does not match ${config.id}/${scheduleId}`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }
      if (!(await refreshSchedulerControlRequestBinding(prefix, request))) {
        await message.ack();
        dispatched += 1;
        continue;
      }
      const pending = await markSchedulerControlPending(prefix, request);
      // A first-delivery message for an already accepted request is a duplicate
      // enqueue. Higher attempts are the original message's transport retry.
      if (
        pending === "terminal"
        || pending === null
        || (pending === "accepted" && message.attempt === 1)
      ) {
        await message.ack();
        dispatched += 1;
        continue;
      }
      const raw = await readSchedulerScheduleRaw(prefix, config.id, scheduleId);
      const schedule = parseSchedule(raw);
      if (!schedule) {
        await markSchedulerControlNotFound(
          prefix,
          request,
          `schedulerControl.runNow: schedule ${request.schedulerId}/${request.scheduleId} not found`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      if (!handlers.has(scheduleId)) {
        await markSchedulerControlUnavailable(
          prefix,
          request,
          `schedulerControl.runNow: no live handler for schedule ${request.schedulerId}/${request.scheduleId}`,
        );
        await message.ack();
        dispatched += 1;
        continue;
      }

      let controlHeartbeat: Promise<void> | null = null;
      const touchTimer = setInterval(() => {
        if (controlHeartbeat) return;

        controlHeartbeat = (async () => {
          const [touch, binding] = await Promise.allSettled([
            message.touch({ leaseMs: controlLeaseMs }),
            refreshSchedulerControlRequestBinding(prefix, request),
          ]);
          if (
            touch.status === "rejected"
            || (touch.status === "fulfilled" && !touch.value)
            || binding.status === "rejected"
          ) {
            metrics.tickErrors += 1;
          }
        })()
          .catch(() => {
            metrics.tickErrors += 1;
          })
          .finally(() => {
            controlHeartbeat = null;
          });
      }, Math.floor(controlLeaseMs / 3));
      try {
        await dispatchSchedule(scheduleId, "manual", {
          onAcquired: async () => {
            if (!(await markSchedulerControlAccepted(prefix, request))) {
              throw new Error(`schedulerControl.runNow: stale request ${request.requestId}`);
            }
          },
          shouldContinue: () => running && generation === dispatchGeneration,
          generation,
        });
        await message.ack();
      } catch (error) {
        // The control queue has effectively unlimited deliveries, so an
        // unconditional nack replayed the user's callback every 250ms for the
        // whole message-age window. Give up after a bounded number of attempts
        // and report it, rather than storming.
        if (message.attempt >= CONTROL_MAX_ATTEMPTS) {
          await markSchedulerControlUnavailable(
            prefix,
            request,
            `schedulerControl.runNow: dispatch failed after ${message.attempt} attempts: ${asError(error).message}`,
          );
          await message.ack();
        } else {
          await message.nack({ delayMs: 250, reason: "control-dispatch-error", error: asError(error).message });
        }
      } finally {
        clearInterval(touchTimer);
      }
      dispatched += 1;
    }
  };

  const loop = async (generation: number): Promise<void> => {
    while (running && generation === dispatchGeneration) {
      try {
        await dispatchControlRequests(generation);
        if (!running || generation !== dispatchGeneration) break;
        await tryAcquireLeadership();
        if (!running || generation !== dispatchGeneration) {
          await relinquishLeadership();
          break;
        }
        if (currentLeaderLock) {
          await dispatchDue(generation);
        }
        metrics.lastTickAt = Date.now();
      } catch {
        metrics.tickErrors += 1;
      }
      await sleep(tickMs);
    }
  };

  // Handler liveness must not depend on the dispatch loop: that loop awaits
  // user callbacks, which may legitimately run longer than the presence TTL.
  const controlHeartbeatLoop = async (generation: number): Promise<void> => {
    while (running && generation === dispatchGeneration) {
      try {
        controlRefreshPromise = refreshControlHandlers();
        await controlRefreshPromise;
      } catch {
        metrics.tickErrors += 1;
      } finally {
        controlRefreshPromise = null;
      }
      await sleep(controlHeartbeatMs);
    }
  };

  // The lease heartbeat runs on its own loop, not on the tick loop. On the tick
  // loop it could not run at all while a dispatch was in flight, so any handler
  // that outlived leaseMs — an ordinary batch job — silently dropped the lease,
  // let a second pod take over and dispatch the same slot, and then wrote its
  // own stale state over the new leader's.
  const heartbeatLoop = async (generation: number): Promise<void> => {
    while (running && generation === dispatchGeneration) {
      try {
        await maintainLeadership();
      } catch {
        metrics.tickErrors += 1;
      }
      await sleep(Math.max(50, Math.floor(heartbeatMs / 2)));
    }
  };

  // ==========================
  // Public API
  // ==========================

  const create = async <R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }> => {
    const tz = cfg.tz ?? "UTC";
    assertValidTimeZone(tz);
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, Date.now());
    const compatibility = await compatibilityKeys(cfg.id);

    const incoming: StoredSchedule = {
      id: cfg.id,
      revision: crypto.randomUUID(),
      cron: cfg.cron,
      tz,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      nextRunAt: firstRunAt,
      runNumber: 0,
      failureCount: 0,
      ...(cfg.meta === undefined ? {} : { metaJson: JSON.stringify(cfg.meta) }),
    };

    const resultRaw = await redis.send("EVAL", [
      UPSERT_SCRIPT,
      "10",
      scheduleKey(cfg.id),
      compatibility.v2Schedule,
      compatibility.legacySchedule,
      tombstoneKey(cfg.id),
      dueKey,
      compatibility.legacyDue,
      indexKey,
      compatibility.legacyIndex,
      `${prefix}:index`,
      schedulerRegistrationKey(prefix, config.id),
      JSON.stringify(incoming),
      String(firstRunAt),
      cfg.id,
      String(Date.now()),
      config.id,
    ]);
    const resultTuple = Array.isArray(resultRaw) ? resultRaw : [resultRaw, firstRunAt];
    const result = Number(resultTuple[0]);
    const storedNextRunAt = Number(resultTuple[1] ?? firstRunAt);

    handlers.set(cfg.id, {
      process: cfg.process as HandlerEntry["process"],
      after: cfg.after as HandlerEntry["after"],
      trace: cfg.trace as HandlerEntry["trace"],
    });
    if (running) {
      await refreshSchedulerControlHandler({
        prefix,
        schedulerId: config.id,
        scheduleId: cfg.id,
        instanceId,
        ttlMs: controlHandlerTtlMs,
      });
    }

    await emitTrace(cfg.trace, {
      type: "scheduled",
      scheduleId: cfg.id,
      cron: cfg.cron,
      tz,
      nextRunAt: storedNextRunAt,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
    });

    return {
      created: result === 1,
      updated: result === 2,
    };
  };

  const deleteSchedule = async (cfg: { id: string }): Promise<void> => {
    const compatibility = await compatibilityKeys(cfg.id);
    await redis.send("EVAL", [
      DELETE_SCRIPT,
      "8",
      scheduleKey(cfg.id),
      compatibility.v2Schedule,
      compatibility.legacySchedule,
      tombstoneKey(cfg.id),
      dueKey,
      compatibility.legacyDue,
      indexKey,
      compatibility.legacyIndex,
      cfg.id,
      String(Date.now()),
    ]);
    handlers.delete(cfg.id);
    // A refresh may have captured this handler immediately before deletion.
    // Wait it out, then make the removal the final Redis write.
    await controlRefreshPromise?.catch(() => {});
    await removeSchedulerControlHandler({ prefix, schedulerId: config.id, scheduleId: cfg.id, instanceId });
  };

  const runNow = async (cfg: { id: string }): Promise<void> => {
    const raw = await readSchedulerScheduleRaw(prefix, config.id, cfg.id);
    const schedule = parseSchedule(raw);
    if (!schedule) throw new Error(`runNow: schedule ${cfg.id} not found`);
    if (!handlers.has(cfg.id)) throw new Error(`runNow: no handler registered for schedule ${cfg.id} on this pod`);

    // `runNow` does not advance cron — regular schedule continues as before,
    // unless user explicitly calls ctx.reschedule in after. Serialising it
    // against tick dispatch is what keeps that promise: an unsynchronised
    // read-modify-write let a manual run persist a stale snapshot and rewind
    // nextRunAt, making the slot immediately due again.
    await dispatchSchedule(cfg.id, "manual");
  };

  const get = async (cfg: { id: string }): Promise<SchedulerInfo | null> => {
    const raw = await readSchedulerScheduleRaw(prefix, config.id, cfg.id);
    const parsed = parseSchedule(raw);
    return parsed ? asInfo(parsed) : null;
  };

  const list = async (): Promise<SchedulerInfo[]> => {
    const legacyAccess = await hasLegacyAccess();
    const idsRaw = await redis.send("SMEMBERS", [indexKey]);
    const legacyIndexTypeRaw = legacyAccess ? await redis.send("TYPE", [legacyIndexKey]) : "none";
    const legacyIndexType = typeof legacyIndexTypeRaw === "string"
      ? legacyIndexTypeRaw
      : (legacyIndexTypeRaw as { ok?: unknown } | null)?.ok;
    const legacyIdsRaw = legacyAccess && legacyIndexType === "set"
      ? await redis.send("SMEMBERS", [legacyIndexKey])
      : [];
    const ids = Array.from(
      new Set(
        [
          ...(Array.isArray(idsRaw) ? idsRaw : []),
          ...(Array.isArray(legacyIdsRaw) ? legacyIdsRaw : []),
        ].map((value) => String(value)),
      ),
    );
    if (ids.length === 0) return [];
    const values = await Promise.all(ids.map((id) => readSchedulerScheduleRaw(prefix, config.id, id)));

    const out: SchedulerInfo[] = [];
    for (const raw of values) {
      const parsed = parseSchedule(typeof raw === "string" ? raw : null);
      if (parsed) out.push(asInfo(parsed));
    }
    out.sort((a, b) => a.id.localeCompare(b.id));
    return out;
  };

  const metric = (): SchedulerMetrics => ({ ...metrics });

  const launch = (): void => {
    running = true;
    const generation = dispatchGeneration;
    loopPromise = loop(generation);
    heartbeatPromise = heartbeatLoop(generation);
    controlHeartbeatPromise = controlHeartbeatLoop(generation);
  };

  const start = (): void => {
    if (running) return;
    if (stopPromise) {
      restartRequested = true;
      return;
    }
    launch();
  };

  const stop = (): Promise<void> => {
    restartRequested = false;
    if (stopPromise) return stopPromise;

    const wasRunning = running;
    running = false;
    dispatchGeneration += 1;
    const pendingDispatches = Array.from(dispatchChains.values());
    // Cancel in-flight callbacks so `ctx.signal.aborted` is a usable
    // cancellation signal instead of something that is always false.
    for (const ac of activeRuns) ac.abort();

    const cleanup = (async (): Promise<void> => {
      if (wasRunning) {
        await Promise.all([loopPromise, heartbeatPromise, controlHeartbeatPromise]);
      }
      await Promise.all(pendingDispatches);
      loopPromise = null;
      heartbeatPromise = null;
      controlHeartbeatPromise = null;
      await removeControlHandlers();
      await relinquishLeadership();
    })();
    stopPromise = cleanup;

    const finish = (): void => {
      if (stopPromise !== cleanup) return;
      stopPromise = null;
      if (!restartRequested) return;
      restartRequested = false;
      launch();
    };
    void cleanup.then(finish, () => {
      if (stopPromise !== cleanup) return;
      stopPromise = null;
      restartRequested = false;
    });
    return cleanup;
  };

  return {
    id: config.id,
    start,
    stop,
    create,
    delete: deleteSchedule,
    runNow,
    get,
    list,
    metric,
  };
};
