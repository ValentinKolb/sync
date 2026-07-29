import { redis, sleep } from "bun";
import { queue, type Queue } from "./queue";

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

const schedulerIndexKey = (prefix: string): string => `${prefix}:index`;
const scheduleIndexKey = (prefix: string, schedulerId: string): string => `${prefix}:${schedulerId}:index`;
export const encodeSchedulerKeyPart = (value: string): string =>
  value.replaceAll("%", "%25").replaceAll(":", "%3A");
export const schedulerScheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:v2:${encodeSchedulerKeyPart(schedulerId)}:schedule:${scheduleId}`;
const legacySchedulerScheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${schedulerId}:schedule:${scheduleId}`;
const handlerIndexKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${encodeSchedulerKeyPart(schedulerId)}:control:${encodeSchedulerKeyPart(scheduleId)}:handlers`;
const handlerInstanceKey = (prefix: string, schedulerId: string, scheduleId: string, instanceId: string): string =>
  `${prefix}:${encodeSchedulerKeyPart(schedulerId)}:control:${encodeSchedulerKeyPart(scheduleId)}:handler:${instanceId}`;
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
  const key = schedulerScheduleKey(prefix, schedulerId, scheduleId);
  const current = await redis.get(key);
  if (current) {
    try {
      const parsed = JSON.parse(current) as { id?: unknown };
      return parsed.id === scheduleId ? current : null;
    } catch {
      return current;
    }
  }
  if (key === legacySchedulerScheduleKey(prefix, schedulerId, scheduleId)) return null;

  const legacy = await redis.get(legacySchedulerScheduleKey(prefix, schedulerId, scheduleId));
  if (!legacy) return null;
  try {
    const parsed = JSON.parse(legacy) as { id?: unknown };
    if (parsed.id !== scheduleId) return null;
  } catch {
    return null;
  }

  await redis.send("SET", [key, legacy, "NX"]);
  return await redis.get(key);
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
      const scheduleIdsRaw = await redis.send("SMEMBERS", [scheduleIndexKey(prefix, schedulerId)]);
      if (!Array.isArray(scheduleIdsRaw) || scheduleIdsRaw.length === 0) continue;

      const scheduleIds = scheduleIdsRaw.map((value) => String(value));
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
