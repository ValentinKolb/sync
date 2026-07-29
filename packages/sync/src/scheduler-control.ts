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
  | { status: "accepted"; acceptedAt: number }
  | { status: "not_found"; error: string }
  | { status: "unavailable"; error: string };

const schedulerIndexKey = (prefix: string): string => `${prefix}:index`;
const scheduleIndexKey = (prefix: string, schedulerId: string): string => `${prefix}:${schedulerId}:index`;
const scheduleKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${schedulerId}:schedule:${scheduleId}`;
const handlerIndexKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${schedulerId}:control:${scheduleId}:handlers`;
const handlerInstanceKey = (prefix: string, schedulerId: string, scheduleId: string, instanceId: string): string =>
  `${prefix}:${schedulerId}:control:${scheduleId}:handler:${instanceId}`;
const responseKey = (prefix: string, requestId: string): string => `${prefix}:control:response:${requestId}`;
const requestKey = (prefix: string, requestId: string): string => `${prefix}:control:request:${requestId}`;

const assertId = (name: string, value: string): void => {
  if (!value) throw new Error(`${name} is required`);
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
    if (parsed.status === "accepted" || parsed.status === "not_found" || parsed.status === "unavailable") {
      return parsed;
    }
  } catch {
    return null;
  }
  return null;
};

const writeResponse = async (
  prefix: string,
  requestId: string,
  response: SchedulerControlResponse,
  ttlMs = DEFAULT_RESPONSE_TTL_MS,
): Promise<void> => {
  await redis.send("SET", [responseKey(prefix, requestId), JSON.stringify(response), "PX", String(ttlMs)]);
};

const bindRequest = async (
  prefix: string,
  requestId: string,
  schedulerId: string,
  scheduleId: string,
  ttlMs: number,
): Promise<void> => {
  const target = JSON.stringify([schedulerId, scheduleId]);
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
    id: `${schedulerId}:${scheduleId}:manual`,
    prefix: `${prefix}:control`,
    delivery: { defaultLeaseMs: 30_000, maxDeliveries: Number.MAX_SAFE_INTEGER },
    limits: { maxMessageAgeMs: 5 * 60_000, dlqRetentionMs: 60_000 },
  });

export const markSchedulerControlAccepted = async (
  prefix: string,
  requestId: string,
): Promise<void> => {
  await writeResponse(prefix, requestId, { status: "accepted", acceptedAt: Date.now() });
};

export const markSchedulerControlNotFound = async (
  prefix: string,
  requestId: string,
  error: string,
): Promise<void> => {
  await writeResponse(prefix, requestId, { status: "not_found", error });
};

export const markSchedulerControlUnavailable = async (
  prefix: string,
  requestId: string,
  error: string,
): Promise<void> => {
  await writeResponse(prefix, requestId, { status: "unavailable", error });
};

export const schedulerControl = (config: SchedulerControlConfig = {}): SchedulerControl => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
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
      const values = await redis.send("MGET", scheduleIds.map((scheduleId) => scheduleKey(prefix, schedulerId, scheduleId)));
      if (!Array.isArray(values)) continue;

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

    const schedule = parseSchedule(await redis.get(scheduleKey(prefix, cfg.schedulerId, cfg.scheduleId)));
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
