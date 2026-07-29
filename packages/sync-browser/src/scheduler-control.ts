import type { SchedulerInfo } from "./scheduler";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_TIMEOUT_MS = 10_000;
const REQUEST_TTL_MS = 5 * 60_000;

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

type BrowserScheduleRegistration = {
  prefix: string;
  instanceId: string;
  schedulerId: string;
  scheduleId: string;
  getInfo: () => SchedulerInfo | null;
  /** Resolves `onAccepted` once the run has been accepted, before it completes. */
  runNow: (onAccepted?: () => void) => Promise<void>;
  available: boolean;
};

const registrations = new Map<string, Map<string, BrowserScheduleRegistration>>();
const requests = new Map<
  string,
  { target: string; accepted: Promise<void>; expiresAt: number; retentionMs: number }
>();
let requestPruneTimer: ReturnType<typeof setTimeout> | undefined;

const groupKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  JSON.stringify([prefix, schedulerId, scheduleId]);

const assertId = (name: string, value: string): void => {
  if (!value) throw new Error(`${name} is required`);
};

const assertPositiveMs = (name: string, value: number | undefined): void => {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
    throw new Error(`${name} must be a finite number greater than 0`);
  }
};

const requestKey = (prefix: string, requestId: string): string => JSON.stringify([prefix, requestId]);

const pruneRequests = (now: number): void => {
  for (const [key, request] of requests) {
    if (request.expiresAt <= now) requests.delete(key);
  }
};

const scheduleRequestPrune = (): void => {
  clearTimeout(requestPruneTimer);
  requestPruneTimer = undefined;
  if (requests.size === 0) return;

  let nextExpiry = Number.POSITIVE_INFINITY;
  for (const request of requests.values()) {
    nextExpiry = Math.min(nextExpiry, request.expiresAt);
  }
  if (!Number.isFinite(nextExpiry)) return;
  requestPruneTimer = setTimeout(() => {
    requestPruneTimer = undefined;
    pruneRequests(Date.now());
    scheduleRequestPrune();
  }, Math.max(1, nextExpiry - Date.now()));
  (requestPruneTimer as unknown as { unref?: () => void }).unref?.();
};

const waitForAcceptance = async (
  accepted: Promise<void>,
  timeoutMs: number,
  schedulerId: string,
  scheduleId: string,
): Promise<void> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      accepted,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new SchedulerControlTimeoutError(
              `schedulerControl.runNow: timed out waiting for live handler to accept ${schedulerId}/${scheduleId}`,
            ),
          );
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
};

export const registerBrowserSchedulerControl = (cfg: {
  prefix: string;
  schedulerId: string;
  scheduleId: string;
  instanceId: string;
  getInfo: () => SchedulerInfo | null;
  /** Resolves `onAccepted` once the run has been accepted, before it completes. */
  runNow: (onAccepted?: () => void) => Promise<void>;
}): void => {
  const key = groupKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId);
  const group = registrations.get(key) ?? new Map<string, BrowserScheduleRegistration>();
  group.set(cfg.instanceId, {
    prefix: cfg.prefix,
    instanceId: cfg.instanceId,
    schedulerId: cfg.schedulerId,
    scheduleId: cfg.scheduleId,
    getInfo: cfg.getInfo,
    runNow: cfg.runNow,
    available: false,
  });
  registrations.set(key, group);
};

export const setBrowserSchedulerControlAvailable = (cfg: {
  prefix: string;
  schedulerId: string;
  instanceId: string;
  available: boolean;
}): void => {
  for (const group of registrations.values()) {
    const first = group.values().next().value as BrowserScheduleRegistration | undefined;
    if (!first || first.prefix !== cfg.prefix || first.schedulerId !== cfg.schedulerId) continue;
    const registration = group.get(cfg.instanceId);
    if (registration) registration.available = cfg.available;
  }
};

export const unregisterBrowserSchedulerControl = (cfg: {
  prefix: string;
  schedulerId: string;
  scheduleId: string;
  instanceId: string;
}): void => {
  const key = groupKey(cfg.prefix, cfg.schedulerId, cfg.scheduleId);
  const group = registrations.get(key);
  if (!group) return;
  group.delete(cfg.instanceId);
  if (group.size === 0) registrations.delete(key);
};

export const schedulerControl = (config: SchedulerControlConfig = {}): SchedulerControl => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  assertPositiveMs("timeoutMs", config.timeoutMs);
  const defaultTimeoutMs = Math.max(1, config.timeoutMs ?? DEFAULT_TIMEOUT_MS);

  const list = async (): Promise<SchedulerControlInfo[]> => {
    const out: SchedulerControlInfo[] = [];
    for (const group of registrations.values()) {
      const first = Array.from(group.values())[0];
      if (!first || first.prefix !== prefix) continue;
      const info = first.getInfo();
      if (!info) continue;
      const available = Array.from(group.values()).some((registration) => registration.available);
      out.push({
        schedulerId: first.schedulerId,
        scheduleId: first.scheduleId,
        cron: info.cron,
        tz: info.tz,
        createdAt: info.createdAt,
        updatedAt: info.updatedAt,
        nextRunAt: info.nextRunAt,
        runNumber: info.runNumber,
        failureCount: info.failureCount,
        state: available ? "available" : "unavailable",
        ...(info.lastError ? { lastError: info.lastError } : {}),
        ...(info.meta ? { meta: info.meta } : {}),
      });
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
    const key = groupKey(prefix, cfg.schedulerId, cfg.scheduleId);

    const now = Date.now();
    pruneRequests(now);
    const idempotencyKey = requestKey(prefix, requestId);
    let request = requests.get(idempotencyKey);
    if (request && request.target !== key) {
      throw new Error(`schedulerControl.runNow: requestId ${requestId} is already bound to another schedule`);
    }
    if (request) {
      request.retentionMs = Math.max(request.retentionMs, REQUEST_TTL_MS, timeoutMs * 2);
      if (Number.isFinite(request.expiresAt)) {
        request.expiresAt = now + request.retentionMs;
      }
      scheduleRequestPrune();
      await waitForAcceptance(request.accepted, timeoutMs, cfg.schedulerId, cfg.scheduleId);
      return;
    }

    const group = registrations.get(key);
    if (!group || group.size === 0) {
      throw new SchedulerControlNotFoundError(
        `schedulerControl.runNow: schedule ${cfg.schedulerId}/${cfg.scheduleId} not found`,
      );
    }

    const registration = Array.from(group.values()).find((entry) => entry.available);
    if (!registration) {
      throw new SchedulerControlUnavailableError(
        `schedulerControl.runNow: no live handler for schedule ${cfg.schedulerId}/${cfg.scheduleId}`,
      );
    }

    let settleAccepted: () => void = () => {};
    let failAccepted: (error: unknown) => void = () => {};
    let acceptanceSettled = false;
    const accepted = new Promise<void>((resolve, reject) => {
      settleAccepted = () => {
        if (acceptanceSettled) return;
        acceptanceSettled = true;
        resolve();
      };
      failAccepted = (error) => {
        if (acceptanceSettled) return;
        acceptanceSettled = true;
        reject(error);
      };
    });
    const retentionMs = Math.max(REQUEST_TTL_MS, timeoutMs * 2);
    request = {
      target: key,
      accepted,
      expiresAt: Number.POSITIVE_INFINITY,
      retentionMs,
    };
    requests.set(idempotencyKey, request);
    scheduleRequestPrune();

    const finishRequest = (): void => {
      if (requests.get(idempotencyKey) !== request) return;
      request.expiresAt = Date.now() + request.retentionMs;
      scheduleRequestPrune();
    };
    try {
      const run = registration.runNow(settleAccepted);
      // A failure before acceptance is the caller's to see; one after
      // acceptance belongs to the scheduler run, not this control request.
      void run
        .catch((error: unknown) => {
          failAccepted(error);
        })
        .finally(finishRequest);
    } catch (error) {
      failAccepted(error);
      finishRequest();
    }

    await waitForAcceptance(accepted, timeoutMs, cfg.schedulerId, cfg.scheduleId);
  };

  return { list, runNow };
};
