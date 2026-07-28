import type { SchedulerInfo } from "./scheduler";

const DEFAULT_PREFIX = "sync:scheduler";
const DEFAULT_TIMEOUT_MS = 10_000;

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
  instanceId: string;
  schedulerId: string;
  scheduleId: string;
  getInfo: () => SchedulerInfo | null;
  /** Resolves `onAccepted` once the run has been accepted, before it completes. */
  runNow: (onAccepted?: () => void) => Promise<void>;
  available: boolean;
};

const registrations = new Map<string, Map<string, BrowserScheduleRegistration>>();

const groupKey = (prefix: string, schedulerId: string, scheduleId: string): string =>
  `${prefix}:${schedulerId}:${scheduleId}`;

const assertId = (name: string, value: string): void => {
  if (!value) throw new Error(`${name} is required`);
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
  const prefix = `${cfg.prefix}:${cfg.schedulerId}:`;
  for (const [key, group] of registrations) {
    if (!key.startsWith(prefix)) continue;
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
  const defaultTimeoutMs = Math.max(1, config.timeoutMs ?? DEFAULT_TIMEOUT_MS);

  const list = async (): Promise<SchedulerControlInfo[]> => {
    const out: SchedulerControlInfo[] = [];
    for (const [key, group] of registrations) {
      if (!key.startsWith(`${prefix}:`)) continue;
      const first = Array.from(group.values())[0];
      if (!first) continue;
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
    const timeoutMs = Math.max(1, cfg.timeoutMs ?? defaultTimeoutMs);
    const key = groupKey(prefix, cfg.schedulerId, cfg.scheduleId);
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

    // Wait for the run to be *accepted*, exactly as the server does. The first
    // arm used to be `Promise.resolve().then(...)`, which settles on the next
    // microtask no matter what the run does: the race always resolved
    // immediately, every error was swallowed by a bare catch, and the timeout
    // branch was unreachable while its timer stayed pending for timeoutMs.
    let settleAccepted: () => void = () => {};
    let failAccepted: (error: unknown) => void = () => {};
    const accepted = new Promise<void>((resolve, reject) => {
      settleAccepted = resolve;
      failAccepted = reject;
    });

    const run = registration.runNow(() => settleAccepted());
    // A failure before acceptance is the caller's to see; one after acceptance
    // is not, because this call does not wait for completion.
    void run.catch((error: unknown) => failAccepted(error));

    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        accepted,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => {
            reject(
              new SchedulerControlTimeoutError(
                `schedulerControl.runNow: timed out waiting for live handler to accept ${cfg.schedulerId}/${cfg.scheduleId}`,
              ),
            );
          }, timeoutMs);
        }),
      ]);
    } finally {
      // Left pending on every call before, so repeated clicks piled up timers.
      clearTimeout(timer);
    }
  };

  return { list, runNow };
};
