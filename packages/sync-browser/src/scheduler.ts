import { mutex, type Lock } from "./mutex";
import { type Store, createMemoryStore } from "./store";
import { retry } from "./retry";
import { sleep } from "./internal/sleep";
import { assertValidTimeZone, nextCronTimestamp, type MisfirePolicy } from "./internal/cron";

const DEFAULT_PREFIX = "sync:scheduler";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 5_000;
const DEFAULT_HEARTBEAT_MS = 500;
const DEFAULT_TICK_MS = 500;
const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_MAX_SUBMITS_PER_TICK = 500;
const DEFAULT_SUBMIT_RETRIES = 3;
const DEFAULT_SUBMIT_BACKOFF_BASE_MS = 100;
const DEFAULT_SUBMIT_BACKOFF_MAX_MS = 2_000;
const DEFAULT_SCHEDULED_JOB_KEY_TTL_MS = 90 * DAY_MS;
const DEFAULT_DISPATCH_DLQ_MAX_ENTRIES = 5_000;
const DEFAULT_MISFIRE: MisfirePolicy = "skip";
const DEFAULT_MAX_CATCH_UP_RUNS = 100;
const DEFAULT_MAX_CONSECUTIVE_DISPATCH_FAILURES = 5;
const DEFAULT_STRICT_HANDLERS = true;

// ==========================
// Types
// ==========================

type JobSubmitter = {
  id: string;
  submit(cfg: {
    input: unknown;
    key?: string;
    keyTtlMs?: number;
    at?: number;
    delayMs?: number;
    meta?: Record<string, unknown>;
  }): Promise<string>;
  validateInput?(input: unknown): void;
};

type StoredSchedule = {
  id: string;
  cron: string;
  tz: string;
  misfire: MisfirePolicy;
  maxCatchUpRuns: number;
  jobId: string;
  input: unknown;
  meta?: Record<string, unknown>;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  consecutiveDispatchFailures: number;
  lastFailedSlotTs?: number;
  lastDispatchError?: string;
};

type PersistedScheduleState = {
  version: 1;
  registeredAt: number;
  lastRunAt?: number;
  updatedAt: number;
};

type DispatchPlan = {
  slots: number[];
  nextRunAt: number;
};

export type SchedulerMetric =
  | { type: "leader_acquired"; ts: number }
  | { type: "leader_lost"; ts: number; reason: "extend_failed" | "stop" }
  | { type: "tick_error"; ts: number; message: string }
  | { type: "schedule_registered"; ts: number; scheduleId: string; created: boolean }
  | { type: "schedule_updated"; ts: number; scheduleId: string }
  | { type: "schedule_unregistered"; ts: number; scheduleId: string }
  | { type: "dispatch_submitted"; ts: number; scheduleId: string; slotTs: number; jobId: string }
  | { type: "dispatch_skipped"; ts: number; scheduleId: string; reason: "missing_handler" | "cas_stale" }
  | { type: "dispatch_failed"; ts: number; scheduleId: string; message: string }
  | { type: "dispatch_dlq"; ts: number; scheduleId: string; slotTs: number; message: string }
  | { type: "dispatch_advanced_after_failures"; ts: number; scheduleId: string; slotTs: number; failures: number }
  | { type: "trigger_submitted"; ts: number; scheduleId: string; jobId: string }
  | {
      type: "trigger_rejected";
      ts: number;
      scheduleId: string;
      reason: "missing_schedule" | "missing_handler" | "invalid_schedule";
    }
  | { type: "trigger_failed"; ts: number; scheduleId: string; message: string };

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
    maxSubmitsPerTick?: number;
    submitRetries?: number;
    submitBackoffBaseMs?: number;
    submitBackoffMaxMs?: number;
    scheduledJobKeyTtlMs?: number;
    dlqMaxEntries?: number;
    maxConsecutiveDispatchFailures?: number;
  };
  strictHandlers?: boolean;
  onMetric?: (metric: SchedulerMetric) => void;
  /** Optional store for persisting scheduler state across tab reloads.
   *  Default: MemoryStore (state lost on refresh).
   *  Pass a localStorage-backed store to survive tab closes. */
  store?: Store;
};

export type SchedulerRegisterConfig = {
  id: string;
  cron: string;
  tz?: string;
  job: JobSubmitter;
  input: unknown;
  misfire?: MisfirePolicy;
  maxCatchUpRuns?: number;
  meta?: Record<string, unknown>;
};

export type SchedulerUnregisterConfig = {
  id: string;
};

export type SchedulerTriggerNowConfig = {
  id: string;
  key?: string;
};

export type SchedulerGetConfig = {
  id: string;
};

export type SchedulerInfo = {
  id: string;
  cron: string;
  tz: string;
  misfire: MisfirePolicy;
  maxCatchUpRuns: number;
  jobId: string;
  nextRunAt: number;
  createdAt: number;
  updatedAt: number;
};

export type SchedulerMetricsSnapshot = {
  isLeader: boolean;
  leaderEpoch: number;
  leaderChanges: number;
  dispatchSubmitted: number;
  dispatchFailed: number;
  dispatchRetried: number;
  dispatchSkipped: number;
  dispatchDlq: number;
  triggerSubmitted: number;
  triggerFailed: number;
  triggerRejected: number;
  tickErrors: number;
  lastTickAt: number | null;
};

export type Scheduler = {
  id: string;
  start(): void;
  stop(): Promise<void>;
  register(cfg: SchedulerRegisterConfig): Promise<{ created: boolean; updated: boolean }>;
  unregister(cfg: SchedulerUnregisterConfig): Promise<void>;
  triggerNow(cfg: SchedulerTriggerNowConfig): Promise<string>;
  get(cfg: SchedulerGetConfig): Promise<SchedulerInfo | null>;
  list(): Promise<SchedulerInfo[]>;
  metrics(): SchedulerMetricsSnapshot;
};

// ==========================
// Helpers
// ==========================

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const safeMetric = (onMetric: SchedulerConfig["onMetric"], metric: SchedulerMetric): void => {
  if (!onMetric) return;
  try {
    onMetric(metric);
  } catch {
    // metric handlers are best effort
  }
};

const asInfo = (schedule: StoredSchedule): SchedulerInfo => ({
  id: schedule.id,
  cron: schedule.cron,
  tz: schedule.tz,
  misfire: schedule.misfire,
  maxCatchUpRuns: schedule.maxCatchUpRuns,
  jobId: schedule.jobId,
  nextRunAt: schedule.nextRunAt,
  createdAt: schedule.createdAt,
  updatedAt: schedule.updatedAt,
});

const computeDispatchPlan = (schedule: StoredSchedule, nowMs: number): DispatchPlan | null => {
  if (schedule.nextRunAt > nowMs) return null;

  if (schedule.misfire === "skip") {
    return {
      slots: [],
      nextRunAt: nextCronTimestamp(schedule.cron, schedule.tz, nowMs),
    };
  }

  if (schedule.misfire === "catch_up_one") {
    return {
      slots: [schedule.nextRunAt],
      nextRunAt: nextCronTimestamp(schedule.cron, schedule.tz, nowMs),
    };
  }

  // catch_up_all
  const slots: number[] = [];
  let cursor = schedule.nextRunAt;
  const maxRuns = Math.max(1, schedule.maxCatchUpRuns);

  while (cursor <= nowMs && slots.length < maxRuns) {
    slots.push(cursor);
    cursor = nextCronTimestamp(schedule.cron, schedule.tz, cursor);
  }

  return {
    slots,
    nextRunAt: cursor,
  };
};

// Module-level shared state for coordination across scheduler instances with the same ID
const schedulerSharedStores = new Map<string, Store>();
const schedulerSharedSchedules = new Map<string, Map<string, StoredSchedule>>();

// ==========================
// Scheduler Factory
// ==========================

export const scheduler = (config: SchedulerConfig): Scheduler => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const leaseMs = Math.max(500, config.leader?.leaseMs ?? DEFAULT_LEASE_MS);
  const heartbeatMs = Math.max(100, config.leader?.heartbeatMs ?? DEFAULT_HEARTBEAT_MS);
  const tickMs = Math.max(50, config.dispatch?.tickMs ?? DEFAULT_TICK_MS);
  const batchSize = Math.max(1, config.dispatch?.batchSize ?? DEFAULT_BATCH_SIZE);
  const maxSubmitsPerTick = Math.max(1, config.dispatch?.maxSubmitsPerTick ?? DEFAULT_MAX_SUBMITS_PER_TICK);
  const submitRetries = Math.max(0, config.dispatch?.submitRetries ?? DEFAULT_SUBMIT_RETRIES);
  const submitBackoffBaseMs = Math.max(10, config.dispatch?.submitBackoffBaseMs ?? DEFAULT_SUBMIT_BACKOFF_BASE_MS);
  const submitBackoffMaxMs = Math.max(submitBackoffBaseMs, config.dispatch?.submitBackoffMaxMs ?? DEFAULT_SUBMIT_BACKOFF_MAX_MS);
  const scheduledJobKeyTtlMs = Math.max(60_000, config.dispatch?.scheduledJobKeyTtlMs ?? DEFAULT_SCHEDULED_JOB_KEY_TTL_MS);
  const dlqMaxEntries = Math.max(1, config.dispatch?.dlqMaxEntries ?? DEFAULT_DISPATCH_DLQ_MAX_ENTRIES);
  const maxConsecutiveDispatchFailures = Math.max(
    1,
    config.dispatch?.maxConsecutiveDispatchFailures ?? DEFAULT_MAX_CONSECUTIVE_DISPATCH_FAILURES,
  );
  const strictHandlers = config.strictHandlers ?? DEFAULT_STRICT_HANDLERS;
  const store = config.store ?? createMemoryStore();

  // Keep the legacy key path for backwards compatibility. Older versions stored a
  // bare number here; newer versions store a small scheduler state object.
  const scheduleStateKey = (scheduleId: string): string => `${prefix}:${config.id}:lastRun:${scheduleId}`;

  const readPersistedScheduleState = (scheduleId: string): PersistedScheduleState | null => {
    const raw = store.get(scheduleStateKey(scheduleId));
    if (raw === undefined) return null;

    if (typeof raw === "number" && Number.isFinite(raw)) {
      return {
        version: 1,
        registeredAt: raw,
        lastRunAt: raw,
        updatedAt: raw,
      };
    }

    if (!raw || typeof raw !== "object") return null;

    const value = raw as Record<string, unknown>;
    const registeredAt = value.registeredAt;
    if (typeof registeredAt !== "number" || !Number.isFinite(registeredAt)) return null;

    const lastRunAt = value.lastRunAt;
    const updatedAt = value.updatedAt;
    return {
      version: 1,
      registeredAt,
      ...(typeof lastRunAt === "number" && Number.isFinite(lastRunAt) ? { lastRunAt } : {}),
      updatedAt: typeof updatedAt === "number" && Number.isFinite(updatedAt) ? updatedAt : registeredAt,
    };
  };

  const writePersistedScheduleState = (
    scheduleId: string,
    patch: {
      registeredAt?: number;
      lastRunAt?: number;
      updatedAt?: number;
    },
  ): PersistedScheduleState => {
    const current = readPersistedScheduleState(scheduleId);
    const nextState: PersistedScheduleState = {
      version: 1,
      registeredAt: patch.registeredAt ?? current?.registeredAt ?? Date.now(),
      updatedAt: patch.updatedAt ?? Date.now(),
    };
    const nextLastRunAt = patch.lastRunAt ?? current?.lastRunAt;
    if (nextLastRunAt !== undefined) {
      nextState.lastRunAt = nextLastRunAt;
    }
    store.set(scheduleStateKey(scheduleId), nextState);
    return nextState;
  };

  // Share a single mutex store across scheduler instances with the same ID
  // so that leader election actually coordinates between them.
  const sharedMutexStoreKey = `${prefix}:${config.id}:leader:store`;
  if (!schedulerSharedStores.has(sharedMutexStoreKey)) {
    schedulerSharedStores.set(sharedMutexStoreKey, createMemoryStore());
  }
  const sharedMutexStore = schedulerSharedStores.get(sharedMutexStoreKey)!;

  const leaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
    store: sharedMutexStore,
  });

  // Share schedules across instances with the same scheduler ID
  const sharedSchedulesKey = `${prefix}:${config.id}:schedules`;
  if (!schedulerSharedSchedules.has(sharedSchedulesKey)) {
    schedulerSharedSchedules.set(sharedSchedulesKey, new Map());
  }
  const schedules = schedulerSharedSchedules.get(sharedSchedulesKey)!;

  // Per-instance maps (handlers are local to the process that registered them)
  const jobsById = new Map<string, JobSubmitter>();
  const scheduleToJobId = new Map<string, string>();
  const dispatchDlq: Array<{ scheduleId: string; slotTs: number; message: string; ts: number }> = [];

  const metricsState: SchedulerMetricsSnapshot = {
    isLeader: false,
    leaderEpoch: 0,
    leaderChanges: 0,
    dispatchSubmitted: 0,
    dispatchFailed: 0,
    dispatchRetried: 0,
    dispatchSkipped: 0,
    dispatchDlq: 0,
    triggerSubmitted: 0,
    triggerFailed: 0,
    triggerRejected: 0,
    tickErrors: 0,
    lastTickAt: null,
  };

  let running = false;
  let loopPromise: Promise<void> | null = null;
  let currentLeaderLock: Lock | null = null;
  let currentLeaderEpoch = 0;
  let lastHeartbeatAt = 0;

  const setLeader = (next: boolean, reason?: "extend_failed" | "stop"): void => {
    if (metricsState.isLeader === next) return;
    metricsState.isLeader = next;
    metricsState.leaderChanges += 1;

    if (next) {
      safeMetric(config.onMetric, { type: "leader_acquired", ts: Date.now() });
      return;
    }
    safeMetric(config.onMetric, { type: "leader_lost", ts: Date.now(), reason: reason ?? "stop" });
  };

  const tryAcquireLeadership = async (): Promise<void> => {
    if (currentLeaderLock) return;
    const acquired = await leaderMutex.acquire("active", leaseMs);
    if (!acquired) return;
    currentLeaderLock = acquired;
    currentLeaderEpoch++;
    metricsState.leaderEpoch = currentLeaderEpoch;
    lastHeartbeatAt = Date.now();
    setLeader(true);
  };

  const maintainLeadership = async (): Promise<void> => {
    if (!currentLeaderLock) return;
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt < heartbeatMs) return;
    const ok = await leaderMutex.extend(currentLeaderLock, leaseMs);
    lastHeartbeatAt = nowMs;
    if (!ok) {
      currentLeaderLock = null;
      setLeader(false, "extend_failed");
    }
  };

  const relinquishLeadership = async (): Promise<void> => {
    if (!currentLeaderLock) return;
    try {
      await leaderMutex.release(currentLeaderLock);
    } catch {
      // best effort
    }
    currentLeaderLock = null;
    setLeader(false, "stop");
  };

  const ensureLeadership = async (): Promise<boolean> => {
    if (!currentLeaderLock) return false;
    const nowMs = Date.now();
    if (nowMs - lastHeartbeatAt >= heartbeatMs) {
      const ok = await leaderMutex.extend(currentLeaderLock, leaseMs);
      lastHeartbeatAt = nowMs;
      if (!ok) {
        currentLeaderLock = null;
        setLeader(false, "extend_failed");
        return false;
      }
    }
    return metricsState.isLeader;
  };

  const submitScheduledJob = async (cfg: {
    jobHandle: JobSubmitter;
    schedule: StoredSchedule;
    key?: string;
    at?: number;
    meta?: Record<string, unknown>;
    requireLeadership: boolean;
    onRetry?: () => void;
  }): Promise<string> => {
    return await retry(
      async () => {
        if (cfg.requireLeadership && !(await ensureLeadership())) {
          throw new Error("leadership lost during dispatch");
        }
        return await cfg.jobHandle.submit({
          input: cfg.schedule.input,
          key: cfg.key,
          keyTtlMs: scheduledJobKeyTtlMs,
          ...(cfg.at !== undefined ? { at: cfg.at } : {}),
          meta: cfg.meta,
        });
      },
      {
        attempts: submitRetries + 1,
        minDelayMs: submitBackoffBaseMs,
        maxDelayMs: submitBackoffMaxMs,
        factor: 2,
        jitter: 0.25,
        retryIf: (error): boolean => {
          const err = asError(error);
          if (err.name === "ZodError") return false;
          if (cfg.requireLeadership && err.message === "leadership lost during dispatch") return false;
          cfg.onRetry?.();
          return true;
        },
      },
    );
  };

  const pushDispatchDlq = (cfg: { scheduleId: string; slotTs: number; message: string }): void => {
    dispatchDlq.unshift({ ...cfg, ts: Date.now() });
    if (dispatchDlq.length > dlqMaxEntries) {
      dispatchDlq.length = dlqMaxEntries;
    }
    metricsState.dispatchDlq += 1;
    safeMetric(config.onMetric, {
      type: "dispatch_dlq",
      ts: Date.now(),
      scheduleId: cfg.scheduleId,
      slotTs: cfg.slotTs,
      message: cfg.message,
    });
  };

  const recordDispatchFailure = (cfg: {
    schedule: StoredSchedule;
    failedSlotTs: number;
    message: string;
    deterministic: boolean;
  }): void => {
    const sameSlot = cfg.schedule.lastFailedSlotTs === cfg.failedSlotTs;
    const failures = sameSlot ? cfg.schedule.consecutiveDispatchFailures + 1 : 1;
    const shouldAdvance = cfg.deterministic || failures >= maxConsecutiveDispatchFailures;

    cfg.schedule.consecutiveDispatchFailures = failures;
    cfg.schedule.lastFailedSlotTs = cfg.failedSlotTs;
    cfg.schedule.lastDispatchError = cfg.message;
    cfg.schedule.updatedAt = Date.now();

    if (shouldAdvance) {
      cfg.schedule.nextRunAt = nextCronTimestamp(cfg.schedule.cron, cfg.schedule.tz, cfg.failedSlotTs);
      cfg.schedule.consecutiveDispatchFailures = 0;
      safeMetric(config.onMetric, {
        type: "dispatch_advanced_after_failures",
        ts: Date.now(),
        scheduleId: cfg.schedule.id,
        slotTs: cfg.failedSlotTs,
        failures,
      });
    }
  };

  // ==========================
  // Dispatch
  // ==========================

  const dispatchDue = async (): Promise<void> => {
    const nowMs = Date.now();

    // Find due schedules
    const dueSchedules: StoredSchedule[] = [];
    for (const schedule of schedules.values()) {
      if (schedule.nextRunAt <= nowMs) {
        dueSchedules.push(schedule);
      }
    }
    dueSchedules.sort((a, b) => a.nextRunAt - b.nextRunAt);
    const batch = dueSchedules.slice(0, batchSize);
    if (batch.length === 0) return;

    let submitsRemaining = maxSubmitsPerTick;

    for (const schedule of batch) {
      if (!(await ensureLeadership())) break;

      if (schedule.nextRunAt > nowMs) continue;

      const plan = computeDispatchPlan(schedule, nowMs);
      if (!plan) continue;

      const jobHandle = jobsById.get(schedule.jobId);
      if (!jobHandle && plan.slots.length > 0) {
        metricsState.dispatchSkipped += 1;
        safeMetric(config.onMetric, {
          type: "dispatch_skipped",
          ts: nowMs,
          scheduleId: schedule.id,
          reason: "missing_handler",
        });
        if (strictHandlers) {
          await relinquishLeadership();
          break;
        }

        schedule.nextRunAt = plan.nextRunAt;
        schedule.updatedAt = Date.now();
        if (plan.slots.length > 0) {
          writePersistedScheduleState(schedule.id, {
            lastRunAt: plan.slots[plan.slots.length - 1]!,
            updatedAt: Date.now(),
          });
        }
        continue;
      }

      let submitFailed = false;
      let submittedAny = false;
      let lastSubmittedSlotTs: number | null = null;

      for (const slotTs of plan.slots) {
        if (submitsRemaining <= 0) break;
        if (!(await ensureLeadership())) {
          submitFailed = true;
          break;
        }
        try {
          const jobId = await submitScheduledJob({
            jobHandle: jobHandle!,
            schedule,
            key: `${schedule.id}:${slotTs}`,
            at: slotTs,
            meta: {
              ...(schedule.meta ?? {}),
              scheduleId: schedule.id,
              scheduleSlotTs: slotTs,
              schedulerId: config.id,
            },
            requireLeadership: true,
            onRetry: (): void => {
              metricsState.dispatchRetried += 1;
            },
          });
          metricsState.dispatchSubmitted += 1;
          submitsRemaining -= 1;
          submittedAny = true;
          lastSubmittedSlotTs = slotTs;
          safeMetric(config.onMetric, {
            type: "dispatch_submitted",
            ts: Date.now(),
            scheduleId: schedule.id,
            slotTs,
            jobId,
          });
        } catch (error) {
          submitFailed = true;
          metricsState.dispatchFailed += 1;
          const err = asError(error);
          pushDispatchDlq({
            scheduleId: schedule.id,
            slotTs,
            message: err.message,
          });
          recordDispatchFailure({
            schedule,
            failedSlotTs: slotTs,
            message: err.message,
            deterministic: err.name === "ZodError",
          });
          safeMetric(config.onMetric, {
            type: "dispatch_failed",
            ts: Date.now(),
            scheduleId: schedule.id,
            message: err.message,
          });
          break;
        }
      }

      if (submitFailed) continue;

      if (submitsRemaining <= 0) {
        if (submittedAny && lastSubmittedSlotTs !== null) {
          schedule.nextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, lastSubmittedSlotTs);
          schedule.updatedAt = Date.now();
          scheduleToJobId.set(schedule.id, schedule.jobId);
          writePersistedScheduleState(schedule.id, {
            lastRunAt: lastSubmittedSlotTs,
            updatedAt: Date.now(),
          });
        }
        break;
      }

      // Reschedule
      schedule.nextRunAt = plan.nextRunAt;
      schedule.updatedAt = Date.now();
      scheduleToJobId.set(schedule.id, schedule.jobId);
      // Persist the last handled cron slot so tab reopen can resume accurately.
      if (plan.slots.length > 0) {
        writePersistedScheduleState(schedule.id, {
          lastRunAt: plan.slots[plan.slots.length - 1]!,
          updatedAt: Date.now(),
        });
      }
    }
  };

  // ==========================
  // Main Loop
  // ==========================

  const loop = async (): Promise<void> => {
    while (running) {
      try {
        await tryAcquireLeadership();
        await maintainLeadership();
        if (currentLeaderLock) {
          await dispatchDue();
        }
        metricsState.lastTickAt = Date.now();
      } catch (error) {
        metricsState.tickErrors += 1;
        safeMetric(config.onMetric, {
          type: "tick_error",
          ts: Date.now(),
          message: asError(error).message,
        });
      }
      await sleep(tickMs);
    }
  };

  // ==========================
  // Public API
  // ==========================

  const register = async (cfg: SchedulerRegisterConfig): Promise<{ created: boolean; updated: boolean }> => {
    const tz = cfg.tz ?? "UTC";
    assertValidTimeZone(tz);
    cfg.job.validateInput?.(cfg.input);
    const nowMs = Date.now();
    const misfire = cfg.misfire ?? DEFAULT_MISFIRE;
    const maxCatchUpRuns = Math.max(1, cfg.maxCatchUpRuns ?? DEFAULT_MAX_CATCH_UP_RUNS);

    // Code is the source of truth for schedule definition.
    // The store only persists scheduler state so we can catch up after tab reopen.
    const persistedState = readPersistedScheduleState(cfg.id);

    // Resume from the last handled cron slot when available. If the schedule has
    // never run yet, fall back to the first time we registered it so the first
    // missed run can still be caught up after a tab reopen.
    const resumeFrom = persistedState?.lastRunAt ?? persistedState?.registeredAt ?? nowMs;
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, resumeFrom);

    const existing = schedules.get(cfg.id);

    const stored: StoredSchedule = {
      id: cfg.id,
      cron: cfg.cron,
      tz,
      misfire,
      maxCatchUpRuns,
      jobId: cfg.job.id,
      input: cfg.input,
      meta: cfg.meta,
      createdAt: existing?.createdAt ?? nowMs,
      updatedAt: nowMs,
      nextRunAt: firstRunAt,
      consecutiveDispatchFailures: 0,
    };

    // If re-registering in the same session with unchanged cron/tz AND
    // no persisted lastRunAt to recover from, keep the in-memory state.
    // Persisted scheduler state takes priority for cross-session recovery.
    if (!persistedState?.lastRunAt && existing && existing.cron === cfg.cron && existing.tz === tz) {
      stored.nextRunAt = existing.nextRunAt;
      stored.consecutiveDispatchFailures = existing.consecutiveDispatchFailures;
      stored.lastFailedSlotTs = existing.lastFailedSlotTs;
      stored.lastDispatchError = existing.lastDispatchError;
    }

    const created = !existing;
    const updated = !!existing;

    schedules.set(cfg.id, stored);
    jobsById.set(cfg.job.id, cfg.job);

    const previousJobId = scheduleToJobId.get(cfg.id);
    scheduleToJobId.set(cfg.id, cfg.job.id);

    writePersistedScheduleState(cfg.id, {
      registeredAt: persistedState?.registeredAt ?? existing?.createdAt ?? nowMs,
      updatedAt: nowMs,
    });

    // Cleanup old job handler if no longer referenced
    if (previousJobId && previousJobId !== cfg.job.id) {
      let stillUsed = false;
      for (const [sid, jid] of scheduleToJobId.entries()) {
        if (sid !== cfg.id && jid === previousJobId) {
          stillUsed = true;
          break;
        }
      }
      if (!stillUsed) jobsById.delete(previousJobId);
    }

    safeMetric(config.onMetric, {
      type: "schedule_registered",
      ts: Date.now(),
      scheduleId: cfg.id,
      created,
    });
    if (updated) {
      safeMetric(config.onMetric, {
        type: "schedule_updated",
        ts: Date.now(),
        scheduleId: cfg.id,
      });
    }
    return { created, updated };
  };

  const unregister = async (cfg: SchedulerUnregisterConfig): Promise<void> => {
    const jobId = scheduleToJobId.get(cfg.id);
    schedules.delete(cfg.id);
    scheduleToJobId.delete(cfg.id);
    store.del(scheduleStateKey(cfg.id));

    safeMetric(config.onMetric, {
      type: "schedule_unregistered",
      ts: Date.now(),
      scheduleId: cfg.id,
    });

    if (jobId) {
      let stillUsed = false;
      for (const mappedJobId of scheduleToJobId.values()) {
        if (mappedJobId === jobId) {
          stillUsed = true;
          break;
        }
      }
      if (!stillUsed) jobsById.delete(jobId);
    }
  };

  const triggerNow = async (cfg: SchedulerTriggerNowConfig): Promise<string> => {
    const schedule = schedules.get(cfg.id);
    if (!schedule) {
      metricsState.triggerRejected += 1;
      safeMetric(config.onMetric, {
        type: "trigger_rejected",
        ts: Date.now(),
        scheduleId: cfg.id,
        reason: "missing_schedule",
      });
      throw new Error(`scheduler trigger rejected: missing schedule ${cfg.id}`);
    }

    const jobHandle = jobsById.get(schedule.jobId);
    if (!jobHandle) {
      metricsState.triggerRejected += 1;
      safeMetric(config.onMetric, {
        type: "trigger_rejected",
        ts: Date.now(),
        scheduleId: schedule.id,
        reason: "missing_handler",
      });
      throw new Error(`scheduler trigger rejected: missing local handler for schedule ${schedule.id}`);
    }

    try {
      const jobId = await submitScheduledJob({
        jobHandle,
        schedule,
        key: cfg.key ? `manual:${schedule.id}:${cfg.key}` : undefined,
        meta: {
          ...(schedule.meta ?? {}),
          scheduleId: schedule.id,
          schedulerId: config.id,
          scheduleTrigger: "manual",
          ...(cfg.key ? { scheduleManualKey: cfg.key } : {}),
        },
        requireLeadership: false,
      });
      metricsState.triggerSubmitted += 1;
      safeMetric(config.onMetric, {
        type: "trigger_submitted",
        ts: Date.now(),
        scheduleId: schedule.id,
        jobId,
      });
      return jobId;
    } catch (error) {
      metricsState.triggerFailed += 1;
      const err = asError(error);
      safeMetric(config.onMetric, {
        type: "trigger_failed",
        ts: Date.now(),
        scheduleId: schedule.id,
        message: err.message,
      });
      throw error;
    }
  };

  const get = async (cfg: SchedulerGetConfig): Promise<SchedulerInfo | null> => {
    const schedule = schedules.get(cfg.id);
    if (!schedule) return null;
    return asInfo(schedule);
  };

  const listSchedules = async (): Promise<SchedulerInfo[]> => {
    return Array.from(schedules.values()).map(asInfo).sort((a, b) => a.id.localeCompare(b.id));
  };

  const start = (): void => {
    if (running) return;
    running = true;
    loopPromise = loop();
  };

  const stop = async (): Promise<void> => {
    if (!running) return;
    running = false;
    setLeader(false, "stop");
    await relinquishLeadership();
    if (loopPromise) {
      await loopPromise;
      loopPromise = null;
    }
  };

  return {
    id: config.id,
    start,
    stop,
    register,
    unregister,
    triggerNow,
    get,
    list: listSchedules,
    metrics: () => ({ ...metricsState }),
  };
};
