import { redis, sleep } from "bun";
import { mutex, type Lock } from "./mutex";
import { assertValidTimeZone, nextCronTimestamp, type MisfirePolicy } from "./internal/cron";
import { retry } from "./retry";

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

const UPSERT_SCRIPT = `
  local raw = redis.call("GET", KEYS[1])
  local incomingRaw = ARGV[1]
  local firstRunAt = tonumber(ARGV[2])
  local scheduleId = ARGV[3]
  local now = tonumber(ARGV[4])

  local created = 1
  local incoming = cjson.decode(incomingRaw)

  if raw then
    created = 0
    local ok, existing = pcall(cjson.decode, raw)
    if ok then
      incoming.createdAt = tonumber(existing.createdAt) or incoming.createdAt

      local shouldResetNext = tostring(existing.cron) ~= tostring(incoming.cron)
        or tostring(existing.tz) ~= tostring(incoming.tz)

      if shouldResetNext then
        incoming.nextRunAt = firstRunAt
        incoming.consecutiveDispatchFailures = 0
        incoming.lastFailedSlotTs = nil
        incoming.lastDispatchError = nil
      else
        incoming.nextRunAt = tonumber(existing.nextRunAt) or firstRunAt
        incoming.consecutiveDispatchFailures = tonumber(existing.consecutiveDispatchFailures) or 0
        incoming.lastFailedSlotTs = tonumber(existing.lastFailedSlotTs) or nil
        incoming.lastDispatchError = existing.lastDispatchError
      end
    else
      incoming.nextRunAt = firstRunAt
      incoming.consecutiveDispatchFailures = 0
      incoming.lastFailedSlotTs = nil
      incoming.lastDispatchError = nil
    end
  else
    incoming.nextRunAt = firstRunAt
    incoming.consecutiveDispatchFailures = 0
    incoming.lastFailedSlotTs = nil
    incoming.lastDispatchError = nil
  end

  incoming.updatedAt = now

  redis.call("SET", KEYS[1], cjson.encode(incoming))
  redis.call("ZADD", KEYS[2], tostring(incoming.nextRunAt), scheduleId)
  redis.call("SADD", KEYS[3], scheduleId)

  if created == 1 then
    return 1
  end
  return 2
`;

const UNREGISTER_SCRIPT = `
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  return 1
`;

const CLEANUP_BROKEN_SCRIPT = `
  redis.call("DEL", KEYS[1])
  redis.call("ZREM", KEYS[2], ARGV[1])
  redis.call("SREM", KEYS[3], ARGV[1])
  return 1
`;

const RESCHEDULE_CAS_SCRIPT = `
  local epochKey = KEYS[4]
  local raw = redis.call("GET", KEYS[1])
  local scheduleId = ARGV[1]
  local expectedEpoch = tonumber(ARGV[2])
  local expectedNext = ARGV[3]
  local nextRunAt = tonumber(ARGV[4])
  local now = tonumber(ARGV[5])

  local currentEpoch = tonumber(redis.call("GET", epochKey) or "0")
  if currentEpoch ~= expectedEpoch then
    return -2
  end

  if not raw then
    redis.call("ZREM", KEYS[2], scheduleId)
    redis.call("SREM", KEYS[3], scheduleId)
    return 0
  end

  local ok, schedule = pcall(cjson.decode, raw)
  if not ok then
    redis.call("DEL", KEYS[1])
    redis.call("ZREM", KEYS[2], scheduleId)
    redis.call("SREM", KEYS[3], scheduleId)
    return 0
  end

  if tonumber(schedule.nextRunAt) ~= tonumber(expectedNext) then
    return -1
  end

  schedule.nextRunAt = nextRunAt
  schedule.updatedAt = now

  redis.call("SET", KEYS[1], cjson.encode(schedule))
  redis.call("ZADD", KEYS[2], tostring(nextRunAt), scheduleId)
  redis.call("SADD", KEYS[3], scheduleId)

  return 1
`;

const PUSH_DISPATCH_DLQ_SCRIPT = `
  redis.call("LPUSH", KEYS[1], ARGV[1])
  redis.call("LTRIM", KEYS[1], "0", ARGV[2])
  return 1
`;

const RECORD_FAILURE_CAS_SCRIPT = `
  local epochKey = KEYS[4]
  local raw = redis.call("GET", KEYS[1])
  local scheduleId = ARGV[1]
  local expectedEpoch = tonumber(ARGV[2])
  local expectedNext = tonumber(ARGV[3])
  local failedSlotTs = tonumber(ARGV[4])
  local nextRunAt = tonumber(ARGV[5])
  local failures = tonumber(ARGV[6])
  local shouldAdvance = tonumber(ARGV[7])
  local errorMessage = ARGV[8]
  local now = tonumber(ARGV[9])

  local currentEpoch = tonumber(redis.call("GET", epochKey) or "0")
  if currentEpoch ~= expectedEpoch then
    return -2
  end

  if not raw then
    redis.call("ZREM", KEYS[2], scheduleId)
    redis.call("SREM", KEYS[3], scheduleId)
    return 0
  end

  local ok, schedule = pcall(cjson.decode, raw)
  if not ok then
    redis.call("DEL", KEYS[1])
    redis.call("ZREM", KEYS[2], scheduleId)
    redis.call("SREM", KEYS[3], scheduleId)
    return 0
  end

  if tonumber(schedule.nextRunAt) ~= expectedNext then
    return -1
  end

  schedule.consecutiveDispatchFailures = failures
  schedule.lastFailedSlotTs = failedSlotTs
  schedule.lastDispatchError = errorMessage
  schedule.updatedAt = now

  if shouldAdvance == 1 then
    schedule.nextRunAt = nextRunAt
    schedule.consecutiveDispatchFailures = 0
  end

  redis.call("SET", KEYS[1], cjson.encode(schedule))
  redis.call("ZADD", KEYS[2], tostring(schedule.nextRunAt), scheduleId)
  redis.call("SADD", KEYS[3], scheduleId)
  return 1
`;

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

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

const safeMetric = (onMetric: SchedulerConfig["onMetric"], metric: SchedulerMetric): void => {
  if (!onMetric) return;
  try {
    onMetric(metric);
  } catch {
    // metric handlers are best effort
  }
};

const parseSchedule = (raw: string | null): StoredSchedule | null => {
  if (!raw) return null;
  try {
    return JSON.parse(raw) as StoredSchedule;
  } catch {
    return null;
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

  const scheduleKey = (scheduleId: string): string => `${prefix}:${config.id}:schedule:${scheduleId}`;
  const dueKey = `${prefix}:${config.id}:due`;
  const indexKey = `${prefix}:${config.id}:index`;
  const dispatchDlqKey = `${prefix}:${config.id}:dispatch:dlq`;
  const leaderEpochKey = `${prefix}:${config.id}:leader:epoch`;
  const leaderMutex = mutex({
    id: `${config.id}:leader`,
    prefix: `${prefix}:leader`,
    defaultTtl: leaseMs,
    retryCount: 0,
  });

  const jobsById = new Map<string, JobSubmitter>();
  const scheduleToJobId = new Map<string, string>();
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
  let currentLeaderEpoch: number | null = null;
  let lastHeartbeatAt = 0;

  const setLeader = (next: boolean, reason?: "extend_failed" | "stop"): void => {
    if (metricsState.isLeader === next) return;
    metricsState.isLeader = next;
    metricsState.leaderChanges += 1;

    if (next) {
      safeMetric(config.onMetric, { type: "leader_acquired", ts: Date.now() });
      return;
    }

    metricsState.leaderEpoch = currentLeaderEpoch ?? metricsState.leaderEpoch;
    safeMetric(config.onMetric, { type: "leader_lost", ts: Date.now(), reason: reason ?? "stop" });
  };

  const tryAcquireLeadership = async (): Promise<void> => {
    if (currentLeaderLock) return;
    const acquired = await leaderMutex.acquire("active", leaseMs);
    if (!acquired) return;
    currentLeaderLock = acquired;
    currentLeaderEpoch = Number(await redis.incr(leaderEpochKey));
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
    if (ok) {
      if (currentLeaderEpoch !== null) {
        const latestEpoch = Number(await redis.get(leaderEpochKey) ?? "0");
        if (latestEpoch === currentLeaderEpoch) return;
      } else {
        return;
      }
    }
    currentLeaderLock = null;
    currentLeaderEpoch = null;
    setLeader(false, "extend_failed");
  };

  const relinquishLeadership = async (): Promise<void> => {
    if (!currentLeaderLock) return;
    try {
      await leaderMutex.release(currentLeaderLock);
    } catch {
      // best effort
    }
    currentLeaderLock = null;
    currentLeaderEpoch = null;
    setLeader(false, "extend_failed");
  };

  const ensureLeadership = async (cfg?: { forceRefresh?: boolean }): Promise<boolean> => {
    if (!currentLeaderLock || currentLeaderEpoch === null) return false;
    const nowMs = Date.now();
    const shouldRefresh = (cfg?.forceRefresh ?? false) || nowMs - lastHeartbeatAt >= heartbeatMs;
    if (!shouldRefresh) return metricsState.isLeader;
    const ok = await leaderMutex.extend(currentLeaderLock, leaseMs);
    lastHeartbeatAt = nowMs;
    if (ok) {
      const latestEpoch = Number(await redis.get(leaderEpochKey) ?? "0");
      if (latestEpoch === currentLeaderEpoch) return true;
    }
    currentLeaderLock = null;
    currentLeaderEpoch = null;
    setLeader(false, "extend_failed");
    return false;
  };

  const cleanupBrokenSchedule = async (scheduleId: string): Promise<void> => {
    await redis.send("EVAL", [
      CLEANUP_BROKEN_SCRIPT,
      "3",
      scheduleKey(scheduleId),
      dueKey,
      indexKey,
      scheduleId,
    ]);
    scheduleToJobId.delete(scheduleId);
  };

  const rescheduleCas = async (schedule: StoredSchedule, nextRunAt: number, nowMs: number): Promise<"ok" | "missing" | "stale"> => {
    if (currentLeaderEpoch === null) return "stale";
    const result = await redis.send("EVAL", [
      RESCHEDULE_CAS_SCRIPT,
      "4",
      scheduleKey(schedule.id),
      dueKey,
      indexKey,
      leaderEpochKey,
      schedule.id,
      String(currentLeaderEpoch),
      String(schedule.nextRunAt),
      String(nextRunAt),
      String(nowMs),
    ]);

    const code = Number(result);
    if (code === 1) return "ok";
    if (code === 0) return "missing";
    return "stale";
  };

  const pushDispatchDlq = async (cfg: { scheduleId: string; slotTs: number; message: string }): Promise<void> => {
    const payload = JSON.stringify({
      ...cfg,
      ts: Date.now(),
      schedulerId: config.id,
      leaderEpoch: currentLeaderEpoch,
    });
    await redis.send("EVAL", [
      PUSH_DISPATCH_DLQ_SCRIPT,
      "1",
      dispatchDlqKey,
      payload,
      String(dlqMaxEntries - 1),
    ]);
    metricsState.dispatchDlq += 1;
    safeMetric(config.onMetric, {
      type: "dispatch_dlq",
      ts: Date.now(),
      scheduleId: cfg.scheduleId,
      slotTs: cfg.slotTs,
      message: cfg.message,
    });
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
        if (cfg.requireLeadership && !(await ensureLeadership({ forceRefresh: true }))) {
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

  const recordDispatchFailure = async (cfg: {
    schedule: StoredSchedule;
    failedSlotTs: number;
    message: string;
    deterministic: boolean;
  }): Promise<"ok" | "missing" | "stale"> => {
    if (currentLeaderEpoch === null) return "stale";

    const sameSlot = cfg.schedule.lastFailedSlotTs === cfg.failedSlotTs;
    const failures = sameSlot ? cfg.schedule.consecutiveDispatchFailures + 1 : 1;
    const shouldAdvance = cfg.deterministic || failures >= maxConsecutiveDispatchFailures;
    const nextRunAt = shouldAdvance
      ? nextCronTimestamp(cfg.schedule.cron, cfg.schedule.tz, cfg.failedSlotTs)
      : cfg.schedule.nextRunAt;

    const result = Number(
      await redis.send("EVAL", [
        RECORD_FAILURE_CAS_SCRIPT,
        "4",
        scheduleKey(cfg.schedule.id),
        dueKey,
        indexKey,
        leaderEpochKey,
        cfg.schedule.id,
        String(currentLeaderEpoch),
        String(cfg.schedule.nextRunAt),
        String(cfg.failedSlotTs),
        String(nextRunAt),
        String(failures),
        shouldAdvance ? "1" : "0",
        cfg.message,
        String(Date.now()),
      ]),
    );

    if (result === 1 && shouldAdvance) {
      safeMetric(config.onMetric, {
        type: "dispatch_advanced_after_failures",
        ts: Date.now(),
        scheduleId: cfg.schedule.id,
        slotTs: cfg.failedSlotTs,
        failures,
      });
    }

    if (result === 1) return "ok";
    if (result === 0) return "missing";
    return "stale";
  };

  const dispatchDue = async (): Promise<void> => {
    const nowMs = Date.now();
    const dueIds = await redis.send("ZRANGEBYSCORE", [dueKey, "-inf", String(nowMs), "LIMIT", "0", String(batchSize)]);
    if (!Array.isArray(dueIds) || dueIds.length === 0) return;
    let submitsRemaining = maxSubmitsPerTick;

    for (const idRaw of dueIds) {
      if (!(await ensureLeadership())) break;

      const scheduleId = String(idRaw);
      const raw = await redis.get(scheduleKey(scheduleId));
      const schedule = parseSchedule(raw);

      if (!schedule) {
        await cleanupBrokenSchedule(scheduleId);
        continue;
      }

      if (schedule.nextRunAt > nowMs) {
        continue;
      }

      const plan = computeDispatchPlan(schedule, nowMs);
      if (!plan) continue;

      const jobHandle = jobsById.get(schedule.jobId);
      if (!jobHandle && plan.slots.length > 0) {
        metricsState.dispatchSkipped += 1;
        safeMetric(config.onMetric, { type: "dispatch_skipped", ts: nowMs, scheduleId: schedule.id, reason: "missing_handler" });
        if (strictHandlers) {
          await relinquishLeadership();
          break;
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
          await pushDispatchDlq({
            scheduleId: schedule.id,
            slotTs,
            message: err.message,
          });
          await recordDispatchFailure({
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
          if (!(await ensureLeadership({ forceRefresh: true }))) break;

          const partialNextRunAt = nextCronTimestamp(schedule.cron, schedule.tz, lastSubmittedSlotTs);
          const partialCas = await rescheduleCas(schedule, partialNextRunAt, Date.now());
          if (partialCas !== "ok") {
            metricsState.dispatchSkipped += 1;
            safeMetric(config.onMetric, {
              type: "dispatch_skipped",
              ts: Date.now(),
              scheduleId: schedule.id,
              reason: "cas_stale",
            });
          }
          scheduleToJobId.set(schedule.id, schedule.jobId);
        }
        break;
      }

      if (!(await ensureLeadership({ forceRefresh: true }))) break;

      const cas = await rescheduleCas(schedule, plan.nextRunAt, Date.now());
      if (cas !== "ok") {
        metricsState.dispatchSkipped += 1;
        safeMetric(config.onMetric, {
          type: "dispatch_skipped",
          ts: Date.now(),
          scheduleId: schedule.id,
          reason: "cas_stale",
        });
      }

      scheduleToJobId.set(schedule.id, schedule.jobId);
    }
  };

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

  const register = async (cfg: SchedulerRegisterConfig): Promise<{ created: boolean; updated: boolean }> => {
    const tz = cfg.tz ?? "UTC";
    assertValidTimeZone(tz);
    cfg.job.validateInput?.(cfg.input);
    const nowMs = Date.now();
    const misfire = cfg.misfire ?? DEFAULT_MISFIRE;
    const maxCatchUpRuns = Math.max(1, cfg.maxCatchUpRuns ?? DEFAULT_MAX_CATCH_UP_RUNS);
    const firstRunAt = nextCronTimestamp(cfg.cron, tz, nowMs);
    const previous = parseSchedule(await redis.get(scheduleKey(cfg.id)));

    const stored: StoredSchedule = {
      id: cfg.id,
      cron: cfg.cron,
      tz,
      misfire,
      maxCatchUpRuns,
      jobId: cfg.job.id,
      input: cfg.input,
      meta: cfg.meta,
      createdAt: nowMs,
      updatedAt: nowMs,
      nextRunAt: firstRunAt,
      consecutiveDispatchFailures: 0,
    };

    const upsertResult = Number(
      await redis.send("EVAL", [
        UPSERT_SCRIPT,
        "3",
        scheduleKey(cfg.id),
        dueKey,
        indexKey,
        JSON.stringify(stored),
        String(firstRunAt),
        cfg.id,
        String(nowMs),
      ]),
    );
    const created = upsertResult === 1;
    const updated = upsertResult === 2;

    jobsById.set(cfg.job.id, cfg.job);
    scheduleToJobId.set(cfg.id, cfg.job.id);

    if (previous && previous.jobId !== cfg.job.id) {
      let stillUsed = false;
      for (const [sid, jid] of scheduleToJobId.entries()) {
        if (sid !== cfg.id && jid === previous.jobId) {
          stillUsed = true;
          break;
        }
      }
      if (!stillUsed) jobsById.delete(previous.jobId);
    }

    safeMetric(config.onMetric, {
      type: "schedule_registered",
      ts: Date.now(),
      scheduleId: cfg.id,
      created,
    });
    if (updated && !created) {
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
    await redis.send("EVAL", [
      UNREGISTER_SCRIPT,
      "3",
      scheduleKey(cfg.id),
      dueKey,
      indexKey,
      cfg.id,
    ]);
    safeMetric(config.onMetric, {
      type: "schedule_unregistered",
      ts: Date.now(),
      scheduleId: cfg.id,
    });
    scheduleToJobId.delete(cfg.id);

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
    const raw = await redis.get(scheduleKey(cfg.id));
    if (!raw) {
      metricsState.triggerRejected += 1;
      safeMetric(config.onMetric, {
        type: "trigger_rejected",
        ts: Date.now(),
        scheduleId: cfg.id,
        reason: "missing_schedule",
      });
      throw new Error(`scheduler trigger rejected: missing schedule ${cfg.id}`);
    }

    const schedule = parseSchedule(raw);
    if (!schedule) {
      metricsState.triggerRejected += 1;
      safeMetric(config.onMetric, {
        type: "trigger_rejected",
        ts: Date.now(),
        scheduleId: cfg.id,
        reason: "invalid_schedule",
      });
      throw new Error(`scheduler trigger rejected: invalid schedule ${cfg.id}`);
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
      throw err;
    }
  };

  const get = async (cfg: SchedulerGetConfig): Promise<SchedulerInfo | null> => {
    const raw = await redis.get(scheduleKey(cfg.id));
    const parsed = parseSchedule(raw);
    if (!parsed) return null;
    return asInfo(parsed);
  };

  const list = async (): Promise<SchedulerInfo[]> => {
    const idsRaw = await redis.send("SMEMBERS", [indexKey]);
    if (!Array.isArray(idsRaw) || idsRaw.length === 0) return [];
    const ids = idsRaw.map((v) => String(v));
    const values = await redis.send("MGET", ids.map((id) => scheduleKey(id)));
    if (!Array.isArray(values)) return [];

    const out: SchedulerInfo[] = [];
    for (const raw of values) {
      const parsed = parseSchedule(typeof raw === "string" ? raw : null);
      if (!parsed) continue;
      out.push(asInfo(parsed));
    }
    out.sort((a, b) => a.id.localeCompare(b.id));
    return out;
  };

  const start = (): void => {
    if (running) return;
    running = true;
    loopPromise = loop();
  };

  const stop = async (): Promise<void> => {
    if (!running) return;
    running = false;
    await loopPromise;
    loopPromise = null;
    if (currentLeaderLock) {
      try {
        await leaderMutex.release(currentLeaderLock);
      } catch {
        // best effort
      }
      currentLeaderLock = null;
      currentLeaderEpoch = null;
    }
    setLeader(false, "stop");
  };

  const metrics = (): SchedulerMetricsSnapshot => ({ ...metricsState });

  return {
    id: config.id,
    start,
    stop,
    register,
    unregister,
    triggerNow,
    get,
    list,
    metrics,
  };
};
