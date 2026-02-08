import { redis, RedisClient, sleep } from "bun";
import { randomBytes } from "crypto";
import type { z } from "zod";

// ==========================
// Types
// ==========================

type JobStatus = "waiting" | "delayed" | "active" | "completed" | "failed";

export type Job<T> = {
  id: string;
  data: T;
  status: JobStatus;
  attempts: number;
  maxRetries: number;
  timeout: number;
  backoff?: number;
  interval?: number;
  tag?: string;
  key?: string;
  createdAt: number;
  scheduledAt?: number;
  startedAt?: number;
  completedAt?: number;
  error?: string;
  invalidData?: boolean;
};

type BaseOptions = {
  /** Number of retries on failure (default: 0) */
  retries?: number;
  /** Max processing time in ms before job is marked as failed (default: 30000) */
  timeout?: number;
  /** Base delay in ms for exponential backoff between retries (default: none, immediate retry) */
  backoff?: number;
  /** Optional tag for filtering with list() */
  tag?: string;
  /** Optional deduplication key. Only one job with this key can be active (waiting/delayed/active) at a time. */
  key?: string;
};

type DelayOptions = BaseOptions & {
  /** Delay execution by this many milliseconds */
  delay: number;
  at?: never;
  interval?: never;
  startImmediately?: never;
};

type AtOptions = BaseOptions & {
  /** Schedule execution at this timestamp (ms since epoch) */
  at: number;
  delay?: never;
  interval?: never;
  startImmediately?: never;
};

type IntervalOptions = BaseOptions & {
  /** Repeat job every X milliseconds (for periodic tasks) */
  interval: number;
  /** Start first run immediately instead of waiting for first interval (default: false) */
  startImmediately?: boolean;
  delay?: never;
  at?: never;
};

type ImmediateOptions = BaseOptions & {
  delay?: never;
  at?: never;
  interval?: never;
  startImmediately?: never;
};

export type SendOptions = DelayOptions | AtOptions | IntervalOptions | ImmediateOptions;

export type ListOptions = {
  /** Filter by tag */
  tag?: string;
  /** Filter by status */
  status?: JobStatus;
  /** Pagination offset (default: 0) */
  offset?: number;
  /** Max results (default: 100) */
  limit?: number;
};

export type ListResult<T> = {
  total: number;
  jobs: Job<T>[];
};

export type JobsConfig<T extends z.ZodType> = {
  /** Job queue name (used as Redis key prefix) */
  name: string;
  /** Zod schema for job data validation */
  schema: T;
  /** Prefix for all Redis keys (default: "jobs") */
  prefix?: string;
};

export type ProcessOptions = {
  /** Number of concurrent jobs to process (default: 1) */
  concurrency?: number;
  /** Blocking read timeout in seconds (overrides pollInterval if set) */
  blockingTimeoutSecs?: number;
  /** Blocking read timeout fallback in ms (default: 1000) */
  pollInterval?: number;
  /** Maintenance loop interval in ms (default: pollInterval) */
  maintenanceIntervalMs?: number;
  /** Called when a job completes successfully */
  onSuccess?: (job: Job<unknown>) => void | Promise<void>;
  /** Called when a job fails (after all retries exhausted, or interval job rescheduled after failure) */
  onError?: (job: Job<unknown>, error: Error) => void | Promise<void>;
  /** Called after every job attempt (success or failure) */
  onFinally?: (job: Job<unknown>) => void | Promise<void>;
};

export type Jobs<T> = {
  /** Send a job to the queue */
  send: (data: T, options?: SendOptions) => Promise<Job<T>>;
  /** Process jobs from the queue. Returns a stop function that waits for active jobs to finish. */
  process: (handler: (job: Job<T>) => Promise<void> | void, options?: ProcessOptions) => () => Promise<void>;
  /** List jobs with optional filtering */
  list: (options?: ListOptions) => Promise<ListResult<T>>;
  /** Cancel a job by ID */
  cancel: (jobId: string) => Promise<boolean>;
};

// ==========================
// Defaults
// ==========================

const DEFAULT_TIMEOUT = 30000;
const DEFAULT_RETRIES = 0;
const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_CONCURRENCY = 1;

// ==========================
// Errors
// ==========================

export class JobsError extends Error {
  readonly code: string;

  constructor(message: string, code: string) {
    super(message);
    this.name = "JobsError";
    this.code = code;
  }
}

export class ValidationError extends JobsError {
  readonly issues: z.ZodIssue[];

  constructor(issues: z.ZodIssue[]) {
    super("Job data validation failed", "VALIDATION_ERROR");
    this.name = "ValidationError";
    this.issues = issues;
  }
}

export class DuplicateJobError extends JobsError {
  readonly key: string;

  constructor(key: string) {
    super(`Job with key "${key}" already exists`, "DUPLICATE_JOB");
    this.name = "DuplicateJobError";
    this.key = key;
  }
}

// ==========================
// Lua Scripts for Atomic Operations
// ==========================

// Atomically claim a job by ID (already moved to active) and update status
const CLAIM_JOB_BY_ID_SCRIPT = `
  local jobId = ARGV[1]
  local jobData = redis.call("HGET", KEYS[1], jobId)
  if not jobData then
    redis.call("LREM", KEYS[2], 1, jobId)
    return nil
  end

  local ok, job = pcall(cjson.decode, jobData)
  if not ok then
    local now = tonumber(ARGV[2])
    local failedJob = {
      id = jobId,
      data = cjson.null,
      status = "failed",
      attempts = 1,
      maxRetries = 0,
      timeout = 0,
      createdAt = now,
      completedAt = now,
      error = "Invalid job payload in Redis",
      invalidData = true
    }

    redis.call("LREM", KEYS[2], 1, jobId)
    redis.call("HSET", KEYS[1], jobId, cjson.encode(failedJob))
    redis.call("SADD", KEYS[4], jobId)

    return {jobId, cjson.encode(failedJob), "invalid"}
  end

  job.status = "active"
  job.startedAt = tonumber(ARGV[2])
  job.attempts = job.attempts + 1

  redis.call("HSET", KEYS[1], jobId, cjson.encode(job))

  local deadline = tonumber(ARGV[2]) + job.timeout
  redis.call("ZADD", KEYS[3], deadline, jobId)

  return {jobId, cjson.encode(job)}
`;

// Atomically complete a job (and reschedule if interval)
// KEYS[6] = unique keys SET
const COMPLETE_JOB_SCRIPT = `
  local jobId = ARGV[1]
  local completedAt = tonumber(ARGV[2])

  local jobData = redis.call("HGET", KEYS[1], jobId)
  if not jobData then
    return 0
  end

  local job = cjson.decode(jobData)

  redis.call("LREM", KEYS[2], 1, jobId)
  redis.call("ZREM", KEYS[4], jobId)

  -- If job has interval, reschedule it (keep unique key active)
  if job.interval and job.interval > 0 then
    job.status = "delayed"
    job.attempts = 0
    job.error = cjson.null
    job.startedAt = cjson.null
    job.completedAt = cjson.null
    job.scheduledAt = completedAt + job.interval

    redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
    redis.call("ZADD", KEYS[5], job.scheduledAt, jobId)
    return 2
  else
    job.status = "completed"
    job.completedAt = completedAt

    redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
    redis.call("SADD", KEYS[3], jobId)

    -- Release unique key
    if job.key then
      redis.call("HDEL", KEYS[6], job.key)
    end

    return 1
  end
`;

// Atomically fail or retry a job
// Returns: 1 = retried (immediate), 2 = failed permanently, 3 = interval rescheduled, 4 = retried (delayed with backoff)
const FAIL_OR_RETRY_SCRIPT = `
  local jobId = ARGV[1]
  local errorMsg = ARGV[2]
  local completedAt = tonumber(ARGV[3])

  local jobData = redis.call("HGET", KEYS[1], jobId)
  if not jobData then
    return 0
  end

  local job = cjson.decode(jobData)
  job.error = errorMsg

  redis.call("LREM", KEYS[2], 1, jobId)
  redis.call("ZREM", KEYS[5], jobId)

  -- Check if we can retry (attempts <= maxRetries means 1 initial + N retries)
  if job.attempts <= job.maxRetries then
    -- Check for backoff
    if job.backoff and job.backoff > 0 then
      -- Exponential backoff: backoff * 2^(attempts-1)
      local delay = job.backoff * math.pow(2, job.attempts - 1)
      job.status = "delayed"
      job.scheduledAt = completedAt + delay
      redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
      redis.call("ZADD", KEYS[6], job.scheduledAt, jobId)
      return 4
    else
      job.status = "waiting"
      redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
      redis.call("LPUSH", KEYS[3], jobId)
      return 1
    end
  end

  -- No more retries - check if interval job should be rescheduled
  if job.interval and job.interval > 0 then
    job.status = "delayed"
    job.attempts = 0
    job.startedAt = cjson.null
    job.scheduledAt = completedAt + job.interval
    redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
    redis.call("ZADD", KEYS[6], job.scheduledAt, jobId)
    return 3
  end

  -- Permanent failure
  job.status = "failed"
  job.completedAt = completedAt
  redis.call("HSET", KEYS[1], jobId, cjson.encode(job))
  redis.call("SADD", KEYS[4], jobId)

  -- Release unique key
  if job.key then
    redis.call("HDEL", KEYS[7], job.key)
  end

  return 2
`;

// Fail timed out jobs (hard fail, no recovery)
// KEYS[5] = unique keys HASH
const FAIL_TIMED_OUT_SCRIPT = `
  local now = tonumber(ARGV[1])

  local timedOutJobs = redis.call("ZRANGEBYSCORE", KEYS[1], "0", tostring(now))
  local failed = 0

  for _, jobId in ipairs(timedOutJobs) do
    local jobData = redis.call("HGET", KEYS[2], jobId)
    if jobData then
      local job = cjson.decode(jobData)

      redis.call("ZREM", KEYS[1], jobId)
      redis.call("LREM", KEYS[3], 0, jobId)

      job.status = "failed"
      job.completedAt = now
      job.error = "Job timed out"
      redis.call("HSET", KEYS[2], jobId, cjson.encode(job))
      redis.call("SADD", KEYS[4], jobId)

      -- Release unique key
      if job.key then
        redis.call("HDEL", KEYS[5], job.key)
      end

      failed = failed + 1
    else
      redis.call("ZREM", KEYS[1], jobId)
    end
  end

  return failed
`;

// Recover stuck active jobs after worker crash/restart
// Returns: number of recovered jobs
// KEYS[7] = unique keys HASH
const RECOVER_ACTIVE_JOBS_SCRIPT = `
  local now = tonumber(ARGV[1])
  local activeJobs = redis.call("LRANGE", KEYS[1], 0, -1)
  local recovered = 0

  for _, jobId in ipairs(activeJobs) do
    local jobData = redis.call("HGET", KEYS[2], jobId)
    if jobData then
      local job = cjson.decode(jobData)
      local startedAt = tonumber(job.startedAt) or 0
      local timeout = tonumber(job.timeout) or 30000

      if startedAt > 0 and (startedAt + timeout) < now then
        redis.call("LREM", KEYS[1], 1, jobId)
        redis.call("ZREM", KEYS[3], jobId)
        job.error = "Recovered after worker failure"

        if job.attempts <= job.maxRetries then
          job.status = "waiting"
          redis.call("HSET", KEYS[2], jobId, cjson.encode(job))
          redis.call("LPUSH", KEYS[4], jobId)
        elseif job.interval and job.interval > 0 then
          job.status = "delayed"
          job.attempts = 0
          job.startedAt = cjson.null
          job.scheduledAt = now + job.interval
          redis.call("HSET", KEYS[2], jobId, cjson.encode(job))
          redis.call("ZADD", KEYS[6], job.scheduledAt, jobId)
        else
          job.status = "failed"
          job.completedAt = now
          redis.call("HSET", KEYS[2], jobId, cjson.encode(job))
          redis.call("SADD", KEYS[5], jobId)

          -- Release unique key on permanent failure
          if job.key then
            redis.call("HDEL", KEYS[7], job.key)
          end
        end
        recovered = recovered + 1
      end
    else
      redis.call("LREM", KEYS[1], 1, jobId)
      redis.call("ZREM", KEYS[3], jobId)
    end
  end

  return recovered
`;

// List jobs with optional tag and status filtering (all server-side)
const LIST_JOBS_SCRIPT = `
  local filterByTag = ARGV[1] == "1"
  local filterByStatus = ARGV[2] == "1"
  local statusFilter = ARGV[3]
  local offset = tonumber(ARGV[4])
  local limit = tonumber(ARGV[5])

  local candidateIds
  if filterByTag then
    candidateIds = redis.call("SMEMBERS", KEYS[2])
  else
    candidateIds = redis.call("HKEYS", KEYS[1])
  end

  local matched = {}
  for _, jobId in ipairs(candidateIds) do
    local jobData = redis.call("HGET", KEYS[1], jobId)
    if jobData then
      if filterByStatus then
        local job = cjson.decode(jobData)
        if job.status == statusFilter then
          matched[#matched + 1] = { id = jobId, data = jobData }
        end
      else
        matched[#matched + 1] = { id = jobId, data = jobData }
      end
    end
  end

  table.sort(matched, function(a, b) return a.id < b.id end)

  local total = #matched
  local startIdx = offset + 1
  local endIdx = math.min(startIdx + limit - 1, total)

  local results = {}
  for i = startIdx, endIdx do
    results[#results + 1] = matched[i].data
  end

  if #results == 0 then
    return { total }
  end
  return { total, unpack(results) }
`;

// Cancel a job atomically (remove from all data structures)
// Returns: 1 = cancelled, 0 = not found
const CANCEL_JOB_SCRIPT = `
  local jobId = ARGV[1]
  local prefix = ARGV[2]

  local jobData = redis.call("HGET", KEYS[1], jobId)
  if not jobData then
    return 0
  end

  local job = cjson.decode(jobData)

  if job.status == "waiting" then
    redis.call("LREM", KEYS[2], 0, jobId)
  elseif job.status == "delayed" then
    redis.call("ZREM", KEYS[3], jobId)
  elseif job.status == "active" then
    redis.call("LREM", KEYS[4], 0, jobId)
    redis.call("ZREM", KEYS[5], jobId)
  elseif job.status == "completed" then
    redis.call("SREM", KEYS[6], jobId)
  elseif job.status == "failed" then
    redis.call("SREM", KEYS[7], jobId)
  end

  redis.call("HDEL", KEYS[1], jobId)

  if job.tag then
    redis.call("SREM", prefix .. ":tag:" .. job.tag, jobId)
  end

  -- Release unique key
  if job.key then
    redis.call("HDEL", KEYS[8], job.key)
  end

  return 1
`;

// Promote delayed jobs that are ready
const PROMOTE_DELAYED_SCRIPT = `
  local now = tonumber(ARGV[1])
  local readyJobs = redis.call("ZRANGEBYSCORE", KEYS[1], "0", tostring(now))
  local promoted = 0

  for _, jobId in ipairs(readyJobs) do
    local removed = redis.call("ZREM", KEYS[1], jobId)
    if removed > 0 then
      local jobData = redis.call("HGET", KEYS[3], jobId)
      if jobData then
        local job = cjson.decode(jobData)
        job.status = "waiting"
        redis.call("HSET", KEYS[3], jobId, cjson.encode(job))
        redis.call("LPUSH", KEYS[2], jobId)
        promoted = promoted + 1
      end
    end
  end

  return promoted
`;

// ==========================
// Jobs Factory
// ==========================

/**
 * Create a typed job queue with Zod schema validation.
 * All state is stored in Redis, making it safe to use across multiple processes.
 *
 * @example
 * ```ts
 * import { z } from "zod";
 * import { jobs } from "@valentinkolb/sync";
 *
 * const emailJobs = jobs.create({
 *   name: "emails",
 *   schema: z.object({
 *     to: z.string().email(),
 *     subject: z.string(),
 *     body: z.string(),
 *   }),
 * });
 *
 * // Send a job immediately
 * await emailJobs.send({ to: "user@example.com", subject: "Hello", body: "World" });
 *
 * // Send with delay and retries
 * await emailJobs.send(data, { delay: 5000, retries: 3 });
 *
 * // Send scheduled with custom timeout
 * await emailJobs.send(data, { at: Date.now() + 60000, timeout: 60000 });
 *
 * // Send periodic job (runs every hour)
 * await emailJobs.send(data, { interval: 3600000 });
 *
 * // Process jobs (in worker process)
 * const stop = emailJobs.process(async (job) => {
 *   await sendEmail(job.data);
 * }, { concurrency: 10 });
 *
 * // Stop processing
 * stop();
 * ```
 */
const create = <T extends z.ZodType>(config: JobsConfig<T>): Jobs<z.infer<T>> => {
  type Data = z.infer<T>;

  const { name, schema, prefix = "jobs" } = config;

  // Redis keys
  const keyPrefix = `${prefix}:${name}`;
  const keys = {
    jobs: `${keyPrefix}:data`,
    waiting: `${keyPrefix}:waiting`,
    delayed: `${keyPrefix}:delayed`,
    active: `${keyPrefix}:active`,
    deadlines: `${keyPrefix}:deadlines`,
    completed: `${keyPrefix}:completed`,
    failed: `${keyPrefix}:failed`,
    id: `${keyPrefix}:id`,
    tags: `${keyPrefix}:tags`,
    unique: `${keyPrefix}:unique`,
  };

  // ==========================
  // Internal Helpers
  // ==========================

  const generateId = async (): Promise<string> => {
    const counter = await redis.incr(keys.id);
    const random = randomBytes(4).toString("hex");
    return `${counter}-${random}`;
  };

  const deserializeJob = (data: string): Job<Data> => {
    let job: Job<Data>;
    try {
      job = JSON.parse(data) as Job<Data>;
    } catch (error) {
      throw new JobsError("Invalid job payload in Redis", "INVALID_JOB_DATA");
    }

    if (job.invalidData) {
      return job;
    }

    const result = schema.safeParse(job.data);
    if (!result.success) {
      throw new ValidationError(result.error.issues);
    }
    return job;
  };

  const markPoisonedJob = async (jobId: string, jobData: string, error: Error): Promise<void> => {
    const now = Date.now();
    let parsed: Job<Data> | null = null;

    try {
      parsed = JSON.parse(jobData) as Job<Data>;
    } catch {
      parsed = null;
    }

    const poisoned: Job<Data> = parsed
      ? {
          ...parsed,
          status: "failed",
          completedAt: now,
          error: error.message,
          invalidData: true,
        }
      : {
          id: jobId,
          data: null as unknown as Data,
          status: "failed",
          attempts: 1,
          maxRetries: 0,
          timeout: 0,
          createdAt: now,
          completedAt: now,
          error: error.message,
          invalidData: true,
        };

    await redis.send("LREM", [keys.active, "0", jobId]);
    await redis.send("ZREM", [keys.deadlines, jobId]);
    await redis.send("HSET", [keys.jobs, jobId, JSON.stringify(poisoned)]);
    await redis.send("SADD", [keys.failed, jobId]);

    if (parsed?.key) {
      await redis.send("HDEL", [keys.unique, parsed.key]);
    }
  };

  // ==========================
  // Public API
  // ==========================

  const send = async (data: Data, options: SendOptions = {}): Promise<Job<Data>> => {
    const result = schema.safeParse(data);
    if (!result.success) {
      throw new ValidationError(result.error.issues);
    }

    const id = await generateId();
    const now = Date.now();

    // Extract interval from options (TypeScript knows it's only present in IntervalOptions)
    const interval = "interval" in options ? options.interval : undefined;
    const startImmediately = "startImmediately" in options ? options.startImmediately : undefined;
    const tag = options.tag;
    const key = options.key;
    const backoff = options.backoff;

    // Deduplication check
    if (key) {
      const added = await redis.send("HSETNX", [keys.unique, key, id]);
      if (added === 0) {
        throw new DuplicateJobError(key);
      }
    }

    const job: Job<Data> = {
      id,
      data: result.data,
      status: "waiting",
      attempts: 0,
      maxRetries: options.retries ?? DEFAULT_RETRIES,
      timeout: options.timeout ?? DEFAULT_TIMEOUT,
      backoff,
      interval,
      tag,
      key,
      createdAt: now,
    };

    // Handle scheduling
    if ("delay" in options && options.delay !== undefined) {
      // Delayed job
      job.status = "delayed";
      job.scheduledAt = now + options.delay;
      await redis.hset(keys.jobs, id, JSON.stringify(job));
      await redis.send("ZADD", [keys.delayed, job.scheduledAt.toString(), id]);
    } else if ("at" in options && options.at !== undefined) {
      // Scheduled job
      job.status = "delayed";
      job.scheduledAt = options.at;
      await redis.hset(keys.jobs, id, JSON.stringify(job));
      await redis.send("ZADD", [keys.delayed, job.scheduledAt.toString(), id]);
    } else if (interval !== undefined && !startImmediately) {
      // Interval job, start after first interval
      job.status = "delayed";
      job.scheduledAt = now + interval;
      await redis.hset(keys.jobs, id, JSON.stringify(job));
      await redis.send("ZADD", [keys.delayed, job.scheduledAt.toString(), id]);
    } else {
      // Immediate job (or interval with startImmediately)
      await redis.hset(keys.jobs, id, JSON.stringify(job));
      await redis.lpush(keys.waiting, id);
    }

    // Index tag
    if (tag) {
      await redis.sadd(keys.tags, tag);
      await redis.sadd(`${keyPrefix}:tag:${tag}`, id);
    }

    return job;
  };

  const process = (
    handler: (job: Job<Data>) => Promise<void> | void,
    options: ProcessOptions = {},
  ): (() => Promise<void>) => {
    const {
      concurrency = DEFAULT_CONCURRENCY,
      blockingTimeoutSecs,
      pollInterval,
      maintenanceIntervalMs,
      onSuccess,
      onError,
      onFinally,
    } = options;

    const blockingTimeoutSecsValue =
      blockingTimeoutSecs ?? Math.max(1, Math.ceil((pollInterval ?? DEFAULT_POLL_INTERVAL) / 1000));
    const maintenanceIntervalMsValue = Math.max(
      10,
      maintenanceIntervalMs ?? pollInterval ?? DEFAULT_POLL_INTERVAL,
    );

    let running = true;
    let activeCount = 0;
    let drainResolve: (() => void) | null = null;

    // Recover stuck active jobs from previous crashes
    redis
      .send("EVAL", [
        RECOVER_ACTIVE_JOBS_SCRIPT,
        "7",
        keys.active,
        keys.jobs,
        keys.deadlines,
        keys.waiting,
        keys.failed,
        keys.delayed,
        keys.unique,
        Date.now().toString(),
      ])
      .catch((err) => {
        console.error("Recovery of active jobs failed:", err);
      });

    const processLoop = async (client: RedisClient): Promise<void> => {
      if (!client.connected) {
        await client.connect();
      }

      while (running) {
        try {
          const jobId = (await client.send("BRPOPLPUSH", [
            keys.waiting,
            keys.active,
            blockingTimeoutSecsValue.toString(),
          ])) as string | null;

          if (!jobId) {
            continue;
          }

          const now = Date.now();
          const claimResult = (await redis.send("EVAL", [
            CLAIM_JOB_BY_ID_SCRIPT,
            "4",
            keys.jobs,
            keys.active,
            keys.deadlines,
            keys.failed,
            jobId,
            now.toString(),
          ])) as [string, string, string?] | null;

          if (!claimResult) {
            continue;
          }

          const [, jobData, claimStatus] = claimResult;
          if (claimStatus === "invalid") {
            continue;
          }
          let job: Job<Data>;

          try {
            job = deserializeJob(jobData);
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));
            await markPoisonedJob(jobId, jobData, err);
            continue;
          }
          activeCount++;

          try {
            await handler(job);

            await redis.send("EVAL", [
              COMPLETE_JOB_SCRIPT,
              "6",
              keys.jobs,
              keys.active,
              keys.completed,
              keys.deadlines,
              keys.delayed,
              keys.unique,
              job.id,
              Date.now().toString(),
            ]);

            // Call onSuccess
            if (onSuccess) {
              try {
                await onSuccess(job);
              } catch (onSuccessError) {
                console.error("onSuccess callback failed:", onSuccessError);
              }
            }
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));

            // Returns: 1 = retried (immediate), 2 = failed permanently, 3 = interval rescheduled, 4 = retried (backoff)
            const result = (await redis.send("EVAL", [
              FAIL_OR_RETRY_SCRIPT,
              "7",
              keys.jobs,
              keys.active,
              keys.waiting,
              keys.failed,
              keys.deadlines,
              keys.delayed,
              keys.unique,
              job.id,
              err.message,
              Date.now().toString(),
            ])) as number;

            // Call onError if job permanently failed or interval rescheduled after failure
            if (onError && (result === 2 || result === 3)) {
              try {
                await onError(job, err);
              } catch (onErrorError) {
                console.error("onError callback failed:", onErrorError);
              }
            }
          } finally {
            activeCount--;

            // Call onFinally after every attempt
            if (onFinally) {
              try {
                await onFinally(job);
              } catch (onFinallyError) {
                console.error("onFinally callback failed:", onFinallyError);
              }
            }

            // Signal drain if shutting down and no active jobs
            if (!running && activeCount === 0 && drainResolve) {
              drainResolve();
            }
          }
        } catch (error) {
          if (!running) break;
          console.error("Worker error:", error);
          await sleep(100);
        }
      }
    };

    // Start workers
    const workerPromises: Promise<void>[] = [];
    const workerClients: RedisClient[] = [];
    for (let i = 0; i < concurrency; i++) {
      const workerClient = new RedisClient();
      workerClients.push(workerClient);
      workerPromises.push(processLoop(workerClient));
    }

    const maintenanceLoop = async (): Promise<void> => {
      while (running) {
        try {
          await redis.send("EVAL", [
            PROMOTE_DELAYED_SCRIPT,
            "3",
            keys.delayed,
            keys.waiting,
            keys.jobs,
            Date.now().toString(),
          ]);

          await redis.send("EVAL", [
            FAIL_TIMED_OUT_SCRIPT,
            "5",
            keys.deadlines,
            keys.jobs,
            keys.active,
            keys.failed,
            keys.unique,
            Date.now().toString(),
          ]);
        } catch (error) {
          if (!running) break;
          console.error("Worker error:", error);
        }

        await sleep(maintenanceIntervalMsValue);
      }
    };

    maintenanceLoop();

    const safeClose = (client: RedisClient): void => {
      if (!client.connected) return;
      try {
        client.close();
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        if (!("code" in err) || (err as Error & { code?: string }).code !== "ERR_REDIS_CONNECTION_CLOSED") {
          console.error("Failed to close worker client:", error);
        }
      }
    };

    // Return graceful stop function
    return async () => {
      running = false;
      for (const client of workerClients) {
        safeClose(client);
      }
      if (activeCount > 0) {
        await new Promise<void>((resolve) => {
          drainResolve = resolve;
        });
      }
    };
  };

  const list = async (options: ListOptions = {}): Promise<ListResult<Data>> => {
    const { tag, status, offset = 0, limit = 100 } = options;

    const filterByTag = tag ? "1" : "0";
    const filterByStatus = status ? "1" : "0";
    const tagKey = tag ? `${keyPrefix}:tag:${tag}` : "";

    const result = (await redis.send("EVAL", [
      LIST_JOBS_SCRIPT,
      "2",
      keys.jobs,
      tagKey,
      filterByTag,
      filterByStatus,
      status ?? "",
      offset.toString(),
      limit.toString(),
    ])) as [number, ...string[]];

    const [total, ...jobStrings] = result;

    const jobList: Job<Data>[] = jobStrings.map((str) => deserializeJob(str));

    return { total, jobs: jobList };
  };

  const cancel = async (jobId: string): Promise<boolean> => {
    const result = (await redis.send("EVAL", [
      CANCEL_JOB_SCRIPT,
      "8",
      keys.jobs,
      keys.waiting,
      keys.delayed,
      keys.active,
      keys.deadlines,
      keys.completed,
      keys.failed,
      keys.unique,
      jobId,
      keyPrefix,
    ])) as number;

    return result === 1;
  };

  return { send, process, list, cancel };
};

// ==========================
// Export
// ==========================

export const jobs = { create };
