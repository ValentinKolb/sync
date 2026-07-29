import { redis, sleep } from "bun";
import { queue } from "./queue";
import { expBackoff, isRetryableTransportError, retry, type BackoffOptions } from "./retry";
import { emitTrace, type TraceHandler } from "./trace";

const DEFAULT_PREFIX = "sync:job";
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_KEY_TTL_MS = 24 * 60 * 60 * 1000;
const MIN_KEY_TTL_MS = 1_000;
const MAX_KEY_TTL_MS = 30 * DAY_MS;
const MAX_JOB_DELAY_MS = MAX_KEY_TTL_MS - MIN_KEY_TTL_MS;
const MAX_JOB_MESSAGE_AGE_MS = Number.MAX_SAFE_INTEGER;
// How long a claim may stay unenqueued before a later submit re-enqueues it.
// Generous relative to the single `queue.send` it covers, and far below any TTL.
const ENQUEUE_GRACE_MS = 30_000;

// Claim the idempotency key atomically. Returns { jobId, isNew }.
//
// The stored value records whether the work message actually reached the queue.
// A pod that died between claiming the key and enqueuing used to strand the key
// with no message behind it: every later submit returned that orphaned jobId and
// enqueued nothing, for up to keyTtlMs. A claim that is still unenqueued past
// the grace window is therefore handed back as new so the caller re-enqueues it,
// keeping the original jobId valid.
//
// Values written by <= 5.8.0 are the bare jobId; those are treated as enqueued.
const CLAIM_KEY_SCRIPT = `
  local idemKey = KEYS[1]
  local seqKey = KEYS[2]
  local ttlMs = tonumber(ARGV[1])
  local now = tonumber(ARGV[2])
  local graceMs = tonumber(ARGV[3])

  local existing = redis.call("GET", idemKey)
  if existing then
    local ok, record = pcall(cjson.decode, existing)
    if not ok or type(record) ~= "table" or not record.jobId then
      return { existing, 0 }
    end
    if record.enqueued then
      return { record.jobId, 0 }
    end
    if (now - (tonumber(record.claimedAt) or 0)) <= graceMs then
      -- A concurrent submit is still mid-flight; do not enqueue a second time.
      return { record.jobId, 0 }
    end
    record.claimedAt = now
    redis.call("SET", idemKey, cjson.encode(record), "PX", tostring(ttlMs))
    return { record.jobId, 1 }
  end

  local nextId = tostring(redis.call("INCR", seqKey))
  redis.call("SET", idemKey, cjson.encode({ jobId = nextId, enqueued = false, claimedAt = now }), "PX", tostring(ttlMs))
  return { nextId, 1 }
`;

// Mark the claim as backed by a real queue message, and refresh its TTL.
const MARK_ENQUEUED_SCRIPT = `
  local existing = redis.call("GET", KEYS[1])
  if not existing then return 0 end
  local ok, record = pcall(cjson.decode, existing)
  if not ok or type(record) ~= "table" or record.jobId ~= ARGV[1] then return 0 end
  record.enqueued = true
  redis.call("SET", KEYS[1], cjson.encode(record), "PX", tostring(ARGV[2]))
  return 1
`;

// Release only our own claim. An unconditional DEL let a slow job's terminal
// release delete the claim a later submit had already taken over.
const RELEASE_KEY_SCRIPT = `
  local existing = redis.call("GET", KEYS[1])
  if not existing then return 0 end
  local ok, record = pcall(cjson.decode, existing)
  if ok and type(record) == "table" and record.jobId then
    if record.jobId ~= ARGV[1] then return 0 end
  elseif existing ~= ARGV[1] then
    return 0
  end
  redis.call("DEL", KEYS[1])
  return 1
`;

// Keep a claim alive while its job is still running or waiting to retry.
const REFRESH_KEY_SCRIPT = `
  local existing = redis.call("GET", KEYS[1])
  if not existing then return 0 end
  local ok, record = pcall(cjson.decode, existing)
  if ok and type(record) == "table" and record.jobId then
    if record.jobId ~= ARGV[1] then return 0 end
  elseif existing ~= ARGV[1] then
    return 0
  end
  redis.call("PEXPIRE", KEYS[1], tostring(ARGV[2]))
  return 1
`;

// ==========================
// Types
// ==========================

export type JobId = string;

export type JobMetrics = {
  dispatches: number;
  failures: number;
  reschedules: number;
};

export type JobTraceEvent<Input = void, Result = unknown> =
  | { type: "submitted"; jobId: string; key: string; input?: Input; meta?: Record<string, unknown> }
  | { type: "started"; jobId: string; key: string; input?: Input; attempt: number }
  | { type: "succeeded"; jobId: string; key: string; input?: Input; data: Result; durationMs: number }
  | { type: "failed"; jobId: string; key: string; input?: Input; error: Error; durationMs: number }
  | { type: "rescheduled"; jobId: string; key: string; attempt: number; delayMs: number }
  | { type: "finished"; jobId: string; key: string; status: "succeeded" | "failed"; durationMs: number };

export type JobCtx<Input = void> = {
  jobId: JobId;
  key: string;
  input: Input;
  failureCount: number;
  readonly duration: number;
  signal: AbortSignal;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
};

export type JobAfterCtx<Input = void, Result = unknown> = JobCtx<Input> & {
  data?: Result;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: JobMetrics;
};

export type SubmitConfig<Input = void> = {
  key: string;
  keyTtlMs?: number;
  delayMs?: number;
  at?: number;
  leaseMs?: number;
  meta?: Record<string, unknown>;
} & (Input extends void ? { input?: Input } : { input: Input });

export type JobConfig<Input = void, Result = unknown> = {
  id: string;
  prefix?: string;
  defaults?: {
    leaseMs?: number;
    keyTtlMs?: number;
  };
  trace?: TraceHandler<JobTraceEvent<Input, Result>>;
  process: (cfg: { ctx: JobCtx<Input> }) => Promise<Result> | Result;
  after?: (cfg: { ctx: JobAfterCtx<Input, Result> }) => Promise<void> | void;
};

export type JobHandle<Input = void> = {
  id: string;
  submit(cfg: SubmitConfig<Input>): Promise<JobId>;
  metric(): JobMetrics;
  stop(): void;
};

// ==========================
// Internal
// ==========================

type WorkPayload = {
  jobId: JobId;
  key: string;
  input?: unknown;
  keyTtlMs: number;
  leaseMs: number;
  meta?: Record<string, unknown>;
};

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));
const requireSafeInteger = (name: string, value: number, min: number, max = Number.MAX_SAFE_INTEGER): number => {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be a safe integer between ${min} and ${max}`);
  }
  return value;
};

const requireFutureDuration = (name: string, value: number, min: number): number =>
  requireSafeInteger(name, value, min, Number.MAX_SAFE_INTEGER - Date.now());

const resolveDelayMs = (name: string, value = 0): number =>
  requireSafeInteger(name, value, 0, MAX_JOB_DELAY_MS);

// ==========================
// Factory
// ==========================

export const job = <Input = void, Result = unknown>(
  config: JobConfig<Input, Result>,
): JobHandle<Input> => {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const defaultLeaseMs = requireFutureDuration(
    "defaults.leaseMs",
    config.defaults?.leaseMs ?? DEFAULT_LEASE_MS,
    1,
  );
  const defaultKeyTtlMs = requireSafeInteger(
    "defaults.keyTtlMs",
    config.defaults?.keyTtlMs ?? DEFAULT_KEY_TTL_MS,
    MIN_KEY_TTL_MS,
    MAX_KEY_TTL_MS,
  );

  const keys = {
    seq: `${prefix}:${config.id}:seq`,
    idempotencyPrefix: `${prefix}:${config.id}:idempotency`,
  };

  const idempotencyKey = (key: string): string => `${keys.idempotencyPrefix}:${key}`;

  const workQueue = queue<WorkPayload>({
    id: `${config.id}:work`,
    prefix: `${prefix}:queue`,
    delivery: {
      defaultLeaseMs,
      maxDeliveries: Number.MAX_SAFE_INTEGER, // reschedule is user-controlled via ctx.reschedule
    },
    limits: {
      maxMessageAgeMs: MAX_JOB_MESSAGE_AGE_MS,
      maxNackDelayMs: MAX_JOB_DELAY_MS,
    },
  });

  const metrics: JobMetrics = {
    dispatches: 0,
    failures: 0,
    reschedules: 0,
  };

  const evalKeyScript = async (script: string, key: string, args: string[]): Promise<unknown> =>
    await redis.send("EVAL", [script, "1", idempotencyKey(key), ...args]);

  const releaseKey = async (key: string, jobId: JobId): Promise<void> => {
    try {
      await evalKeyScript(RELEASE_KEY_SCRIPT, key, [jobId]);
    } catch {
      // best effort — if release fails, TTL will reclaim eventually
    }
  };

  const extendKey = async (key: string, jobId: JobId, keyTtlMs: number): Promise<boolean> => {
    const result = await evalKeyScript(REFRESH_KEY_SCRIPT, key, [jobId, String(keyTtlMs)]);
    return Number(result) > 0;
  };

  const refreshKey = async (key: string, jobId: JobId, keyTtlMs: number): Promise<void> => {
    try {
      await extendKey(key, jobId, keyTtlMs);
    } catch {
      // best effort — a missed refresh only shortens the dedup window
    }
  };

  const markEnqueued = async (key: string, jobId: JobId, keyTtlMs: number): Promise<void> => {
    await evalKeyScript(MARK_ENQUEUED_SCRIPT, key, [jobId, String(keyTtlMs)]);
  };

  const claimKey = async (key: string, keyTtlMs: number): Promise<{ jobId: JobId; isNew: boolean }> => {
    const result = await redis.send("EVAL", [
      CLAIM_KEY_SCRIPT,
      "2",
      idempotencyKey(key),
      keys.seq,
      String(keyTtlMs),
      String(Date.now()),
      String(ENQUEUE_GRACE_MS),
    ]);
    const arr = result as [string, number];
    return { jobId: String(arr[0]), isNew: Number(arr[1]) === 1 };
  };

  // The worker belongs to this handle, not to the id. Keying it by
  // `${prefix}:${id}` made a second same-id handle a silent no-op: its process,
  // after and trace callbacks were never invoked because all work ran through
  // the first handle's captured closures, and either handle's stop() killed the
  // other's worker. Concurrent consumers on the same id are already resolved by
  // the queue's atomic claim and lease.
  let workerAc: AbortController | null = null;
  // The controller of the callback currently running, so stop() can cancel it.
  let activeJobAc: AbortController | null = null;
  let restartRequested = false;

  const startWorker = (): void => {
    if (workerAc) {
      if (workerAc.signal.aborted) restartRequested = true;
      return;
    }
    restartRequested = false;
    const ac = new AbortController();
    workerAc = ac;

    void (async () => {
      try {
        while (!ac.signal.aborted) {
          try {
            const message = await retry({
              run: () =>
                workQueue.recv({
                  wait: true,
                  timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
                  leaseMs: defaultLeaseMs,
                  signal: ac.signal,
                }),
              after: ({ ctx }) => {
                if (ctx.error && isRetryableTransportError(ctx.error)) {
                  ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 50, maxMs: 1_000 }) });
                }
              },
              signal: ac.signal,
            });

            if (!message) continue;

            const payload = message.data;
            const attempt = message.attempt;
            const failureCount = attempt - 1;
            const startedAt = Date.now();
            const jobAc = new AbortController();
            let leaseLost = false;
            activeJobAc = jobAc;

            try {
              const keepLease = async (leaseMs = payload.leaseMs): Promise<boolean> => {
                requireFutureDuration("heartbeat.leaseMs", leaseMs, 1);
                const held = await message.touch({ leaseMs });
                if (!held) {
                  leaseLost = true;
                  jobAc.abort();
                  return false;
                }
                // A job that outlives keyTtlMs must not lose its dedup claim.
                await refreshKey(payload.key, payload.jobId, payload.keyTtlMs);
                return true;
              };
              const canceled = (): boolean => ac.signal.aborted || leaseLost || jobAc.signal.aborted;
              const renewIfActive = async (leaseMs = payload.leaseMs): Promise<boolean> => {
                if (canceled() || !(await keepLease(leaseMs))) return false;
                return !canceled();
              };

              const ctx = {
                jobId: payload.jobId,
                key: payload.key,
                input: payload.input as Input,
                failureCount,
                signal: jobAc.signal,
                heartbeat: async (cfg?: { leaseMs?: number }): Promise<void> => {
                  await keepLease(cfg?.leaseMs);
                },
              } as JobCtx<Input>;
              Object.defineProperty(ctx, "duration", {
                get: () => Date.now() - startedAt,
                enumerable: true,
              });

              // Queue recv uses the handle default. Apply this submission's
              // lease before trace or user code can outlive the wrong lease.
              if (!(await renewIfActive(payload.leaseMs))) continue;

              const traceInput = payload.input === undefined ? {} : { input: payload.input as Input };

              await emitTrace(config.trace, {
                type: "started",
                jobId: payload.jobId,
                key: payload.key,
                ...traceInput,
                attempt,
              });
              if (!(await renewIfActive())) continue;

              let result: Result | undefined;
              let error: Error | undefined;
              try {
                result = await Promise.resolve(config.process({ ctx }));
              } catch (err) {
                error = asError(err);
              }
              if (!(await renewIfActive())) continue;

              if (error) {
                await emitTrace(config.trace, {
                  type: "failed",
                  jobId: payload.jobId,
                  key: payload.key,
                  ...traceInput,
                  error,
                  durationMs: Date.now() - startedAt,
                });
              } else {
                await emitTrace(config.trace, {
                  type: "succeeded",
                  jobId: payload.jobId,
                  key: payload.key,
                  ...traceInput,
                  data: result as Result,
                  durationMs: Date.now() - startedAt,
                });
              }
              if (!(await renewIfActive())) continue;

              let rescheduleRequested: { delayMs: number } | null = null;
              const afterCtx: JobAfterCtx<Input, Result> = Object.create(ctx) as JobAfterCtx<Input, Result>;
              if (error) afterCtx.error = error;
              if (!error) afterCtx.data = result;
              afterCtx.reschedule = (rcfg?: { delayMs?: number }): void => {
                rescheduleRequested = {
                  delayMs: resolveDelayMs("reschedule.delayMs", rcfg?.delayMs),
                };
              };
              afterCtx.expBackoff = (bcfg?: BackoffOptions): number => expBackoff(failureCount + 1, bcfg);
              afterCtx.metric = metrics;

              if (config.after) {
                try {
                  await Promise.resolve(config.after({ ctx: afterCtx }));
                } catch {
                  // after errors are swallowed — transport decision is made by ctx.reschedule flag
                }
              }
              if (!(await renewIfActive())) continue;

              if (rescheduleRequested) {
                const delayMs = (rescheduleRequested as { delayMs: number }).delayMs;
                const extended = await extendKey(
                  payload.key,
                  payload.jobId,
                  Math.min(MAX_KEY_TTL_MS, delayMs + payload.keyTtlMs),
                );
                if (!extended || canceled()) continue;
                const nacked = await message.nack({
                  delayMs,
                  reason: "reschedule",
                  error: error?.message,
                });
                if (!nacked) {
                  // Lease expired; message will be redelivered. Key stays claimed.
                  continue;
                }
                metrics.reschedules += 1;
                await emitTrace(config.trace, {
                  type: "rescheduled",
                  jobId: payload.jobId,
                  key: payload.key,
                  attempt,
                  delayMs,
                });
                continue;
              }

              // Terminal: ack + release key
              const acked = await message.ack();
              if (!acked) {
                // Lease expired; message will be redelivered. Key stays claimed.
                continue;
              }
              await releaseKey(payload.key, payload.jobId);

              if (error) {
                metrics.failures += 1;
              } else {
                metrics.dispatches += 1;
              }
              await emitTrace(config.trace, {
                type: "finished",
                jobId: payload.jobId,
                key: payload.key,
                status: error ? "failed" : "succeeded",
                durationMs: Date.now() - startedAt,
              });
            } finally {
              // Ordinary completion keeps the signal live through after().
              // Only clear the controller if this attempt still owns the slot.
              if (activeJobAc === jobAc) activeJobAc = null;
            }
          } catch {
            if (ac.signal.aborted) break;
            await sleep(25);
          }
        }
      } finally {
        if (workerAc === ac) workerAc = null;
        if (restartRequested) startWorker();
      }
    })();
  };

  const submit = async (cfg: SubmitConfig<Input>): Promise<JobId> => {
    if (!cfg.key) throw new Error("submit: key is required");

    const leaseMs = requireFutureDuration("submit.leaseMs", cfg.leaseMs ?? defaultLeaseMs, 1);
    const keyTtlMs = requireSafeInteger(
      "submit.keyTtlMs",
      cfg.keyTtlMs ?? defaultKeyTtlMs,
      MIN_KEY_TTL_MS,
      MAX_KEY_TTL_MS,
    );
    const now = Date.now();
    const delayMs =
      cfg.at !== undefined
        ? resolveDelayMs("submit.at", Math.max(0, requireSafeInteger("submit.at", cfg.at, 0) - now))
        : resolveDelayMs("submit.delayMs", cfg.delayMs);
    const initialKeyTtlMs = Math.min(MAX_KEY_TTL_MS, delayMs + keyTtlMs);

    const { jobId, isNew } = await claimKey(cfg.key, initialKeyTtlMs);
    if (!isNew) {
      startWorker();
      return jobId;
    }

    const payload: WorkPayload = {
      jobId,
      key: cfg.key,
      input: (cfg as { input?: Input }).input,
      keyTtlMs,
      leaseMs,
      meta: cfg.meta,
    };

    try {
      await workQueue.send({
        data: payload,
        delayMs,
        idempotencyKey: jobId,
        idempotencyTtlMs: initialKeyTtlMs,
        meta: cfg.meta,
      });
    } catch (error) {
      if (isRetryableTransportError(error)) {
        // Redis may have committed the idempotent queue write and lost only the
        // response. Keep the pending claim and start a worker for that message.
        startWorker();
      } else {
        // Serialization, validation and deterministic Redis errors did not
        // ambiguously accept work, so a corrected submit may claim the key.
        await releaseKey(cfg.key, jobId);
      }
      throw error;
    }

    // Only now is the claim genuinely backed by queued work. A crash before
    // this point leaves the claim pending; a later submit retries the enqueue,
    // while the queue's jobId key prevents a successful send from duplicating.
    await markEnqueued(cfg.key, jobId, initialKeyTtlMs);

    const traceInput = payload.input === undefined ? {} : { input: payload.input as Input };
    await emitTrace(config.trace, {
      type: "submitted",
      jobId,
      key: cfg.key,
      ...traceInput,
      ...(cfg.meta ? { meta: cfg.meta } : {}),
    });
    startWorker();

    return jobId;
  };

  const metric = (): JobMetrics => ({ ...metrics });

  const stop = (): void => {
    restartRequested = false;
    workerAc?.abort();
    // Cancel the in-flight callback too, so `ctx.signal.aborted` is a usable
    // cancellation signal rather than something that is always false.
    activeJobAc?.abort();
  };

  return {
    id: config.id,
    submit,
    metric,
    stop,
  };
};
