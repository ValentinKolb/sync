import { z, type ZodTypeAny } from "zod";
import { queue, type Queue } from "./queue";
import { topic, type Topic } from "./topic";
import { sleep } from "./internal/sleep";
import {
  computeRetryDelay,
  isTerminalStatus,
  withTimeout,
} from "../internal/job-utils";

const DEFAULT_PREFIX = "sync:job";
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_WORKER_RECV_TIMEOUT_MS = 1_000;
const DEFAULT_MAX_ATTEMPTS = 1;
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_STATE_RETENTION_MS = 7 * DAY_MS;

// ==========================
// Types
// ==========================

export type JobId = string;

export type JobStatus = "completed" | "failed" | "cancelled" | "timed_out";

export type JobTerminal<Result = unknown> = {
  id: JobId;
  status: JobStatus;
  result?: Result;
  error?: {
    message: string;
    code?: string;
  };
  finishedAt: number;
};

export type SubmitOptions = {
  key?: string;
  keyTtlMs?: number;
  delayMs?: number;
  at?: number;
  maxAttempts?: number;
  backoff?: {
    kind: "fixed" | "exp";
    baseMs: number;
    maxMs?: number;
  };
  leaseMs?: number;
  meta?: Record<string, unknown>;
};

export type JoinOptions = {
  timeoutMs?: number;
};

export type CancelOptions = {
  reason?: string;
};

export type JobEvent =
  | { type: "submitted"; id: JobId; ts: number }
  | { type: "started"; id: JobId; runId: string; attempt: number; ts: number }
  | { type: "heartbeat"; id: JobId; runId: string; ts: number }
  | { type: "retry"; id: JobId; runId: string; nextAt: number; reason?: string; ts: number }
  | { type: "completed"; id: JobId; ts: number }
  | { type: "failed"; id: JobId; reason?: string; ts: number }
  | { type: "cancelled"; id: JobId; reason?: string; ts: number };

export type JobEvents = Pick<Topic<JobEvent>, "reader" | "live">;

export type JobContext = {
  step<T>(cfg: { id: string; run: () => Promise<T> | T }): Promise<T>;
  heartbeat(cfg?: { leaseMs?: number }): Promise<void>;
  signal: AbortSignal;
};

export type JobHandle<Input, Result = unknown> = {
  id: string;
  submit(cfg: { input: Input } & SubmitOptions): Promise<JobId>;
  validateInput(input: unknown): void;
  join(cfg: { id: JobId } & JoinOptions): Promise<JobTerminal<Result>>;
  cancel(cfg: { id: JobId } & CancelOptions): Promise<void>;
  events(id: JobId): JobEvents;
  stop(): void;
};

export type JobDefinition<TSchema extends ZodTypeAny, Result = unknown> = {
  id: string;
  schema: TSchema;
  defaults?: Omit<SubmitOptions, "key" | "delayMs" | "at" | "meta">;
  process: (cfg: { ctx: JobContext; input: z.infer<TSchema> }) => Promise<Result> | Result;
};

// ==========================
// Internal Types
// ==========================

type WorkPayload = {
  id: JobId;
  input: unknown;
  maxAttempts: number;
  backoff?: {
    kind: "fixed" | "exp";
    baseMs: number;
    maxMs?: number;
  };
  leaseMs: number;
  meta?: Record<string, unknown>;
};

type InternalState<Result> = {
  id: JobId;
  status: "submitted" | "running" | JobStatus;
  attempts: number;
  updatedAt: number;
  finishedAt?: number;
  result?: Result;
  error?: {
    message: string;
    code?: string;
  };
};

const workPayloadSchema = z.object({
  id: z.string(),
  input: z.unknown(),
  maxAttempts: z.number().int().min(1),
  backoff: z
    .object({
      kind: z.enum(["fixed", "exp"]),
      baseMs: z.number().int().min(0),
      maxMs: z.number().int().min(0).optional(),
    })
    .optional(),
  leaseMs: z.number().int().min(1),
  meta: z.record(z.string(), z.unknown()).optional(),
});

const now = (): number => Date.now();

const activeWorkers = new Map<string, AbortController>();

// Module-level shared stores keyed by definition ID.
// This ensures multiple job() calls with the same definition.id share state,
// matching the server behavior where Redis provides the shared state.
type SharedJobState = {
  stateStore: Map<string, InternalState<unknown>>;
  idempotencyStore: Map<string, { jobId: string; expiresAt: number }>;
  workQueue: Queue<WorkPayload>;
  topicCache: Map<string, Topic<JobEvent>>;
  seq: number;
};
const sharedJobStates = new Map<string, SharedJobState>();

const getSharedState = (definitionId: string): SharedJobState => {
  let shared = sharedJobStates.get(definitionId);
  if (!shared) {
    shared = {
      stateStore: new Map(),
      idempotencyStore: new Map(),
      workQueue: queue({
        id: `${definitionId}:work`,
        prefix: `${DEFAULT_PREFIX}:queue`,
        schema: workPayloadSchema,
        delivery: {
          defaultLeaseMs: DEFAULT_LEASE_MS,
          maxDeliveries: Number.MAX_SAFE_INTEGER,
        },
      }) as unknown as Queue<WorkPayload>,
      topicCache: new Map(),
      seq: 0,
    };
    sharedJobStates.set(definitionId, shared);
  }
  return shared;
};

// ==========================
// Job Factory
// ==========================

export const job = <TSchema extends ZodTypeAny, Result = unknown>(
  definition: JobDefinition<TSchema, Result>,
): JobHandle<z.infer<TSchema>, Result> => {
  type Input = z.infer<TSchema>;

  const prefix = DEFAULT_PREFIX;
  const workerId = `${prefix}:${definition.id}`;

  // Shared state across all handles with the same definition.id
  const shared = getSharedState(definition.id);
  const stateStore = shared.stateStore as Map<string, InternalState<Result>>;
  const idempotencyStore = shared.idempotencyStore;
  const workQueue = shared.workQueue;
  const topicCache = shared.topicCache;

  const eventsTopicFor = (jobId: JobId): Topic<JobEvent> => {
    let cached = topicCache.get(jobId);
    if (cached) return cached;
    cached = topic({
      id: `${definition.id}:${jobId}:events`,
      prefix: `${prefix}:events`,
      schema: z.any(),
      retentionMs: 7 * DAY_MS,
    }) as unknown as Topic<JobEvent>;
    topicCache.set(jobId, cached);
    return cached;
  };

  const emitEvent = async (jobId: JobId, event: JobEvent): Promise<void> => {
    await eventsTopicFor(jobId).pub({ data: event });
  };

  const readState = (jobId: JobId): InternalState<Result> | null => {
    return stateStore.get(jobId) ?? null;
  };

  const writeState = (state: InternalState<Result>): void => {
    stateStore.set(state.id, { ...state });
  };

  const writeStateIfAbsent = (state: InternalState<Result>): boolean => {
    if (stateStore.has(state.id)) return false;
    stateStore.set(state.id, { ...state });
    return true;
  };

  /** CAS: write final state, return "ok" | "cancelled" | "missing" */
  const writeFinalState = (state: InternalState<Result>): "ok" | "cancelled" | "missing" => {
    const existing = stateStore.get(state.id);
    if (!existing) return "missing";
    if (existing.status === "cancelled") return "cancelled";
    if (isTerminalStatus(existing.status)) return "missing";
    stateStore.set(state.id, { ...state });
    // Schedule cleanup of terminal state
    setTimeout(() => {
      const current = stateStore.get(state.id);
      if (current && (current.status === "completed" || current.status === "failed" || current.status === "cancelled" || current.status === "timed_out")) {
        stateStore.delete(state.id);
      }
    }, DEFAULT_STATE_RETENTION_MS);
    return "ok";
  };

  /** CAS: write cancelled state only if not already terminal */
  const writeCancelledState = (state: InternalState<Result>): boolean => {
    const existing = stateStore.get(state.id);
    if (!existing) return false;
    if (isTerminalStatus(existing.status)) return false;
    stateStore.set(state.id, { ...state });
    return true;
  };

  // ==========================
  // Worker
  // ==========================

  const startWorker = (): void => {
    if (activeWorkers.has(workerId)) return;
    const workerAc = new AbortController();
    activeWorkers.set(workerId, workerAc);

    void (async () => {
      try {
        while (!workerAc.signal.aborted) {
          try {
            const message = await workQueue.recv({
              wait: true,
              timeoutMs: DEFAULT_WORKER_RECV_TIMEOUT_MS,
              leaseMs: DEFAULT_LEASE_MS,
            });

            if (!message) continue;

            const payload = message.data as WorkPayload;
            let state = readState(payload.id);

            if (!state) {
              writeStateIfAbsent({
                id: payload.id,
                status: "submitted",
                attempts: 0,
                updatedAt: now(),
              });
              state = readState(payload.id);
              if (!state) {
                await message.nack({ delayMs: 250, reason: "state_missing_recover_failed" });
                continue;
              }
            }

            if (state.status === "cancelled" || isTerminalStatus(state.status)) {
              await message.ack();
              continue;
            }

            const attempt = message.attempt;
            const runId = message.deliveryId;
            const startedAt = now();

            writeState({
              ...state,
              status: "running",
              attempts: attempt,
              updatedAt: startedAt,
            });

            await emitEvent(payload.id, {
              type: "started",
              id: payload.id,
              runId,
              attempt,
              ts: startedAt,
            });

            await message.touch({ leaseMs: payload.leaseMs });

            const jobAc = new AbortController();

            const ctx: JobContext = {
              signal: jobAc.signal,
              step: async <T>(cfg: { id: string; run: () => Promise<T> | T }): Promise<T> => {
                return await Promise.resolve(cfg.run());
              },
              heartbeat: async (cfg?: { leaseMs?: number }): Promise<void> => {
                await message.touch({ leaseMs: cfg?.leaseMs ?? payload.leaseMs });
                await emitEvent(payload.id, {
                  type: "heartbeat",
                  id: payload.id,
                  runId,
                  ts: now(),
                });
              },
            };

            try {
              const inputParsed = definition.schema.safeParse(payload.input);
              if (!inputParsed.success) {
                throw inputParsed.error;
              }

              const processPromise = Promise.resolve(definition.process({ ctx, input: inputParsed.data as Input }));
              const result = await withTimeout(processPromise, payload.leaseMs);

              const latest = readState(payload.id);
              if (latest?.status === "cancelled") {
                jobAc.abort();
                await message.ack();
                continue;
              }

              const acked = await message.ack();
              if (!acked) continue;

              const finishedAt = now();
              const writeResult = writeFinalState({
                id: payload.id,
                status: "completed",
                attempts: attempt,
                updatedAt: finishedAt,
                finishedAt,
                result: result as Result,
              });
              if (writeResult !== "ok") continue;

              await emitEvent(payload.id, {
                type: "completed",
                id: payload.id,
                ts: finishedAt,
              });
            } catch (error) {
              jobAc.abort();
              const err = error instanceof Error ? error : new Error(String(error));
              const timedOut = err.name === "JobTimeoutError";
              const canRetry = attempt < payload.maxAttempts;

              if (canRetry) {
                const delayMs = computeRetryDelay(payload.backoff, attempt);
                const nextAt = now() + delayMs;

                const nacked = await message.nack({
                  delayMs,
                  reason: timedOut ? "timed_out" : "error",
                  error: err.message,
                });

                if (!nacked) continue;

                const latestState = readState(payload.id);
                if (latestState?.status === "cancelled") continue;

                writeState({
                  id: payload.id,
                  status: "submitted",
                  attempts: attempt,
                  updatedAt: now(),
                });

                await emitEvent(payload.id, {
                  type: "retry",
                  id: payload.id,
                  runId,
                  nextAt,
                  reason: err.message,
                  ts: now(),
                });

                continue;
              }

              const acked = await message.ack();
              if (!acked) continue;

              const finishedAt = now();
              const status: JobStatus = timedOut ? "timed_out" : "failed";

              const writeResult = writeFinalState({
                id: payload.id,
                status,
                attempts: attempt,
                updatedAt: finishedAt,
                finishedAt,
                error: {
                  message: err.message,
                  code: timedOut ? "TIMEOUT" : undefined,
                },
              });
              if (writeResult !== "ok") continue;

              await emitEvent(payload.id, {
                type: "failed",
                id: payload.id,
                reason: err.message,
                ts: finishedAt,
              });
            }
          } catch {
            if (workerAc.signal.aborted) break;
            await sleep(25);
          }
        }
      } finally {
        const current = activeWorkers.get(workerId);
        if (current === workerAc) {
          activeWorkers.delete(workerId);
        }
      }
    })();
  };

  // ==========================
  // submit
  // ==========================

  const submit = async (cfg: { input: Input } & SubmitOptions): Promise<JobId> => {
    startWorker();

    const parsed = definition.schema.safeParse(cfg.input);
    if (!parsed.success) throw parsed.error;

    const maxAttempts = Math.max(1, cfg.maxAttempts ?? definition.defaults?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS);
    const leaseMs = Math.max(1, cfg.leaseMs ?? definition.defaults?.leaseMs ?? DEFAULT_LEASE_MS);
    const backoff = cfg.backoff ?? definition.defaults?.backoff;
    const delayMs = cfg.at !== undefined ? Math.max(0, cfg.at - now()) : Math.max(0, cfg.delayMs ?? 0);
    const keyTtlMs = Math.max(1_000, cfg.keyTtlMs ?? DEFAULT_STATE_RETENTION_MS);

    // Lazy cleanup of expired idempotency entries
    const nowMs = Date.now();
    for (const [k, v] of idempotencyStore) {
      if (nowMs >= v.expiresAt) idempotencyStore.delete(k);
    }

    let jobId: string;
    let isNewSubmission = true;

    if (cfg.key) {
      // Idempotency check
      const existing = idempotencyStore.get(cfg.key);
      if (existing && Date.now() < existing.expiresAt) {
        jobId = existing.jobId;
        isNewSubmission = false;
      } else {
        jobId = String(++shared.seq);
        idempotencyStore.set(cfg.key, { jobId, expiresAt: Date.now() + keyTtlMs });
      }
    } else {
      jobId = String(++shared.seq);
    }

    const submittedState: InternalState<Result> = {
      id: jobId,
      status: "submitted",
      attempts: 0,
      updatedAt: now(),
    };

    const payload: WorkPayload = {
      id: jobId,
      input: parsed.data,
      maxAttempts,
      backoff,
      leaseMs,
      meta: cfg.meta,
    };

    if (!isNewSubmission) {
      const existingState = readState(jobId);
      if (existingState) return jobId;
    }

    await workQueue.send({
      data: payload,
      delayMs,
      idempotencyKey: cfg.key,
      idempotencyTtlMs: keyTtlMs,
      meta: cfg.meta,
    });

    const wrote = writeStateIfAbsent(submittedState);
    if (wrote) {
      await emitEvent(jobId, {
        type: "submitted",
        id: jobId,
        ts: now(),
      });
    }

    return jobId;
  };

  // ==========================
  // join
  // ==========================

  const join = async (cfg: { id: JobId } & JoinOptions): Promise<JobTerminal<Result>> => {
    const state = readState(cfg.id);
    if (state && isTerminalStatus(state.status)) {
      return {
        id: cfg.id,
        status: state.status,
        result: state.result,
        error: state.error,
        finishedAt: state.finishedAt ?? state.updatedAt,
      };
    }

    const ac = new AbortController();
    let timeout: ReturnType<typeof setTimeout> | null = null;

    if (cfg.timeoutMs !== undefined) {
      timeout = setTimeout(() => ac.abort(), cfg.timeoutMs);
    }

    try {
      for await (const event of eventsTopicFor(cfg.id).live({
        after: "0",
        signal: ac.signal,
        timeoutMs: cfg.timeoutMs ?? 30_000,
      })) {
        if (
          event.data.type === "completed" ||
          event.data.type === "failed" ||
          event.data.type === "cancelled"
        ) {
          const finalState = readState(cfg.id);
          if (finalState && isTerminalStatus(finalState.status)) {
            return {
              id: cfg.id,
              status: finalState.status,
              result: finalState.result,
              error: finalState.error,
              finishedAt: finalState.finishedAt ?? finalState.updatedAt,
            };
          }
        }
      }
    } catch {
      // aborted or timed out
    } finally {
      if (timeout) clearTimeout(timeout);
    }

    // One final check
    const finalState = readState(cfg.id);
    if (finalState && isTerminalStatus(finalState.status)) {
      return {
        id: cfg.id,
        status: finalState.status,
        result: finalState.result,
        error: finalState.error,
        finishedAt: finalState.finishedAt ?? finalState.updatedAt,
      };
    }

    return {
      id: cfg.id,
      status: "timed_out",
      error: {
        message: "join timed out",
        code: "JOIN_TIMEOUT",
      },
      finishedAt: now(),
    };
  };

  // ==========================
  // cancel
  // ==========================

  const cancel = async (cfg: { id: JobId } & CancelOptions): Promise<void> => {
    const existing = readState(cfg.id);
    if (!existing) return;
    if (isTerminalStatus(existing.status)) return;

    const finishedAt = now();
    const wrote = writeCancelledState({
      id: cfg.id,
      status: "cancelled",
      attempts: existing.attempts,
      updatedAt: finishedAt,
      finishedAt,
      error: cfg.reason
        ? { message: cfg.reason, code: "CANCELLED" }
        : undefined,
    });
    if (!wrote) return;

    await emitEvent(cfg.id, {
      type: "cancelled",
      id: cfg.id,
      reason: cfg.reason,
      ts: finishedAt,
    });
  };

  // ==========================
  // events / stop
  // ==========================

  const events = (id: JobId): JobEvents => {
    const t = eventsTopicFor(id);
    return { reader: t.reader, live: t.live };
  };

  const stop = (): void => {
    const ac = activeWorkers.get(workerId);
    if (ac) {
      ac.abort();
      activeWorkers.delete(workerId);
    }
  };

  return {
    id: definition.id,
    submit,
    validateInput: (input: unknown): void => {
      const parsed = definition.schema.safeParse(input);
      if (!parsed.success) throw parsed.error;
    },
    join,
    cancel,
    events,
    stop,
  };
};
