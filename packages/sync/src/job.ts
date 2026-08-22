import { extString } from "./codec.ts";
import type { JsonValue } from "./codec.ts";
import { BatchSubmitError, asError } from "./errors.ts";
import { assertName } from "./naming.ts";
import { createQueueCore } from "./queue.ts";
import type { DeadLetterStore, QueueConfig } from "./queue.ts";
import type { SyncRuntime } from "./runtime.ts";
import type { MessageMeta, PublishReceipt } from "./types.ts";
import type { ProcessOptions, Worker } from "./worker.ts";

// ==========================
// Types
// ==========================

export type JobConfig = QueueConfig & {
  /** Retention for diagnostics and dead letters. Default 7 days. */
  terminalRetentionMs?: number;
};

export type JobSubmit<Input> = {
  /** Required idempotent submission key (the NATS message ID within the dedupe window). */
  key: string;
  input: Input;
  tenantId?: string;
  delayMs?: number;
  at?: Date;
  orderingKey?: string;
  meta?: MessageMeta;
};

export type JobContext<Input> = {
  jobId: string;
  key: string;
  input: Input;
  attempt: number;
  failureCount: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

export type JobFailureDecision = { action: "retry"; delayMs?: number } | { action: "dead_letter"; reason: string };

export type JobProcessOptions<Input> = ProcessOptions & {
  onError?: (input: { context: JobContext<Input>; error: Error }) => JobFailureDecision | Promise<JobFailureDecision>;
};

export type JobSubmitManyOptions = {
  /** Bounded number of in-flight publish promises. Default 16. */
  publishConcurrency?: number;
  /** Bounded bytes of in-flight publishes — local backpressure. Default 8 MiB. */
  maxPendingBytes?: number;
  signal?: AbortSignal;
};

export type Job<Input> = {
  ready(): Promise<void>;
  submit(job: JobSubmit<Input>): Promise<PublishReceipt & { jobId: string }>;
  submitMany(
    jobs: Iterable<JobSubmit<Input>> | AsyncIterable<JobSubmit<Input>>,
    options?: JobSubmitManyOptions,
  ): Promise<{ accepted: number; duplicates: number }>;
  process(options: JobProcessOptions<Input>, handler: (context: JobContext<Input>) => Promise<void>): Promise<Worker>;
  deadLetters: DeadLetterStore<{ key: string; input: Input }>;
};

// ==========================
// Job factory
// ==========================

export const createJob = <Input>(runtime: SyncRuntime, config: JobConfig): Job<Input> => {
  const terminalRetentionMs = config.terminalRetentionMs ?? 7 * 24 * 60 * 60 * 1_000;
  const core = createQueueCore<Input, { key: string; input: Input }>(
    runtime,
    { ...config, dlqMaxAgeMs: terminalRetentionMs },
    "job",
    (envelope) => ({ key: extString(envelope, "key") ?? "", input: envelope.data as Input }),
  );

  const toSend = (job: JobSubmit<Input>) => {
    assertName(job.key, "job key");
    return {
      message: {
        data: job.input,
        tenantId: job.tenantId,
        idempotencyKey: job.key,
        delayMs: job.delayMs,
        at: job.at,
        orderingKey: job.orderingKey,
        meta: job.meta,
      },
      ext: { key: job.key } satisfies Record<string, JsonValue>,
    };
  };

  const submit: Job<Input>["submit"] = async (job) => {
    const { message, ext } = toSend(job);
    const receipt = await core.send(message, ext);
    return { ...receipt, jobId: receipt.messageId };
  };

  const submitMany: Job<Input>["submitMany"] = async (jobs, options = {}) => {
    const publishConcurrency = options.publishConcurrency ?? 16;
    const maxPendingBytes = options.maxPendingBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(publishConcurrency) || publishConcurrency < 1) {
      throw new RangeError("publishConcurrency must be a positive integer");
    }
    if (!Number.isSafeInteger(maxPendingBytes) || maxPendingBytes < 1) {
      throw new RangeError("maxPendingBytes must be a positive integer");
    }

    let accepted = 0;
    let duplicates = 0;
    let pendingBytes = 0;
    let firstError: Error | null = null;
    const inflight = new Set<Promise<void>>();

    const waitForCapacity = async (nextBytes: number): Promise<void> => {
      // Byte backpressure never deadlocks: a single oversized item may fly alone.
      while (
        inflight.size >= publishConcurrency ||
        (inflight.size > 0 && pendingBytes + nextBytes > maxPendingBytes)
      ) {
        await Promise.race(inflight);
        if (firstError !== null) return;
      }
    };

    try {
      for await (const job of jobs) {
        if (options.signal?.aborted) throw new Error("submitMany aborted");
        const { message, ext } = toSend(job);
        const prepared = await core.prepareSend(message, ext);
        await waitForCapacity(prepared.byteLength);
        if (firstError !== null) break;
        pendingBytes += prepared.byteLength;
        const task = prepared
          .publish()
          .then((receipt) => {
            if (receipt.duplicate) duplicates += 1;
            else accepted += 1;
          })
          .catch((error) => {
            firstError ??= asError(error);
          })
          .finally(() => {
            pendingBytes -= prepared.byteLength;
            inflight.delete(task);
          });
        inflight.add(task);
      }
    } catch (error) {
      firstError ??= asError(error);
    }
    await Promise.all(inflight);
    if (firstError !== null) {
      // Not atomic by design: prior accepted items stay accepted.
      throw new BatchSubmitError("submitMany failed", accepted, duplicates, firstError);
    }
    return { accepted, duplicates };
  };

  const process: Job<Input>["process"] = (options, handler) => {
    const { onError, ...processOptions } = options;
    const toContext = (message: {
      messageId: string;
      data: Input;
      attempt: number;
      signal: AbortSignal;
      heartbeat(): Promise<void>;
    }, key: string): JobContext<Input> => ({
      jobId: message.messageId,
      key,
      input: message.data,
      attempt: message.attempt,
      failureCount: message.attempt - 1,
      signal: message.signal,
      heartbeat: message.heartbeat,
    });
    return core.process(
      processOptions,
      async (message, envelope) => {
        await handler(toContext(message, extString(envelope, "key") ?? ""));
      },
      onError === undefined
        ? undefined
        : async ({ message, envelope, error }) => {
            const decision = await onError({
              context: toContext(message, extString(envelope, "key") ?? ""),
              error,
            });
            return decision.action === "retry"
              ? { action: "retry", ...(decision.delayMs !== undefined ? { delayMs: decision.delayMs } : {}) }
              : { action: "dead_letter", reason: decision.reason };
          },
    );
  };

  return {
    ready: () => core.declarationReady(),
    submit,
    submitMany,
    process,
    deadLetters: core.deadLetters,
  };
};
