import { redis, RedisClient, sleep } from "bun";
import type { z } from "zod";

// ==========================
// Types
// ==========================

export type QueueConfig<T extends z.ZodType> = {
  /** Queue name (used as Redis key) */
  name: string;
  /** Zod schema for message data validation */
  schema: T;
  /** Prefix for Redis key (default: "queue") */
  prefix?: string;
};

export type QueueProcessOptions = {
  /** Number of concurrent consumers (default: 1) */
  concurrency?: number;
  /** Blocking read timeout in seconds (overrides pollInterval if set) */
  blockingTimeoutSecs?: number;
  /** Blocking read timeout fallback in ms (default: 1000) */
  pollInterval?: number;
  /** Called when a message is processed successfully */
  onSuccess?: (data: unknown) => void | Promise<void>;
  /** Called when a message handler throws */
  onError?: (data: unknown, error: Error) => void | Promise<void>;
};

export type Queue<T> = {
  /** Send a message to the queue */
  send: (data: T) => Promise<void>;
  /** Send multiple messages to the queue atomically */
  sendBatch: (data: T[]) => Promise<void>;
  /** Process messages from the queue */
  process: (handler: (data: T) => Promise<void> | void, options?: QueueProcessOptions) => () => void;
  /** Get the number of messages in the queue */
  size: () => Promise<number>;
  /** Remove all messages from the queue */
  purge: () => Promise<void>;
};

// ==========================
// Defaults
// ==========================

const DEFAULT_POLL_INTERVAL = 1000;
const DEFAULT_CONCURRENCY = 1;

// ==========================
// Errors
// ==========================

export class QueueError extends Error {
  readonly code: string;

  constructor(message: string, code: string) {
    super(message);
    this.name = "QueueError";
    this.code = code;
  }
}

export class QueueValidationError extends QueueError {
  readonly issues: z.ZodIssue[];

  constructor(issues: z.ZodIssue[]) {
    super("Queue message validation failed", "VALIDATION_ERROR");
    this.name = "QueueValidationError";
    this.issues = issues;
  }
}

// ==========================
// Queue Factory
// ==========================

/**
 * Create a strict FIFO queue with Zod schema validation.
 * Messages are consumed and gone after processing — no status tracking, no retries.
 *
 * @example
 * ```ts
 * import { z } from "zod";
 * import { queue } from "@valentinkolb/sync";
 *
 * const events = queue.create({
 *   name: "events",
 *   schema: z.object({
 *     type: z.string(),
 *     payload: z.unknown(),
 *   }),
 * });
 *
 * // Send messages
 * await events.send({ type: "user.created", payload: { id: 1 } });
 * await events.sendBatch([data1, data2, data3]);
 *
 * // Process messages (strict FIFO)
 * const stop = events.process(async (data) => {
 *   console.log(data.type, data.payload);
 * }, { concurrency: 5 });
 *
 * // Stop processing
 * stop();
 * ```
 */
const create = <T extends z.ZodType>(config: QueueConfig<T>): Queue<z.infer<T>> => {
  type Data = z.infer<T>;

  const { name, schema, prefix = "queue" } = config;
  const key = `${prefix}:${name}`;

  const send = async (data: Data): Promise<void> => {
    const result = schema.safeParse(data);
    if (!result.success) {
      throw new QueueValidationError(result.error.issues);
    }
    await redis.lpush(key, JSON.stringify(result.data));
  };

  const sendBatch = async (data: Data[]): Promise<void> => {
    if (data.length === 0) return;

    const serialized: string[] = [];
    for (const item of data) {
      const result = schema.safeParse(item);
      if (!result.success) {
        throw new QueueValidationError(result.error.issues);
      }
      serialized.push(JSON.stringify(result.data));
    }

    await redis.send("LPUSH", [key, ...serialized]);
  };

  const process = (handler: (data: Data) => Promise<void> | void, options: QueueProcessOptions = {}): (() => void) => {
    const {
      concurrency = DEFAULT_CONCURRENCY,
      blockingTimeoutSecs,
      pollInterval,
      onSuccess,
      onError,
    } = options;

    const blockingTimeoutSecsValue =
      blockingTimeoutSecs ?? Math.max(1, Math.ceil((pollInterval ?? DEFAULT_POLL_INTERVAL) / 1000));

    let running = true;

    const processLoop = async (client: RedisClient): Promise<void> => {
      if (!client.connected) {
        await client.connect();
      }

      while (running) {
        try {
          const popResult = (await client.send("BRPOP", [key, blockingTimeoutSecsValue.toString()])) as
            | [string, string]
            | null;

          if (!popResult) {
            continue;
          }

          const [, raw] = popResult;

          const parsed = JSON.parse(raw);
          const parsedResult = schema.safeParse(parsed);
          if (!parsedResult.success) {
            console.error("Invalid message in queue:", parsedResult.error.issues);
            continue;
          }

          const data: Data = parsedResult.data;

          try {
            await handler(data);

            if (onSuccess) {
              try {
                await onSuccess(data);
              } catch (callbackError) {
                console.error("onSuccess callback failed:", callbackError);
              }
            }
          } catch (error) {
            const err = error instanceof Error ? error : new Error(String(error));

            if (onError) {
              try {
                await onError(data, err);
              } catch (callbackError) {
                console.error("onError callback failed:", callbackError);
              }
            }
          }
        } catch (error) {
          if (!running) break;
          console.error("Queue worker error:", error);
          await sleep(100);
        }
      }
    };

    const workerClients: RedisClient[] = [];
    for (let i = 0; i < concurrency; i++) {
      const workerClient = new RedisClient();
      workerClients.push(workerClient);
      processLoop(workerClient);
    }

    return () => {
      running = false;
      for (const client of workerClients) {
        if (!client.connected) continue;
        try {
          client.close();
        } catch (error) {
          const err = error instanceof Error ? error : new Error(String(error));
          if (!("code" in err) || (err as Error & { code?: string }).code !== "ERR_REDIS_CONNECTION_CLOSED") {
            console.error("Failed to close worker client:", error);
          }
        }
      }
    };
  };

  const size = async (): Promise<number> => {
    return await redis.llen(key);
  };

  const purge = async (): Promise<void> => {
    await redis.del(key);
  };

  return { send, sendBatch, process, size, purge };
};

// ==========================
// Export
// ==========================

export const queue = { create };
