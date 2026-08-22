import type { JsonValue } from "./codec.ts";

// ==========================
// Structured events
// ==========================

/**
 * Bounded structured events for consumers building operational tooling.
 * Events are observations only: an observer can never alter transport state,
 * and a slow observer causes events to be dropped, never work to block.
 */
export type SyncEvent = {
  type:
    | "connection" // status: connected | reconnecting | closed
    | "ready"
    | "resource_created"
    | "resource_verified"
    | "resource_drifted"
    | "worker_started"
    | "worker_stopped"
    | "handler_error"
    | "publish_failed"
    | "redelivery"
    | "dead_letter"
    | "lock_lost"
    | "schedule_tick"
    | "schedule_misfire"
    | "pump_recovered"
    | "object_error"
    | "watch_resync_required"
    | "drain_timeout"
    | "observer_error";
  at: Date;
  /** Sync resource id (not the NATS name) when the event concerns a resource. */
  resource?: string;
  kind?: string;
  detail?: Record<string, JsonValue>;
  error?: string;
};

export type SyncObserver = (event: SyncEvent) => void | Promise<void>;

const EVENT_BUFFER_LIMIT = 1_024;

type EventSubscriber = {
  buffer: SyncEvent[];
  notify: (() => void) | null;
  done: boolean;
};

// ==========================
// Event hub
// ==========================

export type EventHub = {
  emit(event: Omit<SyncEvent, "at">): void;
  subscribe(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;
  readonly dropped: number;
};

export const createEventHub = (observe?: SyncObserver): EventHub => {
  const subscribers = new Set<EventSubscriber>();
  let dropped = 0;

  const emit = (partial: Omit<SyncEvent, "at">): void => {
    const event: SyncEvent = { ...partial, at: new Date() };
    if (observe) {
      try {
        const result = observe(event);
        if (result instanceof Promise) result.catch(() => {});
      } catch {
        // Observer failures are contained and never affect transport work.
      }
    }
    for (const sub of subscribers) {
      if (sub.buffer.length >= EVENT_BUFFER_LIMIT) {
        dropped += 1;
        continue;
      }
      sub.buffer.push(event);
      sub.notify?.();
    }
  };

  const subscribe = (options: { signal?: AbortSignal } = {}): AsyncIterable<SyncEvent> => {
    const sub: EventSubscriber = { buffer: [], notify: null, done: false };
    subscribers.add(sub);
    const stop = (): void => {
      sub.done = true;
      subscribers.delete(sub);
      sub.notify?.();
    };
    options.signal?.addEventListener("abort", stop, { once: true });

    return {
      async *[Symbol.asyncIterator]() {
        try {
          while (!sub.done) {
            if (sub.buffer.length === 0) {
              await new Promise<void>((resolve) => {
                sub.notify = resolve;
              });
              sub.notify = null;
            }
            while (sub.buffer.length > 0) yield sub.buffer.shift()!;
          }
        } finally {
          stop();
        }
      },
    };
  };

  return {
    emit,
    subscribe,
    get dropped() {
      return dropped;
    },
  };
};
