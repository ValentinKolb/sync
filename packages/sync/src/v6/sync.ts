import type { SyncEvent } from "./events.ts";
import { createRuntime } from "./runtime.ts";
import type { DrainResult, SyncConfig, SyncHealth, SyncResourceSummary, SyncRuntime } from "./runtime.ts";
import { createTopic } from "./topic.ts";
import type { Topic, TopicConfig } from "./topic.ts";

// ==========================
// Public Sync surface
// ==========================

export type Sync = {
  ready(): Promise<void>;
  drain(options?: { timeoutMs?: number }): Promise<DrainResult>;
  health(): SyncHealth;
  resources(): Promise<SyncResourceSummary[]>;
  events(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;

  topic<T>(config: TopicConfig): Topic<T>;
};

/**
 * Create a Sync instance on an already connected, caller-owned NATS
 * connection. Performs no I/O; `ready()` verifies the server, provisions
 * declared resources, and fails clearly on configuration drift.
 */
export const createSync = (config: SyncConfig): Sync => {
  const runtime: SyncRuntime = createRuntime(config);
  return {
    ready: () => runtime.ready(),
    drain: (options) => runtime.drain(options),
    health: () => runtime.health(),
    resources: () => runtime.resources(),
    events: (options) => runtime.events.subscribe(options),
    topic: <T>(topicConfig: TopicConfig) => createTopic<T>(runtime, topicConfig),
  };
};
