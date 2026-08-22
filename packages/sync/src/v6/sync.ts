import type { SyncEvent } from "./events.ts";
import { createRuntime } from "./runtime.ts";
import type { DrainResult, SyncConfig, SyncHealth, SyncResourceSummary, SyncRuntime } from "./runtime.ts";
import { createEphemeral } from "./ephemeral.ts";
import type { Ephemeral, EphemeralConfig } from "./ephemeral.ts";
import { createJob } from "./job.ts";
import type { Job, JobConfig } from "./job.ts";
import { createMutex } from "./mutex.ts";
import type { Mutex, MutexConfig } from "./mutex.ts";
import { createPump } from "./pump.ts";
import type { Pump, PumpConfig, PumpItem } from "./pump.ts";
import { createObjectStore } from "./object-store.ts";
import type { ObjectStore, ObjectStoreConfig } from "./object-store.ts";
import { createQueue } from "./queue.ts";
import type { Queue, QueueConfig } from "./queue.ts";
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
  queue<T>(config: QueueConfig): Queue<T>;
  job<Input>(config: JobConfig): Job<Input>;
  ephemeral<T>(config: EphemeralConfig): Ephemeral<T>;
  objectStore(config: ObjectStoreConfig): ObjectStore;
  mutex(config: MutexConfig): Mutex;
  pump<Input, Cursor, Item extends PumpItem>(config: PumpConfig<Input, Cursor, Item>): Pump<Input, Cursor>;
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
    queue: <T>(queueConfig: QueueConfig) => createQueue<T>(runtime, queueConfig),
    job: <Input>(jobConfig: JobConfig) => createJob<Input>(runtime, jobConfig),
    ephemeral: <T>(ephemeralConfig: EphemeralConfig) => createEphemeral<T>(runtime, ephemeralConfig),
    objectStore: (objectStoreConfig: ObjectStoreConfig) => createObjectStore(runtime, objectStoreConfig),
    mutex: (mutexConfig: MutexConfig) => createMutex(runtime, mutexConfig),
    pump: <Input, Cursor, Item extends PumpItem>(pumpConfig: PumpConfig<Input, Cursor, Item>) =>
      createPump<Input, Cursor, Item>(runtime, pumpConfig),
  };
};
