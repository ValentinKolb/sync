# Sync v6 API and NATS resource design

- Status: proposed for `f8qnscky`
- Parent epic: `xoarzo6n`
- Target: `@k2b/sync` v6 on NATS Server 2.14+ and NATS.js 3.4+
- Parent direction: [Sync v6 NATS-native hard cut](sync-v6-nats.md)

## TL;DR

Sync v6 is a small, opinionated layer over Core NATS, JetStream, NATS KV, and
NATS Object Store. It does not hide the distributed semantics:

- `concurrency` always limits handlers in the current process and worker
  handle. It is never a global cluster limit.
- `delivery.maxInFlight` is the optional global ceiling for unacknowledged
  deliveries on one durable NATS consumer, shared by all pods.
- Jobs, queues, durable topic consumers, pumps, and schedule executions are
  at-least-once. Their handlers and sinks must be idempotent.
- Core NATS live subscriptions are best-effort broadcast with no replay or
  acknowledgement.
- Durable topics are retained logs. Each named consumer owns an independent
  cursor; all pods using that consumer compete for its messages.
- Ordering is not implied. Partitioned ordering is available only when declared
  explicitly and has a visible resource and throughput cost.
- Large internal artifacts use an explicit object-store reference. Sync never
  silently moves an oversized queue, job, or topic payload out of band.
- Sync owns its readers and workers. The caller owns the already-connected NATS
  connection and closes it after `sync.drain()`.

The intended Cloud workflow shape is deliberately boring:

```ts
const workflowRuns = sync.job<{ runId: string }>({
  id: "cloud.workflow-runs",
  delivery: {
    ackWaitMs: 60_000,
    maxAttempts: 5,
    maxInFlight: 512,
    backoffMs: [1_000, 5_000, 30_000],
  },
});

await workflowRuns.process({ concurrency: 64 }, async ({ input, signal }) => {
  await runWorkflow(input.runId, { signal });
});
```

With four identical pods this permits at most 64 running handlers per pod and
at most 512 unacknowledged workflow deliveries globally. The normal effective
ceiling is therefore `min(4 * 64, 512) = 256`. A dead pod temporarily occupies
its unacknowledged slots until `ackWaitMs` expires.

## Goals and boundaries

The v6 API must make these patterns straightforward without turning Sync into a
workflow engine:

- Large, embarrassingly parallel Cloud workflow runs;
- High-volume background jobs distributed over many pods;
- Bounded fan-out submission;
- Durable stream processing with independent cursors or competing workers;
- Explicitly partitioned per-key ordering where it is truly required;
- Finite imports and backfills with parallel item dispatch;
- Recurring schedules that continue producing accepted work while every Cloud
  process is offline;
- Transient presence and registry state;
- Bounded, streamed internal artifacts shared by parallel work;
- Distributed leases with fencing tokens.

Sync does not own Cloud workflow graphs or run and step state. It also does not
own authorization, WebSockets, local socket fan-out, audit storage, permanent
end-user file storage, deployment, credentials, or NATS administration. Cloud
remains the source of truth for those domains.

There is no Redis adapter, generic transport interface, v5 state import, browser
parity, or rate limiter in v6.

## Why the API follows NATS

The public concepts map directly to NATS behavior:

| Sync concept | NATS mechanism | Consequence |
| --- | --- | --- |
| Live topic | Core NATS subscription | Broadcast, best-effort, only while connected |
| Durable topic | Limits-retention JetStream stream | Replayable log with independent consumers |
| Queue | Work-queue JetStream stream and durable pull consumer | Competing workers, explicit ack, at-least-once |
| Job | Queue plus required submission key, retry policy, DLQ, and traces | Convenient durable background work, not a workflow record |
| Pump | KV checkpoint plus internal work queue | Recoverable finite cursor processing |
| Scheduler | NATS 2.14 message schedule plus work stream and KV state | Broker produces ticks while application processes are offline |
| Mutex | NATS KV compare-and-set and per-key TTL | Lease ownership with a monotonic fencing token |
| Ephemeral | NATS KV per-key TTL and watch | Snapshot plus transient changes |
| Object store | JetStream Object Store | Chunked, streamed internal artifacts with explicit references |
| Retry | Local timer and `AbortSignal` | No NATS resource |

JetStream durable consumers provide explicit acknowledgement and redelivery,
which is an at-least-once contract. Several processes can pull from one durable
consumer and share its messages. `MaxAckPending` is shared by the whole consumer,
not allocated per worker. Sync exposes that distinction as local `concurrency`
versus global `delivery.maxInFlight`.

NATS 2.14 also supplies recurring message schedules and the official JavaScript
client exposes them. Sync keeps the higher-level schedule record, run numbering,
misfire policy, handler registration, and diagnostics; it does not rebuild the
clock or leader election in application processes.

## Runtime and lifecycle

### Construction

```ts
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "@k2b/sync";

const sync = createSync({
  connection,
  namespace: "cloud-prod",
  application: "grids",
  defaults: {
    replicas: 3,
    storage: "file",
  },
  observe: (event) => telemetry.record(event),
});

const runs = sync.job<{ runId: string }>({ id: "cloud.workflow-runs" });

await sync.ready();
```

Proposed runtime types:

```ts
type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

type SyncConfig = {
  connection: NatsConnection;
  namespace: string;
  application: string;
  defaults?: {
    replicas?: number;
    storage?: "file" | "memory";
  };
  observe?: (event: SyncEvent) => void | Promise<void>;
};

type Sync = {
  ready(): Promise<void>;
  drain(options?: { timeoutMs?: number }): Promise<DrainResult>;
  health(): SyncHealth;
  resources(): Promise<SyncResourceSummary[]>;
  events(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;

  queue<T>(config: QueueConfig): Queue<T>;
  topic<T>(config: TopicConfig): Topic<T>;
  job<Input>(config: JobConfig): Job<Input>;
  pump<Input, Cursor, Item extends PumpItem>(config: PumpConfig<Input, Cursor, Item>): Pump<Input, Cursor>;
  scheduler(config: SchedulerConfig): Scheduler;
  mutex(config: MutexConfig): Mutex;
  ephemeral<T>(config: EphemeralConfig): Ephemeral<T>;
  objectStore(config: ObjectStoreConfig): ObjectStore;
};
```

`createSync()` performs no network I/O. Primitive factories register local
resource declarations and perform no I/O. `ready()`:

1. verifies that the supplied connection is open;
2. verifies the required NATS Server and JetStream capabilities;
3. creates missing declared resources;
4. compares every existing resource with its declaration;
5. fails on incompatible drift;
6. reports ready only after all declarations are usable.

Every Core NATS subscription, JetStream client, consumer pull, and KV operation
uses the supplied connection. Sync never creates a second transport connection
or a connection per worker, reader, tenant, or resource.

A primitive declared after `ready()` enters `pending` state. Its first async
operation waits for that primitive's resource check. Call `await handle.ready()`
when startup must fail before the first business operation.

`application` is ownership and diagnostics metadata, not an identity or access
boundary. Resource identity is exactly `{ namespace, kind, id }`. A shared
cross-application resource declares its owner explicitly:

```ts
const recordEvents = sync.topic<RecordEvent>({
  id: "grids.record-events",
  owner: "grids",
});
```

Every application opening that resource must declare the same resource
configuration and owner. NATS account permissions remain the actual
infrastructure access boundary; tenant and user authorization remain
application logic.

### Workers and shutdown

```ts
type ProcessOptions = {
  concurrency?: number; // default 1; local to this process and handle
  signal?: AbortSignal;
};

type Worker = {
  readonly active: number;
  readonly capacity: number;
  stop(): void;
  drain(options?: { timeoutMs?: number }): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

type DrainResult = {
  completed: number;
  aborted: number;
  timedOut: boolean;
};
```

`await handle.process(...)` waits until the durable consumer exists and the
local pull loop is active, then returns a tracked `Worker`. It does not wait for
the worker's entire lifetime. `sync.drain()` stops new pulls, waits for running
handlers, and drains Core NATS subscriptions created by Sync. On timeout it
aborts handler signals and negatively acknowledges unfinished durable messages
so another process can recover them. It does not close the caller's connection.

The application shutdown order is:

```ts
await sync.drain({ timeoutMs: 30_000 });
await connection.drain();
```

## Concurrency, buffering, and fairness

The word `concurrency` has one meaning throughout v6:

> Maximum number of user handlers started simultaneously by this worker handle
> in this JavaScript process.

It is not multiplied internally, dynamically changed, or coordinated through a
lock. Two `process({ concurrency: 64 })` calls in one process can run 128
handlers. Four pods with one such worker can run 256 handlers.

`delivery.maxInFlight` maps to the durable consumer's NATS `MaxAckPending` and
is shared globally across all pullers of that consumer. It is a delivery ceiling,
not a fair distributed semaphore:

- it includes running but unacknowledged handlers;
- a crashed handler occupies a slot until ack expiry;
- it does not reserve an equal share for each pod;
- it must be at least as large as one worker's desired concurrency;
- changing it is resource configuration and drift-checks across pods.

Sync issues pulls only for currently free local handler slots. It does not fetch
a large hidden batch and queue it in process memory. NATS may have protocol-level
messages in flight, but Sync's user-space waiting buffer is bounded to at most
one delivery per free slot.

For partitioned ordering, each partition consumer has `MaxAckPending = 1` and
the partition count is the global in-flight ceiling; the general
`delivery.maxInFlight` setting does not override it.

Fairness is NATS pull-consumer fairness. Sync does not promise round-robin pods,
tenant fairness, or priority unless the resource declares a corresponding
partition or NATS priority policy in a later reviewed API.

## Shared delivery types

```ts
type DeliveryConfig = {
  ackWaitMs?: number;       // default 30_000
  maxAttempts?: number;     // default 5, including the first attempt
  maxInFlight?: number;     // default 1_000, global per durable consumer
  backoffMs?: number[];     // default [1_000, 5_000, 30_000, 120_000]
};

type RetentionConfig = {
  maxAgeMs: number;
  maxBytes: number;
  maxMessages?: number;
};

type OrderingConfig =
  | { mode: "none" }
  | { mode: "partitioned"; partitions: number };

type PublishReceipt = {
  messageId: string;
  streamSequence: number;
  duplicate: boolean;
};
```

Defaults are documented and visible through `resources()`. Production callers
should normally set retention and global in-flight limits from an operational
budget. Queue, job, topic, pump, scheduler, and ephemeral payloads are UTF-8
JSON snapshots and must be JSON values. Generics do not perform runtime
validation; callers validate at their domain boundary.

Primitive payload limits apply to the complete encoded Sync envelope, not only
to its application `data` field. Sync measures the encoded bytes before publish
and throws `PayloadTooLargeError` locally with the actual size and configured
limit. It does not depend on a NATS server rejection for this public contract.
The initial defaults remain deliberately conservative:

| Payload | Default maximum |
| --- | ---: |
| Queue, job, and topic envelope | 128 KiB |
| Ephemeral value envelope | 4 KiB |
| Persisted pump page | 128 KiB |
| Internal scheduler definition or control envelope | 16 KiB |

The effective transport maximum is still the smallest of the Sync primitive
limit, the backing stream's `MaxMsgSize`, and the NATS account/server limit.
Raising one layer does not raise the others. Sync does not compress application
payloads automatically. Larger data uses `objectStore()` explicitly or an
application-owned permanent store.

Omitted `tenantId` means the literal logical tenant `"default"`. IDs, tenant
IDs, keys, consumer names, and ordering keys are non-empty UTF-8 strings with
documented byte limits. V6 rejects an input whose encoded NATS subject or KV key
would exceed its safe bound instead of truncating or hashing it silently.

Sync awaits the JetStream publish acknowledgement before `publish`, `send`,
`submit`, or schedule mutation succeeds. A successful call means the configured
stream quorum accepted the message, not that a handler started or finished.

## Queue

Queue is the low-level durable work primitive. It supports manual settlement and
an auto-settling processor.

```ts
type QueueConfig = {
  id: string;
  owner?: string;
  delivery?: DeliveryConfig;
  retention?: RetentionConfig;
  dedupeWindowMs?: number; // default 120_000; one stream-level NATS window
  ordering?: OrderingConfig;
  maxPayloadBytes?: number;
  replicas?: number;
};

type QueueSend<T> = {
  data: T;
  tenantId?: string;
  idempotencyKey?: string;
  orderingKey?: string;
  delayMs?: number;
  at?: Date;
  meta?: Record<string, JsonValue>;
};

type QueueDelivery<T> = {
  data: T;
  messageId: string;
  attempt: number;
  publishedAt: Date;
  tenantId: string;
  orderingKey?: string;
  meta?: Record<string, JsonValue>;
  signal: AbortSignal;
  ack(): Promise<void>;
  retry(options?: { delayMs?: number; reason?: string }): Promise<void>;
  deadLetter(options: { reason: string; error?: string }): Promise<void>;
  heartbeat(): Promise<void>;
};

type QueueMessage<T> = Omit<QueueDelivery<T>, "ack" | "retry" | "deadLetter">;

type QueueReader<T> = {
  receive(options?: { waitMs?: number; signal?: AbortSignal }): Promise<QueueDelivery<T> | null>;
  stream(options?: { signal?: AbortSignal }): AsyncIterable<QueueDelivery<T>>;
  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
};

type DeadLetter<T> = {
  messageId: string;
  data: T;
  attempts: number;
  failedAt: Date;
  reason: string;
  error?: string;
};

type DeadLetterStore<T> = {
  list(options?: { limit?: number; after?: string }): Promise<DeadLetter<T>[]>;
  requeue(input: { messageId: string; idempotencyKey: string }): Promise<PublishReceipt>;
  delete(input: { messageId: string }): Promise<boolean>;
};

type Queue<T> = {
  ready(): Promise<void>;
  send(message: QueueSend<T>): Promise<PublishReceipt>;
  process(options: ProcessOptions, handler: (message: QueueMessage<T>) => Promise<void>): Promise<Worker>;
  reader(options?: { signal?: AbortSignal }): Promise<QueueReader<T>>;
  deadLetters: DeadLetterStore<T>;
};
```

Processor semantics are intentionally small:

- handler resolves: confirmed ack;
- handler throws before `maxAttempts`: delayed negative ack using `backoffMs`;
- handler throws on the final attempt: publish to the DLQ with the original
  message ID, await its PubAck, then confirmed-ack the source message;
- process dies before settlement: NATS redelivers after `ackWaitMs`;
- `heartbeat()` sends an in-progress acknowledgement and resets `ackWaitMs`;
- Sync never claims exactly-once execution.

The auto-settling `process()` handler does not receive `ack`, `retry`, or
`deadLetter` methods. Use `reader()` when application code must make those
settlement decisions message by message.

Settlement methods use confirmed acknowledgements and throw
`StaleDeliveryError` when NATS no longer accepts the delivery's ack token. They
do not return an ambiguous boolean that callers might ignore.

DLQ transfer uses the original message ID as the target stream's NATS message
ID. A crash after DLQ publish but before source ack can repeat the transfer, but
the DLQ dedupe window collapses that repeat. This is not a cross-stream atomic
transaction.

`delayMs` and `at` use a one-shot NATS message schedule targeting the queue's
work subject. They do not occupy a consumer slot while waiting. `delayMs` and
`at` are mutually exclusive.

### Ordering

`ordering: { mode: "none" }` is the default. `orderingKey` is then metadata only
and provides no ordering guarantee.

`ordering: { mode: "partitioned", partitions: 64 }` creates 64 non-overlapping
subjects and 64 durable consumers with `MaxAckPending = 1`. `send()` requires an
`orderingKey`, hashes it to one stable partition, and preserves serial delivery
within that partition. Different keys can collide and therefore serialize.
Changing the partition count is an incompatible resource change.

This makes the cost visible: stronger per-key ordering creates more NATS
consumers and caps total in-flight work to the partition count. It is intended
for record-event and per-aggregate processing, not general workflow fan-out.

## Job

Job is the normal API for Cloud background tasks. It composes a queue with a
required idempotency key, automatic retry/DLQ behavior, lifecycle traces, and
bounded fan-out submission. It does not store application results or expose a
`join()` promise; durable domain status belongs in the application's database.

```ts
type JobConfig = Omit<QueueConfig, "ordering"> & {
  ordering?: OrderingConfig;
  terminalRetentionMs?: number; // diagnostics and DLQ, default 7 days
};

type JobSubmit<Input> = {
  key: string;
  input: Input;
  tenantId?: string;
  delayMs?: number;
  at?: Date;
  orderingKey?: string;
  meta?: Record<string, JsonValue>;
};

type JobContext<Input> = {
  jobId: string;
  key: string;
  input: Input;
  attempt: number;
  failureCount: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

type JobFailureDecision =
  | { action: "retry"; delayMs?: number }
  | { action: "dead_letter"; reason: string };

type JobProcessOptions<Input> = ProcessOptions & {
  onError?: (input: {
    context: JobContext<Input>;
    error: Error;
  }) => JobFailureDecision | Promise<JobFailureDecision>;
};

type Job<Input> = {
  ready(): Promise<void>;
  submit(job: JobSubmit<Input>): Promise<PublishReceipt & { jobId: string }>;
  submitMany(
    jobs: Iterable<JobSubmit<Input>> | AsyncIterable<JobSubmit<Input>>,
    options?: { publishConcurrency?: number; signal?: AbortSignal },
  ): Promise<{ accepted: number; duplicates: number }>;
  process(options: JobProcessOptions<Input>, handler: (context: JobContext<Input>) => Promise<void>): Promise<Worker>;
  deadLetters: DeadLetterStore<{ key: string; input: Input }>;
};
```

`key` becomes the stable NATS message ID within the configured dedupe window.
A repeated submission returns the original stream sequence with
`duplicate: true`. After the window expires the same key can be accepted again;
applications needing permanent uniqueness enforce it in their durable store.

Sync derives the NATS message ID from the complete resource identity,
`tenantId`, and `key`; identical keys in different tenants do not deduplicate
one another. Queue and topic idempotency keys use the same scoping rule.

When a handler throws, `onError` can choose an explicit retry delay or immediate
dead-lettering. Without `onError`, Sync retries according to `delivery.backoffMs`
until `maxAttempts`, then dead-letters. If `onError` itself throws, the original
delivery is retried: failure-policy code must not accidentally acknowledge work.
Requesting another retry on the final attempt dead-letters instead. This keeps
the short form simple while allowing Cloud to persist domain failure state
before it chooses transport settlement.

`submitMany()` bounds producer-side publish promises. Its
`publishConcurrency` has no effect on execution concurrency. It is not atomic:
if one publish fails, prior accepted items remain accepted and the thrown
`BatchSubmitError` reports the accepted and duplicate counts. This is suitable
for embarrassingly parallel fan-out because every accepted item owns its own
delivery and retry lifecycle.

```ts
await workflowRuns.submitMany(
  runIds.map((runId) => ({ key: `run:${runId}`, input: { runId } })),
  { publishConcurrency: 128 },
);
```

Adding pods increases execution capacity without changing the durable consumer.
Changing local `process()` concurrency does not create new streams or consumers.

## Topic

Topic exposes four deliberately different reads instead of calling all of them
"live":

1. `live()` is Core NATS broadcast with no durability.
2. `replay()` is an ephemeral JetStream read from an explicit cursor.
3. `follow()` is a continuous ephemeral JetStream read with cursors.
4. `process()` is a named durable JetStream consumer shared by workers.

```ts
type TopicConfig = {
  id: string;
  owner?: string;
  retention: RetentionConfig;
  dedupeWindowMs?: number;
  maxPayloadBytes?: number;
  replicas?: number;
};

type TopicCursor = string; // opaque, resource-bound v6 cursor

type TopicEvent<T> = {
  data: T;
  eventId: string;
  cursor: TopicCursor;
  tenantId: string;
  orderingKey?: string;
  publishedAt: Date;
  meta?: Record<string, JsonValue>;
};

type TopicLiveEvent<T> = Omit<TopicEvent<T>, "cursor">;

type Topic<T> = {
  ready(): Promise<void>;
  publish(input: {
    data: T;
    tenantId?: string;
    idempotencyKey?: string;
    orderingKey?: string;
    meta?: Record<string, JsonValue>;
  }): Promise<PublishReceipt & { eventId: string; cursor: TopicCursor }>;

  latestCursor(options?: { tenantId?: string }): Promise<TopicCursor | null>;
  live(options?: { tenantId?: string; signal?: AbortSignal }): AsyncIterable<TopicLiveEvent<T>>;
  replay(options: {
    tenantId?: string;
    after?: TopicCursor;
    until?: TopicCursor; // defaults to the head captured when replay starts
    signal?: AbortSignal;
  }): AsyncIterable<TopicEvent<T>>;
  follow(options: {
    tenantId?: string;
    after?: TopicCursor;
    signal?: AbortSignal;
  }): AsyncIterable<TopicEvent<T>>;

  process(
    options: ProcessOptions & {
      consumer: string;
      tenantId?: string;
      delivery?: DeliveryConfig;
      start?: "earliest" | "latest" | { after: TopicCursor };
    },
    handler: (event: TopicEvent<T> & { attempt: number; signal: AbortSignal }) => Promise<void>,
  ): Promise<Worker>;
};
```

`publish()` is a JetStream publish to the topic subject. Core NATS subscribers
on that subject receive the same publication without a second application
publish. A Core subscriber can observe it before the publisher receives its
PubAck. Core delivery alone does not prove that durable acceptance succeeded.
`live()` is therefore suitable for invalidation followed by a source-of-truth
read, not as durable acceptance evidence.

`live()` creates a normal NATS subscription on the shared process connection.
Every matching subscription receives the event. Its event deliberately has no
cursor, ack, reconnect replay, or retention-gap signal because Core NATS cannot
know the JetStream sequence returned later in the publisher's PubAck. Cloud
should normally create a small number of process-wide live subscriptions and
fan out locally to its WebSockets.

`replay()` creates an ephemeral ordered JetStream consumer, captures the current
head when `until` is omitted, yields through that head, and closes. `follow()`
uses the same cursor contract but remains open for new events. These consumers
are not shared across pods and do not acknowledge application work. A cursor
identifies the topic resource and JetStream stream sequence; using it with
another topic fails. If the requested sequence is older than retained stream
state, Sync throws `RetentionGapError` before yielding and the caller must
re-snapshot. `follow()` is the explicit higher-resource-cost choice when a
client needs continuous replayable events rather than cheap invalidations.

`process({ consumer: "analytics" })` creates one durable pull consumer. Every
pod using the same name competes for that cursor's messages; a different name
gets an independent copy. Handler success confirmed-acks. Handler failure uses
the declared delivery retry policy and eventually the topic consumer's DLQ.
Consumer configuration is durable resource configuration and drift-checks.

Topic order is the stream's publish order, but concurrent durable handlers can
complete out of order. A consumer that requires serial processing uses
`concurrency: 1`. Key-partitioned work belongs in a queue/job sink rather than
pretending one topic cursor has independent ordered sub-cursors.

## Pump

Pump incrementally drains a finite cursor source. It persists the current page,
per-item completion, and the committed source cursor before requesting another
page.

```ts
type PumpItem = { key: string };

type PumpConfig<Input, Cursor, Item extends PumpItem> = {
  id: string;
  owner?: string;
  batchSize?: number;             // default 100
  dispatchConcurrency?: number;   // default 1, per active run
  maxActiveRuns?: number;         // global internal-consumer maxInFlight
  retention?: { terminalMs: number; maxPageBytes: number };
  retry?: { maxAttempts: number; backoffMs: number[] };
  pull(context: {
    input: Input;
    cursor: Cursor | null;
    limit: number;
    signal: AbortSignal;
  }): Promise<{ items: Item[]; nextCursor: Cursor | null }>;
  dispatch(context: { input: Input; item: Item; signal: AbortSignal }): Promise<void>;
};

type Pump<Input, Cursor> = {
  ready(): Promise<void>;
  start(input: { key: string; input: Input; meta?: Record<string, JsonValue> }): Promise<void>;
  process(options?: ProcessOptions): Promise<Worker>;
  get(input: { key: string }): Promise<PumpState<Input, Cursor> | null>;
  cancel(input: { key: string }): Promise<boolean>;
};

type PumpState<Input, Cursor> = {
  key: string;
  input: Input;
  cursor: Cursor | null;
  status: "queued" | "running" | "waiting" | "completed" | "failed" | "canceled";
  dispatched: number;
  failureCount: number;
  lastError?: string;
  createdAt: Date;
  updatedAt: Date;
};
```

There are two independent concurrency controls:

- `process({ concurrency })`: active pump runs in this process;
- `dispatchConcurrency`: simultaneously dispatched items inside each active
  run.

With `process({ concurrency: 4 })` and `dispatchConcurrency: 32`, one process
can run at most four pulls and 128 dispatch callbacks. `maxActiveRuns` caps
unacknowledged run leases globally across pods. These numbers are not silently
derived from one another.

Within a page, completed item keys are checkpointed individually. A crash can
repeat an item whose sink succeeded before its checkpoint was confirmed, so the
sink remains idempotent by `item.key`. The source cursor advances only after all
page items are checkpointed. `pull()` is never run concurrently for the same
pump run.

Use a job sink when item execution needs an independent long retry lifecycle.
In that pattern `dispatch()` waits only for durable job acceptance, allowing the
pump to fan out quickly while each job is processed independently.

## Scheduler

Scheduler uses NATS 2.14 recurring message schedules as its clock. Sync adds
named definitions, timezone validation, misfire policy, durable execution,
run numbering, failure state, local handler registration, and remote control.

```ts
type SchedulerConfig = {
  id: string;
  owner?: string;
  delivery?: DeliveryConfig;
  retention?: RetentionConfig;
};

type ScheduleContext = {
  scheduleId: string;
  runId: string;
  runNumber: number;
  slot: Date;
  trigger: "schedule" | "manual";
  attempt: number;
  signal: AbortSignal;
  heartbeat(): Promise<void>;
};

type ScheduleInfo = {
  id: string;
  cron: string;
  timezone: string;
  misfire: "latest" | "all";
  nextRunAt: Date;
  runNumber: number;
  failureCount: number;
  handlerAvailable: boolean;
  meta?: Record<string, JsonValue>;
};

type Scheduler = {
  ready(): Promise<void>;
  create(config: {
    id: string;
    cron: string; // standard five-field cron: minute hour day month weekday
    timezone?: string;
    misfire?: "latest" | "all"; // default "latest"
    meta?: Record<string, JsonValue>;
    process(context: ScheduleContext): Promise<void>;
  }): Promise<{ created: boolean; updated: boolean }>;
  start(options?: ProcessOptions): Promise<Worker>;
  delete(input: { id: string }): Promise<boolean>;
  runNow(input: { id: string; requestId: string }): Promise<{ runId: string }>;
  get(input: { id: string }): Promise<ScheduleInfo | null>;
  list(): Promise<ScheduleInfo[]>;
};
```

The NATS schedule publishes ticks to an internal durable stream even when no
Cloud process is running. A tick PubAck is broker-owned; execution remains
at-least-once through the work consumer.

Sync sets generated schedule-message TTL to `never`; the declared work-stream
retention, not NATS' short schedule default, bounds accepted ticks.

Sync validates the public five-field cron and prefixes a zero-second field for
NATS' six-field schedule expression. Sync does not expose sub-minute schedules
in v6. This preserves the established Cloud contract while still delegating the
clock to NATS.

`misfire: "all"` executes every retained slot. `misfire: "latest"` coalesces
unprocessed slots per schedule in KV and executes only the newest known slot
after recovery. It never silently means "drop everything older than a fixed
wall-clock duration". The KV transition and work enqueue use stable message IDs;
duplicates can occur but missing accepted latest work must not.

`start({ concurrency })` is local schedule-handler concurrency across all
schedules registered on that scheduler handle. Per-schedule overlapping runs
are prevented by a partitioned execution key unless a later explicit API adds
overlap. Different schedules can execute concurrently.

`runNow()` returns after the manual run is durably accepted. It does not mean a
live handler accepted it or that execution completed. Repeating the same
`requestId` returns the same run. Handler availability and completion are read
through diagnostics or application-owned audit state.

Changing cron, timezone, or misfire policy replaces the NATS schedule only after
the new definition is durably accepted. `delete()` removes the recurring NATS
schedule and prevents future ticks; already accepted executions remain unless
the application cancels them through its own domain contract.

## Mutex

```ts
type Lock = {
  resource: string;
  ownerToken: string;
  fence: bigint;
  expiresAt: Date;
};

type MutexConfig = {
  id: string;
  owner?: string;
  ttlMs?: number;       // default 10_000
  retry?: { attempts?: number; delayMs?: number };
  replicas?: number;
};

type Mutex = {
  ready(): Promise<void>;
  acquire(resource: string, options?: { ttlMs?: number; signal?: AbortSignal }): Promise<Lock | null>;
  extend(lock: Lock, options?: { ttlMs?: number }): Promise<boolean>;
  release(lock: Lock): Promise<boolean>;
  withLock<T>(resource: string, fn: (lock: Lock) => Promise<T>, options?: { ttlMs?: number; signal?: AbortSignal }): Promise<T | null>;
};
```

Acquire uses KV create/compare-and-set. The KV stream revision at successful
acquisition becomes a monotonic `fence`. Extension compare-and-sets the current
owner record and preserves the acquisition fence. Release succeeds only for the
current owner. Per-key TTL frees abandoned locks.

A lease does not stop a stale owner from writing to PostgreSQL or an external
service after expiry. Consumers requiring strict exclusion persist and compare
`fence`, or make the effect idempotent. Sync does not market the mutex as a
distributed transaction.

## Ephemeral

```ts
type EphemeralConfig = {
  id: string;
  owner?: string;
  ttlMs: number;
  history?: number;       // default 1
  maxEntries?: number;
  maxValueBytes?: number;
  replicas?: number;
};

type Ephemeral<T> = {
  ready(): Promise<void>;
  upsert(input: { tenantId?: string; key: string; value: T; ttlMs?: number }): Promise<EphemeralEntry<T>>;
  touch(input: { tenantId?: string; key: string; ttlMs?: number }): Promise<boolean>;
  remove(input: { tenantId?: string; key: string }): Promise<boolean>;
  snapshot(input?: { tenantId?: string; prefix?: string }): Promise<EphemeralSnapshot<T>>;
  watch(input?: {
    tenantId?: string;
    prefix?: string;
    after?: string;
    signal?: AbortSignal;
  }): AsyncIterable<EphemeralEvent<T>>;
};

type EphemeralEntry<T> = {
  key: string;
  value: T;
  revision: string;
  createdAt: Date;
  updatedAt: Date;
  expiresAt: Date;
};

type EphemeralSnapshot<T> = {
  entries: EphemeralEntry<T>[];
  revision: string;
};

type EphemeralEvent<T> =
  | { type: "upsert"; entry: EphemeralEntry<T>; revision: string }
  | { type: "delete" | "expire"; key: string; revision: string }
  | { type: "resync_required"; requested: string; firstAvailable: string };
```

Entries use NATS KV per-key TTL. Tenant and key are encoded into KV-safe tokens;
the original values remain in the envelope. `snapshot()` returns a revision that
can be passed to `watch({ after })`. If history no longer covers that revision,
the watcher yields one explicit `resync_required` event and stops. The caller
takes a new snapshot; Sync never silently skips to the current head.

`tenantId` is a logical key namespace, not a NATS account security boundary.
`prefix` is a filter within that namespace. Capacity is bucket-wide unless the
resource declares separate physical resources; Sync does not claim a per-tenant
quota that NATS is not enforcing.

## Object store

Object store is the explicit large-artifact primitive. It wraps the official
NATS Object Store implementation rather than reimplementing chunking, streaming,
or digest verification.

```ts
type ObjectStoreConfig = {
  id: string;
  owner?: string;
  storage?: "file" | "memory"; // default inherited from Sync
  replicas?: number;            // default inherited from Sync
  compression?: "none" | "s2"; // default "none"; explicit bucket setting
  retention: {
    maxAgeMs: number;            // bucket-wide; required
    maxBytes: number;            // bucket-wide; required
  };
  maxObjectBytes: number;        // required application safety limit
};

type ObjectRef = {
  storeId: string;
  tenantId: string;
  key: string;
  size: number;
  digest: string; // canonical SHA-256 digest returned by NATS Object Store
};

type StoredObject = {
  ref: ObjectRef;
  metadata: Record<string, string>;
  modifiedAt: Date;
  body: ReadableStream<Uint8Array>;
};

type ObjectInfo = Omit<StoredObject, "body">;

type ObjectStoreEvent =
  | { type: "put"; object: ObjectInfo }
  | { type: "delete"; tenantId: string; key: string };

type ObjectStore = {
  ready(): Promise<void>;
  put(input: {
    tenantId?: string;
    key: string;
    body: ReadableStream<Uint8Array>;
    metadata?: Record<string, string>;
    signal?: AbortSignal;
  }): Promise<ObjectRef>;
  get(ref: ObjectRef, options?: { signal?: AbortSignal }): Promise<StoredObject | null>;
  info(input: { tenantId?: string; key: string }): Promise<ObjectInfo | null>;
  delete(input: { tenantId?: string; key: string }): Promise<boolean>;
  list(input?: { tenantId?: string; prefix?: string }): AsyncIterable<ObjectInfo>;
  watch(input?: {
    tenantId?: string;
    prefix?: string;
    signal?: AbortSignal;
  }): AsyncIterable<ObjectStoreEvent>;
};
```

`put()` streams chunks and succeeds only after NATS has accepted the complete
object metadata. It counts uncompressed input bytes while streaming and aborts
with `ObjectTooLargeError` once `maxObjectBytes` is exceeded. The returned
`ObjectRef` is a JSON value suitable for an ordinary queue or job payload.
`get(ref)` rejects a reference for another store and verifies the expected size
and digest while the body is consumed. Callers that only know a key use
`info()` first and then read the resulting reference.

As with the other primitives, omitted `tenantId` means `"default"`. Tenant and
key are encoded into the physical NATS object name; the original strings remain
in metadata and in `ObjectRef`. This prevents collisions but is not an
authorization boundary.

Putting the same key replaces the visible object. Concurrent puts are
last-completed-writer-wins. Consumers that require immutable inputs use stable,
unique keys and retain the returned digest in the reference. Sync exposes no
claim of a cross-resource transaction between `put()` and a later job submit.
If the object write succeeds and submission fails, retention eventually removes
the unreferenced object.

References do not pin objects. Jobs do not automatically delete their input on
success because retries, redelivery, DLQs, and fan-out may still need it. The
caller chooses bucket retention such that:

```text
object maxAge > maximum queue residence + retry window + operational margin
```

Explicit `delete()` is available when the application knows the artifact is no
longer shared. Sync v6 deliberately has no reference counting, retain/release
protocol, per-object TTL, or hidden cleanup coupled to queue settlement.

Object Store is intended for bounded internal artifacts such as workflow input,
intermediate results, imports, exports, reindex snapshots, and diagnostic
bundles. Permanent end-user files, public downloads, archives, and very large
datasets remain in Filegate, S3, or another application-owned object store.
Sync never auto-offloads an oversized queue, job, or topic payload; callers
upload explicitly and publish the returned reference.

A normal fan-out shares one immutable artifact instead of copying it into every
job message:

```ts
const artifacts = sync.objectStore({
  id: "cloud.workflow-artifacts",
  retention: {
    maxAgeMs: 7 * 24 * 60 * 60 * 1_000,
    maxBytes: 100 * 1024 ** 3,
  },
  maxObjectBytes: 512 * 1024 ** 2,
});

const artifact = await artifacts.put({
  key: `runs/${runId}/input`,
  body,
  metadata: { contentType: "application/json" },
});

await workflowRuns.submit({
  key: runId,
  input: { runId, artifact },
});
```

## Retry

`retry()` remains a top-level local helper. Its v5 callback decision model can
remain, with Redis-specific transport errors removed from
`isRetryableTransportError()`. It creates no Sync or NATS resource and its
`concurrency` is entirely whatever the caller creates.

It is exported from the browser-safe `@k2b/sync/retry` subpath. The old
`@k2b/sync/browser` parity package is removed; queue, topic, job, pump,
scheduler, mutex, ephemeral, and object store are server/NATS APIs only.

## Resource naming and ownership

User-provided names never become raw NATS resource names or subject tokens.
For every declaration Sync computes:

```text
identity = UTF8(JSON.stringify([namespace, kind, id]))
hash     = first 20 base32 characters of SHA-256(identity)
nsHash   = first 12 base32 characters of SHA-256(UTF8(namespace))
```

The shortened hash keeps NATS names predictable; the full identity and full
SHA-256 are stored in metadata. If an existing short hash has a different full
hash, Sync fails with `ResourceIdentityCollisionError` rather than sharing it.

Resource names:

| Primitive | NATS resources |
| --- | --- |
| Namespace metadata | `S6_META_<nsHash>` KV bucket |
| Topic | `S6_T_<hash>` limits-retention stream; `S6_TC_<hash>_<consumerHash>` durable consumers; `S6_TD_<hash>` consumer DLQ stream |
| Queue | `S6_Q_<hash>` work-queue stream; `S6_QC_<hash>[_<partition>]` consumers; `S6_QD_<hash>` DLQ stream |
| Job | Queue resources with kind `job`; `S6_JD_<hash>` DLQ stream |
| Pump | `S6_P_<hash>` KV bucket plus `S6_PQ_<hash>` internal work stream/consumer |
| Scheduler | `S6_S_<hash>` schedule/tick stream, `S6_SQ_<hash>` execution stream, `S6_SK_<hash>` KV state |
| Mutex | `S6_M_<hash>` KV bucket |
| Ephemeral | `S6_E_<hash>` KV bucket |
| Object store | `S6_O_<hash>` Object Store bucket backed by stream `OBJ_S6_O_<hash>` |

Subject roots use lower-case hashes and no application data:

```text
sync.v6.<nsHash>.topic.<hash>.t.<tenantToken>.event
sync.v6.<nsHash>.queue.<hash>.t.<tenantToken>.work[.<partition>]
sync.v6.<nsHash>.queue.<hash>.t.<tenantToken>.dlq
sync.v6.<nsHash>.scheduler.<hash>.tick.<scheduleToken>
sync.v6.<nsHash>.scheduler.<hash>.run.<scheduleToken>
```

`tenantToken`, schedule IDs, keys, and consumer names are injective base64url
encodings when used in subjects or KV keys. Resource metadata always contains:

```ts
type ResourceMetadata = {
  "sync.api": "6";
  "sync.namespace": string;
  "sync.kind": SyncResourceKind;
  "sync.id": string;
  "sync.owner": string;
  "sync.identity_sha256": string;
  "sync.managed": "true";
};
```

The `application` opening a resource appears in runtime diagnostics, but does
not mutate durable ownership metadata on every startup.

## Resource creation and drift

Sync creates a missing managed resource with the declared configuration. If a
resource already exists, Sync compares all semantic fields it relies on,
including:

- subjects and retention policy;
- storage, replicas, max age, bytes, messages, and payload size;
- duplicate window, message TTL, rollup, and schedule feature flags;
- consumer filter, ack policy, ack wait, max delivery, backoff, and
  `MaxAckPending`;
- KV history, TTL, storage, replicas, and limits;
- Object Store TTL, maximum bytes, storage, replicas, compression, and Sync's
  maximum object size;
- Sync identity, API version, kind, ID, and owner metadata.

An incompatible difference throws `ResourceDriftError` with a sanitized field
diff. Sync does not silently update, delete, purge, shrink, or adopt that
resource. Operational tooling can inspect `resources()` and deliberately apply
the desired change. This keeps rolling pods from fighting over configuration.

Two declarations for the same identity but different configurations in one
process fail before NATS I/O with `ConflictingResourceDeclarationError`.

## Diagnostics

`health()` is synchronous local state suitable for an application health
endpoint:

```ts
type SyncHealth = {
  state: "starting" | "ready" | "degraded" | "draining" | "stopped";
  connection: "connected" | "reconnecting" | "closed";
  pendingResources: number;
  driftedResources: number;
  activeWorkers: number;
  activeHandlers: number;
};
```

`resources()` reads sanitized server state and reports stable resource identity,
NATS names, ownership, configuration, message and object counts, bytes, consumer
pending, ack-pending, redelivery, DLQ, schedule, KV, and replica health. It
never returns credentials or message payloads.

`events()` and `observe` expose bounded structured events for connection changes,
resource reconciliation and drift, worker lifecycle, publish failure,
redelivery, ack/nack, dead-lettering, lock loss, pump recovery, schedule ticks,
object put/get/delete failures, and drain timeout. Observer failures are
contained and never alter transport settlement. Sync drops observer events
instead of blocking work after its documented local observation buffer fills,
and reports a dropped-event counter.

Trace callbacks are observations, not audit logs. Cloud persists durable audit
and user-visible job/run state in PostgreSQL.

## Cloud acceptance mapping

The later Cloud migration should be able to express its important consumers as
follows without changing domain ownership:

| Cloud use | Sync v6 shape |
| --- | --- |
| Workflow runs and AI workflow tasks | `job.process({ concurrency })`; PostgreSQL remains run/task truth |
| Large workflow input and temporary results | explicit Object Store upload plus `ObjectRef` in the job payload |
| AI turns, notifications, snapshot work | queue/job competing consumers with local concurrency and global max-in-flight |
| Grids record-event keyed work | partitioned queue by record ID |
| Record/notebook/mail event logs | durable topic with independent named consumers |
| Browser invalidation | one or few process-wide `topic.live()` readers plus Cloud-local WebSocket fan-out |
| Notebook presence and app registry | ephemeral KV snapshot/watch |
| Mail automation backfill and reindexing | pump with bounded page and explicit dispatch concurrency, often into jobs |
| Cron workflows and maintenance | NATS-backed scheduler; accepted ticks survive complete Cloud restart |
| Provider refresh and short critical sections | fenced mutex; PostgreSQL or domain idempotency guards stale effects |

The Sync epic can be complete only when real cluster tests show that these
contracts survive process death, complete client outage, reconnect, one NATS
node loss, duplicate delivery, stale consumers, retention gaps, and resource
drift. Cloud is not production-ready on v6 until its separate migration and
end-user acceptance work is complete.

## Required implementation proofs

Before freezing this proposal as implemented API:

- verify Bun behavior with the official NATS.js connection, JetStream, KV,
  Object Store streaming, message schedules, drain, and reconnect APIs;
- prove local `concurrency` never over-pulls and global `maxInFlight` is shared
  across at least three worker processes;
- prove delayed messages and recurring schedules survive complete client
  outage;
- prove confirmed ack still permits duplicate execution at crash boundaries;
- prove DLQ transfer is repeat-safe;
- prove partitioned ordering under retry, worker death, and multiple pods;
- prove topic cursor mismatch and retention gaps fail explicitly;
- prove pump parallel checkpoints repeat only ambiguous items;
- prove mutex expiry, stale extend/release, and monotonic fencing;
- prove Object Store streaming does not buffer complete objects, enforces
  `maxObjectBytes`, detects digest mismatch, expires orphaned data, and remains
  readable after one NATS node loss;
- prove resource drift never mutates existing state;
- pack the package and run a fresh Bun TypeScript consumer.

## NATS references

- [Delivery, acknowledgement, and at-least-once redelivery](https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment)
- [Scaling several workers on one durable consumer](https://docs.nats.io/learn/jetstream/worker-pool)
- [JetStream retention policies](https://docs.nats.io/learn/jetstream/retention-policies)
- [Pull consumers and bounded fetches](https://docs.nats.io/learn/jetstream/pull-consumers)
- [NATS KV compare-and-set, watch, history, and TTL](https://docs.nats.io/learn/key-value/)
- [JetStream Object Store overview and chunked large-object storage](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/README.md#object-store)
- [Official NATS.js Object Store package and v3 migration notes](https://github.com/nats-io/nats.js/blob/main/migration.md#changes-to-objectstore)
- [NATS subscription and connection drain](https://docs.nats.io/using-nats/developer/receiving/drain)
- [NATS.js 3.4 release with NATS Server 2.14 message schedules](https://github.com/nats-io/nats.js/releases/tag/v3.4.0)
- [NATS Server stream configuration, including message schedules and batch publish](https://github.com/nats-io/nats.docs/blob/master/nats-concepts/jetstream/streams.md)
