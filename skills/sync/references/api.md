# @k2b/sync v6 — API reference

All primitives are created from a `Sync` instance and share these conventions:

- Factories perform no I/O; `await handle.ready()` (or the first operation) waits for provisioning.
- `tenantId?` defaults to `"default"`. Ids, tenant ids, keys, and consumer names are non-empty UTF-8 strings ≤ 96 bytes.
- `meta?: Record<string, JsonValue>` travels with messages/events.
- `Worker` handles: `{ active, capacity, stop(), drain({ timeoutMs? }), [Symbol.asyncDispose] }`.

```ts
type SyncConfig = {
  connection: NatsConnection;         // already connected, caller-owned
  namespace: string;                  // deployment isolation
  application: string;                // ownership/diagnostics metadata
  defaults?: { replicas?: number; storage?: "file" | "memory" }; // default 3 / "file"
  observe?: (event: SyncEvent) => void | Promise<void>;
};

type Sync = {
  ready(): Promise<void>;
  drain(options?: { timeoutMs?: number }): Promise<{ completed: number; aborted: number; timedOut: boolean }>;
  health(): SyncHealth;               // synchronous local state
  resources(): Promise<SyncResourceSummary[]>;
  events(options?: { signal?: AbortSignal }): AsyncIterable<SyncEvent>;
  queue<T>(config: QueueConfig): Queue<T>;
  topic<T>(config: TopicConfig): Topic<T>;
  job<Input>(config: JobConfig): Job<Input>;
  pump<Input, Cursor, Item extends { key: string }>(config: PumpConfig<Input, Cursor, Item>): Pump<Input, Cursor>;
  scheduler(config: SchedulerConfig): Scheduler;
  mutex(config: MutexConfig): Mutex;
  ephemeral<T>(config: EphemeralConfig): Ephemeral<T>;
  objectStore(config: ObjectStoreConfig): ObjectStore;
};
```

Shared delivery types:

```ts
type DeliveryConfig = {
  ackWaitMs?: number;    // default 30_000 — redelivery timer per delivery
  maxAttempts?: number;  // default 5, including the first attempt
  maxInFlight?: number;  // default 1_000 — GLOBAL MaxAckPending per durable consumer
  backoffMs?: number[];  // default [1_000, 5_000, 30_000, 120_000]; last entry repeats
};
type RetentionConfig = { maxAgeMs: number; maxBytes: number; maxMessages?: number };
type OrderingConfig = { mode: "none" } | { mode: "partitioned"; partitions: number };
type PublishReceipt = { messageId: string; streamSequence: number; duplicate: boolean };
```

## queue

```ts
const q = sync.queue<T>({
  id, owner?, delivery?, retention?,        // retention default 7d / 1 GiB
  dedupeWindowMs?,                          // default 120_000
  ordering?,                                // partitioned ⇒ send() requires orderingKey
  maxPayloadBytes?,                         // default 128 KiB (whole envelope)
  replicas?,
});

await q.send({ data, tenantId?, idempotencyKey?, orderingKey?, delayMs?, at?, meta? }); // → PublishReceipt
// delayMs/at (mutually exclusive) use one-shot broker schedules — no consumer slot used.

const worker = await q.process({ concurrency?, signal? }, async (message) => {
  // message: { data, messageId, attempt, publishedAt, tenantId, orderingKey?, meta?, signal, heartbeat() }
  // resolve → confirmed ack; throw → nak(backoff) until maxAttempts → DLQ
});

const reader = await q.reader();            // unpartitioned queues only
const delivery = await reader.receive({ waitMs? }); // null on timeout; min wait 1s
await delivery.ack();                        // confirmed; StaleDeliveryError on hard failure
await delivery.retry({ delayMs?, reason? }); // on final attempt: dead-letters instead
await delivery.deadLetter({ reason, error? });
for await (const d of reader.stream()) { ... }
await reader.close();

await q.deadLetters.list({ limit?, after? });                    // DeadLetter<T>[]
await q.deadLetters.requeue({ messageId, idempotencyKey });      // → PublishReceipt, removes DLQ entry
await q.deadLetters.delete({ messageId });                       // boolean
```

## job

```ts
const j = sync.job<Input>({ ...QueueConfig without ordering, ordering?, terminalRetentionMs? /* DLQ retention, default 7d */ });

await j.submit({ key, input, tenantId?, delayMs?, at?, orderingKey?, meta? });
// → PublishReceipt & { jobId }; key = NATS msgID within dedupe window, per tenant

await j.submitMany(iterableOrAsyncIterable, {
  publishConcurrency?,   // default 16 in-flight publish promises
  maxPendingBytes?,      // default 8 MiB in-flight encoded bytes (local backpressure)
  signal?,
}); // → { accepted, duplicates }; throws BatchSubmitError (accepted stay accepted)

await j.process({
  concurrency?, signal?,
  onError?: async ({ context, error }) =>
    ({ action: "retry", delayMs? } | { action: "dead_letter", reason }),
}, async (context) => {
  // context: { jobId, key, input, attempt, failureCount, signal, heartbeat() }
});

j.deadLetters // DeadLetterStore<{ key, input }>
```

## topic

```ts
const t = sync.topic<T>({ id, owner?, retention /* required */, dedupeWindowMs?, maxPayloadBytes?, replicas? });

const r = await t.publish({ data, tenantId?, idempotencyKey?, orderingKey?, meta? });
// → PublishReceipt & { eventId, cursor }

await t.latestCursor({ tenantId? });        // TopicCursor | null, per tenant

for await (const e of t.live({ tenantId?, signal? })) {}    // broadcast, no cursor/replay
for await (const e of t.replay({ tenantId?, after?, until?, signal? })) {} // to head-at-start
for await (const e of t.follow({ tenantId?, after?, signal? })) {}         // stays open
// events: { data, eventId, cursor, tenantId, orderingKey?, publishedAt, meta? }
// after below retention → RetentionGapError; foreign cursor → CursorMismatchError
// mid-follow retention loss → RetentionGapError (never silently skips)

await t.process({
  consumer,                       // same name competes, different names = independent cursors
  tenantId?, concurrency?, signal?,
  delivery?,                      // consumer config — drift-checked across pods
  start?: "earliest" | "latest" | { after: TopicCursor },
}, async (event /* TopicEvent & { attempt, signal } */) => { ... });
// failure: backoff retries → per-consumer DLQ stream
```

## pump

```ts
const p = sync.pump<Input, Cursor, Item extends { key: string }>({
  id, owner?,
  batchSize?,             // default 100 (pull limit)
  dispatchConcurrency?,   // default 1, per active run
  maxActiveRuns?,         // default 100 — global leased runs (consumer MaxAckPending)
  retention?: { terminalMs?, maxPageBytes? },  // 7d / 128 KiB record incl. page
  retry?: { maxAttempts?, backoffMs? },        // default 5 / [1s, 5s, 30s]
  leaseMs?,               // default 60_000, min 2_000 — crash takeover latency
  pull:     async ({ input, cursor, limit, signal }) => ({ items, nextCursor }), // nextCursor null = done
  dispatch: async ({ input, item, signal }) => {},  // idempotent by item.key
});

await p.start({ key, input, meta? });   // idempotent while active; restarts terminal runs
await p.process({ concurrency? });      // reconciles lost wake-ups on start
await p.get({ key });                   // PumpState | null: status queued|running|waiting|completed|failed|canceled
await p.cancel({ key });                // boolean, stops at the next checkpoint
await p.reconcile();                    // { requeued } — re-enqueue wake-ups from KV truth
```

Cursor advances only after every page item is checkpointed; a crash repeats only items finished after their last confirmed checkpoint.

## scheduler

```ts
const s = sync.scheduler({ id, owner?, delivery?, retention? /* tick retention, default 7d / 64 MiB */ });

await s.create({
  id, cron /* five-field, minute resolution */, timezone? /* IANA, default UTC */,
  misfire?: "latest" | "all",   // default "latest": only newest retained slot after downtime
  meta?,
  process: async (context) => {},
  // context: { scheduleId, runId, runNumber, slot, trigger: "schedule"|"manual", attempt, signal, heartbeat() }
}); // → { created, updated }; republish is self-healing and idempotent

await s.start({ concurrency? });   // serves locally created schedules; serial per schedule
await s.runNow({ id, requestId }); // → { runId }; durably accepted, idempotent per requestId
await s.get({ id });               // ScheduleInfo | null: cron, timezone, misfire, nextRunAt, runNumber, failureCount, handlerAvailable
await s.list();
await s.delete({ id });            // cancels the broker schedule; accepted ticks remain
```

The broker produces ticks while every app process is offline; a later `start()` executes them per misfire policy. Handler failure retries per `delivery`, then increments `failureCount` and drops the slot.

## mutex

```ts
const m = sync.mutex({ id, owner?, ttlMs? /* default 10_000, rounds up to seconds */, retry? /* { attempts: 10, delayMs: 200 } */, replicas? });

const lock = await m.acquire(resource, { ttlMs?, signal? }); // Lock | null
// Lock: { resource, ownerToken, fence: bigint /* monotonic, stable across extends */, expiresAt }
await m.extend(lock, { ttlMs? });   // boolean; false = lease lost
await m.release(lock);              // boolean; owner-only
await m.withLock(resource, async (lock) => value, { ttlMs?, signal? }); // value | null; always releases
```

A lease cannot stop an expired owner writing to external systems afterwards — persist and compare `fence`, or make effects idempotent.

## ephemeral

```ts
const e = sync.ephemeral<T>({ id, owner?, ttlMs /* required; seconds resolution */, history?, maxEntries? /* snapshot bound, default 10k */, maxValueBytes? /* default 4 KiB */, replicas? });

await e.upsert({ tenantId?, key, value, ttlMs? });  // → EphemeralEntry { key, value, revision, createdAt, updatedAt, expiresAt }
await e.touch({ tenantId?, key, ttlMs? });          // boolean; republishes last value with fresh TTL
await e.remove({ tenantId?, key });                 // boolean
await e.snapshot({ tenantId?, prefix? });           // { entries, revision }; SnapshotOverflowError beyond maxEntries
for await (const ev of e.watch({ tenantId?, prefix?, after?, signal? })) {
  // { type: "upsert", entry } | { type: "delete"|"expire", key } | { type: "resync_required", requested, firstAvailable }
}
```

## objectStore

```ts
const o = sync.objectStore({
  id, owner?, storage?, replicas?, compression?: "none" | "s2",
  retention: { maxAgeMs, maxBytes },  // required, bucket-wide
  maxObjectBytes,                     // required per-object cap, enforced mid-stream
});

const ref = await o.put({ tenantId?, key, body: ReadableStream, metadata?, signal? });
// → ObjectRef { storeId, tenantId, key, size, digest } — plain JSON, safe in payloads
const obj = await o.get(ref);        // StoredObject | null (null if deleted OR replaced since)
await o.info({ tenantId?, key });    // SyncObjectInfo | null
await o.delete({ tenantId?, key });  // boolean
for await (const i of o.list({ tenantId?, prefix? })) {}
for await (const ev of o.watch({ tenantId?, prefix?, signal? })) {} // put | delete
```

Refs don't pin: pick `retention.maxAgeMs > max queue residence + retry window + margin`.

## retry (`@k2b/sync/retry`, browser-safe)

```ts
const value = await retry<T>({
  run: async ({ ctx }) => produce(),          // ctx: { attempt }
  after: async ({ ctx }) => {                  // ctx adds { data?, error?, reschedule(), expBackoff() }
    if (ctx.error && ctx.attempt < 5) ctx.reschedule({ delayMs: ctx.expBackoff() });
  },
  signal?,
});
expBackoff(attempt, { baseMs?, maxMs?, jitter? });
isRetryableTransportError(error);   // network-vocabulary heuristics (no Redis codes in v6)
```

## Errors

`SyncError` base; `SyncLifecycleError`, `UnsupportedServerError`, `InvalidNameError`, `ConflictingResourceDeclarationError`, `ResourceIdentityCollisionError`, `ResourceDriftError { resource, differences }`, `PayloadTooLargeError { actualBytes, maxBytes }`, `ObjectTooLargeError`, `StaleDeliveryError`, `RetentionGapError { requested, firstAvailable }`, `CursorMismatchError`, `BatchSubmitError { accepted, duplicates }`, `SnapshotOverflowError { maxEntries }`.
