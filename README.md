# @k2b/sync

Synchronization primitives for TypeScript, published as one package with two runtimes:

- **`@k2b/sync`**: Redis-backed (6.2+, Valkey, Dragonfly), built for [Bun](https://bun.sh). For horizontally scaled systems where multiple service instances coordinate via shared state.
- **`@k2b/sync/browser`**: fully in-memory, zero dependencies. For local-first browser apps that want the same primitives without a server.

> Package migration: `@valentinkolb/sync` and the retired `@valentinkolb/sync-browser` package are deprecated. Use `@k2b/sync` and its `/browser` export.

Both runtimes share an **identical public API** — change the import, and code generally works. Type parity is enforced at compile-time (see `parity/`).

Provides nine modules: **ratelimit**, **mutex**, **queue**, **topic**, **ephemeral**, **job**, **pump**, **scheduler**, and **retry**. They compose — `job` uses `queue` internally, `scheduler` uses `mutex` for leader election.

## Installation

```bash
# Server and browser runtimes
bun add @k2b/sync
```

No runtime dependencies. TypeScript is a peer dependency.

> **Upgrading?** v4 users must migrate the public API. Deployments on `<=5.8.0`
> must also complete the durable namespace maintenance in
> [MIGRATION.md](./MIGRATION.md) before rolling out a newer version.

### Agent Skills (optional)

This repository ships a single `sync` agent skill in [`skills/sync/`](./skills/sync) with one reference file per module plus a v4→v5 migration guide. Install with the [Vercel Skills CLI](https://github.com/vercel-labs/skills):

```bash
bunx skills add https://github.com/k2b-dev/sync --skill sync
```

---

## Rate Limit

Sliding window rate limiter.

```ts
import { ratelimit, RateLimitError } from "@k2b/sync";

const limiter = ratelimit({ id: "api", limit: 100, windowSecs: 60 });

const result = await limiter.check("user:123");
// { limited: false, remaining: 99, resetIn: 58432 }

await limiter.checkOrThrow("user:123"); // throws RateLimitError if over
```

## Mutex

Distributed lock with retry, TTL auto-expiry, owner-only release.

```ts
import { mutex, LockError } from "@k2b/sync";

const m = mutex({ id: "checkout", defaultTtl: 5000 });

await m.withLock("order:123", async (lock) => {
  await m.extend(lock, 10_000);
  await processOrder();
});

await m.withLockOrThrow("order:123", async () => {
  await doExclusiveWork();
});
```

## Queue

Durable work queue with at-least-once delivery, lease-based visibility, delayed messages, idempotency, DLQ.

```ts
import { queue } from "@k2b/sync";

const q = queue<{ to: string; subject: string }>({
  id: "mail.send",
  delivery: { defaultLeaseMs: 60_000, maxDeliveries: 5 },
});

await q.send({
  data: { to: "user@example.com", subject: "Welcome" },
  idempotencyKey: "welcome:user@example.com",
  delayMs: 5_000,
});

const msg = await q.recv({ wait: true, timeoutMs: 30_000 });
if (msg) {
  try {
    await sendMail(msg.data);
    await msg.ack();
  } catch (err) {
    await msg.nack({ delayMs: 5_000, error: String(err) });
  }
}
```

Messages exceeding `maxDeliveries` move to DLQ. Extend active leases with `msg.touch()`. Optional `tenantId` for isolated queues.

## Topic

Pub/sub with Redis Streams. Consumer groups for at-least-once delivery, `live()` for best-effort fan-out.

```ts
import { topic } from "@k2b/sync";

const t = topic<{ type: string; orderId: string }>({
  id: "order.events",
  retentionMs: 7 * 24 * 60 * 60 * 1000,
});

await t.pub({ data: { type: "order.confirmed", orderId: "o1" } });

const startCursor = (await t.latestCursor()) ?? "0-0";

const reader = t.reader("analytics");

// Recover deliveries abandoned by a crashed consumer. Keep nextCursor between
// calls so one poison prefix cannot starve later pending entries.
let recoveryCursor = "0-0";
do {
  const batch = await reader.reclaim({ minIdleMs: 60_000, cursor: recoveryCursor });
  for (const entry of batch.entries) {
    if (entry.kind === "invalid") {
      await recordPoisonMessage(entry.eventId, entry.error);
      await entry.commit();
      continue;
    }
    await process(entry.delivery.data);
    await entry.delivery.commit();
  }
  recoveryCursor = batch.nextCursor;
} while (recoveryCursor !== "0-0");

// Consumer group (at-least-once, acked)
for await (const msg of reader.stream()) {
  await process(msg.data);
  await msg.commit();
}

// Live (best-effort, all listeners)
for await (const event of t.live({ after: startCursor })) {
  console.log(event.data);
}
```

Pass `invalidPayload: "throw"` to `recv()` or `stream()` to receive a
`TopicPayloadError` for malformed transport envelopes and leave the entry
pending. The default remains `"ack"` for compatibility. `reclaim()` returns
pending malformed entries with `kind: "invalid"` so the application can record
or dead-letter them before acknowledging. The browser runtime keeps at most 256
uncommitted deliveries per group and persists them when a `Store` is configured.
`reclaim()` can recover their snapshots until topic retention expires; an
expired snapshot is discarded only after its event has also left the bounded log.

## Ephemeral

TTL-based key/value with tenant isolation, snapshots with optional prefix filter, and change-stream reader.

```ts
import { ephemeral } from "@k2b/sync";

const presence = ephemeral<{ userId: string; displayName: string }>({
  id: "notebook.presence",
  ttlMs: 30_000,
});

await presence.upsert({
  tenantId: "notebook-abc",
  key: "peer-1",
  value: { userId: "u1", displayName: "Alice" },
});

await presence.touch({ tenantId: "notebook-abc", key: "peer-1" });

const snap = await presence.snapshot({ tenantId: "notebook-abc" });
// Filter by prefix (useful for replacing registry patterns):
const apps = await presence.snapshot({ prefix: "apps/" });

for await (const event of presence.reader({ tenantId: "notebook-abc" }).stream()) {
  // event.type: "upsert" | "touch" | "delete" | "expire" | "overflow"
}
```

`tenantId` isolates event streams, TTL zones, and `maxEntries` quota. `prefix` filters reads inside a tenant.

## Job

Durable background tasks with callback-based lifecycle.

```ts
import { job, isRetryableTransportError } from "@k2b/sync";

const sendMail = job<{ to: string }, { sent: boolean }>({
  id: "send-mail",
  defaults: { leaseMs: 30_000 },
  trace: async (event) => {
    await cloudTrace({ source: "send-mail", event });
  },

  process: async ({ ctx }) => {
    // ctx.input: { to: string } — typed
    // ctx.key, ctx.jobId, ctx.failureCount, ctx.duration, ctx.signal, ctx.heartbeat
    return { sent: true };
  },

  after: async ({ ctx }) => {
    // ctx.data?: result (on success)
    // ctx.error?: Error (on failure)
    // ctx.reschedule({ delayMs }) — re-queue, key stays claimed
    // ctx.expBackoff({ baseMs, maxMs, jitter }) — helper
    // ctx.metric — live JobMetrics reference

    if (ctx.error && ctx.failureCount < 3) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 1000, maxMs: 30_000 }) });
    }
  },
});

await sendMail.submit({ key: "welcome:42", input: { to: "u@x.com" } });
sendMail.metric(); // { dispatches, failures, reschedules }
```

Key lifecycle: claimed on submit, held during run and pending retry, released on terminal (success or failure without reschedule).
`trace` is observability-only: handler errors are logged and swallowed. On the server, `submitted` is a best-effort first-delivery-attempt event emitted immediately before the first `started`; it can be delayed by `delayMs` or absent if activation fails before tracing. In the browser runtime, `submitted` is emitted after the local queue accepts a new submission. `finished` fires only after terminal transport completion and key release; a job that calls `ctx.reschedule()` emits `rescheduled` instead.

**Input is optional** — simple jobs can omit both the input generic and the `input` submit field:

```ts
const sync = job({
  id: "sync",
  process: async () => { await doSync(); },
});
await sync.submit({ key: "daily" });
```

## Pump

Durably drain a cursor-based source into an idempotent consumer, one persisted page at a time.

```ts
import { pump } from "@k2b/sync";

type Cursor = { internalDate: string; id: string };

const messages = pump<
  { mailboxId: string; workflowId: string },
  Cursor,
  { key: string; messageId: string }
>({
  id: "mail.sender-rule-backfill",

  pull: async ({ input, cursor, limit, signal }) => {
    const rows = await loadMessages({
      mailboxId: input.mailboxId,
      after: cursor,
      limit,
      signal,
    });

    return {
      items: rows.map((row) => ({ key: row.id, messageId: row.id })),
      nextCursor: rows.length === limit
        ? { internalDate: rows.at(-1)!.internalDate, id: rows.at(-1)!.id }
        : null,
    };
  },

  dispatch: async ({ input, item, signal }) => {
    await emitWorkflowEvent({
      scopeId: input.mailboxId,
      targetWorkflowId: input.workflowId,
      type: "mail.messageReceived",
      dedupeKey: item.key,
      data: { messageId: item.messageId },
      signal,
    });
  },
});

await messages.start({
  key: "sender-rule:rule-1:revision-4",
  input: { mailboxId: "mailbox-1", workflowId: "workflow-1" },
});

await messages.get({ key: "sender-rule:rule-1:revision-4" });
await messages.cancel({ key: "sender-rule:rule-1:revision-4" });
messages.stop(); // local worker only; the durable execution is not canceled
```

`pump` persists each pulled page before dispatching it and checkpoints the item
index after every successful dispatch. A crashed node can therefore duplicate
the current item, but cannot skip it; `dispatch` must use `item.key` with an
idempotent consumer. The committed cursor advances only after the full page.
`nextCursor: null` completes the run.

Only `id`, `pull`, and `dispatch` are required. Defaults are `batchSize: 100`,
no delay between successful pages, a 30-second automatically heartbeated lease,
10 exponential retry attempts, 128 KiB maximum serialized page size, and seven
days of terminal-state retention. `input`, `cursor`, items, and `meta` must be
JSON-serializable. Repeated `start()` calls with the same key return the
existing execution.

Common durable sinks are `queue.send({ idempotencyKey: item.key })`,
`job.submit({ key: item.key })`, workflow events with a dedupe key, and
application-owned idempotent writes. See
[`skills/sync/references/pump.md`](./skills/sync/references/pump.md) for
reindexing and external-API examples.

## Scheduler

Distributed cron with leader election, callback-based dispatch.

```ts
import { scheduler, schedulerControl } from "@k2b/sync";

const sched = scheduler({ id: "platform" });

sched.start();

await sched.create<{ cleaned: number }>({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "Europe/Berlin",
  trace: async (event) => {
    await cloudTrace({ source: "cleanup", event });
  },
  process: async ({ ctx }) => {
    // ctx.scheduleId, ctx.slotTs, ctx.runNumber, ctx.failureCount, ctx.duration, ctx.signal
    const cleaned = await doCleanup();
    return { cleaned };
  },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ maxMs: 5 * 60_000 }) });
    }
  },
});

await sched.runNow({ id: "cleanup" });        // manual trigger, no cron advance
await sched.delete({ id: "cleanup" });          // remove schedule
await sched.list();                             // all schedules
sched.metric();                                 // { isLeader, leaderChanges, dispatches, ... }

await sched.stop();
```

- Multiple pods running the same scheduler id coordinate via mutex-based leader election.
- After downtime, one persisted overdue slot runs; the scheduler then jumps to the next future cron slot.
- `ctx.runNumber` is 1-indexed and monotonic, persisted across restarts.
- `ctx.failureCount` tracks consecutive failures, resets on success.
- `trace` is per schedule and observability-only. Scheduler traces have no `finished` event because schedules are recurring definitions; use `succeeded`, `failed`, and `rescheduled` for run outcomes.

External processes can trigger a live schedule without owning the handler:

```ts
const control = schedulerControl();

await control.list(); // [{ schedulerId, scheduleId, state, cron, tz, meta, ... }]
await control.runNow({ schedulerId: "platform", scheduleId: "cleanup" });
```

`schedulerControl.runNow()` waits until a live scheduler instance with the registered handler accepts the request. It does not wait for the handler's business result. Missing schedules throw `SchedulerControlNotFoundError`; schedules without a live handler throw `SchedulerControlUnavailableError`. The manual run still uses `ctx.trigger === "manual"` and does not advance cron unless `after` calls `ctx.reschedule()`.

### Batch item retry via job fanout

For "process N items, retry only failed ones" patterns, submit one job per item inside the scheduler's `process`:

```ts
const summarize = job<{ chatId: string }>({
  id: "summarize-chat",
  process: async ({ ctx }) => {
    await aiSummarize(ctx.input.chatId);
  },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 60_000, maxMs: 30 * 60_000 }) });
    }
  },
});

await sched.create({
  id: "summarize-dirty-chats",
  cron: "*/10 * * * *",
  process: async () => {
    for (const chat of await getDirtyChats()) {
      await summarize.submit({
        key: `chat:${chat.id}`,    // idempotent per chat — concurrent ticks dedupe
        input: { chatId: chat.id },
      });
    }
  },
});
```

Each item has its own retry lifecycle. Failed items retry independently. Already-running items skip duplicate submits.

## Retry

General-purpose retry wrapper with the same callback pattern.

```ts
import { retry, isRetryableTransportError } from "@k2b/sync";

const user = await retry({
  run: () => fetchUser(id),
  after: ({ ctx }) => {
    if (ctx.error && isRetryableTransportError(ctx.error) && ctx.attempt < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 100, maxMs: 5_000 }) });
    }
  },
  signal,
});
```

No `after` defined → first error throws immediately. No `ctx.reschedule` call → terminal.

## Differences between server and browser

The browser runtime (`@k2b/sync/browser`) has parity for the shared public API
with additive persistence and migration helpers:

- State is in-memory by default. `scheduler`/`mutex`/`ratelimit`/`topic`/`pump`
  optionally accept `store?: Store`; use `createLocalStorageStore()` when that
  state must survive a reload.
- Browser leader election coordinates handles through the selected `Store`.
  Cross-tab ownership with `localStorage` is best-effort because it has no
  atomic compare-and-set.
- Default in-memory stores are process-wide. Handles with the same primitive
  identity share state; an explicit store creates an explicit persistence scope.
- `queue`, `job`, and `ephemeral` remain in-memory and do not accept `store`.
- The browser entrypoint additionally exports `createMemoryStore`,
  `createLocalStorageStore`, `StoreWriteError`, and
  `migrateLegacyPumpState()`.

Parity is enforced at compile time:
```bash
bun run typecheck:parity
```

## License

MIT — see [LICENSE](./LICENSE).
