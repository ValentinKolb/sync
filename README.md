# @valentinkolb/sync

Synchronization primitives for TypeScript — available as two packages:

- **[`@valentinkolb/sync`](./packages/sync)**: Redis-backed (6.2+, Valkey, Dragonfly), built for [Bun](https://bun.sh). For horizontally scaled systems where multiple service instances coordinate via shared state.
- **[`@valentinkolb/sync-browser`](./packages/sync-browser)**: fully in-memory, zero dependencies. For local-first browser apps that want the same primitives without a server.

Both share an **identical public API** — change the import, and code generally works. Type parity is enforced at compile-time (see `parity/`).

Provides eight modules: **ratelimit**, **mutex**, **queue**, **topic**, **ephemeral**, **job**, **scheduler**, and **retry**. They compose — `job` uses `queue` internally, `scheduler` uses `mutex` for leader election.

## Installation

```bash
# Server (Redis-backed)
bun add @valentinkolb/sync

# Browser (in-memory, no Redis)
bun add @valentinkolb/sync-browser
```

No runtime dependencies. TypeScript is a peer dependency.

> **Upgrading from v4?** See [MIGRATION.md](./MIGRATION.md). v5 is a major rewrite.

### Agent Skills (optional)

This repository ships a single `sync` agent skill in [`skills/sync/`](./skills/sync) with one reference file per module plus a v4→v5 migration guide. Install with the [Vercel Skills CLI](https://github.com/vercel-labs/skills):

```bash
bunx skills add https://github.com/valentinkolb/sync --skill sync
```

---

## Rate Limit

Sliding window rate limiter.

```ts
import { ratelimit, RateLimitError } from "@valentinkolb/sync";

const limiter = ratelimit({ id: "api", limit: 100, windowSecs: 60 });

const result = await limiter.check("user:123");
// { limited: false, remaining: 99, resetIn: 58432 }

await limiter.checkOrThrow("user:123"); // throws RateLimitError if over
```

## Mutex

Distributed lock with retry, TTL auto-expiry, owner-only release.

```ts
import { mutex, LockError } from "@valentinkolb/sync";

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
import { queue } from "@valentinkolb/sync";

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
import { topic } from "@valentinkolb/sync";

const t = topic<{ type: string; orderId: string }>({
  id: "order.events",
  retentionMs: 7 * 24 * 60 * 60 * 1000,
});

await t.pub({ data: { type: "order.confirmed", orderId: "o1" } });

const startCursor = (await t.latestCursor()) ?? "0-0";

// Consumer group (at-least-once, acked)
const reader = t.reader("analytics");
for await (const msg of reader.stream()) {
  await process(msg.data);
  await msg.commit();
}

// Live (best-effort, all listeners)
for await (const event of t.live({ after: startCursor })) {
  console.log(event.data);
}
```

## Ephemeral

TTL-based key/value with tenant isolation, snapshots with optional prefix filter, and change-stream reader.

```ts
import { ephemeral } from "@valentinkolb/sync";

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
import { job, isRetryableTransportError } from "@valentinkolb/sync";

const sendMail = job<{ to: string }, { sent: boolean }>({
  id: "send-mail",
  defaults: { leaseMs: 30_000 },

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

**Input is optional** — simple jobs can omit both the input generic and the `input` submit field:

```ts
const sync = job({
  id: "sync",
  process: async () => { await doSync(); },
});
await sync.submit({ key: "daily" });
```

## Scheduler

Distributed cron with leader election, callback-based dispatch.

```ts
import { scheduler } from "@valentinkolb/sync";

const sched = scheduler({ id: "platform" });

sched.start();

await sched.create<{ cleaned: number }>({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "Europe/Berlin",
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
- `misfire` is always "skip" — missed slots (e.g. from downtime) jump to the next cron slot.
- `ctx.runNumber` is 1-indexed and monotonic, persisted across restarts.
- `ctx.failureCount` tracks consecutive failures, resets on success.

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
import { retry, isRetryableTransportError } from "@valentinkolb/sync";

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

Browser package (`@valentinkolb/sync-browser`) has the **same public API** but:

- All state is in-memory (no Redis). Survives within a page/tab; resets on reload unless you pass a `store?: Store`.
- `scheduler`/`mutex`/`ratelimit`/`topic` optionally accept `store?: Store` for `createLocalStorageStore()` persistence.
- Leader election (scheduler mutex) trivially succeeds in a single tab.
- Multiple instances with the same id in the same tab share state via module-level maps.

Parity is enforced at compile time:
```bash
bun run typecheck:parity
```

## License

MIT — see [LICENSE](./LICENSE).
