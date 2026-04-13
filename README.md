# @valentinkolb/sync

Synchronization primitives for TypeScript — available as two packages:

- **[`@valentinkolb/sync`](./packages/sync)**: backed by Redis (6.2+, Valkey, Dragonfly), built for [Bun](https://bun.sh). Designed for horizontally scaled systems where multiple service instances need coordinated access to shared state.
- **[`@valentinkolb/sync-browser`](./packages/sync-browser)**: fully in-memory, zero dependencies beyond `zod`. Designed for local-first browser apps that need the same primitives (rate limiting, queues, schedulers) without a server.

Both share mostly the same API. Code written for one generally works on the other — just change the import. See [Differences between server and browser](#differences-between-server-and-browser) for the details.

Provides nine modules: **ratelimit**, **mutex**, **queue**, **topic**, **job**, **scheduler**, **registry**, **ephemeral**, and **retry**. They work independently or compose — `job` uses `queue` + `topic` internally, `scheduler` uses `job` + `mutex`.

Requires `zod` as peer dependency for payload validation.

## Installation

```bash
# Server (Redis-backed)
bun add @valentinkolb/sync zod

# Browser (in-memory, no Redis)
bun add @valentinkolb/sync-browser zod
# or: npm install @valentinkolb/sync-browser zod
```

### Agent Skills (optional)

This repository ships reusable agent skills in [`skills/`](./skills). Install them with the [Vercel Skills CLI](https://github.com/vercel-labs/skills):

```bash
bunx skills add https://github.com/valentinkolb/sync --skill '*'
```

---

## Rate Limit

Sliding window rate limiter.

```ts
import { ratelimit, RateLimitError } from "@valentinkolb/sync";

const limiter = ratelimit({
  id: "api",
  limit: 100,
  windowSecs: 60,
});

const result = await limiter.check("user:123");
// { limited: false, remaining: 99, resetIn: 58432 }

try {
  await limiter.checkOrThrow("user:123");
} catch (error) {
  if (error instanceof RateLimitError) {
    console.log(`Retry in ${error.resetIn}ms`);
  }
}
```

## Mutex

Distributed lock with retry, TTL auto-expiry, and owner-only release.

```ts
import { mutex, LockError } from "@valentinkolb/sync";

const m = mutex({ id: "checkout", defaultTtl: 5000 });

// Automatic acquire + release
const result = await m.withLock("order:123", async (lock) => {
  await m.extend(lock, 10_000); // extend if needed
  return await processOrder();
});

// Throws LockError if lock cannot be acquired
await m.withLockOrThrow("order:123", async () => {
  await doExclusiveWork();
});

// Manual acquire/release
const lock = await m.acquire("order:123");
if (lock) {
  try { /* work */ } finally { await m.release(lock); }
}
```

## Queue

Durable work queue with at-least-once delivery, lease-based visibility, delayed messages, idempotency, and dead-letter queue.

```ts
import { z } from "zod";
import { queue } from "@valentinkolb/sync";

const q = queue({
  id: "mail.send",
  schema: z.object({ to: z.string().email(), subject: z.string() }),
  delivery: { defaultLeaseMs: 60_000, maxDeliveries: 5 },
  limits: { maxMessageAgeMs: 7 * 24 * 60 * 60 * 1000 },
});

// Send
await q.send({
  data: { to: "user@example.com", subject: "Welcome" },
  idempotencyKey: "welcome:user@example.com",
  delayMs: 5_000,               // optional: deliver after 5s
  meta: { traceId: "abc-123" }, // optional metadata
});

// Receive + process
const msg = await q.recv({ wait: true, timeoutMs: 30_000 });
if (msg) {
  try {
    await sendMail(msg.data);
    await msg.ack();
  } catch (error) {
    await msg.nack({ delayMs: 5_000, error: String(error) });
  }
}

// Stream processing
for await (const m of q.stream()) {
  await handle(m.data);
  await m.ack();
}

// Multiple readers (independent blocking clients)
const reader = q.reader();
const msg2 = await reader.recv({ signal: abortController.signal });
```

Messages exceeding `maxDeliveries` move to DLQ. Extend active leases with `msg.touch()`. Optional `tenantId` for isolated queues.

## Topic

Pub/sub with Redis Streams. Consumer groups for at-least-once delivery, `live()` for best-effort streaming to all listeners.

```ts
import { z } from "zod";
import { topic } from "@valentinkolb/sync";

const t = topic({
  id: "order.events",
  schema: z.object({ type: z.string(), orderId: z.string() }),
  retentionMs: 7 * 24 * 60 * 60 * 1000,
});

// Publish
await t.pub({
  data: { type: "order.confirmed", orderId: "o1" },
  idempotencyKey: "confirm:o1",
  meta: { source: "checkout" },
});

// Consumer group reader (at-least-once, load-balanced across consumers)
const reader = t.reader("mailer");
for await (const event of reader.stream()) {
  await sendConfirmationEmail(event.data);
  await event.commit();
}

// Multiple groups receive the same events independently
const analytics = t.reader("analytics");
const billing = t.reader("billing");

// Live stream (best-effort, no consumer group, no commit needed)
for await (const event of t.live({ signal: ac.signal })) {
  console.log(event.data);
}

// Replay from a specific cursor
for await (const event of t.live({ after: "0-0" })) {
  // receives all stored events from the beginning
}
```

Each consumer group tracks its own position. Retention via automatic XTRIM. Optional `tenantId` for isolated streams.

## Job

Durable job processing built on queue + topic. Retries with backoff, cancellation, per-job event stream.

```ts
import { z } from "zod";
import { job } from "@valentinkolb/sync";

const sendOrderMail = job({
  id: "mail.send-order",
  schema: z.object({ orderId: z.string(), to: z.string().email() }),
  defaults: { maxAttempts: 3, backoff: { kind: "exp", baseMs: 1000 } },
  process: async ({ ctx, input }) => {
    if (ctx.signal.aborted) return;

    await ctx.heartbeat(); // extend lease
    await ctx.step({ id: "send", run: () => mailProvider.send(input) });
    return { ok: true };
  },
});

// Submit
const id = await sendOrderMail.submit({
  input: { orderId: "o1", to: "user@example.com" },
  key: "mail:o1",         // idempotency key
  delayMs: 5_000,         // schedule for later
  maxAttempts: 3,
  backoff: { kind: "exp", baseMs: 1000, maxMs: 30_000 },
});

// Wait for completion
const terminal = await sendOrderMail.join({ id, timeoutMs: 60_000 });
// terminal.status: "completed" | "failed" | "cancelled" | "timed_out"

// Cancel
await sendOrderMail.cancel({ id, reason: "user-request" });

// Event stream (every state transition emits a typed event)
const events = sendOrderMail.events(id);
for await (const e of events.reader("orchestrator").stream({ wait: false })) {
  console.log(e.data.type);
  // "submitted" | "started" | "heartbeat" | "retry" | "completed" | "failed" | "cancelled"
  await e.commit();
}

// Graceful shutdown
sendOrderMail.stop();
```

Jobs exceeding `leaseMs` are timed out automatically. `ctx.signal` is aborted on timeout or cancellation. Job state in Redis expires after a configurable TTL (default 7 days).

## Scheduler

Distributed cron scheduler. One leader per `id` dispatches due slots as durable jobs. Registration is idempotent.

```ts
import { z } from "zod";
import { job, scheduler } from "@valentinkolb/sync";

const cleanup = job({
  id: "cleanup-temp",
  schema: z.object({ scope: z.string() }),
  process: async ({ input }) => {
    await runCleanup(input.scope);
  },
});

const sched = scheduler({
  id: "platform",
  onMetric: (metric) => console.log(metric),
});

sched.start();

await sched.register({
  id: "cleanup-hourly",    // idempotent key
  cron: "0 * * * *",       // every hour
  tz: "Europe/Berlin",
  job: cleanup,
  input: { scope: "tmp" },
  misfire: "skip",         // "skip" | "catch_up_one" | "catch_up_all"
  meta: { owner: "ops" },
});

// Manual trigger (does not alter cron state)
await sched.triggerNow({
  id: "cleanup-hourly",
  key: "ops-manual-run-1", // optional: idempotent manual trigger
});
```

Leader election via renewable Redis lease with epoch fencing. Each cron slot maps to a deterministic job key to prevent duplicates. `triggerNow()` does not require `start()` and reuses the registered input. Misfire policies: `skip` (default), `catch_up_one`, `catch_up_all`.

## Registry

Typed key/value registry with prefix listing, compare-and-swap, optional TTL-backed liveness, and change streams.

```ts
import { z } from "zod";
import { registry } from "@valentinkolb/sync";

const services = registry({
  id: "services",
  schema: z.object({
    appId: z.string(),
    kind: z.enum(["instance", "setting", "flag"]),
    url: z.string().url().optional(),
  }),
});

await services.upsert({
  key: "apps/contacts/instances/i-1",
  value: { appId: "contacts", kind: "instance", url: "https://contacts-1.internal" },
  ttlMs: 15_000,
});

await services.touch({ key: "apps/contacts/instances/i-1" });

const active = await services.list({
  prefix: "apps/contacts/instances/",
  status: "active",
});

// Snapshot + cursor for replay-safe handoff into stream
const snap = await services.list({ prefix: "apps/" });
const ac = new AbortController();

for await (const ev of services.reader({ prefix: "apps/", after: snap.cursor }).stream({ signal: ac.signal })) {
  console.log(ev.type);
}
```

CAS via `cas({ key, version, value })`. Records with `ttlMs` are refreshed via `touch()`; expired entries remain queryable via `status: "expired"`. Streams can watch a single key, a prefix, or the whole registry.

## Ephemeral

TTL-based key/value store with event stream. Each key expires independently. Useful for presence, heartbeats, or temporary state.

```ts
import { z } from "zod";
import { ephemeral } from "@valentinkolb/sync";

const presence = ephemeral({
  id: "presence",
  schema: z.object({ nodeId: z.string(), status: z.enum(["up", "down"]) }),
  ttlMs: 30_000,
});

await presence.upsert({ key: "worker:42", value: { nodeId: "42", status: "up" } });
await presence.touch({ key: "worker:42" }); // extend TTL

const snap = await presence.snapshot();
console.log(snap.entries.length, snap.cursor);

for await (const event of presence.reader({ after: snap.cursor }).stream()) {
  console.log(event.type);
  // "upsert" | "touch" | "delete" | "expire" | "overflow"
}
```

`snapshot()` returns entries + cursor for handoff into `reader().stream()`. Optional `tenantId` for keyspace isolation.

## Retry

Retry helper with exponential backoff for transient Redis/network errors.

```ts
import { retry, DEFAULT_RETRY_OPTIONS } from "@valentinkolb/sync";

const value = await retry(() => fragileCall());

// Per-call override
const value2 = await retry(
  () => fragileCall(),
  { attempts: 12, maxDelayMs: 5_000 },
);

console.log(DEFAULT_RETRY_OPTIONS);
// { attempts: 8, minDelayMs: 100, maxDelayMs: 2000, factor: 2, jitter: 0.2, retryIf: isRetryableTransportError }
```

Long-lived stream loops (`stream({ wait: true })`, `live()`) use transport retries internally. One-shot calls surface errors directly unless wrapped with `retry(...)`.

---

## Browser (`@valentinkolb/sync-browser`)

All nine modules are available as a separate browser-compatible package. State lives in-memory by default, or in `localStorage` for persistence.

```ts
import { queue, ratelimit, scheduler, createLocalStorageStore } from "@valentinkolb/sync-browser";
```

The API is identical to the server version. Just use `@valentinkolb/sync-browser` instead of `@valentinkolb/sync`.

```ts
import { z } from "zod";
import { queue } from "@valentinkolb/sync-browser";

const q = queue({
  id: "tasks",
  schema: z.object({ url: z.string().url() }),
});

await q.send({ data: { url: "https://example.com" } });

for await (const msg of q.stream({ wait: false })) {
  await fetch(msg.data.url);
  await msg.ack();
}
```

### Key differences from server

| | Server | Browser |
|---|---|---|
| **Package** | `@valentinkolb/sync` | `@valentinkolb/sync-browser` |
| **State** | Redis | JS heap (default) or localStorage |
| **Atomicity** | Lua scripts | JS single-threading |
| **Blocking reads** | Redis `BRPOPLPUSH` / `XREAD BLOCK` | Promise-based event emitters |
| **TTL** | Redis key expiry + reconciliation | `setTimeout` callbacks |
| **Cross-process** | Yes (multi-pod) | No (single-tab) |

### Store abstraction

Browser modules use a `MemoryStore` by default (state lost on refresh). For persistence across tab reloads, use `LocalStorageStore`:

```ts
import { scheduler, job, createLocalStorageStore } from "@valentinkolb/sync-browser";

// Scheduler with persistence — catches up missed runs after tab reopen
const sched = scheduler({
  id: "app",
  store: createLocalStorageStore(),
});
```

Two built-in implementations:

```ts
import { createMemoryStore, createLocalStorageStore } from "@valentinkolb/sync-browser";

createMemoryStore()              // default — fast, lost on refresh
createLocalStorageStore()        // persistent — survives tab close (~5MB limit)
createLocalStorageStore("myapp") // custom prefix to avoid collisions
```

The `Store` interface is minimal — implement your own for IndexedDB or other backends:

```ts
interface Store {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown, ttlMs?: number): void;
  del(key: string): void;
  keys(prefix?: string): string[];
}
```

---

## Development

This is a Bun workspace monorepo:

```bash
git clone https://github.com/valentinkolb/sync.git
cd sync && bun install
```

Server tests require a Redis-compatible server on port 6399:

```bash
docker run -d --name valkey -p 6399:6379 valkey/valkey:latest
cd packages/sync && bun test --preload ./tests/preload.ts
```

Browser tests run without Redis:

```bash
cd packages/sync-browser && bun test
```

### Project structure

```
packages/
  sync/                  # @valentinkolb/sync (server, Redis)
    index.ts
    src/                 # Module implementations
    tests/               # Integration + unit tests (require Redis)
  sync-browser/          # @valentinkolb/sync-browser (browser, in-memory)
    index.ts
    src/                 # Module implementations
    tests/               # Browser tests (no Redis)
skills/                  # Agent skills with per-feature API references
```

### Guidelines

- Server: every Redis mutation must be in a Lua script for atomicity.
- Browser: JS single-threading provides atomicity — no locks needed.
- All modules follow the `moduleName({ id, ...config })` factory pattern.
- Validate at boundaries, trust internal data.

## Differences between server and browser

The two packages target fundamentally different environments. While the factory signatures and method names are the same, there are semantic differences worth understanding.

### Consistency & scope

| | Server (`@valentinkolb/sync`) | Browser (`@valentinkolb/sync-browser`) |
|---|---|---|
| **Scope** | Multi-process, multi-pod | Single tab |
| **Atomicity** | Redis Lua scripts — atomic across all clients | JS single-threading — atomic within one tab, no cross-tab coordination |
| **Mutex** | True distributed lock across pods | In-tab async coordination only. Useful for serializing concurrent `fetch()` calls or IndexedDB writes, not for cross-tab locking |
| **Topic consumer groups** | Real consumer groups with pending entry tracking, load-balanced across consumers, `commit()` advances group cursor | Simplified: each reader tracks its own cursor, `commit()` is a no-op. Two readers in the same group both see all events |
| **Job durability** | State survives process restarts (Redis-backed) | State lives in JS heap — lost on page refresh unless scheduler uses a persistent store |
| **Scheduler leader election** | Real leader election across pods via mutex | Trivially succeeds (single tab = always leader) |

### State persistence

| | Server | Browser |
|---|---|---|
| **Default** | Redis — survives restarts | `MemoryStore` — lost on refresh |
| **Persistent option** | n/a (Redis is always persistent) | `createLocalStorageStore()` — survives tab close, ~5MB limit, main-thread only |
| **Custom backends** | n/a | Implement the `Store` interface for IndexedDB, sessionStorage, etc. |

The browser scheduler persists `lastRunAt` timestamps in the store. On tab reopen, `register()` reads the persisted timestamp and applies the misfire policy (skip / catch_up_one / catch_up_all) to catch up missed runs. Cron expression, handler, input, and misfire policy always come from code — only `lastRunAt` is persisted.

### Other differences

- **Hashing**: long identifiers (>128 chars) in ratelimit/mutex use SHA-256 on server, djb2 on browser. Different hash = different key — don't mix environments for the same logical resource.
- **Blocking reads**: server uses dedicated Redis connections (`BRPOPLPUSH`, `XREAD BLOCK`). Browser uses `Promise`-based event emitters with `setTimeout` for timeouts.
- **TTL precision**: server relies on Redis key expiry (millisecond precision). Browser uses `setTimeout` which may be throttled in background tabs (~1s minimum in most browsers). Misfire policies compensate for this.
- **Transport retries**: server `stream({ wait: true })` auto-retries transient Redis connection errors. Browser streams don't retry since there is no network involved.
- **`localStorage` caveats**: `LocalStorageStore` is synchronous and main-thread only. It may throw `SecurityError` in sandboxed iframes or private browsing modes. It does not coordinate TTL timers across tabs — two tabs with the same store prefix can interfere.

## License

MIT
