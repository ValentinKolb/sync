# Migrating from v4 to v5

v5 is a major breaking rewrite. The surface of every module changed. This guide walks through each migration.

## TL;DR

- **Zod is no longer a peer dependency.** Payloads are typed via generics; runtime validation is your responsibility.
- **`registry` module removed.** Use `ephemeral` with `snapshot({ prefix })` / `reader({ prefix })`.
- **Unified callback API** across `retry`, `job`, and `scheduler`: all use `process` + optional `after` + `ctx.reschedule({ delayMs })`.
- **`job.input` is still available** — optional and typed via `<Input>` generic.
- **No more `maxAttempts`/retry-count config** — retry decisions happen in `after` via `ctx.reschedule()`.
- **`registry`, `job.join/cancel/events/step`, `scheduler.triggerNow/register/unregister`, `catch_up` misfire policies, `strictHandlers`, epoch fencing, DLQ** — all removed.

---

## 1. Zod removal

### Before (v4)

```ts
import { z } from "zod";
import { queue } from "@valentinkolb/sync";

const q = queue({
  id: "mail",
  schema: z.object({ to: z.string().email() }),
});
```

### After (v5)

```ts
import { queue } from "@valentinkolb/sync";

const q = queue<{ to: string }>({ id: "mail" });
```

**What changed:**
- `schema` config field removed from `queue`, `topic`, `ephemeral`, `job`, `scheduler`.
- Type parameter `<T>` replaces `z.infer<TSchema>`.
- Runtime validation (if needed) happens in your handler — e.g. `const parsed = MySchema.parse(ctx.input)`.

**Why:** Zod schemas imply runtime validation across boundaries, but JSON round-trip semantics already limit what survives (`Date` → string, `Map` → plain object, etc.). User responsibility is clearer.

---

## 2. `registry` → `ephemeral` with `prefix` filter

### Before (v4)

```ts
import { registry } from "@valentinkolb/sync";

const apps = registry({ id: "apps", schema: AppSchema });
await apps.upsert({ key: "apps/backend", value: { ... } });
const list = await apps.list({ prefix: "apps/", status: "active" });
```

### After (v5)

```ts
import { ephemeral } from "@valentinkolb/sync";

const apps = ephemeral<AppValue>({
  id: "apps",
  ttlMs: 60_000, // heartbeat interval × 3 or similar
});
await apps.upsert({ key: "apps/backend", value: { ... } });
const snap = await apps.snapshot({ prefix: "apps/" });
// snap.entries is already filtered to live (TTL-valid) entries with matching prefix
```

**What changed:**
- `registry` module fully removed from both packages.
- `ephemeral.snapshot({ tenantId?, prefix? })` and `ephemeral.reader({ tenantId?, prefix?, after? })` now accept optional `prefix`.
- `tenantId` still isolates event streams / TTL zones / `maxEntries`; `prefix` is a read-filter within a tenant.

**Why:** `registry` was 95% a superset of `ephemeral`. One concept is simpler.

---

## 3. `job` — new callback API, optional typed input

### Before (v4)

```ts
const sendMail = job({
  id: "send-mail",
  schema: z.object({ to: z.string() }),
  defaults: { maxAttempts: 3, backoff: { kind: "exp", baseMs: 1000 } },
  process: async ({ ctx, input }) => {
    await ctx.step({ id: "send", run: () => provider.send(input) });
    return { ok: true };
  },
});

const id = await sendMail.submit({ input: { to: "x@y" }, key: "mail:1" });
const terminal = await sendMail.join({ id, timeoutMs: 60_000 });
await sendMail.cancel({ id, reason: "..." });
for await (const ev of sendMail.events(id).reader("orchestrator").stream()) { ... }
```

### After (v5)

```ts
const sendMail = job<{ to: string }, { ok: boolean }>({
  id: "send-mail",
  defaults: { leaseMs: 30_000 },
  process: async ({ ctx }) => {
    //                 ^ ctx.input: { to: string }
    return { ok: true };
  },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 3) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 1000 }) });
    }
  },
});

await sendMail.submit({ key: "mail:1", input: { to: "x@y" } });
```

**What changed:**
- `schema` → `<Input, Result>` generics (both optional, default `void` / `unknown`).
- `ctx.step()` removed — no durable step caching. Handlers re-run from scratch on redelivery.
- `defaults.maxAttempts`, `defaults.backoff`, `defaults.keyTtlMs` removed — retries are user-controlled.
- `onSuccess`/`onFailure` → unified `after({ ctx })` where `ctx.data?` or `ctx.error?` is set. `ctx.reschedule({ delayMs })` re-queues (holds key). No call to reschedule → terminal (releases key).
- `join()`, `cancel()`, `events()` removed. If you need status visibility: write an audit row in `after` and query your DB.
- `ctx.expBackoff({ baseMs?, maxMs?, jitter? })` computes backoff delay using `ctx.failureCount + 1`.
- `JobMetrics = { dispatches, failures, reschedules }` accessible via `jobHandle.metric()` and `ctx.metric` (live reference).
- `ctx.signal`, `ctx.heartbeat`, `ctx.jobId`, `ctx.key`, `ctx.failureCount`, `ctx.duration` all still present.

**Key lifecycle (same in v4 and v5, documented here):**
- `submit({ key })` claims the idempotency key atomically.
- During `process`: key stays claimed.
- `process` returns / `after` doesn't call `reschedule` → ack, release key (terminal).
- `after` calls `ctx.reschedule({ delayMs })` → nack with delay, key stays claimed for retry.

---

## 4. `scheduler` — new callback API

### Before (v4)

```ts
const sched = scheduler({
  id: "platform",
  strictHandlers: true,
  onMetric: (m) => log.info("metric", m),
});

sched.start();

await sched.register({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "UTC",
  job: cleanupJob,
  input: { scope: "temp" },
  misfire: "skip",
});

await sched.triggerNow({ id: "cleanup", key: "manual-1" });
await sched.unregister({ id: "cleanup" });
```

### After (v5)

```ts
const sched = scheduler({ id: "platform" });

sched.start();

await sched.create<{ processed: number }>({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "UTC",
  process: async ({ ctx }) => {
    //                ^ ctx.scheduleId, slotTs, runNumber, failureCount
    const n = await doCleanup();
    return { processed: n };
  },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 3) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 60_000, maxMs: 5 * 60_000 }) });
    }
  },
});

await sched.runNow({ id: "cleanup" });
await sched.delete({ id: "cleanup" });
```

**What changed:**
- `register` → `create`, takes `process` directly (no `job`/`input` indirection).
- `unregister` → `delete`.
- `triggerNow` → `runNow`; no `key` argument (schedules don't produce jobs anymore).
- `misfire` policies `catch_up_one` / `catch_up_all` / `maxCatchUpRuns` removed — behavior is always implicit "skip" (advance `nextRunAt` past missed slots).
- `strictHandlers` removed — always strict (missing handler advances slot without running).
- `onMetric` callback + 13 `SchedulerMetric` event types removed → simple `metric()` snapshot: `{ isLeader, leaderChanges, dispatches, failures, reschedules, tickErrors, lastTickAt }`.
- CAS Lua scripts, leader epoch fencing, DLQ list — all gone.
- **NEW:** `ctx.runNumber` (1-indexed, monotonic, persisted), `ctx.failureCount` (consecutive failures, resets on success), `ctx.expBackoff`, `ctx.reschedule`.
- Fanout pattern for batch item retry: in `process`, call `job.submit({ key: `item:${id}`, input })` for each item. Per-item retry semantics come from the job layer.

---

## 5. `retry` — new callback API

### Before (v4)

```ts
import { retry, isRetryableTransportError } from "@valentinkolb/sync";

const user = await retry(async () => await fetchUser(id), {
  attempts: 5,
  minDelayMs: 100,
  maxDelayMs: 5_000,
  factor: 2,
  jitter: 0.2,
  retryIf: isRetryableTransportError,
  signal,
});
```

### After (v5)

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

**What changed:**
- Config-object API instead of `(fn, opts)`.
- `attempts` / `minDelayMs` / `maxDelayMs` / `factor` / `jitter` / `retryIf` / `DEFAULT_RETRY_OPTIONS` all removed.
- User decides retry policy in `after` via `ctx.reschedule({ delayMs })`.
- `ctx.expBackoff({ baseMs?, maxMs?, jitter? })` helper uses `ctx.attempt` internally for exponential calculation.
- Same mental model as `job` and `scheduler`.

**Simple cases:**

```ts
// Retry forever on transport errors
await retry({
  run: () => someIO(),
  after: ({ ctx }) => {
    if (ctx.error && isRetryableTransportError(ctx.error)) {
      ctx.reschedule({ delayMs: ctx.expBackoff() });
    }
  },
});

// Fixed retry count
await retry({
  run: () => callApi(),
  after: ({ ctx }) => {
    if (ctx.error && ctx.attempt < 3) ctx.reschedule({ delayMs: 1000 });
  },
});
```

---

## 6. `queue`, `topic`, `mutex`, `ratelimit`, `ephemeral`

**Only Zod removal.** Everything else unchanged functionally.

### Before
```ts
const q = queue({ id: "mail", schema: MailSchema });
```

### After
```ts
const q = queue<Mail>({ id: "mail" });
```

`send/recv/ack/nack/touch/idempotencyKey/delayMs/leaseMs/DLQ` semantics all the same.

---

## Code sizes (v4 → v5)

| Module | v4 LOC | v5 LOC | Change |
|---|---|---|---|
| `job.ts` (server) | 722 | 322 | −55% |
| `job.ts` (browser) | 700 | 310 | −56% |
| `scheduler.ts` (server) | 1288 | 476 | −63% |
| `scheduler.ts` (browser) | 902 | 415 | −54% |
| `registry.ts` | 1778 | **deleted** | −100% |
| `retry.ts` (server) | 122 | 161 | callback API + expBackoff helper |

---

## Version bump

```bash
# package.json
"@valentinkolb/sync": "5.0.0"
"@valentinkolb/sync-browser": "5.0.0"
```

`zod` is no longer a `peerDependency`. Remove it from your install if you're not using it directly.
