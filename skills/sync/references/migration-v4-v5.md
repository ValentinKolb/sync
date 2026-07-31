# Migrating from v4 to v5

v5 is a major breaking rewrite. The surface of every module changed. This guide walks through each migration.

## Package scope migration (v5.7.0)

Starting with v5.7.0, the package and repository moved:

- npm: `@valentinkolb/sync` -> `@k2b/sync`
- GitHub: `ValentinKolb/sync` -> `k2b-dev/sync`

The public API is unchanged. Replace the package scope in dependencies and
imports:

```diff
-import { queue } from "@valentinkolb/sync";
-import { topic } from "@valentinkolb/sync/browser";
+import { queue } from "@k2b/sync";
+import { topic } from "@k2b/sync/browser";
```

The retired standalone `@valentinkolb/sync-browser` package is also deprecated.
Use the `/browser` export from `@k2b/sync`.

## Durable namespace upgrade

Versions `<=5.8.0` use the old colon-concatenated durable namespaces. The
collision-free namespaces introduced after v5.8.0 cover queues, topics, job
claims/internal queues, server pumps, and schedulers. They cannot safely infer
ownership from the old keys. Treat an upgrade that crosses this boundary as a
maintenance migration, not as a rolling minor update.

1. Stop every old producer and worker. Mixed versions are not supported.
2. Before deploying the new version, drain or export queue, topic, job, pump,
   and scheduler-control work that must survive:
   - queues: ready, delayed, and active messages must be empty; handle the DLQ;
   - topics: consume or export retained stream entries and pending groups;
   - jobs: let the internal queue settle and claims expire;
   - pumps: let active pages settle, or export the cursor and run state;
   - scheduler control: let pending manual-run requests settle.
3. If queue idempotency must survive, wait for its configured TTL before
   removing the old queue namespace. Otherwise a repeated send after the
   upgrade is intentionally treated as new.
4. Inspect and remove the drained queue/topic/job/pump/control namespaces,
   including permanent counters and maintenance keys.
5. Preserve legacy scheduler definitions, index, and due state. Deploy the new
   version, call `list()`, `get()`, or `create()` for every scheduler, and verify
   the collision-free schedule records. Current schedulers still mirror
   compatible writes to legacy keys, so keep those keys until a future release
   explicitly removes the dual-write compatibility path.
6. A remaining ambiguous legacy key fails with a
   `namespace migration required` error instead of being claimed by the wrong
   tenant or primitive.

Do not bulk-rename an old queue/topic/pump key into a new namespace by guessing
how the old string splits into `prefix`, tenant, and id. That ambiguity is the
reason the new namespace exists.

Server pump runs have no generic migrator. With every old worker stopped, finish,
export, or remove each legacy run and its due state before using the new pump.
If an exact legacy run key remains, `start()`, `get()`, and `cancel()` fail with
a `namespace migration required` error and leave that state untouched.

Persisted browser pump runs have an explicit one-time migrator:

```ts
import {
  createLocalStorageStore,
  migrateLegacyPumpState,
  pump,
} from "@k2b/sync/browser";

const store = createLocalStorageStore("app");

// Run once with all old tabs closed, before constructing this pump.
migrateLegacyPumpState({
  id: "mail.sender-rule-backfill",
  prefix: "cloud:mail",
  key: "sender-rule:rule-1:revision-4",
  store,
});

const messages = pump({
  id: "mail.sender-rule-backfill",
  prefix: "cloud:mail",
  store,
  pull,
  dispatch,
});
```

The migrator moves one explicit run key per call. With a legacy collision such
as `(prefix: "root:a", id: "b")` versus `(prefix: "root", id: "a:b")`, the
library cannot prove which pump owned the old state. The operator must select
the intended identity and key; do not call the migrator for both. It copies
validated state before deleting the old entry, preserves active cursor/page
checkpoints, and keeps terminal state bounded by retention. Without this
explicit migration, access fails instead of silently starting from a null
cursor.

Browser topic checkpoints, pending deliveries, and idempotency state from the
old concatenated namespace are not imported automatically. Before upgrading,
drain or export state that must survive and close every old tab. Otherwise,
close every old tab and intentionally restart the topic from empty state.

Browser scheduler checkpoints from the old concatenated namespace are not
imported automatically, and there is no API for restoring their runtime
counters. Recreating a schedule resets `runNumber` and `failureCount` and
recomputes `nextRunAt` from its cron definition.

## TL;DR

- **Zod is no longer a peer dependency.** Payloads are typed via generics; runtime validation is your responsibility.
- **`registry` module removed.** Use `ephemeral` with `snapshot({ prefix })` / `reader({ prefix })`.
- **Unified lifecycle decision model** across `retry`, `job`, and `scheduler`: `retry` uses `run`, while `job`/`scheduler` use `process`; all support optional `after` + `ctx.reschedule({ delayMs })`.
- **`job.input` is still available** — optional and typed via `<Input>` generic.
- **No job/scheduler retry-count policy config** — their retry decisions happen in `after` via `ctx.reschedule()`. `pump.retry.maxAttempts` is a separate bounded transport policy.
- **Removed public surfaces:** `registry`, `job.join/cancel/events/step`, `scheduler.triggerNow/register/unregister`, `catch_up` misfire policies, `strictHandlers`, and the old scheduler DLQ. Current scheduler fencing is internal and intentionally not a public API.

---

## 1. Zod removal

### Before (v4)

```ts
import { z } from "zod";
import { queue } from "@k2b/sync";

const q = queue({
  id: "mail",
  schema: z.object({ to: z.string().email() }),
});
```

### After (v5)

```ts
import { queue } from "@k2b/sync";

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
import { registry } from "@k2b/sync";

const apps = registry({ id: "apps", schema: AppSchema });
await apps.upsert({ key: "apps/backend", value: { ... } });
const list = await apps.list({ prefix: "apps/", status: "active" });
```

### After (v5)

```ts
import { ephemeral } from "@k2b/sync";

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
- `defaults.maxAttempts` and `defaults.backoff` were removed — retries are user-controlled. `defaults.keyTtlMs` remains available to bound non-terminal idempotency claims (default 24h).
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
- `misfire` policies `catch_up_one` / `catch_up_all` / `maxCatchUpRuns` removed — one persisted overdue slot runs after downtime, then `nextRunAt` advances past the remaining missed slots.
- `strictHandlers` removed — a handler-less leader yields ownership without advancing the due slot so another live handler can run it.
- `onMetric` callback + 13 `SchedulerMetric` event types removed → simple `metric()` snapshot: `{ isLeader, leaderChanges, dispatches, failures, reschedules, tickErrors, staleWrites, unservedSlots, lastTickAt }`.
- The old public epoch/CAS model and scheduler DLQ were removed. Current releases still use internal Lua fencing, revisions, tombstones, and leader/dispatch leases to protect durable state; those details are not application APIs.
- **NEW:** `ctx.runNumber` (1-indexed, monotonic, persisted), `ctx.failureCount` (consecutive failures, resets on success), `ctx.expBackoff`, `ctx.reschedule`.
- Fanout pattern for batch item retry: in `process`, call `job.submit({ key: `item:${id}`, input })` for each item. Per-item retry semantics come from the job layer.

---

## 5. `retry` — new callback API

### Before (v4)

```ts
import { retry, isRetryableTransportError } from "@k2b/sync";

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

For the original v5 migration, the recognizable factory/operation model remains, but do not assume current v5 behavior differs only by Zod removal. Later v5 releases added queue DLQ inspection and namespace hardening, topic cursor/reclaim APIs, and browser persistence safeguards. Read the current per-module references before upgrading durable state.

### Before
```ts
const q = queue({ id: "mail", schema: MailSchema });
```

### After
```ts
const q = queue<Mail>({ id: "mail" });
```

The core queue operations remain recognizable, but current delivery, DLQ retention, namespace migration, and browser-store behavior are documented in [queue.md](queue.md).

---

## Package entrypoints

Server and browser code use the same package; browser code selects the subpath:

```ts
import { scheduler } from "@k2b/sync/browser";
```

The old `@valentinkolb/sync-browser` package is not released separately anymore.

`zod` is no longer a `peerDependency`. Remove it from your install if you're not using it directly.
