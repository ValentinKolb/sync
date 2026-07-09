---
name: sync
description: "Use this skill for @valentinkolb/sync and @valentinkolb/sync/browser — distributed synchronization primitives for TypeScript/Bun (server) and browsers. Covers all 8 modules: ratelimit, mutex, queue, topic, ephemeral, job, scheduler, retry. Use when code imports from `@valentinkolb/sync` or `@valentinkolb/sync/browser`, or when building features that need: rate limiting, distributed locks, durable work queues with at-least-once delivery + DLQ + idempotency, pub/sub with cursor-based replay, TTL-based presence/registry, durable background jobs with retry + lifecycle callbacks, distributed cron scheduling with leader election, or retry helpers with exponential backoff. Also use for migrating v4 code to the v5 rewrite (unified callback API, no Zod peer dep, no registry module)."
---

# @valentinkolb/sync — v5

Distributed synchronization primitives with an **identical public API** between server (Redis-backed) and browser (in-memory). Change the import; code generally works on both sides.

## When to use this skill

Trigger for any imports from:
- `@valentinkolb/sync` (server — Bun + Redis/Valkey/Dragonfly 6.2+)
- `@valentinkolb/sync/browser` (browser — in-memory)

Or when the user is building features that need one of the eight modules:

| Module | Use for |
|---|---|
| `ratelimit` | Sliding-window rate limiter per identifier |
| `mutex` | Distributed lock with retry, TTL, owner-only release |
| `queue` | Durable work queue: at-least-once, lease-based visibility, delayMs, idempotency, DLQ |
| `topic` | Pub/sub with cursor-based replay; consumer groups OR `live()` broadcast |
| `ephemeral` | TTL key/value with `tenantId` isolation, `prefix` filter, change-stream reader |
| `job` | Durable background tasks with `process` + `after` lifecycle callbacks, typed input, optional trace callback |
| `scheduler` | Distributed cron with leader election, `runNumber`, `failureCount`, `ctx.reschedule`, optional per-schedule trace callback, remote manual control |
| `retry` | General-purpose retry wrapper with the same callback pattern |

## v5 API core pattern

All three lifecycle-aware modules (`retry`, `job`, `scheduler`) share the same shape:

```ts
mod({
  id,
  process: async ({ ctx }): Promise<Result> => { /* work */ },
  after?: async ({ ctx }) => {
    // ctx.data?      — if process returned
    // ctx.error?     — if process threw
    // ctx.reschedule({ delayMs }) — re-run, holds key/slot
    // ctx.expBackoff({ baseMs, maxMs, jitter }) — helper
    // ctx.metric     — live counters reference
  },
});
```

**No call to `ctx.reschedule` in `after`** → terminal. Key released (job), cron advances to next slot (scheduler), error rethrown (retry).

`job` and `scheduler.create` accept optional `trace` callbacks for observability. Trace handlers are awaited for deterministic order, but handler errors are logged and swallowed; tracing must never decide transport state.

`schedulerControl()` is the generic remote control plane for scheduler-backed background work. It can list known schedules across scheduler ids and request a manual run by `{ schedulerId, scheduleId }`. The request is executed only by a live scheduler instance that registered the handler. `runNow` waits for accepted, not completed; use trace or app-owned audit storage for run outcomes.

## Per-module reference

Read the module's reference file in `references/` for full API, gotchas, and usage patterns:

- [references/queue.md](references/queue.md) — `send/recv/ack/nack/touch`, DLQ, idempotency
- [references/topic.md](references/topic.md) — `pub/reader/live`, cursor replay, consumer groups
- [references/ephemeral.md](references/ephemeral.md) — TTL KV with `tenantId` isolation, `prefix` filter (replaces old registry)
- [references/mutex.md](references/mutex.md) — distributed lock primitives
- [references/ratelimit.md](references/ratelimit.md) — sliding-window limiter
- [references/job.md](references/job.md) — durable jobs with `process`/`after`/`ctx.reschedule`, optional typed `<Input, Result>`
- [references/scheduler.md](references/scheduler.md) — cron + leader election, `create/runNow/delete`, `schedulerControl`, `ctx.runNumber`
- [references/retry.md](references/retry.md) — general retry wrapper, `ctx.expBackoff` helper
- [references/migration-v4-v5.md](references/migration-v4-v5.md) — breaking-change guide for code using v4

## Browser runtime specifics

`@valentinkolb/sync/browser` has the same API with additive options:

- Most configs accept optional `store?: Store` for `createLocalStorageStore()` persistence.
- Leader election trivially succeeds in a single tab.
- Multiple instances with the same id in the same tab share state via module-level maps.

API parity is enforced at compile time (see `parity/` in repo).

## High-impact patterns

### Batch item retry (scheduler + job fanout)
When you need "every N minutes, process all dirty items; failed items retry independently":

```ts
const summarize = job<{ chatId: string }>({
  id: "summarize-chat",
  process: async ({ ctx }) => aiSummarize(ctx.input.chatId),
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 60_000 }) });
    }
  },
});

await sched.create({
  id: "summarize-dirty", cron: "*/10 * * * *",
  process: async () => {
    for (const chat of await getDirtyChats()) {
      await summarize.submit({
        key: `chat:${chat.id}`,    // idempotent per chat
        input: { chatId: chat.id },
      });
    }
  },
});
```

Each item has its own `ctx.failureCount`. Running items skip duplicate submits.

### Registry replacement (ephemeral with prefix)
`registry` module was removed in v5. For service discovery / app registry patterns:

```ts
const apps = ephemeral<{ version: string; endpoints: string[] }>({
  id: "apps",
  ttlMs: 60_000, // heartbeat × 3
});
await apps.upsert({ key: "apps/backend", value: { ... } });
await apps.touch({ key: "apps/backend" }); // extend TTL
const snap = await apps.snapshot({ prefix: "apps/" });
```

### Zod is no longer a peer dependency
v5 uses generics (`queue<T>({ id })`) instead of `schema: z.object(...)`. If you want runtime validation, call your validator explicitly in the handler.

## Common migration mistakes from v4

See [references/migration-v4-v5.md](references/migration-v4-v5.md). High-impact gotchas:

- `job.submit({ input, key })` still works (input is optional via `<Input>` generic)
- `job.join()`, `job.cancel()`, `job.events()`, `ctx.step()` are **removed** — use `after` callback and write your own audit row
- `scheduler.register` → `scheduler.create`; `unregister` → `delete`; `triggerNow` → `runNow`
- `registry` module deleted → use `ephemeral.snapshot({ prefix })`
- `misfire: "catch_up_one" | "catch_up_all"` gone → only implicit skip (via `nextCronTimestamp` advancing from now)

## Redis data lifecycle (server package)

Clean by design. Summary:

- **Queue messages** → `ack`/`nack` handles cleanup; terminal = DEL
- **Job idempotency keys** → released on terminal (DEL), else TTL (default 24h)
- **Schedule records** → stay (they ARE the schedule); only `delete({ id })` removes
- **Scheduler control requests** → queued until a live handler accepts them; ack removes the request
- **No per-job event streams** (removed in v5)
- **No DLQ buildup** for jobs (internal queue uses `maxDeliveries: Number.MAX_SAFE_INTEGER`)

No long-term buildup. A stable production deploy doesn't accumulate cruft.
