---
name: sync
description: "Use this skill for @k2b/sync and @k2b/sync/browser — distributed synchronization primitives for TypeScript/Bun (server) and browsers. Covers all 9 modules: ratelimit, mutex, queue, topic, ephemeral, job, pump, scheduler, retry. Use when code imports from `@k2b/sync` or `@k2b/sync/browser`, or when building features that need: rate limiting, distributed locks, durable work queues with at-least-once delivery + DLQ + idempotency, pub/sub with cursor-based replay, TTL-based presence/registry, durable background jobs with retry + lifecycle callbacks, durable cursor-based backfills/imports/reindexing, distributed cron scheduling with leader election, or retry helpers with exponential backoff. Also use for migrating v4 code to the v5 rewrite (unified callback API, no Zod peer dep, no registry module)."
---

# @k2b/sync — v5

Distributed synchronization primitives with compile-time parity for their shared server (Redis-backed) and browser APIs. Change the import and shared code generally works on both sides; the browser export adds optional `Store` persistence helpers and explicit migration utilities.

## When to use this skill

Trigger for any imports from:
- `@k2b/sync` (server — Bun + Redis/Valkey/Dragonfly 6.2+)
- `@k2b/sync/browser` (browser — in-memory by default, optional `Store` persistence)

Or when the user is building features that need one of the nine modules:

| Module | Use for |
|---|---|
| `ratelimit` | Weighted sliding-window rate limiter per identifier |
| `mutex` | Distributed lock with retry, TTL, owner-only release |
| `queue` | Durable work queue: at-least-once, lease-based visibility, delayMs, idempotency, DLQ |
| `topic` | Pub/sub with cursor-based replay, consumer-group pending recovery, or `live()` broadcast |
| `ephemeral` | TTL key/value with `tenantId` isolation, `prefix` filter, change-stream reader |
| `job` | Durable background tasks with `process` + `after` lifecycle callbacks, typed input, optional trace callback |
| `pump` | Durable cursor pump for backfills, imports, reindexing, reconciliation, and paginated APIs |
| `scheduler` | Distributed cron with leader election, `runNumber`, `failureCount`, `ctx.reschedule`, optional per-schedule trace callback, remote manual control |
| `retry` | General-purpose retry wrapper with the same callback pattern |

## v5 API core pattern

The lifecycle-aware modules share the same `after`/`ctx.reschedule` decision model. `job` and `scheduler` execute `process`; `retry` executes `run`:

```ts
job({
  id: "send-mail",
  process: async ({ ctx }): Promise<Result> => { /* work */ },
  after: async ({ ctx }) => {
    // ctx.data?      — if process returned
    // ctx.error?     — if process threw
    // ctx.reschedule({ delayMs }) — re-run, holds key/slot
    // ctx.expBackoff({ baseMs, maxMs, jitter }) — helper
    // ctx.metric     — live counters reference
  },
});

retry({
  run: async ({ ctx }): Promise<Result> => { /* work */ },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.attempt < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff() });
    }
  },
});
```

**No call to `ctx.reschedule` in `after`** → terminal. Key released (job), cron advances to next slot (scheduler), error rethrown (retry).

`job` and `scheduler.create` accept optional `trace` callbacks for observability. Trace handlers are awaited for deterministic order, but handler errors are logged and swallowed; tracing must never decide transport state.

`schedulerControl()` is the generic remote control plane for scheduler-backed background work. It can list known schedules across scheduler ids and request a manual run by `{ schedulerId, scheduleId }`. The request is executed only by a live scheduler instance that registered the handler. `runNow` waits for accepted, not completed; use trace or app-owned audit storage for run outcomes.

`pump()` is separate from the lifecycle callback pattern. It calls `pull()` to
persist one bounded page, then calls `dispatch()` sequentially and checkpoints
each accepted item. Its guarantee is at-least-once: a crash can repeat the
current item, so every item needs a stable `key` and an idempotent sink.

## Per-module reference

Read the module's reference file in `references/` for full API, gotchas, and usage patterns:

- [references/queue.md](references/queue.md) — `send/recv/ack/nack/touch`, DLQ inspection/removal, idempotency
- [references/topic.md](references/topic.md) — `pub/reader/reclaim/live/latestCursor`, cursor replay, consumer groups, pending recovery
- [references/ephemeral.md](references/ephemeral.md) — TTL KV with `tenantId` isolation, `prefix` filter (replaces old registry)
- [references/mutex.md](references/mutex.md) — distributed lock primitives
- [references/ratelimit.md](references/ratelimit.md) — sliding-window limiter
- [references/job.md](references/job.md) — durable jobs with `process`/`after`/`ctx.reschedule`, optional typed `<Input, Result>`
- [references/pump.md](references/pump.md) — durable cursor pump, recovery semantics, cancellation, and sink composition
- [references/scheduler.md](references/scheduler.md) — cron + leader election, `create/runNow/delete`, `schedulerControl`, `ctx.runNumber`
- [references/retry.md](references/retry.md) — general retry wrapper, `ctx.expBackoff` helper
- [references/migration-v4-v5.md](references/migration-v4-v5.md) — breaking-change guide for code using v4

## Browser runtime specifics

`@k2b/sync/browser` has parity for the shared API with additive options:

- `ratelimit`, `mutex`, `topic`, `pump`, and `scheduler` accept optional `store?: Store`; use `createLocalStorageStore()` when their state must survive a reload.
- Browser leader election coordinates handles through the selected `Store`; cross-tab ownership with `localStorage` is best-effort because it has no atomic compare-and-set.
- Default in-memory stores are process-wide. Handles with the same primitive identity share state; an explicit store creates an explicit persistence scope.

API parity is enforced at compile time (see `parity/` in repo).

The browser entrypoint additionally exports `createMemoryStore`,
`createLocalStorageStore`, `MemoryStore`, `LocalStorageStore`, `StoreWriteError`,
and the `Store` interface. Store writes are synchronous. `StoreWriteError`
distinguishes clone, serialization, and quota failures from primitive/user
errors. Browser storage has no Redis-style atomic compare-and-set; use one
active writer per persisted topic or pump identity. Browser `queue`, `job`, and
`ephemeral` remain in-memory and do not accept `store`.

## Background runtime robustness

Long-lived queue/topic/ephemeral readers and job/pump workers contain retryable Redis/transport failures and retry them with bounded, abort-aware backoff. Scheduler loops catch tick and control failures so later work can continue. Internal timers and fire-and-forget tasks observe their promises, so a rejected heartbeat cannot escape as an unhandled rejection. Where exposed, `stop()` stops local work cooperatively; it does not change durable state unless that module explicitly documents cancellation or deletion.

User callback behavior remains module-specific: `process`, `after`, `pull`, and `dispatch` failures follow the primitive's documented retry/lifecycle rules, while trace failures are observability-only and never determine transport state.

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
- `misfire: "catch_up_one" | "catch_up_all"` gone → one persisted overdue slot runs, then scheduling advances from now

## Redis data lifecycle (server package)

Clean by design. Summary:

- **Queue messages** → `ack` deletes, `nack` requeues, and exhausted delivery attempts move to the retention-bounded DLQ
- **Job idempotency keys** → released on terminal (DEL), else TTL (default 24h)
- **Pump active pages** → deleted after cursor commit; terminal state expires after seven days by default
- **Schedule records** → stay (they ARE the schedule); only `delete({ id })` removes
- **Scheduler control requests** → queued until a live handler accepts them; queue entries are acknowledged on acceptance and short-lived request bindings/responses expire automatically
- **No per-job event streams** (removed in v5)
- **No DLQ buildup** for jobs (internal queue uses `maxDeliveries: Number.MAX_SAFE_INTEGER`)

Transient execution state is bounded or explicitly cleaned. Schedule definitions, queue counters, and other intentional durable indices remain until their documented delete/drain lifecycle is applied.
