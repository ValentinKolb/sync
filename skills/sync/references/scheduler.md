# scheduler

Distributed cron with leader election and callback-based dispatch. Same API on server and browser.

## Factory

```ts
import { scheduler } from "@k2b/sync";

const sched = scheduler({
  id: "platform",
  // prefix?: string,                               // default: "sync:scheduler"
  // leader?: { leaseMs: 5_000, heartbeatMs: 500 },
  // dispatch?: { tickMs: 500, batchSize: 200 },
  // store?: Store,                                 // browser only (additive)
});
```

Must call `sched.start()` to begin the tick loop. Call `sched.stop()` for graceful shutdown.

## Types

```ts
type ScheduleCtx = {
  scheduleId: string;
  slotTs: number;           // the cron slot timestamp this dispatch is for
  runNumber: number;        // 1-indexed, persistent across restarts, monotonic
  failureCount: number;     // consecutive failures before this run (resets on success)
  trigger: "cron" | "manual"; // what caused this run
  readonly duration: number;
  signal: AbortSignal;
};

type ScheduleAfterCtx<Result> = ScheduleCtx & {
  data?: Result;
  error?: Error;
  reschedule(cfg?: { delayMs?: number }): void;
  expBackoff(cfg?: BackoffOptions): number;
  metric: SchedulerMetrics;
};

type ScheduleConfig<Result = unknown> = {
  id: string;
  cron: string;             // standard 5-field cron (min hour dom month dow)
  tz?: string;              // IANA tz, default "UTC"
  meta?: Record<string, unknown>;
  trace?: TraceHandler<SchedulerTraceEvent<Result>>;
  process: (cfg: { ctx: ScheduleCtx }) => Promise<Result> | Result;
  after?: (cfg: { ctx: ScheduleAfterCtx<Result> }) => Promise<void> | void;
};

type TraceHandler<Event> = (event: Event) => void | Promise<void>;

type SchedulerTraceEvent<Result = unknown> =
  | { type: "scheduled"; scheduleId: string; cron: string; tz: string; nextRunAt: number; meta?: Record<string, unknown> }
  | { type: "started"; scheduleId: string; runNumber: number; trigger: "cron" | "manual"; slotTs: number }
  | { type: "succeeded"; scheduleId: string; runNumber: number; data: Result; durationMs: number }
  | { type: "failed"; scheduleId: string; runNumber: number; error: Error; durationMs: number }
  | { type: "rescheduled"; scheduleId: string; runNumber: number; delayMs: number };

type SchedulerMetrics = {
  isLeader: boolean;
  leaderChanges: number;
  dispatches: number;
  failures: number;
  reschedules: number;
  tickErrors: number;
  lastTickAt: number | null;
};

type SchedulerInfo = {
  id: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  lastError?: string;
  meta?: Record<string, unknown>;
};

type Scheduler = {
  id: string;
  start(): void;
  stop(): Promise<void>;
  create<R>(cfg: ScheduleConfig<R>): Promise<{ created: boolean; updated: boolean }>;
  delete(cfg: { id: string }): Promise<void>;
  runNow(cfg: { id: string }): Promise<void>;    // no cron advance
  get(cfg: { id: string }): Promise<SchedulerInfo | null>;
  list(): Promise<SchedulerInfo[]>;
  metric(): SchedulerMetrics;
};

type SchedulerControlState = "available" | "unavailable";

type SchedulerControlInfo = {
  schedulerId: string;
  scheduleId: string;
  cron: string;
  tz: string;
  createdAt: number;
  updatedAt: number;
  nextRunAt: number;
  runNumber: number;
  failureCount: number;
  state: SchedulerControlState;
  lastError?: string;
  meta?: Record<string, unknown>;
};

type SchedulerControl = {
  list(): Promise<SchedulerControlInfo[]>;
  runNow(cfg: {
    schedulerId: string;
    scheduleId: string;
    requestId?: string;
    timeoutMs?: number;
  }): Promise<void>;
};
```

## Usage

```ts
sched.start();

await sched.create<{ cleaned: number }>({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "Europe/Berlin",
  trace: async (event) => {
    await cloudTrace({ source: "cleanup", event });
  },
  process: async ({ ctx }) => {
    const n = await cleanupOldRecords();
    return { cleaned: n };
  },
  after: async ({ ctx }) => {
    if (ctx.error && ctx.failureCount < 5) {
      ctx.reschedule({ delayMs: ctx.expBackoff({ baseMs: 60_000, maxMs: 5 * 60_000 }) });
    }
  },
});

// Manual trigger — doesn't alter cron schedule
await sched.runNow({ id: "cleanup" });  // ctx.trigger === "manual" inside the handler

// Remove
await sched.delete({ id: "cleanup" });

// Inspect
await sched.get({ id: "cleanup" });
await sched.list();
sched.metric();

await sched.stop();
```

## Remote manual control

Use `schedulerControl()` in an external process, such as an admin API, when that process should trigger scheduler-backed work without registering or importing the handler.

```ts
import {
  SchedulerControlNotFoundError,
  SchedulerControlUnavailableError,
  schedulerControl,
} from "@k2b/sync";

const control = schedulerControl();

const schedules = await control.list();
// [{ schedulerId: "platform", scheduleId: "cleanup", state: "available", meta: ... }]

try {
  await control.runNow({ schedulerId: "platform", scheduleId: "cleanup" });
} catch (error) {
  if (error instanceof SchedulerControlNotFoundError) {
    // The schedule record does not exist.
  }
  if (error instanceof SchedulerControlUnavailableError) {
    // The schedule exists, but no live scheduler has its handler registered.
  }
}
```

`schedulerControl.runNow()` waits for a live scheduler instance to accept the request. It does not wait for the handler to finish and it does not serialize handler results or errors. Use `trace`, metrics, or app-owned audit storage for completion visibility.

**A timeout does not cancel the request.** `SchedulerControlTimeoutError` means no instance accepted within `timeoutMs`; the request stays queued and may still be picked up. Retrying with a fresh `requestId` therefore risks a second execution — pass the *same* `requestId` to retry idempotently. If dispatch keeps failing, the request is reported as unavailable after a few attempts instead of being replayed indefinitely.

A `requestId` is bound to one `{ schedulerId, scheduleId }` target for its idempotency window. Reusing it for another schedule throws instead of triggering ambiguous work.

## Common pattern: cron + job fanout (batch item retry)

When you need "every N minutes, process all dirty items; each item retries independently":

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
  id: "summarize-dirty",
  cron: "*/10 * * * *",
  process: async () => {
    for (const chat of await getDirtyChats()) {
      await summarize.submit({
        key: `chat:${chat.id}`,     // idempotent per chat
        input: { chatId: chat.id },
      });
    }
  },
});
```

Each item has its own `ctx.failureCount`. Already-running items skip duplicate submits. Failed items retry independently of the cron tick.

## Trace semantics

`trace` is per schedule, not on the scheduler factory. A scheduler can host unrelated schedules, so each schedule chooses its own observability sink.

- **`scheduled`** fires after `create()` successfully creates or updates the schedule record and handler.
- **`started`** fires before `process`.
- **`succeeded` / `failed`** describe the `process` result for one run. `after` can still call `ctx.reschedule()` after either event.
- **`rescheduled`** fires after `after` calls `ctx.reschedule()` and the schedule state has been updated.
- **There is no `finished` event**. A schedule is a recurring definition, not a terminal unit of work. Use `succeeded`, `failed`, and `rescheduled` for run outcomes.
- **Trace handler errors are swallowed**. The library logs `[sync trace] trace handler failed` and keeps scheduler execution unchanged.
- **Trace handlers are awaited** so events for one run keep a deterministic order. If you want buffering or fire-and-forget behavior, implement it inside the trace handler.

## Gotchas

- **`cron` is validated** at `create` time (invalid cron throws).
- **`tz` is validated** (invalid IANA tz throws).
- **Misfire behavior**: always "skip" — if the system was down when a slot was due, `nextRunAt` advances past all missed slots to the next future cron slot. There's no `catch_up_one` / `catch_up_all` in v5.
- **`create` is idempotent by id**: second call with same id updates. If `cron`/`tz` changed, `nextRunAt` resets; otherwise it's preserved.
- **`runNow` does NOT advance cron**: the regular schedule continues unchanged, unless you call `ctx.reschedule` inside `after`.
- **`schedulerControl.runNow` is remote accepted, not completed**: it returns when a live scheduler with the handler accepts the request. The handler then runs with `ctx.trigger === "manual"`.
- **Unavailable is explicit**: `SchedulerControlUnavailableError` means the schedule exists but no live handler heartbeat is present. Start a scheduler instance that calls `create()` for that schedule.
- **`ctx.trigger`**: `"cron"` when dispatched by the tick loop; `"manual"` when invoked via `runNow`. Useful for conditionals like "skip expensive validation on manual runs" or "log admin runs separately". Available in both `process` and `after` ctx.
- **`ctx.runNumber` is persistent**: preserved across restarts, re-registrations, and (different) cron changes. Only `delete` resets.
- **`ctx.failureCount` persists across cron slots**: resets to 0 on any successful run. A consistently failing schedule grows this counter indefinitely — use it to decide when to give up in `after`.
- **Handler missing on the current leader pod**: the scheduler steps down without advancing the slot so another pod can serve it. All pods should still register all schedules on startup.
- **Multiple pods coordinate via leader mutex**: one leader dispatches at a time. Leader lease is 5s by default. Brief overlap during handoff can cause at-least-once slot dispatch — make `process` idempotent.
- **Leader timing is normalized**: the lease is at least 500ms and the heartbeat is capped at one third of the lease, so an oversized heartbeat cannot silently let leadership expire during a long callback.
- **`after` errors are swallowed**: don't throw inside `after` — use `ctx.reschedule` to signal intent.
- **Trace is not an audit log by itself**: it is an in-process callback. Persist events yourself if you need durable audit history.

## Redis keys (server)

- `sync:scheduler:{sid}:schedule:{id}` — stored schedule JSON
- `sync:scheduler:{sid}:due` — sorted set of scheduleId by nextRunAt
- `sync:scheduler:{sid}:index` — set of all schedule ids
- `sync:scheduler:leader:{sid}:leader:active` — leader mutex key
- `sync:scheduler:index` — set of scheduler ids for `schedulerControl().list()`
- `sync:scheduler:{sid}:control:{id}:handler:{instance}` — live handler heartbeat
- `sync:scheduler:control:*` — durable manual-run control queues and short-lived accepted responses

**Removed in v5**: `leader:epoch`, CAS Lua scripts, `dispatch:dlq`.

## Browser differences

- Handlers registered per-instance (can't serialize functions). Multiple instances with same `id` share schedule records but each has local handlers.
- `store?: Store` lets `runNumber`/`nextRunAt`/`failureCount` survive tab reloads (via `createLocalStorageStore()`).
- Leader election always succeeds in single tab.
- Tick loop uses `setTimeout` — may be throttled in background tabs. Misfire-skip handles catch-up implicitly.
