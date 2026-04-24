# scheduler

Distributed cron with leader election and callback-based dispatch. Same API on server and browser.

## Factory

```ts
import { scheduler } from "@valentinkolb/sync";

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
  process: (cfg: { ctx: ScheduleCtx }) => Promise<Result> | Result;
  after?: (cfg: { ctx: ScheduleAfterCtx<Result> }) => Promise<void> | void;
};

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
```

## Usage

```ts
sched.start();

await sched.create<{ cleaned: number }>({
  id: "cleanup",
  cron: "0 * * * *",
  tz: "Europe/Berlin",
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

## Gotchas

- **`cron` is validated** at `create` time (invalid cron throws).
- **`tz` is validated** (invalid IANA tz throws).
- **Misfire behavior**: always "skip" — if the system was down when a slot was due, `nextRunAt` advances past all missed slots to the next future cron slot. There's no `catch_up_one` / `catch_up_all` in v5.
- **`create` is idempotent by id**: second call with same id updates. If `cron`/`tz` changed, `nextRunAt` resets; otherwise it's preserved.
- **`runNow` does NOT advance cron**: the regular schedule continues unchanged, unless you call `ctx.reschedule` inside `after`.
- **`ctx.trigger`**: `"cron"` when dispatched by the tick loop; `"manual"` when invoked via `runNow`. Useful for conditionals like "skip expensive validation on manual runs" or "log admin runs separately". Available in both `process` and `after` ctx.
- **`ctx.runNumber` is persistent**: preserved across restarts, re-registrations, and (different) cron changes. Only `delete` resets.
- **`ctx.failureCount` persists across cron slots**: resets to 0 on any successful run. A consistently failing schedule grows this counter indefinitely — use it to decide when to give up in `after`.
- **Handler missing on the current leader pod**: the scheduler silently advances past the slot. Another pod with the handler will pick up the next slot. All pods should register all schedules on startup.
- **Multiple pods coordinate via leader mutex**: one leader dispatches at a time. Leader lease is 5s by default. Brief overlap during handoff can cause at-least-once slot dispatch — make `process` idempotent.
- **`after` errors are swallowed**: don't throw inside `after` — use `ctx.reschedule` to signal intent.

## Redis keys (server)

- `sync:scheduler:{sid}:schedule:{id}` — stored schedule JSON
- `sync:scheduler:{sid}:due` — sorted set of scheduleId by nextRunAt
- `sync:scheduler:{sid}:index` — set of all schedule ids
- `sync:scheduler:leader:{sid}:leader:active` — leader mutex key

**Removed in v5**: `leader:epoch`, CAS Lua scripts, `dispatch:dlq`.

## Browser differences

- Handlers registered per-instance (can't serialize functions). Multiple instances with same `id` share schedule records but each has local handlers.
- `store?: Store` lets `runNumber`/`nextRunAt`/`failureCount` survive tab reloads (via `createLocalStorageStore()`).
- Leader election always succeeds in single tab.
- Tick loop uses `setTimeout` — may be throttled in background tabs. Misfire-skip handles catch-up implicitly.
