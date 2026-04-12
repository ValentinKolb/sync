---
name: sync-scheduler
description: "Use this skill when implementing distributed cron scheduling with @valentinkolb/sync scheduler: registering idempotent schedules across pods, leader-fenced dispatch via job.submit, misfire policy (skip/catch_up_one/catch_up_all), triggerNow for manual dispatch, unregister, metrics/health, and multi-pod leader election. Depends on sync-job for execution and sync-mutex for leader lock. Also works in the browser via `@valentinkolb/sync/browser` for single-tab cron scheduling — same API, no Redis needed."
---

# Sync Scheduler

Cron scheduling with durable dispatch. Server version does distributed leader election across pods, browser version runs single-tab. Dispatch goes through the `job` system.

**Dependencies**: requires `sync-job` handles for execution. Uses `sync-mutex` internally for leader election. Read those skills for deeper semantics if needed.

## Setup Pattern

```ts
// 1. Define jobs first
const cleanupJob = job({ id: "cleanup", schema, process: handler });

// 2. Create scheduler
const sched = scheduler({ id: "platform", onMetric: console.log });

// 3. Start on every pod
sched.start();

// 4. Register schedules (idempotent — safe to call on every startup)
await sched.register({
  id: "cleanup-hourly", cron: "0 * * * *", tz: "Europe/Berlin",
  job: cleanupJob, input: { scope: "temp" }, misfire: "skip",
});

// 5. Graceful shutdown
await sched.stop();
```

## Misfire Policies

- `skip` (default): ignore missed slots, jump to next future cron time.
- `catch_up_one`: submit one overdue slot, then jump to next.
- `catch_up_all`: submit all overdue slots up to `maxCatchUpRuns` (default 100).

## Gotchas

- `register()` is idempotent by schedule `id`. If cron/tz change, `nextRunAt` is recalculated; otherwise preserved.
- `triggerNow({ id })` submits immediately through `job.submit()`. It reuses stored schedule input — use `job.submit()` directly for custom per-run input.
- `triggerNow()` does NOT require `start()` and does NOT alter cron dispatch state.
- `key` in `triggerNow()` is recommended for retry-safe manual triggers.
- `strictHandlers: true` (default): dispatch fails if no job handler is registered. Set to `false` to skip silently.
- Leader lease is 5s by default. Any pod can become leader. Epoch-based CAS prevents stale leaders from dispatching.
- Dispatch uses deterministic submit keys `${scheduleId}:${slotTs}` — the job system deduplicates naturally.
- After `maxConsecutiveDispatchFailures` (default 5), the schedule slot is advanced and failure is logged to DLQ.
- `onMetric` callback is best-effort — errors in the callback are swallowed.

## Browser

```ts
import { scheduler } from "@valentinkolb/sync/browser";
import { job } from "@valentinkolb/sync/browser";
```

Same API (start/stop/register/unregister/triggerNow/get/list/metrics). Browser-specific notes:
- Leader election trivially succeeds (single tab = always leader).
- The tick loop uses `setInterval`-style polling via `setTimeout` + async loop.
- `setTimeout` may be throttled in background tabs — tick precision is best-effort. Misfire policies handle catch-up.
- Schedules and state are in-memory, lost on page refresh.
- Cron parsing uses `Intl.DateTimeFormat` for timezone support — works in all modern browsers.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
