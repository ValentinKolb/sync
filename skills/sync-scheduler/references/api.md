# API

## Factory

```ts
import { scheduler } from "@valentinkolb/sync";

const sched = scheduler({
  id: "platform",
  // prefix: "sync:scheduler",
  // leader: { leaseMs: 5000, heartbeatMs: 500 },
  // dispatch: { tickMs, batchSize, maxSubmitsPerTick, submitRetries, ... },
  // strictHandlers: true,
  // onMetric: (m) => console.log(m),
});
```

## Types

```ts
type SchedulerConfig = {
  id: string;
  prefix?: string;
  leader?: {
    leaseMs?: number;
    heartbeatMs?: number;
  };
  dispatch?: {
    tickMs?: number;
    batchSize?: number;
    maxSubmitsPerTick?: number;
    submitRetries?: number;
    submitBackoffBaseMs?: number;
    submitBackoffMaxMs?: number;
    scheduledJobKeyTtlMs?: number;
    dlqMaxEntries?: number;
    maxConsecutiveDispatchFailures?: number;
  };
  strictHandlers?: boolean;
  onMetric?: (metric: SchedulerMetric) => void;
};

type SchedulerRegisterConfig = {
  id: string;
  cron: string;
  tz?: string; // default: "UTC"
  job: {
    id: string;
    submit(cfg: {
      input: unknown;
      key?: string;
      keyTtlMs?: number;
      at?: number;
      delayMs?: number;
      meta?: Record<string, unknown>;
    }): Promise<string>;
    validateInput?(input: unknown): void;
  };
  input: unknown;
  misfire?: "skip" | "catch_up_one" | "catch_up_all"; // default: "skip"
  maxCatchUpRuns?: number; // default: 100
  meta?: Record<string, unknown>;
};

type SchedulerInfo = {
  id: string;
  cron: string;
  tz: string;
  misfire: "skip" | "catch_up_one" | "catch_up_all";
  maxCatchUpRuns: number;
  jobId: string;
  nextRunAt: number;
  createdAt: number;
  updatedAt: number;
};

type SchedulerMetricsSnapshot = {
  isLeader: boolean;
  leaderEpoch: number;
  leaderChanges: number;
  dispatchSubmitted: number;
  dispatchFailed: number;
  dispatchRetried: number;
  dispatchSkipped: number;
  dispatchDlq: number;
  tickErrors: number;
  lastTickAt: number | null;
};

type Scheduler = {
  id: string;
  start(): void;
  stop(): Promise<void>;
  register(cfg: SchedulerRegisterConfig): Promise<{ created: boolean; updated: boolean }>;
  unregister(cfg: { id: string }): Promise<void>;
  get(cfg: { id: string }): Promise<SchedulerInfo | null>;
  list(): Promise<SchedulerInfo[]>;
  metrics(): SchedulerMetricsSnapshot;
};
```

## SchedulerMetric Events

- `leader_acquired`
- `leader_lost` (`reason: "extend_failed" | "stop"`)
- `tick_error`
- `schedule_registered`
- `schedule_updated`
- `schedule_unregistered`
- `dispatch_submitted`
- `dispatch_skipped` (`reason: "missing_handler" | "cas_stale"`)
- `dispatch_failed`
- `dispatch_dlq`
- `dispatch_advanced_after_failures`

## Config Options and Defaults

- `prefix`: `sync:scheduler`
- leader:
  - `leaseMs`: `5000` (min clamp 500)
  - `heartbeatMs`: `500` (min clamp 100)
- dispatch:
  - `tickMs`: `500` (min clamp 50)
  - `batchSize`: `200` (min clamp 1)
  - `maxSubmitsPerTick`: `500` (min clamp 1)
  - `submitRetries`: `3`
  - `submitBackoffBaseMs`: `100` (min clamp 10)
  - `submitBackoffMaxMs`: `2000` (>= base)
  - `scheduledJobKeyTtlMs`: `90d` (min clamp 60000)
  - `dlqMaxEntries`: `5000` (min clamp 1)
  - `maxConsecutiveDispatchFailures`: `5` (min clamp 1)
- `strictHandlers`: `true`

## Misfire Policy

- `skip`: submit no backlog slots; jump to next future cron timestamp.
- `catch_up_one`: submit one overdue slot; then jump to next future cron timestamp.
- `catch_up_all`: submit all overdue slots up to `maxCatchUpRuns`.

## Usage Pattern

```ts
import { z } from "zod";
import { job, scheduler } from "@valentinkolb/sync";

const cleanupJob = job({
  id: "cleanup",
  schema: z.object({ scope: z.string() }),
  process: async ({ input }) => {
    await cleanup(input.scope);
  },
});

const sched = scheduler({
  id: "platform",
  onMetric: (m) => console.log(m),
});

sched.start();

await sched.register({
  id: "cleanup-hourly",
  cron: "0 * * * *",
  tz: "Europe/Berlin",
  job: cleanupJob,
  input: { scope: "temp" },
  misfire: "skip",
});
```

## Internals Summary

- Leader lock uses mutex namespace under `{prefix}:leader`.
- Leader epoch key fences CAS scripts.
- Schedule register/unregister are atomic Lua scripts updating schedule key + due zset + index set.
- Reschedule and failure-recording are CAS-based against expected epoch and expected `nextRunAt`.
- Dispatch failures are optionally pushed to bounded list `dispatch:dlq`.

## Redis Keys

- `{prefix}:{schedulerId}:schedule:{scheduleId}`
- `{prefix}:{schedulerId}:due`
- `{prefix}:{schedulerId}:index`
- `{prefix}:{schedulerId}:dispatch:dlq`
- `{prefix}:{schedulerId}:leader:epoch`
- leader mutex keys under `{prefix}:leader:{schedulerId}:leader:*`
