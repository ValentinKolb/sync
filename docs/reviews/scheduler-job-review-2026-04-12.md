# Scheduler/Job Review - 2026-04-12

Scope:
- `src/scheduler.ts`
- `src/job.ts`
- related tests under `tests/scheduler.test.ts`, `tests/fault-scheduler.test.ts`, `tests/jobs.test.ts`, `tests/fault-jobs.test.ts`

Goal:
- look for bugs that can prevent scheduled tasks from being dispatched or completed

Status on this branch:
- fixed server/browser job cancellation propagation so successful `cancel()` aborts the active handler signal for the matching job id
- fixed scheduler non-strict missing-handler starvation by advancing overdue schedules instead of leaving them permanently due
- added regression coverage in `tests/jobs.test.ts`, `tests/browser/jobs.test.ts`, and `tests/scheduler.test.ts`

Environment notes:
- Browser suite passed locally (`bun run test:browser`)
- Full server suite was not stable in this environment because the Redis/Valkey test setup on port `6399` dropped connections under load
- Findings below are based on source review plus small targeted Redis-backed repros

## Confirmed Findings

### 1. Server job cancellation does not abort `ctx.signal` for running handlers

Severity: high

Files:
- `src/job.ts:340`
- `src/job.ts:367`
- `src/job.ts:653`

Why this matters:
- The docs promise that `ctx.signal` is aborted on cancellation.
- In the server implementation, `cancel()` only writes durable state and emits an event.
- The running handler's `AbortController` (`jobAc`) is local to the worker loop and is never stored or reached by `cancel()`.
- A cancelled handler therefore keeps running until it returns or throws on its own.
- If the handler keeps calling `ctx.heartbeat()`, it can continue extending the queue lease after cancellation and monopolize the single worker loop for that job definition.
- Downstream effect: later scheduled runs for the same job type can be delayed or appear "stuck" behind a cancelled long-running task.

Observed repro:

```bash
REDIS_URL=redis://localhost:6399 bun -e '
import { z } from "zod";
import { job } from "./index";
let aborted = false, started = false;
const w = job({
  id: `repro-cancel-${Date.now()}`,
  schema: z.object({}),
  process: async ({ ctx }) => {
    started = true;
    ctx.signal.addEventListener("abort", () => { aborted = true; });
    await Bun.sleep(200);
    return "ok";
  },
});
const id = await w.submit({ input: {}, leaseMs: 5000 });
while (!started) await Bun.sleep(5);
await w.cancel({ id, reason: "cancel" });
await Bun.sleep(50);
console.log(JSON.stringify({ aborted }));
console.log(JSON.stringify(await w.join({ id, timeoutMs: 3000 })));
w.stop();
'
```

Observed output:

```json
{"aborted":false}
{"id":"1","status":"cancelled","error":{"message":"cancel","code":"CANCELLED"},"finishedAt":...}
```

### 2. `strictHandlers: false` can starve valid schedules behind a due schedule with no local handler

Severity: high

Files:
- `src/scheduler.ts:697`
- `src/scheduler.ts:705`
- `src/scheduler.ts:772`

Why this matters:
- In non-strict mode, a due schedule with a missing local handler is only counted as `dispatch_skipped`.
- The scheduler does not advance `nextRunAt`, remove the schedule, or otherwise de-queue it.
- That same schedule remains due forever.
- Because dispatch reads only the first `batchSize` due schedules, enough permanently-due missing-handler schedules can block valid schedules from ever being reached.
- With `batchSize: 1`, a single stale missing-handler schedule blocks all later valid schedules indefinitely.
- This is a direct "scheduler stops executing tasks" failure mode.

Observed repro:

```bash
REDIS_URL=redis://localhost:6399 bun -e '
import { scheduler, job } from "./index";
import { z } from "zod";
import { redis } from "bun";
let ran = 0;
const sid = `repro-skip-${Date.now()}`;
const worker = job({
  id: `good-${Date.now()}`,
  schema: z.object({ ok: z.boolean() }),
  process: async () => { ran++; return "ok"; },
});
const s = scheduler({
  id: sid,
  strictHandlers: false,
  leader: { leaseMs: 700, heartbeatMs: 100 },
  dispatch: { tickMs: 30, batchSize: 1 },
});
const now = Date.now();
await redis.send("SET", [`sync:scheduler:${sid}:schedule:a-missing`, JSON.stringify({
  id: "a-missing",
  cron: "* * * * *",
  tz: "UTC",
  misfire: "catch_up_one",
  maxCatchUpRuns: 1,
  jobId: "missing-job",
  input: { ok: true },
  createdAt: now,
  updatedAt: now,
  nextRunAt: now - 60000,
  consecutiveDispatchFailures: 0,
})]);
await redis.send("ZADD", [`sync:scheduler:${sid}:due`, String(now - 60000), "a-missing"]);
await redis.send("SADD", [`sync:scheduler:${sid}:index`, "a-missing"]);
await s.register({
  id: "z-good",
  cron: "* * * * *",
  tz: "UTC",
  misfire: "catch_up_one",
  job: worker,
  input: { ok: true },
});
const key = `sync:scheduler:${sid}:schedule:z-good`;
const raw = await redis.get(key);
const parsed = JSON.parse(raw);
parsed.nextRunAt = now - 60000;
parsed.updatedAt = Date.now();
await redis.send("SET", [key, JSON.stringify(parsed)]);
await redis.send("ZADD", [`sync:scheduler:${sid}:due`, String(now - 60000), "z-good"]);
await redis.send("SADD", [`sync:scheduler:${sid}:index`, "z-good"]);
s.start();
await Bun.sleep(1200);
console.log(JSON.stringify({ ran, metrics: s.metrics(), info: await s.get({ id: "z-good" }) }));
await s.stop();
worker.stop();
'
```

Observed output shape:

```json
{
  "ran": 0,
  "metrics": {
    "dispatchSubmitted": 0,
    "dispatchSkipped": 23,
    "isLeader": true
  },
  "info": {
    "id": "z-good",
    "nextRunAt": <still overdue>
  }
}
```

## Operational assumptions to preserve

- Every runtime that may become leader must call `register(...)` for the schedules it is expected to execute, so that `jobsById` is populated locally.
- `triggerNow()` and normal dispatch both require a local job handler; persisted schedule data in Redis is not enough by itself.
- If `strictHandlers` stays `true`, handler-less leaders should yield quickly rather than continuing to own leadership.

## Suggested next fixes

1. In `job`, track running job abort controllers by job id and abort them from `cancel()` when the state transition to `cancelled` succeeds.
2. In `scheduler`, when `strictHandlers: false` and a due schedule has no local handler, advance or quarantine that schedule so it cannot stay permanently at the head of the due set.
3. Add a server test that asserts `ctx.signal` becomes aborted after `cancel()` on a running job.
4. Add a scheduler starvation test for non-strict mode with `batchSize: 1` and one stale missing-handler schedule ahead of a valid schedule.
