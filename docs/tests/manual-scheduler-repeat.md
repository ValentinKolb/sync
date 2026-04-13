# Manual Scheduler Repeat Tests

Purpose:
- verify that schedules keep dispatching beyond the first successful run
- verify restart, failover, `triggerNow()`, cancellation, and shared-job repeat behavior
- verify that `strictHandlers: false` does not starve valid repeating schedules
- verify the same repeat semantics for the browser implementation

Test files:
- `tests/manual/scheduler-repeat.manual.test.ts`
- `tests/manual/browser-scheduler-repeat.manual.test.ts`

Server suite coverage:
- same schedule across multiple overdue cycles
- scheduled dispatch after `triggerNow()`
- non-strict missing-handler does not starve a valid repeating schedule
- repeat dispatch across scheduler restarts
- two schedules sharing the same job across repeated cycles
- multiple `triggerNow()` calls interleaved with later scheduled cycles
- cancelled long-running scheduled run aborts and later cycles still dispatch
- repeated dispatch continuity across leader handover

Browser suite coverage:
- repeated scheduled dispatch after multiple `triggerNow()` calls
- repeat continuity across browser scheduler restart with shared store
- two schedules sharing the same job across repeated cycles
- non-strict missing-handler does not starve a valid repeating schedule
- unregister during active run keeps current run and stops future cycles

Run commands:

```bash
REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/scheduler-repeat.manual.test.ts
bun test tests/manual/browser-scheduler-repeat.manual.test.ts
```

Important note:
- in this environment, setting `REDIS_URL` at process start is reliable
- the preload-based path was not reliable enough for the Redis-backed manual suite

Last verified command results:

```text
REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/scheduler-repeat.manual.test.ts
8 pass
0 fail

bun test tests/manual/browser-scheduler-repeat.manual.test.ts
5 pass
0 fail
```
