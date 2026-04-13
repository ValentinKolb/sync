# Manual Longhaul Test Suites

Purpose:
- provide repeat-heavy integration coverage for all public sync primitives without slowing the default pipeline
- keep scheduler/job long-run cases separate from the remaining primitive suites
- document the exact commands that were verified in this environment

Covered modules:
- `scheduler`
- `job`
- `queue`
- `topic`
- `registry`
- `ephemeral`
- `mutex`
- `ratelimit`
- `retry`

Manual suites:
- `tests/manual/scheduler-repeat.manual.test.ts`
  Server/Redis repeat scheduling, trigger interleaving, cancellation, failover
- `tests/manual/browser-scheduler-repeat.manual.test.ts`
  Browser repeat scheduling, restart/store reuse, missing-handler starvation, unregister during active run
- `tests/manual/primitives-longhaul.manual.test.ts`
  Server/Redis longhaul coverage for ratelimit, mutex, queue, topic, registry, ephemeral, retry
- `tests/manual/browser-primitives-longhaul.manual.test.ts`
  Browser longhaul coverage for ratelimit, mutex, queue, topic, registry, ephemeral, retry

Verified commands:

```bash
REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/scheduler-repeat.manual.test.ts
bun test tests/manual/browser-scheduler-repeat.manual.test.ts
REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/primitives-longhaul.manual.test.ts
bun test tests/manual/browser-primitives-longhaul.manual.test.ts
```

Last verified results:

```text
REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/scheduler-repeat.manual.test.ts
8 pass
0 fail

bun test tests/manual/browser-scheduler-repeat.manual.test.ts
5 pass
0 fail

REDIS_URL=redis://127.0.0.1:6399 bun test tests/manual/primitives-longhaul.manual.test.ts
7 pass
0 fail

bun test tests/manual/browser-primitives-longhaul.manual.test.ts
7 pass
0 fail
```

Notes:
- in this environment, the Redis-backed suites were reliable when `REDIS_URL` was set at process start
- the preload path was not reliable enough for these longhaul manual suites
- these files are intentionally kept under `tests/manual/` so they do not burden the default `bun test` scripts
