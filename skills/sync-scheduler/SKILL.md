---
name: sync-scheduler
description: "Use this skill when implementing distributed cron scheduling with @valentinkolb/sync scheduler: idempotent schedule registration across pods, leader-fenced dispatch, misfire policy design, durable dispatch via job.submit, and scheduler reliability/metrics handling."
---

# Sync Scheduler

Use this skill for distributed cron execution with durable dispatch.

Dependency: prefer reading `../sync-job/SKILL.md` for deeper job semantics. For common scheduler tasks, the high-level job subset in this skill is sufficient.

## High-Level Job API (enough for scheduler usage)

Scheduler requires a `job` handle with:

- `id: string`
- `submit({ input, key?, keyTtlMs?, at?, delayMs?, meta? }): Promise<string>`
- optional `validateInput(input: unknown): void`

Scheduler dispatches by calling `job.submit()` with deterministic key `${scheduleId}:${slotTs}` and `at: slotTs`.

## Workflow

1. Create job handlers first.
2. Instantiate one scheduler per scheduler domain (`id`).
3. Call `start()` on each pod.
4. On startup, call `register()` for each schedule (idempotent upsert).
5. Keep callback logic in job handler, not in scheduler.
6. Use `triggerNow({ id, key? })` for durable manual runs of an existing schedule.
7. Observe `onMetric` and `metrics()` for health.
8. Call `stop()` during shutdown.

## Behavioral Guarantees

- No fixed leader node: any pod can lead via lease-based election.
- Leader fencing via epoch CAS prevents stale leaders from rescheduling.
- Registration is idempotent by schedule id (`created` vs `updated`).
- Dispatch is durable through job system with deterministic submit keys.
- `triggerNow()` is durable after it returns a `jobId`; it reuses the registered schedule input and submits locally through the same job path.
- Missing handler policy can fail-safe (`strictHandlers: true` default).

## Non-Guarantees

- Do not run every missed slot by default (`misfire` default is `skip`).
- Do not execute exactly once end-to-end (job system remains at-least-once).
- Do not override schedule input via `triggerNow()`; use direct `job.submit(...)` if per-run input differs.
- Do not emit metrics reliably if user callback throws (metric hook is best effort).

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
