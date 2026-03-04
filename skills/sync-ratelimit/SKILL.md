---
name: sync-ratelimit
description: "Use this skill when working with @valentinkolb/sync rate limiting in Bun/TypeScript: creating per-identifier limits, handling RateLimitError, tuning windows, and reasoning about sliding-window guarantees and Redis keys."
---

# Sync RateLimit

Use this skill to implement or review `ratelimit()` usage.

## Workflow

1. Instantiate once per logical limiter id.
2. Call `check(identifier)` for soft-limit flows.
3. Call `checkOrThrow(identifier)` when control flow should fail fast.
4. Handle `RateLimitError` and use `resetIn` for retry/backoff.
5. Validate key namespace or `prefix` only when isolation is required.

## Behavioral Guarantees

- Execute each check atomically via a single Lua script.
- Apply sliding-window weighting using current + previous window.
- Hash identifiers longer than 128 chars (`sha256`) before writing Redis keys.
- Return bounded `remaining >= 0` and millisecond `resetIn`.

## Non-Guarantees

- Do not queue or delay requests automatically.
- Do not offer cross-limiter fairness.
- Do not persist historical analytics beyond active Redis keys.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
