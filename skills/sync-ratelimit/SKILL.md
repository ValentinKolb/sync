---
name: sync-ratelimit
description: "Use this skill when working with @valentinkolb/sync rate limiting in Bun/TypeScript: creating per-identifier sliding-window limiters, choosing check() vs checkOrThrow(), handling RateLimitError with resetIn/Retry-After headers, tuning window size, and reasoning about Redis key layout. Also works in the browser via `@valentinkolb/sync-browser` with an in-memory store — same API, no Redis needed."
---

# Sync RateLimit

Atomic sliding-window rate limiter. Server version uses Redis Lua, browser version uses an in-memory store. One limiter instance per logical limit — reuse across requests.

## Decision Guide

- **Soft limit** (custom response): use `check(identifier)` and inspect `result.limited`.
- **Hard limit** (fail fast): use `checkOrThrow(identifier)` and catch `RateLimitError`.
- **Retry-After header**: `Math.ceil(error.resetIn / 1000)`.

## Gotchas

- `windowSecs` defaults to `1` — very short. For API rate limiting set explicitly (e.g. `60`).
- Identifiers > 128 chars are auto-hashed with sha256 before becoming Redis keys.
- The window is sliding (weighted carry-over from previous window), not fixed-bucket.
- One Lua script per `check()` call — no batching across identifiers.
- No automatic request queuing or delay — rate-limited callers must handle backoff themselves.
- Redis key pattern: `{prefix}:{id}:{identifier}:{windowNumber}`, keys expire after `windowSecs * 2`.

## Browser

```ts
import { ratelimit, RateLimitError } from "@valentinkolb/sync-browser";
```

Same API. The `store` config option lets you inject a custom store (default: `MemoryStore`). Use `createLocalStorageStore()` for persistence across tab reloads. Browser-specific notes:
- Identifiers > 128 chars use a simple djb2 hash instead of SHA-256.
- With `MemoryStore` (default), state is lost on page refresh.
- With `LocalStorageStore`, rate limit counters persist across reloads.
- No Redis keys — counters are held in the store with `setTimeout`-based TTL.

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
