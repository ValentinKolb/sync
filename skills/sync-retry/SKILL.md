---
name: sync-retry
description: Use this skill when handling transient transport failures with @valentinkolb/sync retry utility: applying sensible defaults, per-call overrides, AbortSignal cancellation, and retryIf classification for Redis/network operations.
---

# Sync Retry

Use this skill for minimal, transport-aware retries.

## Design Rule

- Prefer defaults (`retry(fn)`) in almost all cases.
- Override per-call only for genuine edge cases.
- Do not use retry for business/domain errors.

## Behavioral Guarantees

- Retries attempts with exponential backoff + jitter.
- Stops immediately on non-retryable errors.
- Supports cooperative cancellation via `AbortSignal`.

## API Reference

Read full API/types/defaults in [references/api.md](references/api.md).
