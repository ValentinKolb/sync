---
name: sync-retry
description: "Use this skill when handling transient transport failures with @valentinkolb/sync retry utility: wrapping Redis/network calls with exponential backoff, configuring per-call retry overrides, using AbortSignal for cooperative cancellation, and classifying errors with retryIf. Also use when writing stream reader loops that must survive brief outages. Works identically in the browser via `@valentinkolb/sync/browser` — no Redis dependency, uses setTimeout-based sleep."
---

# Sync Retry

Minimal, transport-aware retry wrapper. Prefer `retry(fn)` with defaults in almost all call sites.

## Decision Guide

- **One-shot Redis/network call**: `retry(() => call())` — defaults are sufficient.
- **Blocking stream loop** (queue/topic/ephemeral reader): use `attempts: Number.POSITIVE_INFINITY` + `signal` so the loop retries indefinitely until aborted.
- **Business/domain error**: do NOT wrap with retry. Retry is only for transient transport errors.

## Gotchas

- `retryIf` default (`isRetryableTransportError`) matches error codes (`ECONNRESET`, `ETIMEDOUT`, `ECONNREFUSED`, `ENOTFOUND`, `EPIPE`, `EHOSTUNREACH`, `ECONNABORTED`) and message substrings (`connection`, `socket`, `broken pipe`, `network`, `clusterdown`, `tryagain`, `loading`). Any error not matching is thrown immediately.
- `signal.aborted` is checked before each attempt — no wasted work after cancellation.
- Library internals (queue/topic/ephemeral `stream({ wait: true })`) already use retry with unbounded attempts. Do not double-wrap those calls.
- `attempts: 1` means no retry — only one try. The default is `8`.

## Browser

```ts
import { retry, isRetryableTransportError } from "@valentinkolb/sync/browser";
```

Identical API and defaults. Uses `setTimeout`-based sleep instead of Bun's `sleep`. No transport errors occur in-memory, but `retry()` is still useful for wrapping flaky `fetch()` calls or other async operations in browser apps.

## API Reference

Read full API/types/defaults in [references/api.md](references/api.md).
