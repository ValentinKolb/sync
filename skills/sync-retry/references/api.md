# API

## Browser

```ts
import { retry, isRetryableTransportError, DEFAULT_RETRY_OPTIONS } from "@valentinkolb/sync-browser";
```

Same API. Uses `setTimeout` for delays instead of Bun's `sleep`. All options and behavior are identical.

---

## Exports

```ts
import {
  retry,
  DEFAULT_RETRY_OPTIONS,
  isRetryableTransportError,
  type RetryOptions,
} from "@valentinkolb/sync";
```

## Types

```ts
type RetryOptions = {
  attempts?: number; // default: 8
  minDelayMs?: number; // default: 100
  maxDelayMs?: number; // default: 2000
  factor?: number; // default: 2
  jitter?: number; // default: 0.2
  signal?: AbortSignal;
  retryIf?: (error: unknown) => boolean; // default: isRetryableTransportError
};

function retry<T>(
  fn: (attempt: number) => Promise<T> | T,
  opts?: RetryOptions,
): Promise<T>;

function isRetryableTransportError(error: unknown): boolean;
```

## Defaults

```ts
const DEFAULT_RETRY_OPTIONS = {
  attempts: 8,
  minDelayMs: 100,
  maxDelayMs: 2000,
  factor: 2,
  jitter: 0.2,
  retryIf: isRetryableTransportError,
};
```

## Usage

### Default-first (recommended)

```ts
const result = await retry(() => fragileRedisCall());
```

### Per-call override (rare)

```ts
const result = await retry(
  () => fragileRedisCall(),
  {
    attempts: 12,
    maxDelayMs: 5000,
    retryIf: isRetryableTransportError,
  },
);
```

### Reader loop example

```ts
while (!signal.aborted) {
  const item = await retry(
    () => reader.recv({ wait: true, timeoutMs: 30_000, signal }),
    {
      attempts: Number.POSITIVE_INFINITY,
      signal,
      retryIf: isRetryableTransportError,
    },
  );

  if (!item) continue;
  await handle(item);
}
```

## Library Internal Usage (important)

- Queue/topic/ephemeral stream loops (`wait: true`) use retry with effectively unbounded attempts until aborted.
- This improves liveness across brief Redis outages.
- One-shot calls keep explicit failure behavior unless you wrap them with `retry(...)` yourself.

## What not to retry

- Schema validation failures
- Application/business rule failures
- Deterministic errors where retry cannot help
