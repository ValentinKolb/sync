# API

## Factory

```ts
import { ratelimit, RateLimitError } from "@valentinkolb/sync";

const limiter = ratelimit({
  id: "api",
  limit: 100,
  windowSecs: 60,
  // prefix: "sync:ratelimit"
});
```

## Types

```ts
type RateLimitConfig = {
  id: string;
  limit: number;
  windowSecs?: number; // default: 1
  prefix?: string; // default: "sync:ratelimit"
};

type RateLimitResult = {
  limited: boolean;
  remaining: number;
  resetIn: number; // milliseconds
};

type RateLimiter = {
  id: string;
  check(identifier: string): Promise<RateLimitResult>;
  checkOrThrow(identifier: string): Promise<RateLimitResult>;
};
```

## Error

`RateLimitError extends Error`

- `name`: `"RateLimitError"`
- `remaining: number`
- `resetIn: number`

## Config Options

- `id`: required limiter namespace.
- `limit`: required max weighted requests inside the active sliding window.
- `windowSecs`: optional window size in seconds. Default `1`.
- `prefix`: optional Redis prefix override. Default `sync:ratelimit`.

## Usage Patterns

### Soft-limit

```ts
const result = await limiter.check(`user:${userId}`);
if (result.limited) {
  return new Response("Too Many Requests", { status: 429 });
}
```

### Exception-driven

```ts
try {
  await limiter.checkOrThrow(`ip:${ip}`);
} catch (error) {
  if (error instanceof RateLimitError) {
    return new Response("Too Many Requests", {
      status: 429,
      headers: { "Retry-After": String(Math.ceil(error.resetIn / 1000)) },
    });
  }
  throw error;
}
```

## Redis Keys

Pattern: `{prefix}:{id}:{identifier}:{windowNumber}`

- current window key expires after `windowSecs * 2` seconds.
- previous window key contributes weighted carry-over.

## Operational Notes

- One limiter instance is cheap; reuse it instead of re-creating per request.
- Use stable identifiers (user, tenant, API key, IP) depending on abuse model.
