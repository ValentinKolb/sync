# ratelimit

Sliding-window rate limiter per identifier. Same API on server and browser.

## Factory

```ts
import { ratelimit, RateLimitError } from "@k2b/sync";

const limiter = ratelimit({
  id: "api",
  limit: 100,
  windowSecs: 60,
  // prefix?: string,    // default: "sync:rl"
  // store?: Store,      // browser only (additive)
});
```

## API

```ts
type RateLimitResult = {
  limited: boolean;
  remaining: number;
  resetIn: number; // ms until the window resets
};

type RateLimiter = {
  id: string;
  check(identifier: string): Promise<RateLimitResult>;
  checkOrThrow(identifier: string): Promise<RateLimitResult>;  // throws RateLimitError if over
};
```

## Usage

```ts
// Soft check — returns a result object
const result = await limiter.check("user:123");
if (result.limited) {
  return c.json({ error: `Retry in ${result.resetIn}ms` }, 429);
}

// Strict check — throws
try {
  await limiter.checkOrThrow("user:123");
} catch (e) {
  if (e instanceof RateLimitError) {
    console.log(`Over limit. Retry in ${e.resetIn}ms`);
  }
}
```

## Gotchas

- `identifier` is the rate-limit bucket key (e.g. user id, IP). Same `identifier` shares the same window.
- `check` counts the current request toward the limit (it's not a peek).
- Window is sliding, not calendar-aligned. "100 per 60s" means 100 in the trailing 60s from now.
- Both packages support `store?: Store` in the browser version; server uses Redis directly.
- During browser rolling upgrades, checks read and advance both the current and
  legacy Store counters. This prevents a bundle upgrade from resetting an active
  limit; identities that collided under the old layout conservatively share the
  counter until those windows expire.

## Redis keys (server)

- `{prefix}:{id}:{identifier}:{window}` — counter per fixed window, expired automatically. The sliding window is computed by weighting the previous window's counter against elapsed time; no sorted set of timestamps is kept.
- Auto-expires past-window entries during each `check`
