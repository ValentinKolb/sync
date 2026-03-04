---
name: sync-mutex
description: "Use this skill when working with @valentinkolb/sync distributed locks: exclusive critical sections across pods, lease extension, retry tuning, owner-safe release, and lock-failure handling."
---

# Sync Mutex

Use this skill to implement or review `mutex()` usage.

## Workflow

1. Create one mutex instance per lock namespace.
2. Prefer `withLockOrThrow()` for strict exclusivity.
3. Use `withLock()` when lock acquisition is optional.
4. For long work, call `extend(lock, ttl)` before expiry.
5. Always treat lock acquisition failure as expected control flow.

## Behavioral Guarantees

- Acquire via Redis `SET NX PX` (lease-based mutual exclusion).
- Release/extend only when caller owns the lock token (Lua owner check).
- Add retry + jitter between acquire attempts.
- Auto-expire stale locks via TTL if process dies.

## Non-Guarantees

- Do not provide fairness ordering between contenders.
- Do not survive Redis data loss without persistence.
- Do not automatically renew leases (manual `extend()` required).

## API Reference

Read full API/types/config/defaults in [references/api.md](references/api.md).
