import { expect, test } from "bun:test";
import { redis } from "bun";
import { z } from "zod";
import { registry } from "../index";

const uid = (name: string): string => `${name}-${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;

const registryBase = (id: string): string => `sync:registry:default:${id}`;

test("reconcile drains expired entries across multiple batches", async () => {
  const id = uid("registry-batch-reconcile");
  const store = registry({
    id,
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      reconcileBatchSize: 50,
      tombstoneRetentionMs: 5_000,
    },
  });

  const count = 125;
  await Promise.all(
    Array.from({ length: count }, (_, idx) =>
      store.upsert({
        key: `apps/batch/instances/${idx}`,
        value: { appId: "batch", kind: "instance" },
        ttlMs: 25,
      }),
    ),
  );

  await Bun.sleep(60);

  const active = await store.list({ prefix: "apps/batch/instances/", status: "active" });
  const expired = await store.list({ prefix: "apps/batch/instances/", status: "expired" });

  expect(active.entries.length).toBe(0);
  expect(expired.entries.length).toBe(count);
});

test("exact-key reader survives delete, tombstone cleanup, and recreate", async () => {
  const store = registry({
    id: uid("registry-key-stream"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("setting"),
      value: z.string(),
    }),
    limits: {
      tombstoneRetentionMs: 40,
    },
  });

  const key = "settings/smtp";
  const reader = store.reader({ key });

  const first = reader.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({
    key,
    value: { appId: "platform", kind: "setting", value: "smtp-1" },
  });
  expect((await first)?.type).toBe("upsert");

  const deleted = reader.recv({ wait: true, timeoutMs: 500 });
  await store.remove({ key, reason: "rotate" });
  expect((await deleted)?.type).toBe("delete");

  await Bun.sleep(60);
  await store.list({ prefix: "settings/", status: "active" });

  const recreated = reader.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({
    key,
    value: { appId: "platform", kind: "setting", value: "smtp-2" },
  });

  const event = await recreated;
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.value.value).toBe("smtp-2");
  }
});

test("concurrent remove and cas never leave a key in both active and dead state", async () => {
  for (let round = 0; round < 25; round += 1) {
    const id = uid(`registry-remove-cas-${round}`);
    const store = registry({
      id,
      schema: z.object({
        appId: z.string(),
        kind: z.literal("flag"),
        enabled: z.boolean(),
      }),
    });

    const saved = await store.upsert({
      key: "flags/contacts/new-navbar",
      value: { appId: "contacts", kind: "flag", enabled: false },
    });

    const [removed, updated] = await Promise.all([
      store.remove({ key: saved.key, reason: "cleanup" }),
      store.cas({
        key: saved.key,
        version: saved.version,
        value: { appId: "contacts", kind: "flag", enabled: true },
      }),
    ]);

    expect(!(removed && updated.ok)).toBe(true);

    const base = registryBase(id);
    const activeRaw = await redis.send("HGET", [`${base}:state`, saved.key]);
    const deadRaw = await redis.send("HGET", [`${base}:dead`, saved.key]);

    const hasActive = typeof activeRaw === "string" && activeRaw.length > 0;
    const hasDead = typeof deadRaw === "string" && deadRaw.length > 0;

    expect(hasActive && hasDead).toBe(false);

    if (hasActive) {
      const parsed = JSON.parse(String(activeRaw));
      expect(parsed.status).toBe("active");
    }

    if (hasDead) {
      const parsed = JSON.parse(String(deadRaw));
      expect(parsed.status).toBe("deleted");
    }
  }
});

test("expired pagination stays dense across multiple pages with mixed tombstones", async () => {
  const store = registry({
    id: uid("registry-expired-pages"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 1_000,
    },
  });

  const keys = [
    "apps/pages/instances/001-deleted",
    "apps/pages/instances/002-expired",
    "apps/pages/instances/003-deleted",
    "apps/pages/instances/004-expired",
    "apps/pages/instances/005-expired",
  ];

  for (const key of keys) {
    await store.upsert({
      key,
      value: { appId: "pages", kind: "instance" },
      ttlMs: 30,
    });
  }

  await store.remove({ key: "apps/pages/instances/001-deleted" });
  await store.remove({ key: "apps/pages/instances/003-deleted" });
  await Bun.sleep(60);

  const first = await store.list({
    prefix: "apps/pages/instances/",
    status: "expired",
    limit: 2,
  });
  expect(first.entries.map((entry) => entry.key)).toEqual([
    "apps/pages/instances/002-expired",
    "apps/pages/instances/004-expired",
  ]);
  expect(first.nextKey).toBe("apps/pages/instances/004-expired");

  const second = await store.list({
    prefix: "apps/pages/instances/",
    status: "expired",
    limit: 2,
    afterKey: first.nextKey,
  });
  expect(second.entries.map((entry) => entry.key)).toEqual([
    "apps/pages/instances/005-expired",
  ]);
});
