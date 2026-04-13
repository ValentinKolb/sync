import { expect, test } from "bun:test";
import { z } from "zod";
import {
  registry,
  RegistryCapacityError,
  RegistryPayloadTooLargeError,
} from "../index";

const testId = (suffix: string): string => `test-reg-${suffix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

test("upsert + get returns static entry", async () => {
  const store = registry({
    id: testId("basic"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("setting"),
      value: z.string(),
    }),
  });

  const saved = await store.upsert({
    key: "settings/smtp",
    value: {
      appId: "platform",
      kind: "setting",
      value: "smtp.internal",
    },
  });

  expect(saved.key).toBe("settings/smtp");
  expect(saved.status).toBe("active");
  expect(saved.ttlMs).toBeNull();

  const fetched = await store.get({ key: "settings/smtp" });
  expect(fetched).not.toBeNull();
  expect(fetched?.value.value).toBe("smtp.internal");
  expect(fetched?.version).toBe(saved.version);
});

test("touch extends ttl without changing payload version", async () => {
  const store = registry({
    id: testId("touch"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
      url: z.string().url(),
    }),
  });

  const saved = await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: {
      appId: "contacts",
      kind: "instance",
      url: "https://contacts-1.internal",
    },
    ttlMs: 80,
  });

  await Bun.sleep(40);
  const touched = await store.touch({ key: "apps/contacts/instances/i-1" });
  expect(touched.ok).toBe(true);
  expect(touched.version).toBe(saved.version);

  const afterTouch = await store.get({ key: "apps/contacts/instances/i-1" });
  expect(afterTouch?.version).toBe(saved.version);
  expect((afterTouch?.expiresAt ?? 0) > (saved.expiresAt ?? 0)).toBe(true);

  const cas = await store.cas({
    key: "apps/contacts/instances/i-1",
    version: saved.version,
    value: {
      appId: "contacts",
      kind: "instance",
      url: "https://contacts-1b.internal",
    },
  });

  expect(cas.ok).toBe(true);
  expect(cas.entry?.value.url).toBe("https://contacts-1b.internal");
});

test("root snapshot cursor replays later writes", async () => {
  const store = registry({
    id: testId("root-cursor"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
  });

  await store.upsert({
    key: "settings/smtp",
    value: { appId: "platform", kind: "setting" },
  });

  const snap = await store.list();
  const replay = store.reader({ after: snap.cursor });
  const pending = replay.recv({ wait: true, timeoutMs: 500 });

  await store.upsert({
    key: "flags/contacts/new-navbar",
    value: { appId: "contacts", kind: "flag" },
  });

  const event = await pending;
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.key).toBe("flags/contacts/new-navbar");
  }
});

test("prefix reader only receives matching namespace events", async () => {
  const store = registry({
    id: testId("prefix-reader"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
  });

  const reader = store.reader({ prefix: "apps/contacts/" });
  const pending = reader.recv({ wait: true, timeoutMs: 500 });

  await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance" },
    ttlMs: 100,
  });

  const event = await pending;
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.key).toBe("apps/contacts/instances/i-1");
  }

  await store.upsert({
    key: "apps/billing/instances/i-2",
    value: { appId: "billing", kind: "instance" },
    ttlMs: 100,
  });

  const none = await reader.recv({ wait: false });
  expect(none).toBeNull();
});

test("exact-key reader only receives its own key events", async () => {
  const store = registry({
    id: testId("key-reader"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
  });

  const reader = store.reader({ key: "settings/smtp" });
  const pending = reader.recv({ wait: true, timeoutMs: 500 });

  await store.upsert({
    key: "settings/smtp",
    value: { appId: "platform", kind: "setting" },
  });

  const event = await pending;
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.key).toBe("settings/smtp");
  }

  await store.upsert({
    key: "settings/imap",
    value: { appId: "platform", kind: "setting" },
  });

  const none = await reader.recv({ wait: false });
  expect(none).toBeNull();
});

test("cold registry expires ttl-backed entries on first later list", async () => {
  const store = registry({
    id: testId("cold-expire"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 1_000,
    },
  });

  await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance" },
    ttlMs: 40,
  });

  await Bun.sleep(80);

  const active = await store.list({ prefix: "apps/contacts/instances/", status: "active" });
  expect(active.entries.length).toBe(0);

  const expired = await store.list({ prefix: "apps/contacts/instances/", status: "expired" });
  expect(expired.entries.length).toBe(1);
  expect(expired.entries[0]?.status).toBe("expired");
});

test("tombstone cleanup runs during list", async () => {
  const store = registry({
    id: testId("tomb-clean"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 50,
    },
  });

  await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance" },
    ttlMs: 30,
  });

  await Bun.sleep(50);
  const expired = await store.list({ prefix: "apps/contacts/instances/", status: "expired" });
  expect(expired.entries.length).toBe(1);

  await Bun.sleep(70);
  const cleaned = await store.list({ prefix: "apps/contacts/instances/", status: "expired" });
  expect(cleaned.entries.length).toBe(0);
});

test("maxEntries rejects new active keys but allows updates", async () => {
  const store = registry({
    id: testId("capacity"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
    limits: {
      maxEntries: 1,
    },
  });

  await store.upsert({
    key: "settings/smtp",
    value: { appId: "platform", kind: "setting" },
  });

  await expect(
    store.upsert({
      key: "settings/imap",
      value: { appId: "platform", kind: "setting" },
    }),
  ).rejects.toBeInstanceOf(RegistryCapacityError);

  await expect(
    store.upsert({
      key: "settings/smtp",
      value: { appId: "platform", kind: "setting-updated" },
    }),
  ).resolves.toBeDefined();
});

test("payload size limit rejects oversized values", async () => {
  const store = registry({
    id: testId("payload"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
      payload: z.string(),
    }),
    limits: {
      maxPayloadBytes: 64,
    },
  });

  await expect(
    store.upsert({
      key: "settings/huge",
      value: { appId: "platform", kind: "setting", payload: "x".repeat(1_024) },
    }),
  ).rejects.toBeInstanceOf(RegistryPayloadTooLargeError);
});

test("list supports pagination with nextKey", async () => {
  const store = registry({
    id: testId("paging"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
  });

  await store.upsert({ key: "apps/a/instances/1", value: { appId: "a", kind: "instance" } });
  await store.upsert({ key: "apps/a/instances/2", value: { appId: "a", kind: "instance" } });
  await store.upsert({ key: "apps/a/instances/3", value: { appId: "a", kind: "instance" } });

  const first = await store.list({ prefix: "apps/a/instances/", limit: 2 });
  expect(first.entries.length).toBe(2);
  expect(first.nextKey).toBeDefined();

  const second = await store.list({ prefix: "apps/a/instances/", limit: 2, afterKey: first.nextKey });
  expect(second.entries.length).toBe(1);
  expect(second.entries[0]?.key).toBe("apps/a/instances/3");
});

test("get includeExpired returns ttl-expired tombstone", async () => {
  const store = registry({
    id: testId("include-expired"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
  });

  await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance" },
    ttlMs: 30,
  });

  await Bun.sleep(60);

  const active = await store.get({ key: "apps/contacts/instances/i-1" });
  expect(active).toBeNull();

  const expired = await store.get({
    key: "apps/contacts/instances/i-1",
    includeExpired: true,
  });
  expect(expired?.status).toBe("expired");
});

test("touch returns false for static entries", async () => {
  const store = registry({
    id: testId("static-touch"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("setting"),
    }),
  });

  await store.upsert({
    key: "settings/smtp",
    value: { appId: "platform", kind: "setting" },
  });

  const touched = await store.touch({ key: "settings/smtp" });
  expect(touched.ok).toBe(false);
});

test("concurrent CAS contenders produce exactly one winner", async () => {
  const store = registry({
    id: testId("cas-race"),
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

  const [a, b] = await Promise.all([
    store.cas({
      key: saved.key,
      version: saved.version,
      value: { appId: "contacts", kind: "flag", enabled: true },
    }),
    store.cas({
      key: saved.key,
      version: saved.version,
      value: { appId: "contacts", kind: "flag", enabled: false },
    }),
  ]);

  expect(Number(a.ok) + Number(b.ok)).toBe(1);
  const current = await store.get({ key: saved.key });
  expect(current?.version).not.toBe(saved.version);
});

test("CAS refreshes ttl-backed liveness window", async () => {
  const store = registry({
    id: testId("cas-refresh"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
      url: z.string().url(),
    }),
  });

  const saved = await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance", url: "https://contacts-1.internal" },
    ttlMs: 80,
  });

  await Bun.sleep(50);

  const updated = await store.cas({
    key: saved.key,
    version: saved.version,
    value: { appId: "contacts", kind: "instance", url: "https://contacts-2.internal" },
  });

  expect(updated.ok).toBe(true);

  await Bun.sleep(45);
  const stillAlive = await store.get({ key: saved.key });
  expect(stillAlive?.value.url).toBe("https://contacts-2.internal");
});

test("CAS fails after remove and after expiry", async () => {
  const store = registry({
    id: testId("cas-fail"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
  });

  const removed = await store.upsert({
    key: "apps/remove/instances/i-1",
    value: { appId: "remove", kind: "instance" },
  });
  await store.remove({ key: removed.key });
  await expect(
    store.cas({
      key: removed.key,
      version: removed.version,
      value: { appId: "remove", kind: "instance" },
    }),
  ).resolves.toEqual({ ok: false });

  const expiring = await store.upsert({
    key: "apps/expire/instances/i-1",
    value: { appId: "expire", kind: "instance" },
    ttlMs: 30,
  });
  await Bun.sleep(60);
  await expect(
    store.cas({
      key: expiring.key,
      version: expiring.version,
      value: { appId: "expire", kind: "instance" },
    }),
  ).resolves.toEqual({ ok: false });
});

test("get includeExpired respects tombstone retention", async () => {
  const store = registry({
    id: testId("get-retention"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 50,
    },
  });

  await store.upsert({
    key: "apps/contacts/instances/i-1",
    value: { appId: "contacts", kind: "instance" },
    ttlMs: 20,
  });

  await Bun.sleep(40);
  const beforeCleanup = await store.get({
    key: "apps/contacts/instances/i-1",
    includeExpired: true,
  });
  expect(beforeCleanup?.status).toBe("expired");

  await Bun.sleep(70);
  const afterCleanup = await store.get({
    key: "apps/contacts/instances/i-1",
    includeExpired: true,
  });
  expect(afterCleanup).toBeNull();
});

test("expired pagination skips deleted tombstones without empty pages", async () => {
  const store = registry({
    id: testId("expired-pages"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 500,
    },
  });

  await store.upsert({
    key: "apps/paging/instances/001-deleted",
    value: { appId: "paging", kind: "instance" },
    ttlMs: 30,
  });
  await store.upsert({
    key: "apps/paging/instances/002-expired",
    value: { appId: "paging", kind: "instance" },
    ttlMs: 30,
  });
  await store.upsert({
    key: "apps/paging/instances/003-expired",
    value: { appId: "paging", kind: "instance" },
    ttlMs: 30,
  });

  await store.remove({ key: "apps/paging/instances/001-deleted" });
  await Bun.sleep(60);

  const page = await store.list({
    prefix: "apps/paging/instances/",
    status: "expired",
    limit: 2,
  });

  expect(page.entries.map((entry) => entry.key)).toEqual([
    "apps/paging/instances/002-expired",
    "apps/paging/instances/003-expired",
  ]);
});

test("prefix reader survives expiry cleanup and receives later recreate", async () => {
  const store = registry({
    id: testId("prefix-recreate"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
    limits: {
      tombstoneRetentionMs: 40,
    },
  });

  const reader = store.reader({ prefix: "apps/recreate/" });

  const first = reader.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({
    key: "apps/recreate/instances/i-1",
    value: { appId: "recreate", kind: "instance" },
    ttlMs: 20,
  });
  expect((await first)?.type).toBe("upsert");

  const expireEvent = reader.recv({ wait: true, timeoutMs: 500 });
  await Bun.sleep(30);
  await store.list({ prefix: "apps/recreate/", status: "active" });
  expect((await expireEvent)?.type).toBe("expire");

  await Bun.sleep(50);
  await store.list({ prefix: "apps/recreate/", status: "expired" });

  const recreated = reader.recv({ wait: true, timeoutMs: 500 });
  await store.upsert({
    key: "apps/recreate/instances/i-2",
    value: { appId: "recreate", kind: "instance" },
    ttlMs: 20,
  });
  const event = await recreated;
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.key).toBe("apps/recreate/instances/i-2");
  }
});

test("snapshot plus reader handoff does not miss overlapping writes", async () => {
  const store = registry({
    id: testId("handoff"),
    schema: z.object({
      appId: z.string(),
      kind: z.literal("instance"),
    }),
  });

  for (let i = 0; i < 220; i += 1) {
    await store.upsert({
      key: `apps/handoff/instances/stale-${i}`,
      value: { appId: "handoff", kind: "instance" },
      ttlMs: 20,
    });
  }

  await Bun.sleep(40);

  const listPromise = store.list({ prefix: "apps/handoff/", status: "active" });
  const writePromise = (async () => {
    await Bun.sleep(2);
    await store.upsert({
      key: "apps/handoff/instances/fresh",
      value: { appId: "handoff", kind: "instance" },
      ttlMs: 100,
    });
  })();

  const snap = await listPromise;
  await writePromise;

  const inSnapshot = snap.entries.some((entry) => entry.key === "apps/handoff/instances/fresh");
  if (inSnapshot) {
    expect(inSnapshot).toBe(true);
    return;
  }

  const replay = store.reader({ prefix: "apps/handoff/", after: snap.cursor });
  const event = await replay.recv({ wait: true, timeoutMs: 500 });
  expect(event?.type).toBe("upsert");
  if (event?.type === "upsert") {
    expect(event.entry.key).toBe("apps/handoff/instances/fresh");
  }
});

test("reader emits overflow when replay cursor is trimmed away", async () => {
  const store = registry({
    id: testId("overflow"),
    schema: z.object({
      appId: z.string(),
      kind: z.string(),
    }),
    limits: {
      eventMaxLen: 2,
    },
  });

  await store.upsert({
    key: "apps/overflow/instances/1",
    value: { appId: "overflow", kind: "instance" },
  });
  await store.upsert({
    key: "apps/overflow/instances/2",
    value: { appId: "overflow", kind: "instance" },
  });
  await store.upsert({
    key: "apps/overflow/instances/3",
    value: { appId: "overflow", kind: "instance" },
  });

  const replay = store.reader({ after: "0-0" });
  const first = await replay.recv({ wait: false });
  expect(first?.type).toBe("overflow");

  await store.upsert({
    key: "apps/overflow/instances/4",
    value: { appId: "overflow", kind: "instance" },
  });

  const second = await replay.recv({ wait: false });
  expect(second?.type).toBe("upsert");
  if (second?.type === "upsert") {
    expect(second.entry.key).toBe("apps/overflow/instances/4");
  }
});
