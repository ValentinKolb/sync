import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../src/sync.ts";
import type { Sync } from "../src/sync.ts";
import { SnapshotOverflowError } from "../src/errors.ts";
import type { EphemeralEvent } from "../src/ephemeral.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

type Presence = { user: string; status: string };

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

beforeAll(async () => {
  nc = await connectToCluster({ name: "ephemeral-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("ephemeral state", () => {
  test("upsert, snapshot, remove — tenant and prefix scoped", async () => {
    const registry = sync.ephemeral<Presence>({ id: "registry", ttlMs: 60_000 });
    await registry.upsert({ key: "svc/api-1", value: { user: "api-1", status: "up" } });
    await registry.upsert({ key: "svc/api-2", value: { user: "api-2", status: "up" } });
    await registry.upsert({ key: "worker/w-1", value: { user: "w-1", status: "up" } });
    await registry.upsert({ key: "svc/other", tenantId: "acme", value: { user: "x", status: "up" } });

    const all = await registry.snapshot();
    expect(all.entries.map((e) => e.key).toSorted()).toEqual(["svc/api-1", "svc/api-2", "worker/w-1"]);

    const svc = await registry.snapshot({ prefix: "svc/" });
    expect(svc.entries).toHaveLength(2);

    const acme = await registry.snapshot({ tenantId: "acme" });
    expect(acme.entries.map((e) => e.key)).toEqual(["svc/other"]);

    expect(await registry.delete({ key: "svc/api-2" })).toBe(true);
    expect(await registry.delete({ key: "svc/api-2" })).toBe(false);
    expect((await registry.snapshot({ prefix: "svc/" })).entries).toHaveLength(1);
  });

  test("keys expire by TTL; touch refreshes the TTL", async () => {
    const presence = sync.ephemeral<Presence>({ id: "presence", ttlMs: 1_000 });
    await presence.upsert({ key: "gone", value: { user: "a", status: "up" } });
    await presence.upsert({ key: "kept", value: { user: "b", status: "up" } });
    await Bun.sleep(700);
    expect(await presence.touch({ key: "kept" })).toBe(true);
    await Bun.sleep(800);
    // "gone" expired (1s TTL), "kept" was refreshed at ~700ms.
    const snap = await presence.snapshot();
    expect(snap.entries.map((e) => e.key)).toEqual(["kept"]);
    expect(await presence.touch({ key: "gone" })).toBe(false);
  }, 15_000);

  test("watch streams upserts, deletes, and TTL expiry", async () => {
    const live = sync.ephemeral<Presence>({ id: "watch", ttlMs: 1_000 });
    const controller = new AbortController();
    const events: EphemeralEvent<Presence>[] = [];
    (async () => {
      for await (const event of live.watch({ signal: controller.signal })) events.push(event);
    })();
    await Bun.sleep(250);
    await live.upsert({ key: "a", value: { user: "a", status: "up" }, ttlMs: 60_000 });
    await live.upsert({ key: "b", value: { user: "b", status: "up" }, ttlMs: 1_000 });
    await live.delete({ key: "a" });
    await waitFor(() => events.length >= 4, 15_000); // upsert a, upsert b, delete a, expire b
    controller.abort();
    expect(events[0]).toMatchObject({ type: "upsert" });
    expect(events[1]).toMatchObject({ type: "upsert" });
    expect(events[2]).toMatchObject({ type: "delete", key: "a" });
    expect(events[3]).toMatchObject({ type: "expire", key: "b" });
  }, 20_000);

  test("watch after snapshot revision sees only later changes; stale revision requires resync", async () => {
    const state = sync.ephemeral<Presence>({ id: "resume", ttlMs: 60_000, history: 1 });
    await state.upsert({ key: "one", value: { user: "1", status: "up" } });
    const snap = await state.snapshot();
    await state.upsert({ key: "two", value: { user: "2", status: "up" } });

    const events: EphemeralEvent<Presence>[] = [];
    const controller = new AbortController();
    (async () => {
      for await (const event of state.watch({ after: snap.revision, signal: controller.signal })) events.push(event);
    })();
    await waitFor(() => events.length >= 1);
    controller.abort();
    expect(events[0]).toMatchObject({ type: "upsert", entry: { key: "two" } });

    // A revision far below the bucket's first sequence demands a resync.
    const stale: EphemeralEvent<Presence>[] = [];
    // Force first_seq forward: overwrite "one" repeatedly (history=1 evicts).
    for (let i = 0; i < 5; i++) await state.upsert({ key: "one", value: { user: "1", status: `v${i}` } });
    for await (const event of state.watch({ after: "0" })) {
      stale.push(event);
      break;
    }
    expect(stale[0]!.type).toBe("resync_required");
  }, 20_000);

  test("bounded snapshot overflows explicitly", async () => {
    const tiny = sync.ephemeral<number>({ id: "tiny", ttlMs: 60_000, maxEntries: 2 });
    await tiny.upsert({ key: "a", value: 1 });
    await tiny.upsert({ key: "b", value: 2 });
    await tiny.upsert({ key: "c", value: 3 });
    await expect(tiny.snapshot()).rejects.toBeInstanceOf(SnapshotOverflowError);
  });
});

describe("hardening regressions", () => {
  test("touch never reverts a concurrent upsert (CAS heartbeat)", async () => {
    const state = sync.ephemeral<{ v: number }>({ id: "touch-cas", ttlMs: 60_000 });
    await state.upsert({ key: "hot", value: { v: 0 } });
    // Interleave value-bearing upserts with value-less touches; the final
    // value must always be the last upsert's, never a stale touch replay.
    for (let round = 1; round <= 15; round++) {
      await Promise.all([
        state.upsert({ key: "hot", value: { v: round } }),
        state.touch({ key: "hot" }),
        state.touch({ key: "hot" }),
      ]);
      const snap = await state.snapshot();
      expect(snap.entries.find((e) => e.key === "hot")!.value.v).toBe(round);
    }
  }, 30_000);

  test("watch prefix filters and snapshot omits unknowable expiresAt", async () => {
    const state = sync.ephemeral<number>({ id: "watch-prefix", ttlMs: 60_000 });
    const seen: string[] = [];
    const controller = new AbortController();
    (async () => {
      for await (const event of state.watch({ prefix: "a/", signal: controller.signal })) {
        if (event.type === "upsert") {
          seen.push(event.entry.key);
          expect(event.entry.expiresAt).toBeUndefined();
        }
      }
    })();
    await Bun.sleep(250);
    await state.upsert({ key: "a/1", value: 1 });
    await state.upsert({ key: "b/1", value: 2 });
    await state.upsert({ key: "a/2", value: 3 });
    await waitFor(() => seen.length >= 2, 15_000);
    controller.abort();
    expect(seen).toEqual(["a/1", "a/2"]);
    const upserted = await state.upsert({ key: "a/3", value: 4, ttlMs: 5_000 });
    expect(upserted.expiresAt).toBeInstanceOf(Date);
  }, 30_000);

  test("oversized tenant+key combinations are rejected up front", async () => {
    const state = sync.ephemeral<number>({ id: "keylen", ttlMs: 60_000 });
    const tenant = "t".repeat(96);
    const key = "k".repeat(96);
    await expect(state.upsert({ tenantId: tenant, key, value: 1 })).rejects.toThrow("characters");
  });
});

describe("watch signal handling", () => {
  test("a pre-aborted signal ends the watch immediately instead of hanging", async () => {
    const state = sync.ephemeral<number>({ id: "pre-abort", ttlMs: 60_000 });
    await state.upsert({ key: "x", value: 1 });
    const events = [];
    for await (const event of state.watch({ signal: AbortSignal.abort() })) events.push(event);
    expect(events).toEqual([]);
  }, 10_000);
});
