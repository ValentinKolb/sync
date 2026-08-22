import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { NatsConnection } from "@nats-io/nats-core";
import { createSync } from "../../src/v6/sync.ts";
import type { Sync } from "../../src/v6/sync.ts";
import { InvalidNameError, ObjectTooLargeError } from "../../src/v6/errors.ts";
import { connectToCluster } from "./cluster.ts";
import { cleanupNamespaces, testNamespace, waitFor } from "./helpers.ts";

let nc: NatsConnection;
let sync: Sync;
const namespace = testNamespace();

const store = () =>
  sync.objectStore({
    id: "artifacts",
    retention: { maxAgeMs: 60 * 60 * 1_000, maxBytes: 256 * 1024 * 1024 },
    maxObjectBytes: 4 * 1024 * 1024,
  });

const chunkedBody = (chunks: number, chunkBytes: number, fill = 7): ReadableStream<Uint8Array> => {
  let produced = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (produced >= chunks) return controller.close();
      produced += 1;
      controller.enqueue(new Uint8Array(chunkBytes).fill(fill));
    },
  });
};

const readAll = async (body: ReadableStream<Uint8Array>): Promise<number> => {
  let total = 0;
  const reader = body.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value!.length;
  }
  return total;
};

beforeAll(async () => {
  nc = await connectToCluster({ name: "object-test" });
  sync = createSync({ connection: nc, namespace, application: "tests" });
  await sync.ready();
});

afterAll(async () => {
  await sync.drain({ timeoutMs: 2_000 });
  await cleanupNamespaces(nc, [namespace]);
  await nc.close();
});

describe("object store", () => {
  test("streams a chunked object and reads it back with digest verification", async () => {
    const artifacts = store();
    const ref = await artifacts.put({
      key: "runs/1/input",
      body: chunkedBody(16, 64 * 1024),
      metadata: { contentType: "application/octet-stream" },
    });
    expect(ref.storeId).toBe("artifacts");
    expect(ref.size).toBe(16 * 64 * 1024);
    expect(ref.digest).toStartWith("SHA-256=");

    const stored = await artifacts.get(ref);
    expect(stored).not.toBeNull();
    expect(stored!.metadata.contentType).toBe("application/octet-stream");
    expect(await readAll(stored!.body)).toBe(ref.size);
  });

  test("maxObjectBytes aborts the streaming put explicitly", async () => {
    const artifacts = store();
    await expect(
      artifacts.put({ key: "too-large", body: chunkedBody(128, 64 * 1024) }), // 8 MiB > 4 MiB
    ).rejects.toBeInstanceOf(ObjectTooLargeError);
    expect(await artifacts.info({ key: "too-large" })).toBeNull();
  });

  test("a replaced object no longer resolves through the old ref", async () => {
    const artifacts = store();
    const original = await artifacts.put({ key: "replace-me", body: chunkedBody(2, 1_024, 1) });
    const replaced = await artifacts.put({ key: "replace-me", body: chunkedBody(3, 1_024, 2) });
    expect(await artifacts.get(original)).toBeNull();
    const current = await artifacts.get(replaced);
    expect(current).not.toBeNull();
    expect(await readAll(current!.body)).toBe(3 * 1_024);
  });

  test("refs are store-bound; unknown keys yield null", async () => {
    const artifacts = store();
    const other = sync.objectStore({
      id: "other",
      retention: { maxAgeMs: 60_000, maxBytes: 1024 * 1024 },
      maxObjectBytes: 1024,
    });
    const ref = await artifacts.put({ key: "bound", body: chunkedBody(1, 128) });
    await expect(other.get(ref)).rejects.toBeInstanceOf(InvalidNameError);
    expect(await artifacts.info({ key: "missing" })).toBeNull();
  });

  test("list and delete are tenant- and prefix-scoped", async () => {
    const artifacts = store();
    await artifacts.put({ key: "exp/a", body: chunkedBody(1, 64) });
    await artifacts.put({ key: "exp/b", body: chunkedBody(1, 64) });
    await artifacts.put({ key: "imp/c", body: chunkedBody(1, 64) });
    await artifacts.put({ key: "exp/tenant", tenantId: "acme", body: chunkedBody(1, 64) });

    const keys: string[] = [];
    for await (const entry of artifacts.list({ prefix: "exp/" })) keys.push(entry.ref.key);
    expect(keys.toSorted()).toEqual(["exp/a", "exp/b"]);

    const acme: string[] = [];
    for await (const entry of artifacts.list({ tenantId: "acme" })) acme.push(entry.ref.key);
    expect(acme).toEqual(["exp/tenant"]);

    expect(await artifacts.delete({ key: "exp/a" })).toBe(true);
    expect(await artifacts.delete({ key: "exp/a" })).toBe(false);
    const after: string[] = [];
    for await (const entry of artifacts.list({ prefix: "exp/" })) after.push(entry.ref.key);
    expect(after).toEqual(["exp/b"]);
  });

  test("watch reports puts and deletes", async () => {
    const artifacts = store();
    const events: string[] = [];
    const controller = new AbortController();
    (async () => {
      for await (const event of artifacts.watch({ signal: controller.signal })) {
        events.push(event.type === "put" ? `put:${event.object.ref.key}` : `delete:${event.key}`);
      }
    })();
    await Bun.sleep(250);
    await artifacts.put({ key: "watched", body: chunkedBody(1, 64) });
    await artifacts.delete({ key: "watched" });
    await waitFor(() => events.length >= 2);
    controller.abort();
    expect(events).toContain("put:watched");
    expect(events).toContain("delete:watched");
  });

  test("bucket retention expires orphaned objects", async () => {
    const shortLived = sync.objectStore({
      id: "short",
      retention: { maxAgeMs: 1_000, maxBytes: 1024 * 1024 },
      maxObjectBytes: 64 * 1024,
    });
    await shortLived.put({ key: "orphan", body: chunkedBody(1, 128) });
    expect(await shortLived.info({ key: "orphan" })).not.toBeNull();
    await Bun.sleep(2_500);
    expect(await shortLived.info({ key: "orphan" })).toBeNull();
  }, 15_000);
});
