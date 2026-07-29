import { expect, test } from "bun:test";

test("recurring queue maintenance never enumerates the full deliveries hash", async () => {
  const source = await Bun.file(new URL("../src/queue.ts", import.meta.url)).text();
  const maintenance = source.slice(
    source.indexOf("const MAINTENANCE_SCRIPT"),
    source.indexOf("const CLAIM_SCRIPT"),
  );

  expect(maintenance).not.toContain('redis.call("HVALS"');
  expect(maintenance).toContain('redis.call("HSCAN"');
  expect(maintenance).toContain('"COUNT", tostring(batch)');
});
