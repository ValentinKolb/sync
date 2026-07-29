import { expect, test } from "bun:test";

test("recurring queue maintenance incrementally scans deliveries instead of using HVALS", async () => {
  const source = await Bun.file(new URL("../src/queue.ts", import.meta.url)).text();
  const maintenance = source.slice(
    source.indexOf("const MAINTENANCE_SCRIPT"),
    source.indexOf("const CLAIM_SCRIPT"),
  );

  expect(maintenance).not.toContain('redis.call("HVALS"');
  expect(maintenance).toContain('redis.call("HSCAN"');
  expect(maintenance).toContain('"COUNT", tostring(batch)');
  expect(maintenance).not.toContain('"activeOffset"');
  expect(maintenance).toContain('redis.call("RPOPLPUSH", KEYS[6], KEYS[6])');
  expect(maintenance).toContain("observedAttempt ~= (tonumber(message.attempt) or 0)");
  expect(maintenance).toContain("observedGeneration + 2 <= generation");
  expect(maintenance).toContain("local orphanGraceMs = tonumber(ARGV[6])");
  expect(maintenance).toContain('string.match(candidate, "^[^:]+:[^:]+:(.+)$")');
  expect(maintenance).toContain("now - observedAt >= orphanGraceMs");
});
