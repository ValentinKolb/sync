import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const directories: string[] = [];

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

test("rewrites static, side-effect, and dynamic relative imports exactly once", async () => {
  const directory = await mkdtemp(join(tmpdir(), "sync-esm-rewrite-"));
  directories.push(directory);
  const fixture = join(directory, "fixture.js");
  await writeFile(
    fixture,
    [
      'import value from "./value";',
      'export { value } from "../shared";',
      'import "./setup";',
      'const lazy = import("./lazy");',
      'import json from "./data.json";',
      'import ready from "./ready.js";',
      'import external from "external";',
    ].join("\n"),
  );

  const run = Bun.spawnSync({
    cmd: ["node", "scripts/rewrite-esm-imports.mjs", directory],
    cwd: import.meta.dir + "/..",
  });
  expect(run.exitCode).toBe(0);

  expect(await readFile(fixture, "utf8")).toBe(
    [
      'import value from "./value.js";',
      'export { value } from "../shared.js";',
      'import "./setup.js";',
      'const lazy = import("./lazy.js");',
      'import json from "./data.json";',
      'import ready from "./ready.js";',
      'import external from "external";',
    ].join("\n"),
  );
});
