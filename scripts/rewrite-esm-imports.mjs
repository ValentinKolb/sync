import { readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const roots = process.argv.slice(2);
if (roots.length === 0) {
  throw new Error("usage: node scripts/rewrite-esm-imports.mjs <directory> [...]");
}

const withJsExtension = (specifier) =>
  /\.[cm]?js$|\.json$/.test(specifier) ? specifier : `${specifier}.js`;

const walk = async (dir) => {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      await walk(path);
      continue;
    }
    if (!entry.isFile() || (!path.endsWith(".js") && !path.endsWith(".d.ts"))) continue;

    const source = await readFile(path, "utf8");
    const rewritten = source
      .replace(
        /((?:from|import)\s+["'])(\.{1,2}\/[^"']+?)(["'])/g,
        (_match, prefix, specifier, suffix) => `${prefix}${withJsExtension(specifier)}${suffix}`,
      )
      .replace(
        /(import\(\s*["'])(\.{1,2}\/[^"']+?)(["']\s*\))/g,
        (_match, prefix, specifier, suffix) => `${prefix}${withJsExtension(specifier)}${suffix}`,
      );
    if (rewritten !== source) await writeFile(path, rewritten);
  }
};

for (const root of roots) await walk(root);
