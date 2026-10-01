import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build as esbuild } from "esbuild";
import packageJson from "../../package.json" with { type: "json" };
import { canonicalMemoryTestSupportModuleUrl } from "./doctor-session-canonical-keys.memory.test-support.js";

export async function buildCanonicalSessionRepairChild(bundleDir: string): Promise<string> {
  for (const schema of ["openclaw-agent-schema.sql", "openclaw-state-schema.sql"]) {
    fs.copyFileSync(path.join(process.cwd(), "src/state", schema), path.join(bundleDir, schema));
  }
  await esbuild({
    bundle: true,
    entryPoints: { child: fileURLToPath(canonicalMemoryTestSupportModuleUrl) },
    format: "esm",
    // Keep generated source out of the heap budget, retaining runtime dispatch names.
    minify: true,
    keepNames: true,
    outdir: bundleDir,
    outExtension: { ".js": ".mjs" },
    // Unused lazy provider imports must not consume the memory proof's child heap.
    splitting: true,
    external: Object.entries(packageJson.dependencies)
      .filter(([, version]) => !version.startsWith("workspace:"))
      .map(([name]) => name),
    platform: "node",
    target: "node22",
  });
  return path.join(bundleDir, "child.mjs");
}
