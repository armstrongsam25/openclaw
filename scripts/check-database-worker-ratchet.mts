import { inventory } from "./database-worker-inventory.mjs";
import { isDirectRunUrl } from "./lib/direct-run.mjs";
import {
  compareRatchetCounts,
  listRatchetRenames,
  parseRatchetArgs,
  reportRatchetFailures,
  reportRatchetSuccess,
  resolveRatchetBase,
} from "./lib/shrink-ratchet.mts";

export function main(root = process.cwd(), argv = process.argv.slice(2)) {
  try {
    const args = parseRatchetArgs(argv);
    if (args.prune) {
      throw new Error("SQLite worker ratchet has no baseline to prune.");
    }
    const base = resolveRatchetBase(root, args);
    if (!base) {
      throw new Error("SQLite worker ratchet requires a Git base commit.");
    }
    const counts = (rows: ReturnType<typeof inventory>) =>
      new Map(rows.filter((row) => row.tier === "T1").map((row) => [row.file, row.calls.length]));
    const head = inventory(root, "", args.staged);
    const before = counts(inventory(root, base));
    const oldPaths = new Map(
      listRatchetRenames(root, base, args.staged, []).map(({ from, to }) => [to, from]),
    );
    const after = counts(head);
    const { increased } = compareRatchetCounts(
      after,
      new Map([...after.keys()].map((file) => [file, before.get(oldPaths.get(file) ?? file) ?? 0])),
    );
    if (
      reportRatchetFailures(
        [
          {
            title: "Main-thread SQLite T1 call counts grew:",
            entries: increased.flatMap(({ entry, allowed, current }) =>
              [`${entry}: ${allowed} -> ${current}`].concat(
                head
                  .find((row) => row.file === entry)!
                  .calls.map((call) => `${entry}:${call.line}:${call.column} ${call.primitive}`),
              ),
            ),
          },
        ],
        "Move SQL behind the owner's worker operation: docs/reference/database-schemas/worker-access.md\n" +
          "If the file only executes inside a worker, name it *.worker.ts or add it to workerModules in scripts/database-worker-inventory.mjs with caller evidence.",
      )
    ) {
      return 1;
    }
    reportRatchetSuccess("SQLite worker ratchet OK: no T1 call-count growth against " + base + ".");
    return 0;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }
}

if (isDirectRunUrl(process.argv[1], import.meta.url)) {
  process.exitCode = main();
}
