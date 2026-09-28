import fs from "node:fs/promises";
import { runBestEffortCleanup } from "./non-fatal-cleanup.js";

// Disposable artifacts are advisory; a failed warning must preserve the owning operation's result.
export function removeTemporaryArtifacts(
  directory: string,
  owner: string,
  onError?: (error: unknown) => void,
): Promise<void> {
  return runBestEffortCleanup({
    cleanup: () => fs.rm(directory, { recursive: true, force: true }),
    onError:
      onError ??
      (async (error) => {
        const { reportTemporaryArtifactCleanupFailure } =
          await import("./temp-artifact-cleanup.runtime.js");
        reportTemporaryArtifactCleanupFailure(directory, owner, error);
      }),
  });
}
