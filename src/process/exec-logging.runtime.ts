import { danger, shouldLogVerbose } from "../globals.js";
import { logDebug, logError } from "../logger.js";

export function logExecOutput(stdout: string, stderr: string): void {
  if (shouldLogVerbose()) {
    if (stdout.trim()) {
      logDebug(stdout.trim());
    }
    if (stderr.trim()) {
      logError(stderr.trim());
    }
  }
}

export function logExecFailure(command: string): void {
  if (shouldLogVerbose()) {
    logError(danger(`Command failed: ${command}`));
  }
}
