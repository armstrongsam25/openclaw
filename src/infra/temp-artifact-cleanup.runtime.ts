import { truncateUtf16Safe } from "@openclaw/normalization-core/utf16-slice";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { formatErrorMessage } from "./errors.js";

const log = createSubsystemLogger("infra:temp-artifacts");

export function reportTemporaryArtifactCleanupFailure(
  directory: string,
  owner: string,
  error: unknown,
): void {
  log.warn(
    truncateUtf16Safe(
      formatErrorMessage(
        `${owner} cleanup failed; files may remain in ${directory}. After the worker or session stops, check permissions and remove the retained directory: ${formatErrorMessage(error)}`,
      ),
      1_024,
    ),
  );
}
