import { truncateUtf16Safe } from "@openclaw/normalization-core/utf16-slice";
import { createSubsystemLogger } from "../logging/subsystem.js";

const log = createSubsystemLogger("env");

export function logAcceptedEnvValue(
  option: { key: string; description: string; redact?: boolean },
  value: string,
): void {
  const singleLine = value.replace(/\s+/g, " ").trim();
  const formatted = option.redact
    ? "<redacted>"
    : singleLine.length <= 160
      ? singleLine
      : `${truncateUtf16Safe(singleLine, 160)}…`;
  log.info(`env: ${option.key}=${formatted} (${option.description})`);
}
