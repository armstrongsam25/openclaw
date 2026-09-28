import process from "node:process";
import { format } from "node:util";
import type { RuntimeEnv } from "../runtime.js";
import { formatConsoleDiagnosticBlock } from "./json-console-line.js";

export function writeConsoleDiagnosticError(message: string, error?: unknown): void {
  const formatted = error === undefined ? message : format(message, error);
  process.stderr.write(
    formatConsoleDiagnosticBlock({
      level: "error",
      message: formatted.endsWith("\n") ? formatted : `${formatted}\n`,
    }),
  );
}

export function createDiagnosticRuntime(): RuntimeEnv {
  return {
    log: (...args) => console.log(...args),
    error: (...args) => writeConsoleDiagnosticError(`${format(...args)}\n`),
    exit: (code) => process.exit(code),
  };
}
