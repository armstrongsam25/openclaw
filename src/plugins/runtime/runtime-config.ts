// Runtime config helpers expose scoped OpenClaw config reads to plugin runtimes.
import { getRuntimeConfig } from "../../config/io.runtime.js";
import { mutateConfigFile, replaceConfigFile } from "../../config/mutate.js";
import type { PluginRuntime } from "./types.js";

export function createRuntimeConfig(): PluginRuntime["config"] {
  return {
    current: getRuntimeConfig,
    mutateConfigFile,
    replaceConfigFile,
  };
}
