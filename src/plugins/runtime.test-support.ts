import type { PluginRegistry } from "./registry-types.js";
import "./runtime.js";

type PluginRuntimeTestApi = {
  clearActivePluginRegistry(previousRegistry?: PluginRegistry | null): Promise<void>;
};

export function clearActivePluginRegistry(previousRegistry?: PluginRegistry | null): Promise<void> {
  const api = (globalThis as Record<PropertyKey, unknown>)[
    Symbol.for("openclaw.pluginRuntimeTestApi")
  ] as PluginRuntimeTestApi | undefined;
  if (!api) {
    throw new Error("Plugin runtime cleanup test access is only available in a test worker");
  }
  return api.clearActivePluginRegistry(previousRegistry);
}
