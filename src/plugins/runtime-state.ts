import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import type { PluginHostCleanupResult } from "./host-hook-cleanup.types.js";
import type { PluginInstanceAdmission } from "./plugin-instance.types.js";
import type { PluginRuntimeCloseRetainedError } from "./runtime-close-error.js";
import { PLUGIN_REGISTRY_STATE } from "./runtime-state-key.js";
// Stores plugin runtime registry state for the current process lifecycle.
export { getActivePluginRegistryWorkspaceDirFromStateCore as getActivePluginRegistryWorkspaceDirFromState } from "./runtime-workspace-state.js";

export { PLUGIN_REGISTRY_STATE };

type PluginRegistry = import("./registry-types.js").PluginRegistry;
type MemoryCapabilityRegistrar = import("./types.js").OpenClawPluginApi["registerMemoryCapability"];

export type RegistryState = {
  activeRegistry: PluginRegistry | null;
  activeVersion: number;
  registryVersions?: WeakMap<PluginRegistry, number>;
  agentEventBridgeUnsubscribe?: (() => void) | undefined;
  key: string | null;
  workspaceDir: string | null;
  runtimeSubagentMode: "default" | "explicit" | "gateway-bindable";
  importedPluginIds: Set<string>;
  registrationContext?: {
    registry: PluginRegistry;
    pluginId: string;
    instance?: PluginInstanceAdmission;
    registerMemoryCapability?: MemoryCapabilityRegistrar;
  };
  commandRegistryClearTail?: Promise<void>;
  commandRegistryClearRegistries?: Map<PluginRegistry, number>;
  retiredRegistryCleanups?: Map<
    Promise<void>,
    { registry: PluginRegistry; work: import("../shared/async-work-scope.js").AsyncWorkScope }
  >;
};

type GlobalRegistryState = typeof globalThis & {
  [PLUGIN_REGISTRY_STATE]?: RegistryState;
};

export function getPluginRegistryState(): RegistryState | undefined {
  return (globalThis as GlobalRegistryState)[PLUGIN_REGISTRY_STATE];
}

/** Publication provenance follows the selected registry, including retained request snapshots. */
export function getPluginRegistryVersion(registry: PluginRegistry | null): number | undefined {
  return registry ? getPluginRegistryState()?.registryVersions?.get(registry) : undefined;
}

/** Policy reads the process-active registry, independently of request or registration scopes. */
export function getActivePluginGatewayNodePolicyRegistry(): PluginRegistry | null {
  return getPluginRegistryState()?.activeRegistry ?? null;
}

export type PluginRegistrySnapshot = Pick<
  RegistryState,
  "activeRegistry" | "key" | "runtimeSubagentMode" | "workspaceDir"
>;
export type RegistryOwnerClose = {
  promise: Promise<{
    memoryErrors: readonly unknown[];
    pluginFailures: PluginHostCleanupResult["failures"];
  }>;
  failure?: PluginRuntimeCloseRetainedError;
};
export type RegistryOwner = PluginRegistrySnapshot & {
  activeRegistry: PluginRegistry;
  closing?: RegistryOwnerClose;
};
export const registryOwners = resolveGlobalSingleton(
  Symbol.for("openclaw.pluginRegistryOwners"),
  () => new Set<RegistryOwner>(),
);
