import type { PluginsReloadResult } from "../../packages/gateway-protocol/src/schema/plugins.js";
import { readActiveGatewayLockIdentity } from "../infra/gateway-lock.js";
import type { PluginCapabilityConsentHandler } from "../plugins/capability-consent.js";
import type { PluginInstallBatchReload } from "../plugins/install-runtime-batch.js";

/** Capture the local client before a Claw batch takes any package or plugin lease. */
export async function resolvePluginBatchReload(): Promise<PluginInstallBatchReload | undefined> {
  const gateway = await resolvePluginLifecycleGateway();
  return gateway
    ? async (plugins) => {
        const result = await gateway<PluginsReloadResult>("plugins.reload", {
          plugins,
        });
        if (!result.runtime) {
          throw new Error(
            "Gateway did not confirm the plugin batch runtime generation. Inspect plugin status before retrying.",
          );
        }
        return {
          ...result.runtime,
          ...(result.restartRequired ? { restartRequired: true } : {}),
          ...(result.warnings?.length ? { warnings: result.warnings } : {}),
        };
      }
    : undefined;
}

export type PluginLifecycleGateway = <T>(
  method: string,
  params: Record<string, unknown>,
  onCapabilityConsent?: PluginCapabilityConsentHandler,
) => Promise<T>;

/** Select the local runtime owner before acquiring a lease the Gateway also needs. */
export async function resolvePluginLifecycleGateway(): Promise<PluginLifecycleGateway | null> {
  const owner = await readActiveGatewayLockIdentity({ requireInspection: true });
  if (!owner) {
    return null;
  }
  const { createPluginLifecycleGatewayRuntime } =
    await import("./plugins-lifecycle-client.runtime.js");
  return createPluginLifecycleGatewayRuntime(owner.port);
}
