import { readCapabilityConsentErrorDetails } from "../../packages/gateway-protocol/src/capability-consent-error-details.js";
import {
  GATEWAY_CLIENT_MODES,
  GATEWAY_CLIENT_NAMES,
} from "../../packages/gateway-protocol/src/client-info.js";
import type { PluginsInspectResult } from "../../packages/gateway-protocol/src/schema/plugins.js";
import { callGateway, isGatewayClientRequestError } from "../gateway/call.js";
import { sleepWithAbort } from "../infra/backoff.js";
import type { PluginCapabilityConsentHandler } from "../plugins/capability-consent.js";
import { createDeferredCore } from "../shared/deferred.js";
import { sleep } from "../utils/sleep.js";
import type { PluginLifecycleGateway } from "./plugins-lifecycle-client.js";
import { registerSignalExitGate } from "./signal-exit-barrier.js";

export function createPluginLifecycleGatewayRuntime(port: number): PluginLifecycleGateway {
  const request = async <T>(method: string, params: Record<string, unknown>): Promise<T> => {
    const deadline = Date.now() + 600_000;
    const controller =
      method === "plugins.reload" && params.waitForDrain === true
        ? new AbortController()
        : undefined;
    const finished = createDeferredCore();
    const releaseExitGate = controller
      ? registerSignalExitGate(finished.promise, () => controller.abort())
      : undefined;
    try {
      for (;;) {
        try {
          controller?.signal.throwIfAborted();
          return await callGateway<T>({
            method,
            params,
            localPortOverride: port,
            ignoreEnvUrlOverride: true,
            requiredMethods: [...new Set([method, "plugins.reload"])],
            timeoutMs: controller ? null : Math.max(1, deadline - Date.now()),
            ...(controller ? { signal: controller.signal } : {}),
            scopes: ["operator.admin"],
            clientName: GATEWAY_CLIENT_NAMES.CLI,
            mode: GATEWAY_CLIENT_MODES.CLI,
          });
        } catch (error) {
          // Lifecycle admission rejects before writes so a config reload can drain
          // the request. Honor that response outside the Gateway's admission scope.
          if (
            !isGatewayClientRequestError(error) ||
            error.gatewayCode !== "UNAVAILABLE" ||
            !error.retryable ||
            error.retryAfterMs === undefined ||
            error.retryAfterMs >= deadline - Date.now()
          ) {
            throw error;
          }
          if (controller) {
            await sleepWithAbort(error.retryAfterMs, controller.signal);
          } else {
            await sleep(error.retryAfterMs);
          }
          if (Date.now() >= deadline) {
            throw error;
          }
        }
      }
    } finally {
      finished.resolve();
      releaseExitGate?.();
    }
  };
  return async <T>(
    method: string,
    params: Record<string, unknown>,
    onCapabilityConsent?: PluginCapabilityConsentHandler,
  ) => {
    const reviewedPluginIds = new Set<string>();
    let requestParams = params;
    for (;;) {
      try {
        return await request<T>(method, requestParams);
      } catch (error) {
        const consent = readCapabilityConsentErrorDetails(
          error instanceof Error && "details" in error ? error.details : undefined,
        );
        if (!consent || !onCapabilityConsent || reviewedPluginIds.has(consent.pluginId)) {
          throw error;
        }
        const { plugin, ...inspection } = await request<PluginsInspectResult>("plugins.inspect", {
          pluginId: consent.pluginId,
        });
        const acknowledgeCapabilities = await onCapabilityConsent({
          ...inspection,
          pluginId: plugin.id,
          name: plugin.name,
          ...(plugin.version ? { version: plugin.version } : {}),
          ...(consent.widened ? { widened: consent.widened } : {}),
          ...(consent.acceptedAt ? { acceptedAt: consent.acceptedAt } : {}),
        });
        if (!acknowledgeCapabilities) {
          throw error;
        }
        // A batch can need consent for each package. Never retry a transport failure
        // or a repeated rejection after acknowledging the same plugin.
        reviewedPluginIds.add(consent.pluginId);
        requestParams = { ...params, acknowledgeCapabilities };
      }
    }
  };
}
