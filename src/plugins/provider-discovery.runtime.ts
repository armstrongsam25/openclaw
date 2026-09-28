import { normalizeProviderId } from "@openclaw/model-catalog-core/provider-id";
import {
  hasProviderCatalogHook,
  planPluginDiscoveryRuntime,
} from "./provider-discovery-plan.runtime.js";
import type { ResolveRuntimePluginDiscoveryProvidersParams } from "./provider-discovery.js";
import { resolvePluginProvidersCore } from "./providers.runtime.js";
import type { ProviderPlugin } from "./types.js";

export { planPluginDiscoveryRuntime } from "./provider-discovery-plan.runtime.js";

export function resolvePluginDiscoveryProvidersRuntime(
  params: ResolveRuntimePluginDiscoveryProvidersParams,
): ProviderPlugin[] {
  const plan = planPluginDiscoveryRuntime(params);
  if (plan.kind === "entries") {
    return plan.providers;
  }
  const fullProviders = resolvePluginProvidersCore({
    ...params,
    env: params.env ?? process.env,
    ...(plan.pluginIds ? { onlyPluginIds: plan.pluginIds } : {}),
  });
  const providers = [...plan.providers];
  const entryIndices = new Map(
    providers.map((provider, index) => [normalizeProviderId(provider.id), index]),
  );
  for (const provider of fullProviders) {
    const index = entryIndices.get(normalizeProviderId(provider.id));
    const entry = index === undefined ? undefined : providers[index];
    if (index !== undefined && entry && entry.pluginId === provider.pluginId) {
      // Runtime owns catalog replacement and its auth pair. A lightweight-only
      // auth contribution survives without keeping a superseded catalog hook.
      providers[index] =
        provider.resolveSyntheticAuth || provider.prepareSyntheticAuth
          ? provider
          : {
              ...provider,
              resolveSyntheticAuth: entry.resolveSyntheticAuth,
              prepareSyntheticAuth: entry.prepareSyntheticAuth,
            };
    } else if (hasProviderCatalogHook(provider)) {
      providers.push(provider);
    }
  }
  return providers;
}
