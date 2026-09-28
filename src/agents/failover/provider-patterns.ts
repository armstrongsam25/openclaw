import { classifyProviderFailoverSignalWithPlugin } from "../../plugins/provider-failover.js";
import type { FailoverReason, ProviderSpecificErrorContext } from "./signal.js";

export function classifyProviderPluginError(
  context: ProviderSpecificErrorContext,
): FailoverReason | null {
  const { providerPlugin, ...providerContext } = context;
  // Presentation has no provider owner; explicit absence must not trigger discovery.
  if (providerPlugin === null) {
    return null;
  }
  if (providerPlugin) {
    const ownedContext = { ...providerContext, provider: providerPlugin.id };
    if (providerPlugin.matchesContextOverflowError?.(ownedContext)) {
      return "context_overflow";
    }
    return providerPlugin.classifyFailoverReason?.(ownedContext) ?? null;
  }
  return (
    classifyProviderFailoverSignalWithPlugin({
      provider: context.provider,
      context: providerContext,
    }) ?? null
  );
}
