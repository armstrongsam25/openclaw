import { createLazyStream } from "../utils/lazy-stream.js";

export type {
  AnthropicEffort,
  AnthropicOptions,
  AnthropicThinkingDisplay,
} from "../provider-options.js";
export * from "../providers/anthropic-auth-headers.js";
export * from "../providers/anthropic-model-contract.js";
export * from "../providers/anthropic-refusal.js";
export * from "../providers/anthropic-server-fallback.js";
export * from "../providers/anthropic-thinking-replay.js";
export * from "../providers/anthropic-tool-projection.js";
export * from "../providers/anthropic-usage.js";
export { resolveAnthropicServerCompactionPlan } from "../transports/anthropic-payload-policy.js";

export const streamAnthropic: typeof import("../providers/anthropic.js").streamAnthropic =
  createLazyStream(
    () => import("../providers/anthropic.js"),
    (runtime) => runtime.streamAnthropic,
  );

export const streamSimpleAnthropic: typeof import("../providers/anthropic.js").streamSimpleAnthropic =
  createLazyStream(
    () => import("../providers/anthropic.js"),
    (runtime) => runtime.streamSimpleAnthropic,
  );
