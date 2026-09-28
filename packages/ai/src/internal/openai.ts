import { createLazyStream } from "../utils/lazy-stream.js";

export * from "../providers/azure-deployment-map.js";
export * from "../providers/azure-openai-responses-client-compat.js";
export type { OpenAICompletionsOptions } from "../provider-options.js";
export { convertMessages } from "../openai-completions-messages.js";
export * from "../providers/openai-prompt-cache.js";
export * from "../providers/openai-reasoning-effort.js";
export type { OpenAIResponsesOptions } from "../providers/openai-responses.js";
export * from "../providers/openai-responses-stream-compat.js";
export * from "../providers/openai-responses-terminal-usage.js";
export * from "../providers/openai-responses-tool-call-tracker.js";
export * from "../providers/openai-stop-reason.js";
export { hasResponsesWebSearchTool } from "../providers/openai-web-search-tools.js";
export {
  projectOpenAITools,
  reconcileOpenAICompletionsToolChoice,
  reconcileOpenAIResponsesToolChoice,
  type OpenAICompletionsToolChoice,
  type OpenAIToolProjection,
} from "../providers/openai-tool-projection.js";
export {
  codeModeToolSurfaceObserver,
  reasoningTagTextPolicy,
  type CodeModeToolSurfaceObservation,
} from "../provider-options.js";
export { responsesPromptObserver } from "../transports/openai-responses-contracts.js";
export type { ResponsesPromptObservation } from "../transports/openai-responses-contracts.js";

export { responsesRequestLifecycle } from "../transports/openai-responses-request-lifecycle.js";
export { bindResponsesInputMessage } from "../transports/openai-responses-replay-messages-internal.js";
export {
  encodedModelRequestBodyStream,
  modelRequestBodyState,
  serializeModelRequestBody,
} from "../transports/model-request-body.js";

export const streamOpenAICompletions: typeof import("../providers/openai-completions.js").streamOpenAICompletions =
  createLazyStream(
    () => import("../providers/openai-completions.js"),
    (runtime) => runtime.streamOpenAICompletions,
  );

export const streamSimpleOpenAICompletions: typeof import("../providers/openai-completions.js").streamSimpleOpenAICompletions =
  createLazyStream(
    () => import("../providers/openai-completions.js"),
    (runtime) => runtime.streamSimpleOpenAICompletions,
  );

export const streamOpenAIResponses: typeof import("../providers/openai-responses.js").streamOpenAIResponses =
  createLazyStream(
    () => import("../providers/openai-responses.js"),
    (runtime) => runtime.streamOpenAIResponses,
  );

export const streamSimpleOpenAIResponses: typeof import("../providers/openai-responses.js").streamSimpleOpenAIResponses =
  createLazyStream(
    () => import("../providers/openai-responses.js"),
    (runtime) => runtime.streamSimpleOpenAIResponses,
  );
