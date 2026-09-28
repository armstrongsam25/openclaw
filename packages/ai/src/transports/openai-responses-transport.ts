import { createLazyStream } from "../utils/lazy-stream.js";

export { requestPreparedOpenAIResponsesCompaction } from "./openai-responses-compact-request.js";
export { captureOpenAIResponsesCompaction } from "./openai-responses-compaction-replay.js";

export function createOpenAIResponsesTransportStreamFn() {
  return createLazyStream(
    () => import("./openai-responses-client.js"),
    (runtime) => runtime.createOpenAIResponsesTransportStreamFn(),
  );
}

export function createAzureOpenAIResponsesTransportStreamFn() {
  return createLazyStream(
    () => import("./openai-responses-client.js"),
    (runtime) => runtime.createAzureOpenAIResponsesTransportStreamFn(),
  );
}
