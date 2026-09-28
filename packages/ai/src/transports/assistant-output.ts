import type { Api, AssistantMessage, Model, Usage } from "@openclaw/llm-core";
import { createZeroUsage } from "../utils/usage.js";

export function createAssistantOutput(
  model: Pick<Model, "api" | "provider" | "id">,
  api: Api = model.api,
): AssistantMessage {
  return {
    role: "assistant",
    content: [],
    api,
    provider: model.provider,
    model: model.id,
    usage: createEmptyTransportUsage(),
    stopReason: "stop",
    timestamp: Date.now(),
  };
}

export function createEmptyTransportUsage(): Usage {
  return createZeroUsage();
}
