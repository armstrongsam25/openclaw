import { describe, expect, it, vi } from "vitest";
import { streamAnthropic } from "../internal/anthropic.js";
import { streamOpenAICompletions, streamOpenAIResponses } from "../internal/openai.js";
import {
  createAzureOpenAIResponsesTransportStreamFn,
  createOpenAICompletionsTransportStreamFn,
  createOpenAIResponsesTransportStreamFn,
  stripCompactionReplayCheckpointInPlace,
} from "../transports.js";
import type { Model } from "../types.js";

const sdk = vi.hoisted(() => ({ loads: 0, anthropicLoads: 0 }));
vi.mock("@anthropic-ai/sdk", () => {
  sdk.anthropicLoads++;
  return { default: vi.fn() };
});
vi.mock("openai", () => {
  sdk.loads++;
  const OpenAI = vi.fn(function () {
    throw new Error("synthetic provider construction failure");
  });
  return { default: OpenAI, AzureOpenAI: OpenAI };
});

const model: Model = {
  id: "synthetic-model",
  name: "Synthetic model",
  api: "openai-completions",
  provider: "synthetic-provider",
  baseUrl: "https://provider.example/v1",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 4096,
  maxTokens: 1024,
};

describe("transport loading", () => {
  it("keeps replay and stream preparation SDK-free until a request uses the provider", async () => {
    const message = {
      role: "user" as const,
      content: "Synthetic request",
      timestamp: 1,
      providerReplay: undefined,
    };
    stripCompactionReplayCheckpointInPlace(message);
    const factories = [
      createOpenAICompletionsTransportStreamFn(),
      createOpenAIResponsesTransportStreamFn(),
      createAzureOpenAIResponsesTransportStreamFn(),
    ];
    expect(streamAnthropic).toBeTypeOf("function");
    expect(streamOpenAICompletions).toBeTypeOf("function");
    expect(streamOpenAIResponses).toBeTypeOf("function");
    expect(sdk).toEqual({ loads: 0, anthropicLoads: 0 });
    for (const stream of factories) {
      const result = stream(model, { messages: [message] }, { apiKey: "synthetic-unused-key" });
      expect(result).not.toBeInstanceOf(Promise);
      await expect(result.result()).resolves.toMatchObject({
        stopReason: "error",
        errorMessage: "synthetic provider construction failure",
      });
    }
    expect(sdk).toEqual({ loads: 1, anthropicLoads: 0 });
  });
});
