import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { detectMime, normalizeMimeType } from "@openclaw/media-core/mime";
import {
  GATEWAY_CLIENT_MODES,
  GATEWAY_CLIENT_NAMES,
} from "../../../packages/gateway-protocol/src/client-info.js";
import { DEFAULT_PROVIDER } from "../../agents/defaults.js";
import { completeWithPreparedSimpleCompletionModel } from "../../agents/simple-completion-execution.js";
import type { ThinkLevel } from "../../auto-reply/thinking.shared.js";
import { callGateway, randomIdempotencyKey } from "../../gateway/call.js";
import { ADMIN_SCOPE } from "../../gateway/operator-scopes.js";
import { AsyncWorkScope, captureAsyncWorkTracker } from "../../shared/async-work-scope.js";
import { createDeferredCore } from "../../shared/deferred.js";
import type { CapabilityEnvelope, CapabilityTransport } from "./metadata.js";

const LOCAL_MODEL_RUN_SYSTEM_PROMPT = "You are a personal assistant running inside OpenClaw.";
const HEIC_MODEL_RUN_MIMES = new Set([
  "image/heic",
  "image/heic-sequence",
  "image/heif",
  "image/heif-sequence",
]);

function collectModelRunText(content: Array<{ type: string; text?: string }>): string {
  return content
    .map((block) => (block.type === "text" && typeof block.text === "string" ? block.text : ""))
    .join("")
    .trim();
}

type ModelRunImageFile = {
  path: string;
  fileName: string;
  mimeType: string;
  data: string;
};

async function readModelRunImageFiles(files: string[] | undefined): Promise<ModelRunImageFile[]> {
  if (!files || files.length === 0) {
    return [];
  }
  return await Promise.all(
    files.map(async (filePath) => {
      const resolvedPath = path.resolve(filePath);
      const buffer = await fs.readFile(resolvedPath);
      const mimeType = normalizeMimeType(
        await detectMime({
          buffer,
          filePath: resolvedPath,
        }),
      );
      if (!mimeType?.startsWith("image/")) {
        throw new Error(
          `Unsupported --file for model run: ${resolvedPath}. Only image files are supported; use infer audio transcribe for audio files.`,
        );
      }
      const isHeic = HEIC_MODEL_RUN_MIMES.has(mimeType);
      const imageBuffer = isHeic
        ? await (await import("../../media/media-services.js")).convertHeicToJpeg(buffer)
        : buffer;
      return {
        path: resolvedPath,
        fileName: path.basename(resolvedPath),
        mimeType: isHeic ? "image/jpeg" : mimeType,
        data: imageBuffer.toString("base64"),
      };
    }),
  );
}

export async function runModelRun(params: {
  prompt: string;
  files?: string[];
  model?: string;
  thinking?: ThinkLevel;
  transport: CapabilityTransport;
  agent?: string;
}) {
  const {
    requireProviderModelOverride,
    resolveCapabilityProviderAgentId,
    resolveLocalCapabilityRuntimeConfig,
  } = await import("./shared.js");
  const { getModelsCommandSecretTargetIds } = await import("../command-secret-targets.js");
  const { getRuntimeConfig } = await import("../../config/config.js");
  const { canonicalizeCaseOnlyCatalogModelRef } = await import("../../agents/model-selection.js");
  const { readPreparedModelCatalog } = await import("../../agents/prepared-model-catalog.js");
  const explicitModelOverride = requireProviderModelOverride(params.model);
  const cfg =
    params.transport === "local"
      ? await resolveLocalCapabilityRuntimeConfig({
          commandName: "infer model run",
          targetIds: getModelsCommandSecretTargetIds(),
        })
      : getRuntimeConfig();
  const agentId = resolveCapabilityProviderAgentId(cfg, params.agent, "infer model run");
  const modelRef = await canonicalizeCaseOnlyCatalogModelRef({
    raw: params.model,
    cfg,
    defaultProvider: DEFAULT_PROVIDER,
    loadCatalog: () => readPreparedModelCatalog({ config: cfg, agentId, readOnly: true }),
    preserveAuthProfile: params.transport === "local",
  });
  const hasExplicitProviderModelOverride = Boolean(explicitModelOverride);
  const imageFiles = await readModelRunImageFiles(params.files);
  const messageContent =
    imageFiles.length > 0
      ? [
          { type: "text" as const, text: params.prompt },
          ...imageFiles.map((image) => ({
            type: "image" as const,
            data: image.data,
            mimeType: image.mimeType,
          })),
        ]
      : params.prompt;
  if (params.transport === "local") {
    const { acquireSimpleCompletionModelForAgent } =
      await import("../../agents/simple-completion-runtime.js");
    const callerResult = createDeferredCore<CapabilityEnvelope>();
    const trackOwner = captureAsyncWorkTracker();
    // Command completion can precede response callbacks and cancellation drainage.
    void trackOwner(async () => {
      const { prepareLocalCapabilityAccountSecrets } = await import("./local-account-secrets.js");
      await prepareLocalCapabilityAccountSecrets({ cfg, agentId });
      const prepared = await acquireSimpleCompletionModelForAgent({
        cfg,
        agentId,
        modelRef,
        allowMissingApiKeyModes: ["aws-sdk"],
        ...(hasExplicitProviderModelOverride ? { allowBundledStaticCatalogFallback: true } : {}),
        skipAgentDiscovery: true,
      });
      if ("error" in prepared) {
        throw new Error(prepared.error);
      }
      const work = new AsyncWorkScope();
      try {
        callerResult.resolve(
          await work.track(async () => {
            if (prepared.selection.provider === "codex") {
              throw new Error(
                'The codex provider is served by the Codex app-server agent runtime, not the local simple-completion transport. Use an openai/<model> ref with provider/model agentRuntime.id: "codex", run through the gateway, or use /codex commands.',
              );
            }
            const localModelRunSystemPrompt =
              prepared.model.api === "openai-chatgpt-responses"
                ? LOCAL_MODEL_RUN_SYSTEM_PROMPT
                : undefined;
            const result = await completeWithPreparedSimpleCompletionModel({
              model: prepared.model,
              auth: prepared.auth,
              cfg,
              context: {
                ...(localModelRunSystemPrompt ? { systemPrompt: localModelRunSystemPrompt } : {}),
                messages: [
                  {
                    role: "user",
                    content: messageContent,
                    timestamp: Date.now(),
                  },
                ],
              },
              options: {
                maxTokens:
                  typeof prepared.model.maxTokens === "number" &&
                  Number.isFinite(prepared.model.maxTokens)
                    ? prepared.model.maxTokens
                    : undefined,
                ...(params.thinking ? { reasoning: params.thinking } : {}),
              },
            });
            const text = collectModelRunText(result.content);
            if (!text) {
              const providerErrorMessage = result.errorMessage;
              const detail =
                typeof providerErrorMessage === "string" && providerErrorMessage.trim()
                  ? `: ${providerErrorMessage.trim()}`
                  : "";
              throw new Error(
                `No text output returned for provider "${prepared.selection.provider}" model "${prepared.selection.modelId}"${detail}.`,
              );
            }
            return {
              ok: true,
              capability: "model.run",
              transport: "local" as const,
              provider: prepared.selection.provider,
              model: prepared.selection.modelId,
              attempts: [],
              ...(imageFiles.length > 0
                ? {
                    inputs: imageFiles.map((image) => ({
                      path: image.path,
                      mimeType: image.mimeType,
                    })),
                  }
                : {}),
              outputs: [
                {
                  text,
                  mediaUrl: null,
                },
              ],
            } satisfies CapabilityEnvelope;
          }),
        );
      } catch (error) {
        callerResult.reject(error);
      } finally {
        await work.drain();
        await prepared[Symbol.asyncDispose]();
      }
    }).catch((error: unknown) => callerResult.reject(error));
    return await callerResult.promise;
  }

  const { buildExplicitSessionIdSessionKey } = await import("../../agents/command/session.js");
  const { provider, model } = requireProviderModelOverride(modelRef) ?? {};
  // Provider/model overrides require trusted-operator scope. Use the backend
  // shared-secret lane so local gateway smokes do not depend on paired CLI device scopes.
  const hasModelOverride = Boolean(provider || model);
  const sessionId = `model-run-${randomUUID()}`;
  const sessionKey = buildExplicitSessionIdSessionKey({ agentId, sessionId });
  const response: {
    result?: {
      payloads?: Array<{ text?: string; mediaUrl?: string | null; mediaUrls?: string[] }>;
      meta?: {
        agentMeta?: {
          provider?: string;
          model?: string;
          fallbackAttempts?: Array<Record<string, unknown>>;
        };
      };
    };
  } = await callGateway({
    method: "agent",
    params: {
      agentId,
      sessionId,
      sessionKey,
      message: params.prompt,
      attachments:
        imageFiles.length > 0
          ? imageFiles.map((image) => ({
              type: "image",
              fileName: image.fileName,
              mimeType: image.mimeType,
              content: image.data,
            }))
          : undefined,
      provider,
      model,
      ...(params.thinking ? { thinking: params.thinking } : {}),
      modelRun: true,
      promptMode: "none",
      cleanupBundleMcpOnRunEnd: true,
      idempotencyKey: randomIdempotencyKey(),
    },
    expectFinal: true,
    timeoutMs: 120_000,
    clientName: hasModelOverride ? GATEWAY_CLIENT_NAMES.GATEWAY_CLIENT : GATEWAY_CLIENT_NAMES.CLI,
    mode: hasModelOverride ? GATEWAY_CLIENT_MODES.BACKEND : GATEWAY_CLIENT_MODES.CLI,
    ...(hasModelOverride ? { scopes: [ADMIN_SCOPE] } : {}),
  });
  return {
    ok: true,
    capability: "model.run",
    transport: "gateway" as const,
    provider: response?.result?.meta?.agentMeta?.provider,
    model: response?.result?.meta?.agentMeta?.model,
    attempts: response?.result?.meta?.agentMeta?.fallbackAttempts ?? [],
    outputs: (response?.result?.payloads ?? []).map((payload) => ({
      text: payload.text,
      mediaUrl: payload.mediaUrl,
      mediaUrls: payload.mediaUrls,
    })),
    ...(imageFiles.length > 0
      ? {
          inputs: imageFiles.map((image) => ({
            path: image.path,
            mimeType: image.mimeType,
          })),
        }
      : {}),
  } satisfies CapabilityEnvelope;
}
