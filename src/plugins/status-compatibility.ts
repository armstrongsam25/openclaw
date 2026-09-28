/** Lightweight projection and formatting for plugin compatibility notices. */
import type { OpenClawConfig } from "../config/types.openclaw.js";
import type { PluginCompatCode } from "./compat/registry.js";
import { buildPluginShapeSummary, type PluginInspectShape } from "./inspect-shape.js";
import type { PluginDiagnostic } from "./manifest-types.js";
import type { PluginRegistry } from "./registry-types.js";

export type PluginCompatibilityNotice = {
  pluginId: string;
  code: "hook-only" | "removed-session-transcript-file-api";
  compatCode: PluginCompatCode;
  severity: "warn" | "info";
  message: string;
};

export type PluginCompatibilitySummary = {
  noticeCount: number;
  pluginCount: number;
};

export function formatPluginCompatibilityNotice(notice: PluginCompatibilityNotice): string {
  return `${notice.pluginId} ${notice.message}`;
}

export function summarizePluginCompatibility(
  notices: PluginCompatibilityNotice[],
): PluginCompatibilitySummary {
  return {
    noticeCount: notices.length,
    pluginCount: new Set(notices.map((notice) => notice.pluginId)).size,
  };
}

export function buildCompatibilityNoticesForInspect(inspect: {
  plugin: PluginRegistry["plugins"][number];
  shape: PluginInspectShape;
  diagnostics: readonly PluginDiagnostic[];
}): PluginCompatibilityNotice[] {
  const warnings: PluginCompatibilityNotice[] = [];
  if (inspect.shape === "hook-only") {
    warnings.push({
      pluginId: inspect.plugin.id,
      code: "hook-only",
      compatCode: "hook-only-plugin-shape",
      severity: "info",
      message:
        "is hook-only. This remains a supported compatibility path, but it has not migrated to explicit capability registration yet.",
    });
  }
  if (usesRemovedSessionTranscriptFileApi(inspect)) {
    warnings.push({
      pluginId: inspect.plugin.id,
      code: "removed-session-transcript-file-api",
      compatCode: "removed-session-transcript-file-api",
      severity: "warn",
      message:
        "references removed session/transcript file APIs; migrate to session identity, SessionTranscriptUpdate.target, and Gateway/runtime session helpers.",
    });
  }
  return warnings;
}

const removedSessionTranscriptFileApiMarkers = [
  "saveSessionStore",
  "resolveSessionTranscriptPathInDir",
  "resolveAndPersistSessionFile",
  "readLatestAssistantTextFromSessionTranscript",
  "SessionTranscriptUpdate.sessionFile",
  "sessionFiles",
  "transcriptPath",
  "sessionFile",
] as const;

function usesRemovedSessionTranscriptFileApi(inspect: {
  plugin: PluginRegistry["plugins"][number];
  diagnostics: readonly PluginDiagnostic[];
}): boolean {
  if (inspect.plugin.origin === "bundled") {
    return false;
  }
  const messages = [
    inspect.plugin.error,
    ...inspect.diagnostics.map((diagnostic) => diagnostic.message),
  ].filter((message): message is string => typeof message === "string" && message.length > 0);
  return messages.some((message) =>
    removedSessionTranscriptFileApiMarkers.some((marker) => message.includes(marker)),
  );
}

type PluginCompatibilityParams = {
  config?: OpenClawConfig;
  workspaceDir?: string;
  env?: NodeJS.ProcessEnv;
  report: PluginRegistry & { workspaceDir?: string };
};

export function buildPluginCompatibilityWarnings(params: PluginCompatibilityParams): string[] {
  return buildPluginCompatibilityNotices(params).map(formatPluginCompatibilityNotice);
}

export function buildPluginCompatibilityNotices(
  params: PluginCompatibilityParams,
): PluginCompatibilityNotice[] {
  const registry = params.report;
  return registry.plugins.flatMap((plugin) =>
    buildCompatibilityNoticesForInspect({
      plugin,
      shape: buildPluginShapeSummary({ plugin, report: registry }).shape,
      diagnostics: registry.diagnostics.filter((entry) => entry.pluginId === plugin.id),
    }),
  );
}
