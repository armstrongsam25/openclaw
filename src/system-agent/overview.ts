// OpenClaw overview gathers config, agent, tool, docs, source, and gateway status.
import { resolveAmbientOwnerAgentId } from "../agents/agent-scope-config.js";
import { listAgentEntries } from "../agents/agent-scope.js";
import {
  OPENCLAW_DOCS_URL,
  OPENCLAW_SOURCE_URL,
  resolveOpenClawReferencePaths,
} from "../agents/docs-path.js";
import { readUtilityModelSetting } from "../agents/utility-model-setting.js";
import {
  resolveConfiguredPrimaryModelForAgent,
  resolveConfiguredSetupModelForAgent,
} from "../agents/utility-model.js";
import {
  readConfigFileSnapshot,
  resolveConfigPath,
  resolveGatewayPort,
  type ConfigFileSnapshot,
  type OpenClawConfig,
} from "../config/config.js";
import { resolveAgentModelPrimaryValue } from "../config/model-input.js";
import { buildGatewayConnectionDetails as buildGatewayConnectionDetailsDefault } from "../gateway/call.js";
import { isFastTestRuntimeEnv } from "../infra/env.js";
import { normalizeAgentId } from "../routing/session-key.js";
import { probeGatewayUrl, probeLocalCommand, type LocalCommandProbe } from "./probes.js";

type SystemAgentSummary = {
  id: string;
  name?: string;
  isDefault: boolean;
  model?: string;
  utilityModel?: string;
  workspace?: string;
};

export type SystemAgentOverview = {
  config: {
    path: string;
    exists: boolean;
    valid: boolean;
    issues: string[];
    hash: string | null;
  };
  agents: SystemAgentSummary[];
  defaultAgentId: string;
  defaultModel?: string;
  /** Explicit utility route available to setup while the regular model is unconfigured. */
  setupModel?: string;
  utilityModel?: string;
  tools: {
    codex: LocalCommandProbe;
    claude: LocalCommandProbe;
    gemini: LocalCommandProbe;
    apiKeys: {
      openai: boolean;
      anthropic: boolean;
    };
  };
  gateway: {
    url: string;
    source: string;
    reachable: boolean;
    error?: string;
  };
  references: {
    docsPath?: string;
    docsUrl: string;
    sourcePath?: string;
    sourceUrl: string;
  };
};

type OpenClawReferencePaths = Awaited<ReturnType<typeof resolveOpenClawReferencePaths>>;

type GatewayConnectionDetails = {
  url: string;
  urlSource: string;
  remoteFallbackNote?: string;
};

type SystemAgentOverviewDependencies = {
  readConfigFileSnapshot?: typeof readConfigFileSnapshot;
  resolveConfigPath?: typeof resolveConfigPath;
  resolveGatewayPort?: typeof resolveGatewayPort;
  buildGatewayConnectionDetails?: (input: {
    config: OpenClawConfig;
    configPath: string;
  }) => GatewayConnectionDetails;
  probeLocalCommand?: typeof probeLocalCommand;
  probeGatewayUrl?: typeof probeGatewayUrl;
  resolveOpenClawReferencePaths?: typeof resolveOpenClawReferencePaths;
};

function issueMessages(snapshot: ConfigFileSnapshot): string[] {
  return snapshot.issues.map((issue) => {
    const path = issue.path ? `${issue.path}: ` : "";
    return `${path}${issue.message}`;
  });
}

function buildAgentSummaries(cfg: OpenClawConfig, defaultAgentId: string): SystemAgentSummary[] {
  const entries = listAgentEntries(cfg);
  if (entries.length === 0) {
    const utility = readUtilityModelSetting(cfg, defaultAgentId);
    return [
      {
        id: defaultAgentId,
        isDefault: true,
        model: resolveConfiguredPrimaryModelForAgent({ cfg, agentId: defaultAgentId }),
        ...(utility.kind === "explicit" ? { utilityModel: utility.modelRef } : {}),
      },
    ];
  }
  const seen = new Set<string>();
  const summaries: SystemAgentSummary[] = [];
  // Agent ids are normalized and deduped so config aliases do not produce duplicate setup choices.
  for (const entry of entries) {
    const id = normalizeAgentId(entry.id);
    if (seen.has(id)) {
      continue;
    }
    seen.add(id);
    const summary: SystemAgentSummary = {
      id,
      isDefault: id === defaultAgentId,
    };
    if (typeof entry.name === "string") {
      summary.name = entry.name;
    }
    const model = resolveConfiguredPrimaryModelForAgent({ cfg, agentId: id });
    if (model) {
      summary.model = model;
    }
    const utility = readUtilityModelSetting(cfg, id);
    if (utility.kind === "explicit") {
      summary.utilityModel = utility.modelRef;
    }
    if (typeof entry.workspace === "string") {
      summary.workspace = entry.workspace;
    }
    summaries.push(summary);
  }
  return summaries;
}

function resolveFastTestReferences(env: NodeJS.ProcessEnv): OpenClawReferencePaths | undefined {
  if (!isFastTestRuntimeEnv(env)) {
    return undefined;
  }
  const sourcePath = process.cwd();
  return {
    sourcePath,
    docsPath: `${sourcePath}/docs`,
  };
}

export async function loadSystemAgentOverview(
  opts: { agentId?: string; env?: NodeJS.ProcessEnv; deps?: SystemAgentOverviewDependencies } = {},
): Promise<SystemAgentOverview> {
  const env = opts.env ?? process.env;
  const deps = opts.deps ?? {};
  const readSnapshot = deps.readConfigFileSnapshot ?? readConfigFileSnapshot;
  const snapshot = await readSnapshot();
  const cfg = snapshot.runtimeConfig ?? snapshot.sourceConfig ?? {};
  const defaultAgentId = resolveAmbientOwnerAgentId(cfg, opts.agentId);
  const defaultModel =
    resolveConfiguredPrimaryModelForAgent({ cfg, agentId: defaultAgentId }) ??
    resolveAgentModelPrimaryValue(cfg.agents?.defaults?.model);
  const setupSelection = resolveConfiguredSetupModelForAgent({ cfg, agentId: defaultAgentId });
  const utility = readUtilityModelSetting(cfg, defaultAgentId);
  const configPath = snapshot.path || (deps.resolveConfigPath ?? resolveConfigPath)(env);
  let gatewayUrl = `ws://127.0.0.1:${(deps.resolveGatewayPort ?? resolveGatewayPort)(cfg, env)}`;
  let gatewaySource = "local loopback";
  let gatewayError: string | undefined;
  try {
    const buildGatewayConnectionDetails =
      deps.buildGatewayConnectionDetails ?? buildGatewayConnectionDetailsDefault;
    const details = buildGatewayConnectionDetails({ config: cfg, configPath });
    gatewayUrl = details.url;
    gatewaySource = details.urlSource;
    gatewayError = details.remoteFallbackNote;
  } catch (err) {
    gatewayError = err instanceof Error ? err.message : String(err);
  }
  const resolveReferences = deps.resolveOpenClawReferencePaths ?? resolveOpenClawReferencePaths;
  const commandProbe = deps.probeLocalCommand ?? probeLocalCommand;
  const [codex, claude, gemini, gateway, references] = await Promise.all([
    // Probes run in parallel; each individual probe is timeout-bounded in probes.ts.
    commandProbe("codex"),
    commandProbe("claude"),
    commandProbe("gemini"),
    (deps.probeGatewayUrl ?? probeGatewayUrl)(gatewayUrl),
    resolveFastTestReferences(env) ??
      resolveReferences({
        argv1: process.argv[1],
        cwd: process.cwd(),
        moduleUrl: import.meta.url,
      }),
  ]);
  return {
    config: {
      path: configPath,
      exists: snapshot.exists,
      valid: snapshot.valid,
      issues: issueMessages(snapshot),
      hash: snapshot.hash ?? null,
    },
    agents: buildAgentSummaries(cfg, defaultAgentId),
    defaultAgentId,
    defaultModel,
    ...(setupSelection?.modelTarget === "utility" ? { setupModel: setupSelection.modelRef } : {}),
    ...(utility.kind === "explicit" ? { utilityModel: utility.modelRef } : {}),
    tools: {
      codex,
      claude,
      gemini,
      apiKeys: {
        openai: Boolean(env.OPENAI_API_KEY?.trim()),
        anthropic: Boolean(env.ANTHROPIC_API_KEY?.trim()),
      },
    },
    gateway: {
      url: gateway.url,
      source: gatewaySource,
      reachable: gateway.reachable,
      error: gateway.error ?? gatewayError,
    },
    references: {
      docsPath: references.docsPath ?? undefined,
      docsUrl: OPENCLAW_DOCS_URL,
      sourcePath: references.sourcePath ?? undefined,
      sourceUrl: OPENCLAW_SOURCE_URL,
    },
  };
}
