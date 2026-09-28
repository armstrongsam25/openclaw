import {
  normalizeOptionalString,
  normalizeStringifiedOptionalString,
} from "@openclaw/normalization-core/string-coerce";
import type { Command } from "commander";
import {
  normalizeThinkLevel,
  THINKING_LEVELS_HELP,
  type ThinkLevel,
} from "../../auto-reply/thinking.shared.js";
import type { OpenClawConfig } from "../../config/types.openclaw.js";
import { defaultRuntime } from "../../runtime.js";
import { runCommandWithRuntime } from "../cli-utils.js";
import { collectOption } from "../program/helpers.js";
import { formatEnvelopeForText, providerSummaryText } from "./output.js";
import { registerLocalProvidersCommand, runCapabilityCommand } from "./providers-command.js";

async function loadModelCatalogForInspection(cfg: OpenClawConfig, rawAgentId?: string) {
  const { resolveCapabilityProviderAgentId } = await import("./shared.js");
  const { readPreparedModelCatalog } = await import("../../agents/prepared-model-catalog.js");
  const agentId =
    rawAgentId === undefined ? undefined : resolveCapabilityProviderAgentId(cfg, rawAgentId);
  const prepared = await readPreparedModelCatalog({ config: cfg, agentId, readOnly: true });
  return prepared.toSorted(
    (a, b) => a.provider.localeCompare(b.provider) || a.id.localeCompare(b.id),
  );
}

function requireModelRunPrompt(value: unknown): string {
  if (typeof value !== "string" || normalizeOptionalString(value) === undefined) {
    throw new Error("--prompt cannot be empty or whitespace-only.");
  }
  return value;
}

function normalizeModelRunThinking(value: unknown): ThinkLevel | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error("--thinking must be a string.");
  }
  const normalized = normalizeThinkLevel(value);
  if (!normalized) {
    throw new Error(`Invalid thinking level. Use one of: ${THINKING_LEVELS_HELP}.`);
  }
  return normalized;
}

async function buildModelProviders(cfg: OpenClawConfig, agentId: string) {
  const { providerHasGenericConfig, resolveSelectedProviderFromModelRef } =
    await import("./shared.js");
  const { resolveAgentEffectiveModelPrimary } = await import("../../agents/agent-scope.js");
  const { getProviderEnvVarsCore } = await import("../../secrets/provider-env-vars.js");
  const catalog = await loadModelCatalogForInspection(cfg, agentId);
  const selectedProvider = resolveSelectedProviderFromModelRef(
    resolveAgentEffectiveModelPrimary(cfg, agentId),
  );
  const grouped = new Map<
    string,
    {
      provider: string;
      count: number;
      defaults: string[];
      available: boolean;
      configured: boolean;
      selected: boolean;
    }
  >();
  for (const entry of catalog) {
    const current = grouped.get(entry.provider) ?? {
      provider: entry.provider,
      count: 0,
      defaults: [],
      available: true,
      configured: providerHasGenericConfig({
        cfg,
        providerId: entry.provider,
        agentId,
        envVars: getProviderEnvVarsCore(entry.provider),
      }),
      selected: selectedProvider === entry.provider,
    };
    current.count += 1;
    if (current.defaults.length < 3) {
      current.defaults.push(entry.id);
    }
    grouped.set(entry.provider, current);
  }
  return [...grouped.values()].toSorted((a, b) => a.provider.localeCompare(b.provider));
}

async function runModelAuthStatus(agent: string) {
  const captured: string[] = [];
  const { modelsStatusCommand } = await import("../../commands/models/list.status-command.js");
  await modelsStatusCommand(
    { json: true, agent },
    {
      log: (...args) => captured.push(args.join(" ")),
      error: (message) => {
        throw message instanceof Error ? message : new Error(String(message));
      },
      exit: (code) => {
        throw new Error(`exit ${code}`);
      },
    },
  );
  const raw = captured.find((line) => line.trim().startsWith("{"));
  return raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
}

async function runModelAuthLogout(provider: string, agent: string) {
  const { getRuntimeConfig } = await import("../../config/config.js");
  const { resolveAgentDir } = await import("../../agents/agent-scope.js");
  const { listProfilesForProvider, loadAuthProfileStoreForRuntime } =
    await import("../../agents/auth-profiles.js");
  const { updateAuthProfileStoreWithLock } =
    await import("../../agents/auth-profiles/store-runtime.js");
  const cfg = getRuntimeConfig();
  const agentDir = resolveAgentDir(cfg, agent);
  const store = loadAuthProfileStoreForRuntime(agentDir);
  const profileIds = listProfilesForProvider(store, provider);
  const updated = await updateAuthProfileStoreWithLock({
    agentDir,
    updater: (nextStore) => {
      let changed = false;
      for (const profileId of profileIds) {
        if (nextStore.profiles[profileId]) {
          delete nextStore.profiles[profileId];
          changed = true;
        }
        if (nextStore.usageStats?.[profileId]) {
          delete nextStore.usageStats[profileId];
          changed = true;
        }
      }
      if (nextStore.order?.[provider]) {
        delete nextStore.order[provider];
        changed = true;
      }
      if (nextStore.lastGood?.[provider]) {
        delete nextStore.lastGood[provider];
        changed = true;
      }
      return changed;
    },
  });
  if (!updated) {
    throw new Error(`Failed to remove saved auth profiles for provider ${provider}.`);
  }
  return {
    provider,
    removedProfiles: profileIds,
  };
}

export function registerModelCapabilityCommands(capability: Command): void {
  const model = capability
    .command("model")
    .description("Text inference and model catalog commands")
    .option("--agent <id>", "Agent whose model and auth state should be used");

  model
    .command("run")
    .description("Run a one-shot model turn")
    .requiredOption("--prompt <text>", "Prompt text")
    .option("--file <path>", "Image file", collectOption, [])
    .option("--model <provider/model>", "Model override")
    .option("--thinking <level>", "Thinking level override")
    .option("--local", "Force local execution", false)
    .option("--gateway", "Force gateway execution", false)
    .option(
      "--agent <id>",
      "Agent whose model and credentials own the run (default: agents.defaults.systemAgent.agentId, then the sole agent)",
    )
    .option("--json", "Output JSON", false)
    .action((opts, command) =>
      runCapabilityCommand(opts.json, formatEnvelopeForText, async () => {
        const { resolveCapabilityAgentOption, resolveTransport } = await import("./shared.js");
        const prompt = requireModelRunPrompt(opts.prompt);
        const thinking = normalizeModelRunThinking(opts.thinking);
        const transport = resolveTransport({
          local: Boolean(opts.local),
          gateway: Boolean(opts.gateway),
          supported: ["local", "gateway"],
          defaultTransport: "local",
        });
        const { runModelRun } = await import("./model-runtime.js");
        return runModelRun({
          prompt,
          agent: resolveCapabilityAgentOption(command, opts.agent),
          files: opts.file as string[] | undefined,
          model: opts.model as string | undefined,
          thinking,
          transport,
        });
      }),
    );

  model
    .command("list")
    .description("List known models")
    .option("--json", "Output JSON", false)
    .action((opts, command) =>
      runCapabilityCommand(opts.json, providerSummaryText, async () => {
        const { resolveCapabilityAgentOption } = await import("./shared.js");
        const { getRuntimeConfig } = await import("../../config/config.js");
        return loadModelCatalogForInspection(
          getRuntimeConfig(),
          resolveCapabilityAgentOption(command, opts.agent),
        );
      }),
    );

  model
    .command("inspect")
    .description("Inspect one model catalog entry")
    .requiredOption("--model <provider/model>", "Model id")
    .option("--json", "Output JSON", false)
    .action((opts, command) =>
      runCapabilityCommand(opts.json, undefined, async () => {
        const { resolveCapabilityAgentOption } = await import("./shared.js");
        const { getRuntimeConfig } = await import("../../config/config.js");
        const target = normalizeStringifiedOptionalString(opts.model) ?? "";
        const catalog = await loadModelCatalogForInspection(
          getRuntimeConfig(),
          resolveCapabilityAgentOption(command, opts.agent),
        );
        const entry =
          catalog.find((candidate) => `${candidate.provider}/${candidate.id}` === target) ??
          catalog.find((candidate) => candidate.id === target);
        if (!entry) {
          throw new Error(`Model not found: ${target}`);
        }
        return entry;
      }),
    );

  registerLocalProvidersCommand(
    model,
    "List model providers from the catalog",
    buildModelProviders,
    providerSummaryText,
  );

  const modelAuth = model
    .command("auth")
    .description("Provider auth helpers")
    .option("--agent <id>", "Agent id (default: configured default agent)");

  const resolveModelAuthAgent = async (command: Command, rawAgentId: unknown, surface: string) => {
    const { resolveCapabilityProviderAgentId, resolveCapabilityAgentOption } =
      await import("./shared.js");
    const { getRuntimeConfig } = await import("../../config/config.js");
    return resolveCapabilityProviderAgentId(
      getRuntimeConfig(),
      resolveCapabilityAgentOption(command, rawAgentId),
      surface,
    );
  };

  modelAuth
    .command("login")
    .description("Run provider auth login")
    .requiredOption("--provider <id>", "Provider id")
    .option("--method <id>", "Provider auth method id")
    .option("--agent <id>", "Agent id (default: configured default agent)")
    .action(async (opts, command) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        const agent = await resolveModelAuthAgent(command, opts.agent, "infer model auth login");
        const { modelsAuthLoginCommand } = await import("../../commands/models/auth.js");
        await modelsAuthLoginCommand(
          {
            provider: String(opts.provider),
            method: opts.method ? String(opts.method) : undefined,
            agent,
          },
          defaultRuntime,
        );
      });
    });

  modelAuth
    .command("logout")
    .description("Remove saved auth profiles for one provider")
    .requiredOption("--provider <id>", "Provider id")
    .option(
      "--agent <id>",
      "Agent id (default: agents.defaults.systemAgent.agentId, then the sole agent)",
    )
    .option("--json", "Output JSON", false)
    .action((opts, command) =>
      runCapabilityCommand(opts.json, undefined, async () => {
        return runModelAuthLogout(
          String(opts.provider),
          await resolveModelAuthAgent(command, opts.agent, "infer model auth logout"),
        );
      }),
    );

  modelAuth
    .command("status")
    .description("Show configured auth state")
    .option("--agent <id>", "Agent id (default: configured default agent)")
    .option("--json", "Output JSON", false)
    .action((opts, command) =>
      runCapabilityCommand(opts.json, undefined, async () => {
        return runModelAuthStatus(
          await resolveModelAuthAgent(command, opts.agent, "infer model auth status"),
        );
      }),
    );
}
