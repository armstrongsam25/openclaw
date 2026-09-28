import { logConfigUpdated } from "../../config/logging.js";
import { resolveAgentModelPrimaryValue } from "../../config/model-input.js";
import type { RuntimeEnv } from "../../runtime.js";
import { updateDefaultModelPrimaryConfig } from "./shared.js";

export async function modelsSetCommand(modelRaw: string, runtime: RuntimeEnv) {
  const { updated, warning: catalogWarning } = await updateDefaultModelPrimaryConfig({
    modelRaw,
    field: "model",
  });
  if (catalogWarning) {
    runtime.error?.(catalogWarning);
  }
  const selectedModel = resolveAgentModelPrimaryValue(updated.agents?.defaults?.model) ?? modelRaw;
  const { repairModelSelectionRuntimePlugins } = await import("../runtime-plugin-install.js");
  const repaired = await repairModelSelectionRuntimePlugins({
    cfg: updated,
    model: selectedModel,
  });

  for (const warning of repaired.warnings) {
    runtime.error?.(warning);
  }

  logConfigUpdated(runtime);
  runtime.log(`Default model: ${selectedModel}`);
}
