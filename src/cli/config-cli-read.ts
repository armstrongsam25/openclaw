import { formatConfigIssueLines, normalizeConfigIssues } from "../config/issue-format.js";
import { renderConfigValidationIssueLines } from "../config/issue-location.js";
import { CONFIG_PATH } from "../config/paths.js";
import {
  buildRuntimeConfigSchemaFromRegistry,
  readBestEffortRuntimeConfigSchema,
} from "../config/runtime-schema.js";
import { asSchemaObject } from "../config/schema.shared.js";
import { danger, success, warn } from "../globals.js";
import { formatErrorMessage } from "../infra/errors.js";
import {
  ExitError,
  type RuntimeEnv,
  defaultRuntime,
  writeRuntimeJson,
  writeRuntimeStdout,
} from "../runtime.js";
import { shortenHomePath } from "../utils.js";
import { formatCliCommand } from "./command-format.js";
import { getAtPath, isConfigSchemaPath, parseConfigSetPath } from "./config-cli-path.js";
import {
  ensureValidConfigSnapshotForCli,
  finishConfigValidationForCli,
  formatInvalidConfigRepairHint,
} from "./config-cli-validation.js";
import { formatCliJsonFailure } from "./failure-output.js";
import { exitCliAfterOutput } from "./one-shot-exit.js";
import { quoteCliArg } from "./quote-cli-arg.js";

export async function runConfigGet(opts: { path: string; json?: boolean; runtime?: RuntimeEnv }) {
  const runtime = opts.runtime ?? defaultRuntime;
  try {
    const parsedPath = parseConfigSetPath(opts.path);
    const { readConfigFileSnapshotWithPluginMetadata } = await import("../config/config.js");
    const read = await readConfigFileSnapshotWithPluginMetadata({ observe: false });
    const { snapshot, pluginMetadataSnapshot } = read;
    ensureValidConfigSnapshotForCli(snapshot, runtime, { json: opts.json });
    if (!pluginMetadataSnapshot) {
      throw new Error("Config plugin metadata unavailable; refusing to display config values.");
    }
    const { redactConfigObject } = await import("../config/redact-snapshot.js");
    const { schema, uiHints } = buildRuntimeConfigSchemaFromRegistry(
      pluginMetadataSnapshot.manifestRegistry,
      snapshot.sourceConfig,
    );
    const res = getAtPath(redactConfigObject(snapshot.config, uiHints), parsedPath);
    if (!res.found || res.value === undefined) {
      const message = isConfigSchemaPath(schema, parsedPath)
        ? `Config path is valid but unset: ${opts.path}. The runtime default applies until you set an authored value with ${formatCliCommand(`openclaw config set ${quoteCliArg(opts.path)} <value>`)}.`
        : `Unknown config path: ${opts.path}. Run ${formatCliCommand("openclaw config schema")} to inspect valid paths.`;
      if (opts.json) {
        writeRuntimeJson(runtime, formatCliJsonFailure(message));
        exitCliAfterOutput(runtime, 1);
      }
      runtime.error(danger(message));
      exitCliAfterOutput(runtime, 1);
    }
    if (
      !opts.json &&
      (typeof res.value === "string" ||
        typeof res.value === "number" ||
        typeof res.value === "boolean")
    ) {
      writeRuntimeStdout(runtime, `${String(res.value)}\n`);
    } else {
      writeRuntimeJson(runtime, res.value);
    }
  } catch (err) {
    if (err instanceof ExitError) {
      throw err;
    }
    if (opts.json) {
      writeRuntimeJson(runtime, formatCliJsonFailure(err));
      exitCliAfterOutput(runtime, 1);
    }
    runtime.error(danger(formatErrorMessage(err)));
    exitCliAfterOutput(runtime, 1);
  }
}

export async function runConfigSchema(opts: { runtime?: RuntimeEnv } = {}) {
  const runtime = opts.runtime ?? defaultRuntime;
  try {
    const schema = structuredClone((await readBestEffortRuntimeConfigSchema()).schema);
    schema.properties = { $schema: { type: "string" }, ...asSchemaObject(schema.properties) };
    writeRuntimeJson(runtime, schema);
  } catch (err) {
    runtime.error(danger(`Config schema error: ${formatErrorMessage(err)}`));
    exitCliAfterOutput(runtime, 1);
  }
}

export async function runConfigValidate(opts: { json?: boolean; runtime?: RuntimeEnv } = {}) {
  const runtime = opts.runtime ?? defaultRuntime;
  let outputPath = CONFIG_PATH ?? "openclaw.json";
  try {
    const { readConfigFileSnapshotWithPluginMetadata } = await import("../config/config.js");
    const read = await readConfigFileSnapshotWithPluginMetadata({
      observe: false,
      prepareValidation: "strict",
    });
    const snapshot = await finishConfigValidationForCli(read);
    outputPath = snapshot.path;
    const shortPath = shortenHomePath(outputPath);
    if (!snapshot.exists) {
      if (opts.json) {
        writeRuntimeJson(
          runtime,
          { ...formatCliJsonFailure("file not found"), valid: false, path: outputPath },
          0,
        );
      } else {
        runtime.error(danger(`Config file not found: ${shortPath}`));
        runtime.error(
          `Create one with ${formatCliCommand("openclaw onboard")} or run ${formatCliCommand("openclaw doctor --fix")}.`,
        );
      }
      exitCliAfterOutput(runtime, 1);
    }
    if (!snapshot.valid) {
      const issues = normalizeConfigIssues(snapshot.issues);
      if (opts.json) {
        writeRuntimeJson(runtime, {
          ...formatCliJsonFailure(`OpenClaw config is invalid: ${shortPath}`),
          valid: false,
          path: outputPath,
          issues,
        });
      } else {
        runtime.error(`Config needs correction: ${shortPath}`);
        for (const line of renderConfigValidationIssueLines(snapshot, "-")) {
          runtime.error(`  ${line}`);
        }
        runtime.error("");
        runtime.error(
          formatInvalidConfigRepairHint(snapshot, "to repair, or fix the keys above manually."),
        );
        runtime.error(
          `Run ${formatCliCommand("openclaw config schema")} to inspect supported settings and values, then rerun ${formatCliCommand("openclaw config validate")}.`,
        );
      }
      exitCliAfterOutput(runtime, 1);
    }
    const warnings = normalizeConfigIssues(snapshot.warnings);
    if (opts.json) {
      writeRuntimeJson(runtime, { valid: true, path: outputPath, warnings }, 0);
    } else {
      runtime.log(success(`Config valid: ${shortPath}`));
      if (warnings.length > 0) {
        runtime.log(warn(`${warnings.length} warning(s):`));
        for (const line of formatConfigIssueLines(warnings, warn("!"), { normalizeRoot: true })) {
          runtime.log(`  ${line}`);
        }
      }
    }
  } catch (err) {
    if (err instanceof ExitError) {
      throw err;
    }
    if (opts.json) {
      writeRuntimeJson(
        runtime,
        { ...formatCliJsonFailure(err), valid: false, path: outputPath },
        0,
      );
    } else {
      runtime.error(danger(`Config validation error: ${formatErrorMessage(err)}`));
    }
    exitCliAfterOutput(runtime, 1);
  }
}
