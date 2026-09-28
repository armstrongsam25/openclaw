import type { z } from "zod";
import { UPDATE_RUN_PHASES } from "../../packages/gateway-protocol/src/update-run-vocabulary.js";
import { BUNDLED_OFFICIAL_EXTERNAL_PLUGIN_CATALOG_ENTRIES } from "../plugins/official-external-plugin-bundled-catalogs.js";
import { resolveOfficialExternalPluginId } from "../plugins/official-external-plugin-catalog-source.js";
import { CANARY_CHECKS, isPublicUpdateFailureCode } from "./update-failure-public-codes.js";
import type { UpdateFailureFactSchema } from "./update-run-schema.js";
import { resolvePublicUpdateStepId } from "./update-step-identity.js";

type PublicFailureIdentifiers = Pick<
  z.infer<typeof UpdateFailureFactSchema>,
  "check" | "code" | "pluginId" | "errorName"
>;

// Fixed labels emitted by the canary, finalizer, package runner, and service verifier.
const NATIVE_CHECKS = new Set<string>([
  ...UPDATE_RUN_PHASES,
  ...CANARY_CHECKS,
  "doctor",
  "lint",
  "config-write",
  "preflight",
  "installation-inspection",
  "target-resolution",
  "git update",
  "update",
  "targetConfigValidation",
  "configSnapshot",
  "targetConfigConvergence",
  "completionCache",
  "readyz",
  "startupz",
  "versionMatch",
  "service",
  "pluginErrors",
  "channelsReady",
  "settled",
  "gateway-recovery",
  "node-runtime",
  "managed-service",
  "managed-service-preflight",
  "package-install",
  "package-swap",
  "package-runtime",
  "plugin-update",
  "plugin-sync",
  "plugin-convergence",
  "build",
  "openclaw doctor",
  "post-install verification",
  "package rollback",
  "global install verify",
  "global install swap",
  "npm lifecycle policy preflight",
]);

let publicPluginIds: ReadonlySet<string> | undefined;
let publicDoctorCheckIds: Promise<ReadonlySet<string>> | undefined;

async function loadPublicPluginIds(): Promise<ReadonlySet<string>> {
  // Installation directories can contain private extensions; only shipped catalogs establish public IDs.
  publicPluginIds ??= new Set(
    BUNDLED_OFFICIAL_EXTERNAL_PLUGIN_CATALOG_ENTRIES.map(resolveOfficialExternalPluginId).filter(
      (id) => id !== undefined,
    ),
  );
  return publicPluginIds;
}

function loadPublicDoctorCheckIds(): Promise<ReadonlySet<string>> {
  publicDoctorCheckIds ??= import("../flows/doctor-health-contributions.js")
    .then((doctor) => doctor.resolveDoctorContributionHealthChecks())
    .then((checks) => new Set(checks.map((check) => check.id)));
  return publicDoctorCheckIds;
}

/** Capture catalogs before an interactive update can replace their code. */
export async function preparePublicUpdateFailureIdentifiers(): Promise<void> {
  await Promise.allSettled([loadPublicDoctorCheckIds(), loadPublicPluginIds()]);
}

/** Only source-defined public identities leave the local diagnostic report. */
export async function projectPublicUpdateFailureIdentifiers(
  fact: PublicFailureIdentifiers,
): Promise<PublicFailureIdentifiers> {
  // Admission failures use their reason code as the check ID.
  const nativeCheck =
    NATIVE_CHECKS.has(fact.check) ||
    resolvePublicUpdateStepId(fact.check) === fact.check ||
    isPublicUpdateFailureCode(fact.check);
  // Unavailable metadata cannot establish that an identifier is public.
  const [doctorIds, pluginIds] = await Promise.all([
    nativeCheck ? undefined : loadPublicDoctorCheckIds().catch(() => undefined),
    fact.pluginId ? loadPublicPluginIds().catch(() => undefined) : undefined,
  ]);
  return {
    check: nativeCheck || doctorIds?.has(fact.check) ? fact.check : "[redacted-check]",
    code: isPublicUpdateFailureCode(fact.code)
      ? fact.code
      : fact.errorName
        ? isPublicUpdateFailureCode(fact.errorName)
          ? fact.errorName
          : "[redacted-error-class]"
        : "[redacted-code]",
    ...(fact.pluginId
      ? { pluginId: pluginIds?.has(fact.pluginId) ? fact.pluginId : "[redacted-plugin]" }
      : {}),
  };
}
