// Owns cleanup of transient handoff files and released legacy process claims.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { positiveSecondsToSafeMilliseconds } from "@openclaw/normalization-core/number-coercion";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import type { ManagedHandoffRepairFacts } from "./update-managed-service-handoff-database.js";
import type { ManagedHandoffLease } from "./update-managed-service-handoff-lease-types.js";

/** v1 shipped in 2026.8.2. This proves cleanup eligibility, never v2 authority. */
export function canCleanupLegacyManagedHandoff(
  payload: string,
  processState: (identity: { pid: number; startIdentity: string }) => "live" | "dead" | "unknown",
): boolean {
  let value: unknown;
  try {
    value = JSON.parse(payload);
  } catch {
    return false;
  }
  return (
    isRecord(value) &&
    Object.keys(value).length === 3 &&
    value.version === 1 &&
    typeof value.pid === "number" &&
    Number.isInteger(value.pid) &&
    value.pid > 0 &&
    typeof value.startIdentity === "string" &&
    Number.isSafeInteger(Number(value.startIdentity)) &&
    Number(value.startIdentity) >= 0 &&
    String(Number(value.startIdentity)) === value.startIdentity &&
    // The recorded process may be the updater runner, not the surviving helper.
    processState({ pid: value.pid, startIdentity: value.startIdentity }) === "dead"
  );
}

export const MANAGED_SERVICE_UPDATE_HANDOFF_TEMP_PREFIX = "openclaw-update-run-handoff-";
const MANAGED_SERVICE_UPDATE_HANDOFF_STALE_TTL_MS = 24 * 60 * 60_000;

export async function cleanupStaleManagedServiceUpdateHandoffs(params?: {
  tmpDir?: string;
  nowMs?: number;
  ttlMs?: number;
}): Promise<number> {
  const tmpDir = params?.tmpDir ?? os.tmpdir();
  const nowMs = params?.nowMs ?? Date.now();
  const ttlMs = params?.ttlMs ?? MANAGED_SERVICE_UPDATE_HANDOFF_STALE_TTL_MS;
  let entries: Array<{ name: string; isDirectory: () => boolean }>;
  try {
    entries = await fs.readdir(tmpDir, { withFileTypes: true });
  } catch {
    return 0;
  }

  let removed = 0;
  for (const entry of entries.toSorted((left, right) => left.name.localeCompare(right.name))) {
    if (
      !entry.isDirectory() ||
      !entry.name.startsWith(MANAGED_SERVICE_UPDATE_HANDOFF_TEMP_PREFIX)
    ) {
      continue;
    }
    const dir = path.join(tmpDir, entry.name);
    let stats: { mtimeMs: number };
    try {
      stats = await fs.stat(dir);
    } catch {
      continue;
    }
    if (nowMs - stats.mtimeMs < ttlMs) {
      continue;
    }
    try {
      await fs.rm(dir, { recursive: true, force: true });
      removed += 1;
    } catch {
      // Best effort cleanup only.
    }
  }
  return removed;
}

export async function inspectManagedHandoffRepairFacts(
  previous: ManagedHandoffLease,
  discovered: ManagedHandoffRepairFacts,
  retained?: ManagedHandoffRepairFacts,
): Promise<ManagedHandoffRepairFacts> {
  const facts = {
    runIds: [...new Set([...(retained?.runIds ?? []), ...discovered.runIds])],
    artifactPaths: [...new Set([...(retained?.artifactPaths ?? []), ...discovered.artifactPaths])],
    timeoutMs: Math.max(retained?.timeoutMs ?? 0, discovered.timeoutMs ?? 0) || null,
  };
  const grace = Math.max(45 * 60_000, facts.timeoutMs ?? 0);
  if (Date.now() - previous.updatedAt < grace) {
    throw new Error(
      `Handoff was last recorded at ${new Date(previous.updatedAt).toISOString()}; its recovery grace ends at ${new Date(previous.updatedAt + grace).toISOString()}. Retry openclaw update repair then.`,
    );
  }
  const { inspectOtherOpenClawProcesses } = await import("./openclaw-process-census.js");
  for (const runId of facts.runIds) {
    const census = inspectOtherOpenClawProcesses({ runId, artifactPaths: facts.artifactPaths });
    const pids = [...new Set([...census.matchingPids, ...census.unverifiedPids])];
    if (pids.length || census.error) {
      throw new Error(
        `Handoff descendants remain alive or unverified${pids.length ? `: PID ${pids.join(", ")}` : ""}. ${census.error || "Stop the named work through its owning terminal or service, verify process-inspection permissions, then run openclaw update repair."}`,
      );
    }
  }
  return facts;
}

export async function readManagedHandoffRepairFacts(
  lease: ManagedHandoffLease,
  env: NodeJS.ProcessEnv,
  boundRunId?: string,
): Promise<ManagedHandoffRepairFacts> {
  const { readSecureFile } = await import("./fs-safe.js");
  const { listUpdateRunsAsync } = await import("./update-run-reader.js");
  const { recordedUpdateRunDrivers } = await import("./update-run-activity.js");
  const { sameUpdateRunDriver } = await import("./update-run-driver.js");
  const { assertUpdateRecoveryAdmission } = await import("./update-run-recovery-admission.js");
  const { resolveStateDir } = await import("../config/paths.js");
  const { resolveUpdateCaptureRoot } = await import("./update-capture-paths.js");
  const runIds = new Set<string>(boundRunId ? [boundRunId] : []);
  const artifactPaths = new Set<string>();
  let timeoutMs = 0;
  const root = await fs.realpath(os.tmpdir());
  for (const entry of await fs.readdir(root, { withFileTypes: true })) {
    if (
      !entry.isDirectory() ||
      !entry.name.startsWith(MANAGED_SERVICE_UPDATE_HANDOFF_TEMP_PREFIX)
    ) {
      continue;
    }
    const directory = path.join(root, entry.name);
    let value: unknown;
    try {
      const file = await readSecureFile({
        filePath: path.join(directory, "handoff.json"),
        trust: { trustedDirs: [directory] },
        io: { maxBytes: 1024 * 1024 },
        inject: { env },
      });
      if (file.realPath !== path.join(directory, "handoff.json")) {
        throw new Error("Handoff artifact path changed");
      }
      value = JSON.parse(file.buffer.toString("utf8"));
    } catch {
      value = undefined;
    }
    if (
      !isRecord(value) ||
      typeof value.updateLeaseOwner !== "string" ||
      typeof value.updateLeaseKey !== "string"
    ) {
      artifactPaths.add(directory);
      artifactPaths.add(lease.key);
      continue;
    }
    if (value.updateLeaseOwner !== lease.owner || value.updateLeaseKey !== lease.key) {
      continue;
    }
    if (typeof value.runId === "string" && value.runId.length > 0 && value.runId.length <= 4096) {
      runIds.add(value.runId);
    }
    artifactPaths.add(directory);
    for (const candidate of [value.cwd, value.triageContextPath]) {
      if (typeof candidate === "string" && candidate.length <= 4096 && path.isAbsolute(candidate)) {
        artifactPaths.add(candidate);
      }
    }
    const timeouts = [value.recoveryTimeoutMs, value.parentExitTimeoutMs];
    const argv =
      Array.isArray(value.commandArgv) &&
      value.commandArgv.every((arg): arg is string => typeof arg === "string")
        ? value.commandArgv
        : [];
    for (const [index, arg] of argv.entries()) {
      const seconds = arg === "--timeout" ? argv[index + 1] : arg.match(/^--timeout=(.*)$/u)?.[1];
      timeouts.push(positiveSecondsToSafeMilliseconds(seconds?.trim()));
    }
    for (const candidate of timeouts) {
      if (typeof candidate === "number" && Number.isSafeInteger(candidate) && candidate > 0) {
        timeoutMs = Math.max(timeoutMs, candidate);
      }
    }
  }
  const runs = await listUpdateRunsAsync(
    { limit: 100, includeRunId: [...runIds][0] ?? lease.owner },
    { env },
  );
  const host = os.hostname();
  const owners = [lease.helper, lease.executor].map(({ pid, startIdentity }) => ({
    pid,
    startIdentity,
    host,
  }));
  for (const run of runs) {
    if (
      recordedUpdateRunDrivers(run).some((driver) =>
        owners.some((owner) => sameUpdateRunDriver(driver, owner)),
      )
    ) {
      runIds.add(run.runId);
    }
  }
  const runId = [...runIds][0];
  if (!runId || runIds.size !== 1) {
    throw new Error("Cannot identify handoff run; inspect openclaw update status --json.");
  }
  const capture = runs.find((run) => run.runId === runId)?.origin.updateRecoveryCapture;
  if (capture && !capture.restored && !capture.forwardResolution && !capture.retirement) {
    throw new Error(`Update ${runId} retains restoration; run openclaw doctor --fix.`);
  }
  await assertUpdateRecoveryAdmission({ env });
  if (path.basename(runId) === runId && runId !== "." && runId !== "..") {
    const capturePath = path.join(resolveUpdateCaptureRoot(resolveStateDir(env)), runId);
    const canonical = await fs.realpath(capturePath).catch(() => null);
    if (canonical) {
      artifactPaths.add(capturePath);
      artifactPaths.add(canonical);
    }
  }
  return {
    runIds: [runId],
    artifactPaths: artifactPaths.size ? [...artifactPaths] : [lease.key],
    timeoutMs: timeoutMs || null,
  };
}
