import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { readManagedHandoffRepairFacts } from "./update-managed-service-handoff-cleanup.js";
import type { ManagedHandoffLease } from "./update-managed-service-handoff-lease-types.js";
import { listUpdateRunsAsync } from "./update-run-reader.js";
import type { UpdateRunRecord } from "./update-run-record.js";
import { assertUpdateRecoveryAdmission } from "./update-run-recovery-admission.js";

vi.mock("./update-run-reader.js", () => ({ listUpdateRunsAsync: vi.fn() }));
vi.mock("./update-run-recovery-admission.js", () => ({ assertUpdateRecoveryAdmission: vi.fn() }));

const dirs = useAutoCleanupTempDirTracker(afterEach);
let root: string;
let env: NodeJS.ProcessEnv;
let lease: ManagedHandoffLease;

beforeEach(async () => {
  root = await fs.realpath(dirs.make("handoff-repair-facts-"));
  vi.spyOn(os, "tmpdir").mockReturnValue(root);
  env = { HOME: root, USERPROFILE: root, OPENCLAW_STATE_DIR: path.join(root, "state") };
  const payload = {
    version: 2 as const,
    executor: { pid: 1001, startIdentity: "executor-start" },
    helper: { pid: 1002, startIdentity: "helper-start" },
    action: {
      kind: "triage" as const,
      phase: "uncertain" as const,
      lifetime: {
        kind: "foreground" as const,
        boot: { platform: "linux" as const, identity: "01234567-89ab-cdef-0123-456789abcdef" },
      },
    },
  };
  lease = {
    ...payload,
    payload: JSON.stringify(payload),
    key: path.join(root, "installation"),
    owner: "handoff-correlation",
    updatedAt: 42,
  };
  vi.mocked(listUpdateRunsAsync).mockReset().mockResolvedValue([]);
  vi.mocked(assertUpdateRecoveryAdmission).mockReset().mockResolvedValue(undefined);
});
afterEach(() => vi.restoreAllMocks());

async function helper(name: string, fields: Record<string, unknown> = {}) {
  const directory = path.join(root, `openclaw-update-run-handoff-${name}`);
  await fs.mkdir(directory, { mode: 0o700 });
  await fs.writeFile(
    path.join(directory, "handoff.json"),
    JSON.stringify({
      cwd: directory,
      runId: "retained-run",
      updateLeaseOwner: lease.owner,
      updateLeaseKey: lease.key,
      ...fields,
    }),
    { mode: 0o600 },
  );
  return directory;
}

function run(runId: string, origin: UpdateRunRecord["origin"]): UpdateRunRecord {
  return {
    runId,
    origin,
    target: {},
    before: {},
    after: {},
    trigger: "cli",
    phase: "finished",
    status: "failed",
    reason: "update-failed",
    steps: [],
    verification: {},
    repair: [],
    createdAtMs: 10,
    updatedAtMs: 20,
    confirmedAtMs: null,
    finishedAtMs: 20,
    downtimeMs: null,
  };
}

describe("managed handoff repair facts", () => {
  it("keeps only exact helper owner/key artifacts and their retained capture paths", async () => {
    await helper("wrong-owner", { updateLeaseOwner: "other", runId: "other-run" });
    await helper("wrong-key", { updateLeaseKey: path.join(root, "other"), runId: "other-run" });
    const context = path.join(root, "failure.json");
    const directory = await helper("matching", {
      runId: "retained-run",
      triageContextPath: context,
      recovery: { serviceRestartSafe: false, reason: "runtime-verification-failed" },
    });
    const capture = path.join(`${env.OPENCLAW_STATE_DIR}.update-captures`, "retained-run");
    await fs.mkdir(capture, { recursive: true });

    expect(await readManagedHandoffRepairFacts(lease, env)).toEqual({
      runIds: ["retained-run"],
      artifactPaths: [directory, context, capture],
      timeoutMs: null,
    });
  });

  it("correlates ledger drivers only by matching PID, start identity, and hostname", async () => {
    const driver = { ...lease.executor, host: os.hostname() };
    vi.mocked(listUpdateRunsAsync).mockResolvedValue([
      run("wrong-host", { driver: { ...driver, host: "other-host" } }),
      run("wrong-pid", { driver: { ...driver, pid: 1003 } }),
      run("wrong-start", { driver: { ...driver, startIdentity: "other-start" } }),
      run("matched-run", { previousDrivers: [{ ...lease.helper, host: os.hostname() }] }),
    ]);

    expect(await readManagedHandoffRepairFacts(lease, env)).toEqual({
      runIds: ["matched-run"],
      artifactPaths: [lease.key],
      timeoutMs: null,
    });
  });

  it.each([
    {
      name: "recovery",
      recoveryTimeoutMs: 4_000_000,
      parentExitTimeoutMs: 1_000,
      commandArgv: ["--timeout", "2"],
      expected: 4_000_000,
    },
    {
      name: "parent",
      recoveryTimeoutMs: 1_000,
      parentExitTimeoutMs: 5_000_000,
      commandArgv: ["--timeout", "2"],
      expected: 5_000_000,
    },
    {
      name: "command",
      recoveryTimeoutMs: 1_000,
      parentExitTimeoutMs: 2_000,
      commandArgv: ["node", "update", "--timeout", "8000", "--timeout=7200"],
      expected: 8_000_000,
    },
  ])("preserves the larger recorded $name phase budget", async ({ name, expected, ...fields }) => {
    await helper(name, fields);
    expect((await readManagedHandoffRepairFacts(lease, env)).timeoutMs).toBe(expected);
  });

  it("refuses conflicting helper and ledger run identities", async () => {
    await helper("matching", { runId: "helper-run" });
    vi.mocked(listUpdateRunsAsync).mockResolvedValue([
      run("ledger-run", { driver: { ...lease.executor, host: os.hostname() } }),
    ]);
    await expect(readManagedHandoffRepairFacts(lease, env)).rejects.toThrow(
      "Cannot identify handoff run",
    );
  });

  it("preserves unresolved capture restoration instead of selecting current-installation repair", async () => {
    vi.mocked(listUpdateRunsAsync).mockResolvedValue([
      run("captured-run", {
        driver: { ...lease.executor, host: os.hostname() },
        updateRecoveryCapture: {
          manifestSha256: "a".repeat(64),
          configWrites: [],
          status: "restore-failed",
        },
      }),
    ]);
    await expect(readManagedHandoffRepairFacts(lease, env)).rejects.toThrow("retains restoration");
  });

  it("preserves a native recovery owner's admission refusal", async () => {
    await helper("known-native");
    const refusal = new Error("Retained native recovery still owns its artifacts");
    vi.mocked(assertUpdateRecoveryAdmission).mockRejectedValueOnce(refusal);
    await expect(readManagedHandoffRepairFacts(lease, env)).rejects.toBe(refusal);
  });

  it.each(["unreadable", "invalid-json", "invalid-owner"])(
    "keeps an %s helper directory and installation root in the census scope",
    async (kind) => {
      vi.mocked(listUpdateRunsAsync).mockResolvedValue([
        run("retained-run", { driver: { ...lease.executor, host: os.hostname() } }),
      ]);
      const directory = await helper(kind);
      const file = path.join(directory, "handoff.json");
      if (kind === "unreadable") {
        await fs.unlink(file);
      } else {
        await fs.writeFile(file, kind === "invalid-json" ? "{" : '{"updateLeaseOwner":123}');
      }
      expect(await readManagedHandoffRepairFacts(lease, env)).toEqual({
        runIds: ["retained-run"],
        artifactPaths: [directory, lease.key],
        timeoutMs: null,
      });
    },
  );

  it("refuses a missing original run identity instead of substituting the lease owner", async () => {
    await expect(readManagedHandoffRepairFacts(lease, env)).rejects.toThrow(
      "Cannot identify handoff run",
    );
  });

  it("rechecks retained capture custody using a generation-bound run identity", async () => {
    vi.mocked(listUpdateRunsAsync).mockResolvedValue([
      run("bound-run", {
        updateRecoveryCapture: {
          manifestSha256: "a".repeat(64),
          configWrites: [],
          status: "restore-failed",
        },
      }),
    ]);
    await expect(readManagedHandoffRepairFacts(lease, env, "bound-run")).rejects.toThrow(
      "retains restoration",
    );
    expect(listUpdateRunsAsync).toHaveBeenCalledWith(
      { limit: 100, includeRunId: "bound-run" },
      { env },
    );
  });

  it("uses the CLI timeout parser for retained command arguments", async () => {
    await helper("invalid-duration", {
      commandArgv: ["--timeout=9000.5", "--timeout", "Infinity"],
    });
    expect((await readManagedHandoffRepairFacts(lease, env)).timeoutMs).toBeNull();
  });
});
