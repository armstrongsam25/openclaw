import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, assert, expect, it, vi } from "vitest";
import { useAutoCleanupTempDirTracker } from "../../../test/helpers/temp-dir.js";
import {
  captureManagedUpdateLeaseDatabaseIdentity,
  createManagedHandoffLeaseDatabase,
} from "../../infra/update-managed-service-handoff-database.js";
import { createManagedHandoffLeaseStore } from "../../infra/update-managed-service-handoff-lease.js";
import { childLineageDigest } from "./update-command-executor-children.js";
import { resolveUpdateCommandChildBinding } from "./update-command-executor-grant.js";

const dirs = useAutoCleanupTempDirTracker(afterEach);
afterEach(() => vi.restoreAllMocks());

it.each(["9.6 numeric", "9.6 bridge", "9.7 exact"] as const)(
  "admits a %s grant and fences parent replacement",
  (version) => {
    const root = fs.realpathSync(dirs.make("numeric-lease-parent-"));
    const directory = path.join(root, "leases");
    fs.mkdirSync(directory, { mode: 0o700 });
    const databasePath = path.join(directory, "managed-update-handoffs.sqlite");
    const store = createManagedHandoffLeaseStore({ databasePath, serviceManagerEnv: {} });
    const executor = store.processIdentity(process.ppid);
    const owner = randomUUID();
    const runId = randomUUID();
    const spawnerKey =
      version === "9.6 bridge" ? `${root}/.openclaw-update-child-${randomUUID()}` : root;
    const payload = JSON.stringify({
      version: 2,
      helper: executor,
      executor,
      action: {
        kind: "update",
        ...(version === "9.7 exact" ? { mutationProtocol: "original-cancellation-v1" } : {}),
      },
    });
    const write = createManagedHandoffLeaseDatabase(databasePath);
    write(true, (db) => {
      db.prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)").run(
        root,
        owner,
        payload,
        1,
      );
      if (spawnerKey !== root) {
        db.prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)").run(
          spawnerKey,
          runId,
          payload,
          1,
        );
      }
    });
    const parent = store.read(root);
    assert(parent.kind === "current");
    const spawner = store.read(spawnerKey);
    assert(spawner.kind === "current");

    // Model NTFS metadata only; the grant, live rows and candidate admission are real.
    const lstat = fs.lstatSync;
    let parentInode = 168040561096346671n;
    let readError: Error | undefined;
    vi.spyOn(fs, "lstatSync").mockImplementation((...args) => {
      if (String(args[0]) === directory && readError) {
        throw readError;
      }
      const stat = lstat(...args);
      if (stat && String(args[0]) === directory) {
        Object.defineProperty(stat, "ino", {
          value: typeof stat.ino === "bigint" ? parentInode : Number(parentInode),
        });
      }
      return stat;
    });
    // Published v2026.9.6 used numeric lstatSync for both transported identities.
    const numericIdentity = (pathname: string) => {
      const stat = fs.lstatSync(pathname);
      return `${stat.dev}:${stat.ino}`;
    };
    const databaseIdentity =
      version !== "9.7 exact"
        ? {
            databasePath,
            databaseIdentity: numericIdentity(databasePath),
            parentIdentity: numericIdentity(directory),
          }
        : captureManagedUpdateLeaseDatabaseIdentity(databasePath);
    expect(databaseIdentity.parentIdentity).toMatch(
      version !== "9.7 exact" ? /:168040561096346660$/ : /:168040561096346671$/,
    );
    const childKey = `${spawnerKey}/.openclaw-update-child-${randomUUID()}-lineage-${childLineageDigest(parent.lease, spawner.lease, parent.lease, databaseIdentity)}`;
    const db = new DatabaseSync(databasePath);
    try {
      db.prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)").run(
        childKey,
        runId,
        payload,
        2,
      );
    } finally {
      db.close();
    }
    const grant = {
      runId,
      root,
      databasePath,
      databaseIdentity,
      parent: parent.lease,
      originalParent: parent.lease,
      spawner: spawner.lease,
      childKey,
      originalChildKey: childKey,
    };
    const admitted = resolveUpdateCommandChildBinding(grant, runId, root);
    assert(admitted.databaseIdentity);
    expect(admitted.databaseIdentity.parentIdentity).toMatch(/^\d+:168040561096346671$/);
    expect(admitted.store.read(root)).toMatchObject({ kind: "current" });
    const descendantKey = `${childKey}/.openclaw-update-child-${randomUUID()}-lineage-${childLineageDigest(parent.lease, admitted.child, parent.lease, admitted.databaseIdentity)}`;
    const descendants = new DatabaseSync(databasePath);
    try {
      descendants
        .prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)")
        .run(descendantKey, runId, payload, 3);
    } finally {
      descendants.close();
    }
    const descendantGrant = {
      ...grant,
      spawner: admitted.child,
      databaseIdentity: admitted.databaseIdentity,
      originalChildKey: descendantKey,
      childKey: descendantKey,
    };
    expect(resolveUpdateCommandChildBinding(descendantGrant, runId, root).child.key).toBe(
      descendantKey,
    );
    parentInode -= 1n;
    expect(numericIdentity(directory)).toMatch(/:168040561096346660$/);
    expect(() => admitted.store.acquire(root, "replacement", { kind: "update" })).toThrow(
      "identity changed",
    );
    expect(() => resolveUpdateCommandChildBinding(descendantGrant, runId, root)).toThrow(
      "identity changed",
    );
    if (version === "9.7 exact") {
      expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(
        "identity changed",
      );
    }
    parentInode += 4096n;
    expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow("identity changed");
    readError = new Error("lease parent metadata unavailable");
    expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(readError.message);
    readError = undefined;
    parentInode = 168040561096346671n;
    const damaged = new DatabaseSync(databasePath);
    try {
      damaged.exec("DROP TABLE managed_update_handoffs");
    } finally {
      damaged.close();
    }
    expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(
      /lease is unreadable:.*no such table/i,
    );
  },
);
