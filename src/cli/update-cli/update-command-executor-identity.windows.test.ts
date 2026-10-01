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
import {
  childLineageDigest,
  type UpdateCommandChildGrant,
} from "./update-command-executor-children.js";
import { resolveUpdateCommandChildBinding } from "./update-command-executor-grant.js";

const dirs = useAutoCleanupTempDirTracker(afterEach);
afterEach(() => vi.restoreAllMocks());

it.each(["9.4 identity-less", "9.6 numeric", "9.6 bridge", "9.7 exact"] as const)(
  "admits a %s grant and fences parent replacement",
  (version) => {
    const identityLess = version === "9.4 identity-less";
    const numeric = version === "9.6 numeric" || version === "9.6 bridge";
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
    const initialInode = identityLess ? 9007199254740992n : 168040561096346671n;
    let parentInode = initialInode;
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
    const databaseIdentity = numeric
      ? {
          databasePath,
          databaseIdentity: numericIdentity(databasePath),
          parentIdentity: numericIdentity(directory),
        }
      : captureManagedUpdateLeaseDatabaseIdentity(databasePath);
    expect(databaseIdentity.parentIdentity).toMatch(
      numeric ? /:168040561096346660$/ : new RegExp(`:${initialInode}$`),
    );
    const childKey = `${spawnerKey}/.openclaw-update-child-${randomUUID()}${identityLess ? "" : `-lineage-${childLineageDigest(parent.lease, spawner.lease, parent.lease, databaseIdentity)}`}`;
    const childPayload = identityLess
      ? JSON.stringify({
          version: 2,
          helper: executor,
          executor: store.processIdentity(process.pid),
          action: { kind: "update" },
        })
      : payload;
    const db = new DatabaseSync(databasePath);
    try {
      db.prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)").run(
        childKey,
        runId,
        childPayload,
        2,
      );
    } finally {
      db.close();
    }
    const grant: UpdateCommandChildGrant = {
      runId,
      root,
      databasePath,
      parent: parent.lease,
      childKey,
      ...(identityLess
        ? {}
        : {
            databaseIdentity,
            originalParent: parent.lease,
            spawner: spawner.lease,
            originalChildKey: childKey,
          }),
    };
    const admitted = resolveUpdateCommandChildBinding(grant, runId, root);
    assert(admitted.databaseIdentity);
    expect(admitted.databaseIdentity.parentIdentity).toMatch(new RegExp(`^\\d+:${initialInode}$`));
    expect(admitted.store.read(root)).toMatchObject({ kind: "current" });
    const descendantKey = `${childKey}/.openclaw-update-child-${randomUUID()}-lineage-${childLineageDigest(parent.lease, admitted.child, parent.lease, admitted.databaseIdentity)}`;
    const descendants = new DatabaseSync(databasePath);
    try {
      descendants.prepare("INSERT INTO managed_update_handoffs VALUES (?, ?, ?, ?)").run(
        descendantKey,
        runId,
        JSON.stringify({
          version: 2,
          helper: admitted.child.executor,
          executor: admitted.child.executor,
          action: admitted.child.action,
        }),
        3,
      );
    } finally {
      descendants.close();
    }
    const descendantGrant = {
      ...grant,
      originalParent: parent.lease,
      spawner: admitted.child,
      databaseIdentity: admitted.databaseIdentity,
      originalChildKey: descendantKey,
      childKey: descendantKey,
    };
    const resolveDescendant = () => {
      // Model the next receiver's parent PID without booting another source runtime.
      const descriptor = Object.getOwnPropertyDescriptor(process, "ppid");
      assert(descriptor);
      Object.defineProperty(process, "ppid", {
        configurable: true,
        value: admitted.child.executor.pid,
      });
      try {
        return resolveUpdateCommandChildBinding(descendantGrant, runId, root);
      } finally {
        Object.defineProperty(process, "ppid", descriptor);
      }
    };
    expect(resolveDescendant().child.key).toBe(descendantKey);
    parentInode += identityLess ? 1n : -1n;
    expect(numericIdentity(directory)).toMatch(
      identityLess ? /:9007199254740992$/ : /:168040561096346660$/,
    );
    expect(() => admitted.store.acquire(root, "replacement", { kind: "update" })).toThrow(
      "identity changed",
    );
    expect(resolveDescendant).toThrow("identity changed");
    if (version === "9.7 exact") {
      expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(
        "identity changed",
      );
    }
    parentInode += 4096n;
    if (!identityLess) {
      expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(
        "identity changed",
      );
    }
    readError = new Error("lease parent metadata unavailable");
    expect(() => resolveUpdateCommandChildBinding(grant, runId, root)).toThrow(readError.message);
    readError = undefined;
    parentInode = initialInode;
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
