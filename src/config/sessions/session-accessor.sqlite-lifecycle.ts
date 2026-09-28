import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import { parseAgentSessionKey } from "../../routing/session-key.js";
import {
  isAgentHarnessSessionKey,
  isValidAgentHarnessSessionStoreEntry,
  MODEL_SELECTION_LOCK_REMOVAL_MESSAGE,
  resolveAgentHarnessSessionStoreEntryError,
} from "../../sessions/agent-harness-session-key.js";
import { emitSessionIdentityMutation } from "../../sessions/session-lifecycle-events.js";
import { readOpenClawAgentDatabaseIdentity } from "../../state/openclaw-agent-db-identity.js";
import { withOpenClawAgentDatabaseReadOnly } from "../../state/openclaw-agent-db-readonly.js";
import {
  deferOpenClawAgentPostCommitPublication,
  openOpenClawAgentDatabase,
  type OpenClawAgentDatabase,
} from "../../state/openclaw-agent-db.js";
import type { ResetSessionEntryLifecycleMutation } from "./session-accessor.lifecycle-types.js";
import { publishSessionStateArchives } from "./session-accessor.sqlite-archive-store.js";
import { materializeSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import type {
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
  ResetSessionEntryLifecycleParams,
  ResetSessionEntryLifecycleResult,
  SessionLifecycleArtifactCleanupParams,
  SessionLifecycleArtifactCleanupResult,
  SqliteSessionArtifactPreparationDiagnostics,
  SqliteSessionReclamationDiagnostics,
} from "./session-accessor.sqlite-contract.js";
import {
  hasPreparedNativeSessionDeletion,
  runSqliteSessionDeletionTransaction as runOpenClawAgentWriteTransaction,
  withSqliteSessionDeletions,
} from "./session-accessor.sqlite-deletion.js";
import { sqliteSessionEntriesEqual } from "./session-accessor.sqlite-entry-equality.js";
import {
  assertLifecycleTargetUnchanged,
  readLifecycleTargetSnapshot,
  writeSessionEntry,
} from "./session-accessor.sqlite-entry-store.js";
import { prepareSessionLifecycleArtifactCleanup } from "./session-accessor.sqlite-lifecycle-artifacts.js";
import { deleteSqliteSessionEntryLifecycleLocked } from "./session-accessor.sqlite-lifecycle-delete.js";
import { refreshSqliteSessionPlannerStatisticsBestEffort } from "./session-accessor.sqlite-maintenance.js";
import { runSqliteSessionReclamation } from "./session-accessor.sqlite-reclamation-run.js";
import {
  createLifecycleArtifactReclamationPlan,
  runExclusiveSqliteSessionReclamation,
} from "./session-accessor.sqlite-reclamation.js";
import { appendSessionResetBoundary } from "./session-accessor.sqlite-reset-boundary.js";
import {
  captureLifecycleDatabaseScope,
  resolveSqliteReadScope,
  resolveSqliteScope,
  resolveSqliteStoreScope,
  resolveSqliteTranscriptArchiveDirectory,
  runExclusiveSqliteSessionWrite,
  toDatabaseOptions,
} from "./session-accessor.sqlite-scope.js";
import { kickSessionHistoryDiskBudgetMaintenance } from "./session-history-eviction.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";

// Single-target lifecycle owner: cleanup, reset, guarded delete, and trusted rollback.

async function withCommittedHistoryMaintenance<T>(
  { agentId, env, storePath }: { agentId?: string; env?: NodeJS.ProcessEnv; storePath: string },
  run: (
    recordCommit: (database: OpenClawAgentDatabase) => void,
    markCommitted: () => void,
  ) => Promise<T>,
): Promise<T> {
  let committed = false;
  try {
    return await run(
      (database) => {
        deferOpenClawAgentPostCommitPublication(database, () => {
          committed = true;
        });
      },
      () => {
        committed = true;
      },
    );
  } finally {
    // A partial commit still needs maintenance, but only after archive publication and
    // lifecycle-owner cleanup finish. Rejected preparation or rollback creates no pressure.
    if (committed) {
      kickSessionHistoryDiskBudgetMaintenance({ agentId, env, storePath, force: true });
    }
  }
}

export async function cleanupSessionLifecycleArtifactsCore(
  params: SessionLifecycleArtifactCleanupParams,
): Promise<SessionLifecycleArtifactCleanupResult> {
  const sessionKeySegmentPrefix = params.sessionKeySegmentPrefix.trim();
  const transcriptContentMarker = params.transcriptContentMarker;
  const pluginOwnerId = params.pluginOwnerId?.trim();
  if (!sessionKeySegmentPrefix || !transcriptContentMarker) {
    return { removedEntries: 0, archivedTranscriptArtifacts: 0 };
  }

  const resolved = captureLifecycleDatabaseScope(
    resolveSqliteReadScope({
      ...(params.agentId ? { agentId: params.agentId } : {}),
      storePath: params.storePath,
    }),
  );
  const databaseOptions = toDatabaseOptions(resolved);
  const artifactPreparation: SqliteSessionArtifactPreparationDiagnostics = {};
  const cleanupPlan = await runExclusiveSqliteSessionWrite(
    resolved,
    async () =>
      prepareSessionLifecycleArtifactCleanup(databaseOptions, {
        ...(params.agentId !== undefined ? { agentId: resolved.agentId } : {}),
        archiveRemovedEntryTranscripts: params.archiveRemovedEntryTranscripts !== false,
        archiveDirectory: resolveSqliteTranscriptArchiveDirectory(resolved),
        ...(pluginOwnerId ? { pluginOwnerId } : {}),
        sessionKeySegmentPrefix,
        transcriptContentMarker,
        orphanTranscriptMinAgeMs: params.orphanTranscriptMinAgeMs,
        nowMs: params.nowMs ?? Date.now(),
        diagnostics: artifactPreparation,
      }),
    "session.lifecycle.artifacts-prepare",
    { artifactPreparation },
  );
  if (cleanupPlan.entries.length === 0 && cleanupPlan.deletePlans.length === 0) {
    // Startup probes need no reclamation Worker, but previously committed archives
    // still need their publication retry even when this pass has no deletions.
    await publishSessionStateArchives(resolved, []);
    return { removedEntries: 0, archivedTranscriptArtifacts: 0 };
  }
  const committed = await withSqliteSessionDeletions(
    resolved,
    cleanupPlan.entries.flatMap(({ expectedEntry: entry, sessionKey }) =>
      entry ? [{ entry, sessionKey }] : [],
    ),
    async (assertCurrent) =>
      await runExclusiveSqliteSessionReclamation(async () => {
        const materializedPlans = await materializeSessionStateDeletePlans(cleanupPlan.deletePlans);
        const diagnostics: SqliteSessionReclamationDiagnostics = {};
        const plan = createLifecycleArtifactReclamationPlan({
          agentId: resolved.agentId,
          databaseOptions,
          entries: cleanupPlan.entries,
          materializedPlans,
        });
        const reclaimed = await runSqliteSessionReclamation({
          diagnostics,
          assertCommitAllowed: assertCurrent,
          forceInProcess: hasPreparedNativeSessionDeletion(),
          plan,
        });
        if (reclaimed.kind !== plan.kind) {
          throw new Error(`SQLite session reclamation returned ${reclaimed.kind} for ${plan.kind}`);
        }
        return reclaimed.value;
      }),
    { additionalIdentities: cleanupPlan.deletePlans.map((plan) => plan.sessionId) },
  );
  // The SQL commit survives a later archive-publication failure, so refresh
  // planner statistics before crossing that separate artifact boundary.
  const deletedEntries = Math.max(
    committed.removedEntries,
    new Set(cleanupPlan.deletePlans.map((plan) => plan.sessionId)).size,
  );
  await refreshSqliteSessionPlannerStatisticsBestEffort(resolved, deletedEntries);
  const archivedTranscripts = await publishSessionStateArchives(
    resolved,
    committed.archivedTranscripts,
  );
  return {
    removedEntries: committed.removedEntries,
    archivedTranscriptArtifacts: archivedTranscripts.length,
  };
}

/** Resets one persisted session entry using SQLite session rows. */
export async function resetSessionEntryLifecycle(
  params: ResetSessionEntryLifecycleParams,
): Promise<ResetSessionEntryLifecycleResult> {
  const agentId = params.agentId ?? parseAgentSessionKey(params.target.canonicalKey)?.agentId;
  const resolved = resolveSqliteStoreScope(params.storePath, { agentId });
  if (params.resetBoundary) {
    params.commitGuard?.();
    const source = withOpenClawAgentDatabaseReadOnly(
      (database) => readLifecycleTargetSnapshot(database, params.target)[0]?.entry.sessionId,
      toDatabaseOptions(resolved),
    );
    if (source.found && source.value) {
      const { restoreSessionColdTranscript } = await import("./session-cold-storage.js");
      await restoreSessionColdTranscript({
        agentId: resolved.agentId,
        env: resolved.env,
        storePath: params.storePath,
        sessionId: source.value,
      });
    }
  }
  return await withCommittedHistoryMaintenance(
    { agentId: resolved.agentId, storePath: params.storePath },
    async (recordCommit) =>
      runExclusiveSqliteSessionWrite(
        resolved,
        async () => {
          params.commitGuard?.();
          const database = openOpenClawAgentDatabase(toDatabaseOptions(resolved));
          const targetSnapshot = readLifecycleTargetSnapshot(database, params.target);
          const current = targetSnapshot[0];
          const nextEntry = await params.buildNextEntry({
            currentEntry: current ? structuredClone(current.entry) : undefined,
            primaryKey: params.target.canonicalKey,
          });
          const shouldAppendResetBoundary =
            params.resetBoundary &&
            current?.entry.sessionId &&
            !sqliteSessionEntriesEqual(current.entry, nextEntry);
          const mutation: ResetSessionEntryLifecycleMutation = {
            nextEntry: structuredClone(nextEntry),
            ...(current ? { previousEntry: structuredClone(current.entry) } : {}),
            ...(current?.entry.sessionId ? { previousSessionId: current.entry.sessionId } : {}),
          };
          const databaseIdentity = runOpenClawAgentWriteTransaction(
            (transactionDb) => {
              params.commitGuard?.();
              assertLifecycleTargetUnchanged(transactionDb, params.target, current?.entry, "reset");
              if (shouldAppendResetBoundary && current?.entry.sessionId && params.resetBoundary) {
                const boundaryScope = {
                  ...resolved,
                  sessionId: current.entry.sessionId,
                  sessionKey: current.sessionKey,
                };
                appendSessionResetBoundary(
                  transactionDb,
                  boundaryScope,
                  current.entry,
                  params.resetBoundary,
                );
              }
              writeSessionEntry(transactionDb, params.target.canonicalKey, nextEntry, {
                previousEntry: current?.entry ?? null,
              });
              recordCommit(transactionDb);
              // Reset only advances the live entry and route. Historical rows stay searchable;
              // disk-budget cleanup owns durable extraction before reclaiming them.
              return readOpenClawAgentDatabaseIdentity(transactionDb).identity;
            },
            toDatabaseOptions(resolved),
            { operationLabel: "session.lifecycle.reset" },
          );
          emitSessionIdentityMutation({
            agentId: resolved.agentId,
            databaseIdentity,
            kind: current ? "reset" : "create",
            previous: current
              ? {
                  ...(current.entry.sessionId ? { sessionId: current.entry.sessionId } : {}),
                  sessionKeys: targetSnapshot.map((row) => row.sessionKey),
                }
              : { sessionKeys: [] },
            current: {
              ...(nextEntry.sessionId ? { sessionId: nextEntry.sessionId } : {}),
              sessionKeys: [params.target.canonicalKey],
            },
          });
          await params.afterEntryMutation?.(mutation);
          return {
            ...mutation,
            archivedTranscripts: [],
          };
        },
        "session.lifecycle.reset",
      ),
  );
}

async function deleteSqliteSessionEntryLifecycleInternal(
  params: DeleteSessionEntryLifecycleParams,
  allowLockedEntryRemoval: boolean,
  expectedPluginOwnerId?: string,
): Promise<DeleteSessionEntryLifecycleResult> {
  const agentId = params.agentId ?? parseAgentSessionKey(params.target.canonicalKey)?.agentId;
  const resolved = captureLifecycleDatabaseScope(
    resolveSqliteScope({ agentId, env: params.env, sessionKey: "", storePath: params.storePath }),
  );
  return await withCommittedHistoryMaintenance(
    { ...params, env: resolved.env },
    async (recordCommit, markCommitted) =>
      deleteSqliteSessionEntryLifecycleLocked(
        resolved,
        params,
        allowLockedEntryRemoval,
        expectedPluginOwnerId,
        { recordCommit, markCommitted },
      ),
  );
}

/** Deletes one persisted session entry using SQLite session rows. */
export async function deleteSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams,
): Promise<DeleteSessionEntryLifecycleResult> {
  return await deleteSqliteSessionEntryLifecycleInternal(params, false);
}

/** Rolls back one exact locked row created by failed trusted harness initialization. */
export async function rollbackAgentHarnessSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams & { expectedEntry: SessionEntry },
): Promise<DeleteSessionEntryLifecycleResult> {
  const hasExactTarget =
    params.target.storeKeys.length === 1 &&
    params.target.storeKeys[0] === params.target.canonicalKey;
  const expectedEntryError = resolveAgentHarnessSessionStoreEntryError(
    params.target.canonicalKey,
    params.expectedEntry,
  );
  if (
    !hasExactTarget ||
    expectedEntryError ||
    !isValidAgentHarnessSessionStoreEntry(params.target.canonicalKey, params.expectedEntry)
  ) {
    throw new Error(expectedEntryError ?? MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
  }
  return await deleteSqliteSessionEntryLifecycleInternal(params, true);
}

/** Rolls back one exact locked CLI row created by a failed plugin initializer. */
export async function rollbackPluginOwnedSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams & {
    expectedEntry: SessionEntry;
    expectedPluginOwnerId: string;
  },
): Promise<DeleteSessionEntryLifecycleResult> {
  const expectedEntry = params.expectedEntry;
  const validPluginOwner = normalizeOptionalString(expectedEntry.pluginOwnerId);
  const expectedPluginOwner = normalizeOptionalString(params.expectedPluginOwnerId);
  if (
    isAgentHarnessSessionKey(params.target.canonicalKey) ||
    expectedEntry.agentHarnessId !== undefined ||
    expectedEntry.modelSelectionLocked !== true ||
    !validPluginOwner ||
    validPluginOwner !== expectedPluginOwner
  ) {
    throw new Error(MODEL_SELECTION_LOCK_REMOVAL_MESSAGE);
  }
  return await deleteSqliteSessionEntryLifecycleInternal(params, true, expectedPluginOwner);
}
