import { isMainThread } from "node:worker_threads";
import { parseAgentSessionKey } from "../../routing/session-key.js";
import { collectActiveSessionWorkAdmissions } from "../../sessions/session-lifecycle-admission.js";
import { preparePersonalGitHubSessionReceiptDeletion } from "../../state/github-personal-publication-lifecycle.js";
import type { OpenClawAgentDatabase } from "../../state/openclaw-agent-db-contract.js";
import { readOpenClawAgentDatabaseIdentity } from "../../state/openclaw-agent-db-identity.js";
import { getOpenClawAgentDatabaseIfOpen } from "../../state/openclaw-agent-db.js";
import type { AgentDatabaseExecutionFileIdentity } from "../../state/openclaw-agent-execution-contract.js";
import {
  captureOpenClawAgentDatabaseExecution,
  supportsOpenClawAgentDatabaseExecution,
} from "../../state/openclaw-agent-execution.js";
import { withSqliteTranscriptArchiveSession } from "./session-accessor.sqlite-archive-session.js";
import { publishSessionStateArchives } from "./session-accessor.sqlite-archive-store.js";
import { materializeSessionStateDeletePlans } from "./session-accessor.sqlite-archive.js";
import type {
  SessionLifecycleArchivedTranscript,
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
  SqliteSessionReclamationDiagnostics,
  ResolvedSqliteScope,
} from "./session-accessor.sqlite-contract.js";
import {
  hasPreparedNativeSessionDeletion,
  withSqliteSessionDeletions,
} from "./session-accessor.sqlite-deletion.js";
import { emitArchivedTranscriptUpdates } from "./session-accessor.sqlite-events.js";
import { publishCommittedSessionEntryRemoval } from "./session-accessor.sqlite-identity.js";
import {
  runSessionDeletionPlanning,
  runSqliteSessionReclamation,
} from "./session-accessor.sqlite-reclamation-run.js";
import {
  createHistoricalGenerationReclamationPlan,
  createSessionEntryReclamationPlan,
  expectedEntryMismatchResult,
  prepareHistoricalGenerationDeletions,
  runExclusiveSqliteSessionReclamation,
  resolveSessionReclamationDatabaseOptions,
} from "./session-accessor.sqlite-reclamation.js";
import { prepareSessionEntryReplacementDatabase } from "./session-accessor.sqlite-replacement-worker.js";
import {
  captureLifecycleDatabaseScope,
  resolveSqliteAgentId,
  resolveSqliteTranscriptArchiveDirectory,
  toDatabaseOptions,
  type resolveSqliteStoreScope,
} from "./session-accessor.sqlite-scope.js";

const DELETE_EXPECTED_ENTRY_MISMATCH = Symbol("delete-expected-entry-mismatch");

export async function deleteSqliteSessionEntryLifecycleLocked(
  requestedScope: ReturnType<typeof resolveSqliteStoreScope>,
  params: DeleteSessionEntryLifecycleParams,
  allowLockedEntryRemoval: boolean,
  expectedPluginOwnerId: string | undefined,
  committed?: {
    recordCommit: (database: OpenClawAgentDatabase) => void;
    markCommitted: () => void;
  },
): Promise<DeleteSessionEntryLifecycleResult> {
  const requestedDatabaseOptions = toDatabaseOptions(requestedScope);
  const useWorker =
    isMainThread &&
    params.expectedDatabaseIdentity === undefined &&
    supportsOpenClawAgentDatabaseExecution(requestedDatabaseOptions);
  const opened = useWorker ? getOpenClawAgentDatabaseIfOpen(requestedDatabaseOptions) : undefined;
  const openedIdentity = opened ? readOpenClawAgentDatabaseIdentity(opened) : undefined;
  const expectedIdentity: AgentDatabaseExecutionFileIdentity | undefined =
    openedIdentity && typeof openedIdentity.identity === "string"
      ? {
          kind: "file",
          physicalIdentity: openedIdentity.identity,
          birthtime: openedIdentity.birthtime,
          nativeLocation: openedIdentity.filename,
        }
      : undefined;
  // An opened alias already selected its native owner; later retargeting cannot redirect this deletion.
  const resolved = expectedIdentity
    ? { ...requestedScope, path: expectedIdentity.nativeLocation }
    : requestedScope;
  const databaseOptions = toDatabaseOptions(resolved);
  const reclamationOptions = useWorker
    ? resolveSessionReclamationDatabaseOptions(databaseOptions)
    : undefined;
  const execution = reclamationOptions
    ? captureOpenClawAgentDatabaseExecution(
        reclamationOptions,
        expectedIdentity ? { expectedIdentity } : {},
      )
    : undefined;
  const assertSourceCurrent = () => {
    execution?.assertCurrent();
    params.commitGuard?.();
  };
  try {
    return await withSqliteTranscriptArchiveSession(databaseOptions, async () => {
      if (reclamationOptions) {
        await prepareSessionEntryReplacementDatabase(
          reclamationOptions,
          assertSourceCurrent,
          execution,
        );
      }
      const {
        commitGuard: _commitGuard,
        env: _env,
        expectedDatabaseIdentity: _expectedDatabaseIdentity,
        descendantRunBasis: _descendantRunBasis,
        ...deleteParams
      } = params;
      const preparation = await runSessionDeletionPlanning(
        resolved,
        params,
        {
          operation: "entry",
          input: {
            deleteParams,
            archiveDirectory: resolveSqliteTranscriptArchiveDirectory(resolved),
            admissionIdentities: [
              ...(collectActiveSessionWorkAdmissions().get(params.storePath) ?? []),
            ],
            allowLockedEntryRemoval,
            expectedPluginOwnerId,
          },
        },
        assertSourceCurrent,
      );
      if (preparation.operation !== "entry") {
        throw new Error(
          `SQLite session deletion planning returned ${preparation.operation} for entry`,
        );
      }
      if (preparation.value.kind === "missing") {
        await publishSessionStateArchives(resolved, []);
        return { archivedTranscripts: [], deleted: false };
      }
      if (preparation.value.kind === "expected-entry-mismatch") {
        await publishSessionStateArchives(resolved, []);
        return expectedEntryMismatchResult([]);
      }
      const prepared = preparation.value.value;

      return await withSqliteSessionDeletions(
        resolved,
        prepared.targetSnapshot,
        async (assertCurrent) => {
          const assertDeletionCurrent = () => {
            assertSourceCurrent();
            assertCurrent();
          };
          const deleteReceipts = await preparePersonalGitHubSessionReceiptDeletion({
            agentId: resolved.agentId,
            env: resolved.env,
            generations: [
              ...new Set([
                params.target.canonicalKey,
                ...params.target.storeKeys,
                ...prepared.targetSnapshot.map((row) => row.sessionKey),
              ]),
            ].map((sessionKey) => {
              const entry =
                prepared.targetSnapshot.find((row) => row.sessionKey === sessionKey)?.entry ??
                prepared.current.entry;
              return {
                sessionKey,
                sessionId: entry.sessionId,
                lifecycleRevision: entry.lifecycleRevision ?? null,
              };
            }),
            assertCurrent: assertDeletionCurrent,
          });
          const validation = {
            deleteParams: params,
            preparedTargetSnapshot: prepared.targetSnapshot,
          };
          const historicalArchivedTranscripts: SessionLifecycleArchivedTranscript[] = [];
          for (const generation of prepareHistoricalGenerationDeletions({
            ...validation,
            sessionIds: prepared.historicalGenerationIds,
          })) {
            const { sessionId } = generation;
            const {
              commitGuard: _generationGuard,
              env: _generationEnv,
              expectedDatabaseIdentity: _generationIdentity,
              descendantRunBasis: _generationBasis,
              ...generationParams
            } = generation.deleteParams;
            const generationValidation = {
              deleteParams: generationParams,
              preparedTargetSnapshot: prepared.targetSnapshot,
              scope: generation.scope,
            };
            const planning = await runSessionDeletionPlanning(
              resolved,
              params,
              {
                operation: "history",
                input: {
                  validation: generationValidation,
                  sessionId,
                  archiveDirectory: prepared.archiveDirectory,
                  archiveTranscript: params.archiveTranscript,
                  admissionIdentities: [
                    ...(collectActiveSessionWorkAdmissions().get(params.storePath) ?? []),
                  ],
                },
              },
              assertDeletionCurrent,
            );
            if (planning.operation !== "history") {
              throw new Error(
                `SQLite session deletion planning returned ${planning.operation} for history`,
              );
            }
            if (planning.value.kind === "expected-entry-mismatch") {
              return expectedEntryMismatchResult(historicalArchivedTranscripts);
            }
            if (planning.value.kind === "skip") {
              continue;
            }
            const plan = planning.value.plan;
            const archivedGeneration = await runExclusiveSqliteSessionReclamation(async () => {
              const materializedGeneration = await materializeSessionStateDeletePlans([plan]);
              const diagnostics: SqliteSessionReclamationDiagnostics = {};
              const checked = await runSessionDeletionPlanning(
                resolved,
                params,
                {
                  operation: "check",
                  input: {
                    validation: generationValidation,
                    sessionId,
                    admissionIdentities: [
                      ...(collectActiveSessionWorkAdmissions().get(params.storePath) ?? []),
                    ],
                  },
                },
                assertDeletionCurrent,
                diagnostics,
              );
              if (checked.operation !== "check") {
                throw new Error(
                  `SQLite session deletion planning returned ${checked.operation} for check`,
                );
              }
              if (checked.value.kind === "expected-entry-mismatch") {
                return DELETE_EXPECTED_ENTRY_MISMATCH;
              }
              const reclamationPlan = createHistoricalGenerationReclamationPlan({
                databaseOptions,
                deleteParams: generation.deleteParams,
                materializedPlans: materializedGeneration,
                preparedTargetSnapshot: prepared.targetSnapshot,
                protectedSessionIds: new Set(checked.value.protectedSessionIds),
                sessionId,
              });
              const reclaimed = await runSqliteSessionReclamation({
                diagnostics,
                assertCommitAllowed: assertDeletionCurrent,
                forceInProcess:
                  typeof params.expectedDatabaseIdentity === "symbol" ||
                  hasPreparedNativeSessionDeletion(),
                onInProcessCommit: committed?.recordCommit,
                plan: reclamationPlan,
              });
              if (reclaimed.kind !== reclamationPlan.kind) {
                throw new Error(
                  `SQLite session reclamation returned ${reclaimed.kind} for ${reclamationPlan.kind}`,
                );
              }
              return reclaimed.value;
            });
            if (archivedGeneration === DELETE_EXPECTED_ENTRY_MISMATCH) {
              return expectedEntryMismatchResult(historicalArchivedTranscripts);
            }
            if (archivedGeneration.expectedEntryMismatch) {
              return expectedEntryMismatchResult(historicalArchivedTranscripts);
            }
            if (archivedGeneration.deleted) {
              committed?.markCommitted();
            }
            // Publish each committed generation immediately: a later archive or
            // transaction failure aborts the deletion, and observers must still see
            // the removals that already happened (retry completes the remainder).
            const publishedGeneration = await publishSessionStateArchives(
              resolved,
              archivedGeneration.archivedTranscripts,
            );
            emitArchivedTranscriptUpdates(publishedGeneration);
            historicalArchivedTranscripts.push(...publishedGeneration);
          }

          // Archive materialization is the expensive phase. It must run between short
          // writer-lane sections so unrelated writes to this store can keep progressing.
          let committedDatabaseIdentity: string | symbol | undefined;
          const result = await runExclusiveSqliteSessionReclamation(async () => {
            const materializedPlans = await materializeSessionStateDeletePlans(prepared.entryPlans);
            const diagnostics: SqliteSessionReclamationDiagnostics = {};
            // The reclamation transaction rereads the exact target immediately before mutation.
            const reclamationPlan = createSessionEntryReclamationPlan({
              databaseOptions,
              deleteParams: params,
              materializedPlans,
              preparedTargetSnapshot: prepared.targetSnapshot,
            });
            const reclaimed = await runSqliteSessionReclamation({
              diagnostics,
              assertCommitAllowed: assertDeletionCurrent,
              forceInProcess:
                typeof params.expectedDatabaseIdentity === "symbol" ||
                hasPreparedNativeSessionDeletion(),
              onInProcessCommit: (database) => {
                committedDatabaseIdentity = readOpenClawAgentDatabaseIdentity(database).identity;
                committed?.recordCommit(database);
              },
              onWorkerResult: (_result, databaseIdentity) => {
                committedDatabaseIdentity = databaseIdentity;
              },
              plan: reclamationPlan,
            });
            if (reclaimed.kind !== reclamationPlan.kind) {
              throw new Error(
                `SQLite session reclamation returned ${reclaimed.kind} for ${reclamationPlan.kind}`,
              );
            }
            return reclaimed.value;
          });
          if (result.deleted) {
            committed?.markCommitted();
            if (committedDatabaseIdentity === undefined) {
              throw new Error("Committed session deletion omitted its database identity");
            }
            // The deletion is committed; observers must invalidate even if receipt cleanup fails.
            publishCommittedSessionEntryRemoval(
              resolved.agentId,
              committedDatabaseIdentity,
              prepared.current.entry.sessionId,
              prepared.targetSnapshot.map((row) => row.sessionKey),
            );
            await deleteReceipts(execution ? () => execution.assertCurrent() : undefined);
          }
          result.archivedTranscripts = await publishSessionStateArchives(
            resolved,
            result.archivedTranscripts,
          );
          emitArchivedTranscriptUpdates(result.archivedTranscripts);
          // Historical generations were emitted per commit above; merge them into
          // the result after the final emit so callers still see every archive.
          result.archivedTranscripts.push(...historicalArchivedTranscripts);
          return result;
        },
        { additionalIdentities: prepared.historicalGenerationIds },
      );
    });
  } finally {
    await execution?.release();
  }
}

/** Disk-budget owner: delete one exact archived row without recursively scheduling another pass. */
export async function deleteDiskBudgetSessionEntryLifecycle(
  params: DeleteSessionEntryLifecycleParams,
  resolved: ResolvedSqliteScope,
): Promise<DeleteSessionEntryLifecycleResult> {
  // A shared store lends its physical owner, not the victim's logical identity.
  // Validate against captured ownership so a custom selector cannot retarget cleanup.
  const targetScope = captureLifecycleDatabaseScope({
    ...resolved,
    agentId: resolveSqliteAgentId({
      scopedAgentId: params.agentId ?? parseAgentSessionKey(params.target.canonicalKey)?.agentId,
      storeAgentId: resolved.databaseAgentId ?? resolved.agentId,
      storeShared: resolved.databaseAgentId !== undefined,
    }),
  });
  return await deleteSqliteSessionEntryLifecycleLocked(targetScope, params, false, undefined);
}
