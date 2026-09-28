import type { SqliteWalPeriodicRequest } from "../infra/sqlite-wal-write-admission.js";
import { createSqliteWorkerOperationAdmission } from "../infra/sqlite-worker-operation-admission.js";
import { StateDatabaseReadAdmissionInvalidatedError } from "./openclaw-state-db-async-lifecycle.js";
import type { OpenClawStateWorkerContext } from "./openclaw-state-worker-context.types.js";
import type { OpenClawStateWorkerInspectionOperations } from "./openclaw-state-worker-contract.js";
import { hydrateOpenClawStateWorkerError } from "./openclaw-state-worker-error.js";
import {
  runWithCapturedWorkerContext,
  runWithOpenClawStateWorkerStore,
} from "./openclaw-state-worker-operation.js";
import { getOpenClawStateWorkerOwner as owner } from "./openclaw-state-worker-owner.js";
import { runOpenClawStateWorkerOperation } from "./openclaw-state-worker-store.js";

/** Inspect the existing file without recursively admitting a domain operation. */
export async function inspectOpenClawStateDatabase(
  context: OpenClawStateWorkerContext,
  command: {
    type: "database.generationMatches";
    input: OpenClawStateWorkerInspectionOperations["database.generationMatches"]["input"];
  },
): Promise<boolean | undefined> {
  return runWithCapturedWorkerContext(context, async () => {
    try {
      const store = await owner().open(context, { existingOnly: true });
      context.admission.assertCurrent();
      if (!store) {
        return undefined;
      }
      const releaseOperation = owner().retainOperation(store);
      try {
        context.admission.assertCurrent();
        return await runWithOpenClawStateWorkerStore(store, context, (scope) =>
          scope.execute(command),
        );
      } finally {
        void releaseOperation();
      }
    } catch (error) {
      throw hydrateOpenClawStateWorkerError(error);
    }
  });
}

export function runStateDatabaseWalMaintenance(params: {
  context: OpenClawStateWorkerContext;
  request: SqliteWalPeriodicRequest;
  signal: AbortSignal;
  nativeLocations: string[];
  assertCurrent: () => void;
  onStarted: () => void;
}) {
  const { context, request, signal, nativeLocations, assertCurrent, onStarted } = params;
  return runOpenClawStateWorkerOperation(
    context,
    (worker) => worker.execute({ type: "database.walMaintenance", input: request }, { signal }),
    {
      existingOnly: true,
      assertCurrent,
      createAdmission: () => ({
        nativeLocations,
        admission: createSqliteWorkerOperationAdmission((_request, grant) => {
          assertCurrent();
          if (!grant()) {
            throw new StateDatabaseReadAdmissionInvalidatedError(
              "Shared-state WAL maintenance authority expired",
            );
          }
          onStarted();
        }),
      }),
    },
  );
}
