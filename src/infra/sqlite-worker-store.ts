import { isMainThread } from "node:worker_threads";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { hydrateOpenClawStateWorkerError } from "../state/openclaw-state-worker-error.js";
import { mapRetainedOperation } from "./retained-operation.js";
import { SqliteWorkerBroker } from "./sqlite-worker-broker.js";
import type {
  SqliteWorkerInputPreparation,
  SqliteWorkerInputRetention,
  SqliteWorkerOpenCustody,
  SqliteWorkerStoreOptions,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";
import {
  SqliteWorkerError,
  type SqliteWorkerOperations,
  type SqliteWorkerStore,
} from "./sqlite-worker-contract.js";
import {
  createSqliteWorkerOperationAdmission,
  type SqliteWorkerAdmissionFactory,
  type SqliteWorkerAdmissionRequest,
  type SqliteWorkerOperationAdmission,
} from "./sqlite-worker-operation-admission.js";
import type { RetainedWorkerTransactionAdmission } from "./sqlite-worker-operation-settlement.js";
import type { SqliteWorkerStateContext } from "./sqlite-worker-state-context.js";

function withCallerErrors<T>(result: Promise<T>): Promise<T> {
  return result.catch((error: unknown) => {
    throw hydrateOpenClawStateWorkerError(error);
  });
}

function bindCallerExecute<Operations extends SqliteWorkerOperations>(
  scope: Pick<SqliteWorkerStore<Operations>, "execute">,
): Pick<SqliteWorkerStore<Operations>, "execute"> {
  return {
    execute: (command, options) => {
      const result = withCallerErrors(scope.execute(command, options));
      // The broker also observes abandoned command rejections while draining them.
      void result.catch(() => undefined);
      return result;
    },
  };
}

export {
  SqliteWorkerError,
  type SqliteWorkerBackend,
  type SqliteWorkerCommand,
  type SqliteWorkerOperations,
  type SqliteWorkerStore,
} from "./sqlite-worker-contract.js";

/** Retain one actor through local reconciliation; the callback must not await its own close. */
export function runSqliteWorkerStoreOperation<Operations extends SqliteWorkerOperations, T>(
  store: SqliteWorkerStore<Operations>,
  operation: (scope: Pick<SqliteWorkerStore<Operations>, "execute">) => T | Promise<T>,
  stateContext?: SqliteWorkerStateContext,
  assertCurrent?: (commandType: PropertyKey) => void,
  createAdmission?: SqliteWorkerAdmissionFactory,
): Promise<T> {
  return withCallerErrors(
    resolveSqliteWorkerBroker().runOperation(
      store,
      (scope) => operation(bindCallerExecute(scope)),
      stateContext,
      assertCurrent,
      createAdmission,
    ),
  );
}

function resolveSqliteWorkerBroker() {
  return resolveGlobalSingleton(
    Symbol.for("openclaw.sqliteWorkerBroker"),
    () => new SqliteWorkerBroker(),
    (broker) => withCallerErrors(broker.close()),
  );
}

export type { SqliteWorkerInputPreparation } from "./sqlite-worker-broker.types.js";
export type { SqliteWorkerRetainedReceipt } from "./sqlite-worker-operation-settlement.js";
export type {
  SqliteWorkerRetainedOutcome,
  SqliteWorkerProvisionalReceipt,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";

/** Charge captured input before actor preparation can yield, then hand it to normal dispatch. */
export function reserveSqliteWorkerInputPreparation(
  bytes: number,
  retention: SqliteWorkerInputRetention = "stream",
): SqliteWorkerInputPreparation {
  return resolveSqliteWorkerBroker().reserveInputPreparation(bytes, retention);
}

/**
 * Retain an admitted writer through native settlement. Backends request authority
 * after BEGIN and again immediately before COMMIT; the host never joins a native
 * writer lock. A successful commit grant linearizes against subsequent revocation.
 */
export function runSqliteWorkerStoreWrite<Operations extends SqliteWorkerOperations, T>(
  store: SqliteWorkerStore<Operations>,
  operation: (scope: Pick<SqliteWorkerStore<Operations>, "execute">) => Promise<T>,
  assertCurrent: () => void,
  nativeLocations: readonly string[],
): Promise<T> {
  return runSqliteWorkerStoreOperation(
    store,
    operation,
    undefined,
    assertCurrent,
    createSqliteWorkerWriteAdmission(assertCurrent, nativeLocations),
  );
}

export type SqliteWorkerWriteAdmissionComposer = (
  operation: RetainedWorkerTransactionAdmission,
  prepare: (request: SqliteWorkerAdmissionRequest) => void,
  grant: (grantNative: () => boolean) => void,
) => SqliteWorkerOperationAdmission;

export function createSqliteWorkerWriteAdmission(
  assertCurrent: (request: SqliteWorkerAdmissionRequest) => void,
  nativeLocations: readonly string[],
  compose?: SqliteWorkerWriteAdmissionComposer,
): SqliteWorkerAdmissionFactory {
  return (operation) => {
    let phase: "waiting" | "transaction" | "commit" = "waiting";
    const prepare = (request: SqliteWorkerAdmissionRequest) => {
      if (
        !(
          (phase === "waiting" && request.stage === "transaction") ||
          (phase === "transaction" && request.stage === "commit")
        )
      ) {
        throw new Error("SQLite worker write authority requested out of order");
      }
      assertCurrent(request);
    };
    const grant = (grantNative: () => boolean) => {
      if (!grantNative()) {
        throw new Error("SQLite worker write authority expired");
      }
      phase = phase === "waiting" ? "transaction" : "commit";
    };
    return {
      nativeLocations,
      admission: compose
        ? compose(operation, prepare, grant)
        : createSqliteWorkerOperationAdmission((request, grantNative) => {
            prepare(request);
            grant(grantNative);
          }),
    };
  };
}

/** Read the broker's recorded lifecycle state without probing native storage. */
export function isSqliteWorkerStoreAvailable(store: object): boolean {
  return resolveSqliteWorkerBroker().isAvailable(store);
}

/** Internal identity for the existing canonical actor, never a transferable authority. */
export function getSqliteWorkerActorIdentity(
  store: object,
): ReturnType<SqliteWorkerBroker["getActorIdentity"]> {
  return resolveSqliteWorkerBroker().getActorIdentity(store);
}

export function retireSqliteWorkerActor(identity: object): Promise<void> {
  return withCallerErrors(resolveSqliteWorkerBroker().retireActor(identity));
}

/** Recorded orphan custody at its original shared-state opening path. */
export function hasUnclaimedSharedStateSqliteCleanup(databasePath: string): boolean {
  return resolveSqliteWorkerBroker().hasUnclaimedSharedStateCleanup(databasePath);
}

export function startCloseUnclaimedSharedStateSqliteWorkers(
  databasePath: string,
): SqliteWorkerRetainedResult<void> {
  return resolveSqliteWorkerBroker().closeUnclaimedSharedStateRetained(databasePath);
}

/** Explicit cleanup only; referenced actors and other opening scopes are untouched. */
export function closeUnclaimedSharedStateSqliteWorkers(databasePath: string): Promise<void> {
  return withCallerErrors(resolveSqliteWorkerBroker().closeUnclaimedSharedState(databasePath));
}

export function openSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions & { existingOnly: true },
): Promise<SqliteWorkerStore<Operations> | undefined>;
export function openSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions & { existingOnly?: false },
): Promise<SqliteWorkerStore<Operations>>;
export function openSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions,
): Promise<SqliteWorkerStore<Operations> | undefined>;
export function openSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions,
): Promise<SqliteWorkerStore<Operations> | undefined> {
  if (!isMainThread) {
    return Promise.reject(
      new SqliteWorkerError(
        "SQLite stores in application workers require the host broker connection",
        "unavailable",
      ),
    );
  }
  return resolveSqliteWorkerBroker().open<Operations>(options);
}

/** Admit the canonical per-agent execution group through its retained host owner. */
type AgentSqliteWorkerCustody = {
  stateContext?: SqliteWorkerStateContext;
  stateDatabasePath?: string;
  onNativeStopped?: SqliteWorkerOpenCustody["onNativeStopped"];
  retainCleanup?: SqliteWorkerOpenCustody["retainCleanup"];
  signal?: AbortSignal;
  assertCurrent(): void;
  createAdmission: SqliteWorkerAdmissionFactory;
};

export function reserveAgentDatabaseSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions,
  custody: AgentSqliteWorkerCustody,
): SqliteWorkerRetainedResult<SqliteWorkerStore<Operations> | undefined> {
  if (!isMainThread) {
    throw new SqliteWorkerError("Agent admission requires its host owner", "unavailable");
  }
  custody.assertCurrent();
  const retained = resolveSqliteWorkerBroker().reserveFile<Operations>(
    options,
    custody.stateContext,
    () => custody.assertCurrent(),
    custody,
  );
  const result = withCallerErrors(retained.result);
  void result.catch(() => {});
  return {
    result,
    service: () => retained.service(),
    read() {
      const outcome = retained.read();
      return outcome.status === "rejected"
        ? { status: "rejected", error: hydrateOpenClawStateWorkerError(outcome.error) }
        : outcome;
    },
  };
}

export async function openAgentDatabaseSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions,
  custody: AgentSqliteWorkerCustody,
): Promise<SqliteWorkerStore<Operations> | undefined> {
  return reserveAgentDatabaseSqliteWorkerStore<Operations>(options, custody).result;
}

/** Host-internal admission for the canonical shared-state actor. */
export function reserveSharedStateSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: Omit<SqliteWorkerStoreOptions, "input">,
  stateContext: SqliteWorkerStateContext,
  assertCurrent?: () => void,
  lifecycle: SqliteWorkerOpenCustody = {},
): SqliteWorkerRetainedResult<SqliteWorkerStore<Operations> | undefined> {
  if (!isMainThread) {
    throw new SqliteWorkerError("Shared-state admission requires the host broker", "unavailable");
  }
  const reserved = resolveSqliteWorkerBroker().reserveFile<Operations>(
    { ...options, input: undefined },
    stateContext,
    assertCurrent ?? (() => {}),
    lifecycle,
  );
  const mapped = mapRetainedOperation(reserved, (store) => {
    if (store) {
      const execute = store.execute.bind(store);
      const close = store.close.bind(store);
      // Keep the broker's binding identity while owning errors at this API boundary.
      store.execute = bindCallerExecute<Operations>({ execute }).execute;
      store.close = () => withCallerErrors(close());
    }
    return store;
  });
  const result = withCallerErrors(mapped.result);
  void result.catch(() => {});
  return {
    result,
    service: () => mapped.service(),
    read() {
      const outcome = mapped.read();
      return outcome.status === "rejected"
        ? { status: "rejected", error: hydrateOpenClawStateWorkerError(outcome.error) }
        : outcome;
    },
  };
}

export async function openSharedStateSqliteWorkerStore<Operations extends SqliteWorkerOperations>(
  options: Omit<SqliteWorkerStoreOptions, "input">,
  stateContext: SqliteWorkerStateContext,
  assertCurrent?: () => void,
  lifecycle?: SqliteWorkerOpenCustody,
): Promise<SqliteWorkerStore<Operations> | undefined> {
  return reserveSharedStateSqliteWorkerStore<Operations>(
    options,
    stateContext,
    assertCurrent,
    lifecycle,
  ).result;
}
