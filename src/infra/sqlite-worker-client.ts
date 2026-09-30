import { AsyncLocalStorage } from "node:async_hooks";
import { isPromise } from "node:util/types";
import { serialize } from "node:v8";
import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import { createDeferredCore } from "../shared/deferred.js";
import { getOpenClawDatabaseMaintenanceScope } from "../state/openclaw-state-db-async-lifecycle.js";
import { createRetainedOperation } from "./retained-operation.js";
import type {
  Actor,
  Job,
  OperationScope,
  StoreClient,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";
import {
  SqliteWorkerError,
  type SqliteWorkerOperations,
  type SqliteWorkerStore,
} from "./sqlite-worker-contract.js";
import type { SqliteWorkerAdmissionFactory } from "./sqlite-worker-operation-admission.js";
import {
  captureSqliteWorkerStateContext,
  type SqliteWorkerStateContext,
} from "./sqlite-worker-state-context.js";

export function getSqliteWorkerClientActorIdentity(
  client: StoreClient | undefined,
): Readonly<Pick<Extract<Actor, { kind: "file" }>, "key" | "databasePath">> {
  // Retained facts remain readable after worker failure; dispatch owns liveness.
  if (!client || client.sealed || !client.actor.stateContext) {
    throw new SqliteWorkerError("SQLite shared actor binding is unavailable", "closed");
  }
  return client.actor;
}

export function runSqliteWorkerClientOperation<Operations extends SqliteWorkerOperations, T>(
  client: StoreClient | undefined,
  operation: (scope: Pick<SqliteWorkerStore<Operations>, "execute">) => T | Promise<T>,
  stateContext: SqliteWorkerStateContext | undefined,
  track: (pending: Promise<void>) => () => void,
  assertCurrent?: (commandType: PropertyKey) => void,
  createAdmission?: SqliteWorkerAdmissionFactory,
  settled?: Job["settled"],
): Promise<T> {
  if (!client || client.sealed) {
    const error = toErrorObject(
      client?.actor.openingError
        ? client.actor.openingError.error
        : new SqliteWorkerError("SQLite worker store is closed", "closed"),
      "SQLite worker store is closed",
    );
    settled?.({ status: "rejected", error }, { settlement: { kind: "not-entered", error } });
    return Promise.reject(error);
  }
  const scope: OperationScope = {
    maintenanceScope: getOpenClawDatabaseMaintenanceScope(),
    createAdmission,
    assertCurrent,
    active: true,
    pending: new Set(),
    ...(stateContext ? { stateContext: captureSqliteWorkerStateContext(stateContext) } : {}),
  };
  const released = createDeferredCore();
  client.scopes.add(released.promise);
  const untrack = track(released.promise);
  return (async () => {
    try {
      const result = operation({
        execute: (command, options = {}) =>
          // SAFETY: The exact admitted store retains its typed backend.
          client.execute(command, options, scope, settled) as Promise<
            Operations[typeof command.type]["output"]
          >,
      });
      return isPromise(result) ? await result : result;
    } finally {
      scope.active = false;
      // A callback can throw after dispatch or leave a command unawaited.
      await Promise.allSettled(scope.pending);
      client.scopes.delete(released.promise);
      untrack();
      released.resolve();
    }
  })();
}

export function createSqliteWorkerClient<Operations extends SqliteWorkerOperations>(owner: {
  actor: Actor;
  isDraining: () => boolean;
  isAvailable: () => boolean;
  dispatch: (
    payload: Buffer,
    signal: AbortSignal | undefined,
    scope: OperationScope | undefined,
    assertCurrent: (() => void) | undefined,
    createAdmission: SqliteWorkerAdmissionFactory | undefined,
    settled?: Job["settled"],
  ) => Promise<unknown>;
  releaseRetained: () => SqliteWorkerRetainedResult<void>;
  service(): void;
}) {
  let closed: Promise<void> | undefined;
  let retainedClose: SqliteWorkerRetainedResult<void> | undefined;
  let advanceClose: (() => void) | undefined;
  const pending = new Set<Promise<unknown>>();
  const pendingConsumers = new Set<Promise<void>>();
  const client: StoreClient = {
    actor: owner.actor,
    close: () => store.close(),
    closeRetained: () => closeRetained(),
    sealed: owner.isDraining(),
    isAvailable: owner.isAvailable,
    scopes: new Set(),
    execute: (command, options, scope, settled) => {
      const reject = (failure: unknown) => {
        const error = toErrorObject(failure, "SQLite worker command was refused");
        settled?.({ status: "rejected", error }, { settlement: { kind: "not-entered", error } });
        return Promise.reject(error);
      };
      if (scope ? !scope.active : closed || client.sealed || owner.isDraining()) {
        return reject(
          owner.actor.openingError
            ? owner.actor.openingError.error
            : new SqliteWorkerError("SQLite worker store is closed", "closed"),
        );
      }
      if (options.signal?.aborted) {
        return reject(toErrorObject(options.signal.reason, "SQLite worker operation canceled"));
      }
      const admission = scope?.assertCurrent;
      const createAdmission = scope?.createAdmission;
      // Queued callbacks run from Worker replies, outside this command's async context.
      const inCaller = admission || createAdmission ? AsyncLocalStorage.snapshot() : undefined;
      let commandType: PropertyKey;
      try {
        commandType = command.type;
      } catch (error) {
        return reject(toErrorObject(error, "SQLite worker command could not be serialized"));
      }
      const assertCurrent =
        admission && inCaller ? () => inCaller(admission, commandType) : undefined;
      try {
        assertCurrent?.();
      } catch (error) {
        settled?.({ status: "rejected", error }, { settlement: { kind: "not-entered", error } });
        return (async (): Promise<never> => {
          throw error;
        })();
      }
      let payload: Buffer;
      try {
        // The queued guard and wire command must observe the same captured type.
        payload = serialize({ type: commandType, input: command.input });
      } catch (error) {
        return reject(toErrorObject(error, "SQLite worker command could not be serialized"));
      }
      let completed = false;
      let dispatching = true;
      const releasePending = () => {
        // Dispatch can reject synchronously before its returned Promise exists.
        if (dispatching) {
          return;
        }
        pending.delete(operation);
        scope?.pending.delete(operation);
      };
      const consumer = createDeferredCore();
      pendingConsumers.add(consumer.promise);
      const finishConsumer = () => {
        pendingConsumers.delete(consumer.promise);
        consumer.resolve();
      };
      let operation: Promise<unknown>;
      try {
        operation = owner.dispatch(
          payload,
          options.signal,
          scope,
          assertCurrent,
          createAdmission && inCaller
            ? (admissionOperation) => inCaller(createAdmission, admissionOperation)
            : undefined,
          (outcome, receipt) => {
            completed = true;
            releasePending();
            try {
              settled?.(outcome, receipt);
            } finally {
              advanceClose?.();
            }
          },
        );
      } catch (error) {
        finishConsumer();
        throw error;
      }
      dispatching = false;
      if (!completed) {
        pending.add(operation);
        scope?.pending.add(operation);
      }
      const consumerSettled = () => {
        releasePending();
        finishConsumer();
      };
      void operation.then(consumerSettled, consumerSettled);
      return operation;
    },
  };
  function closeRetained(): SqliteWorkerRetainedResult<void> {
    if (retainedClose && retainedClose.read().status !== "rejected") {
      return retainedClose;
    }
    client.sealed = true;
    let release: SqliteWorkerRetainedResult<void> | undefined;
    let advancing = false;
    const completion = createRetainedOperation<void>(() => {
      owner.service();
      release?.service();
      advance();
    });
    function advance() {
      if (advancing || completion.operation.read().status !== "pending") {
        return;
      }
      advancing = true;
      try {
        // Retained commands remove both records at canonical final settlement.
        // Generic callbacks keep their existing scope until their continuation ends.
        if (client.scopes.size || pending.size) {
          return;
        }
        if (!release) {
          release = owner.releaseRetained();
          void release.result.then(advance, advance);
        }
        const outcome = release.read();
        if (outcome.status === "fulfilled") {
          completion.resolve(undefined);
        } else if (outcome.status === "rejected") {
          completion.reject(outcome.error);
        }
      } catch (error) {
        completion.reject(error);
      } finally {
        advancing = false;
      }
    }
    retainedClose = completion.operation;
    closed = completion.operation.result;
    advanceClose = advance;
    // These are observations for ordinary awaited scopes, never synchronous progress.
    void Promise.allSettled([...client.scopes, ...pending]).then(advance);
    advance();
    return completion.operation;
  }
  const store: SqliteWorkerStore<Operations> = {
    execute: (command, options = {}) =>
      // SAFETY: The typed backend owns this result.
      client.execute(command, options) as Promise<Operations[typeof command.type]["output"]>,
    close: () => {
      // Public close also joins the accepted callers; retained close records native settlement.
      const accepted = [...client.scopes, ...pendingConsumers];
      const native = closeRetained();
      return Promise.allSettled([...accepted, native.result]).then(() => native.result);
    },
  };
  return { store, client };
}
