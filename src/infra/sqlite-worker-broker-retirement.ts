import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import { createDeferredCore } from "../shared/deferred.js";
import { createRetainedOperation } from "./retained-operation.js";
import { createSqliteLifecycleAggregateError } from "./sqlite-lifecycle-errors.js";
import { closeUnclaimedSharedStateActors } from "./sqlite-worker-broker-admission.js";
import { failSqliteWorkerExecution } from "./sqlite-worker-broker-execution-failure.js";
import {
  settleFailedSqliteWorkerJobs,
  type CompletedSqliteWorkerOutcome,
} from "./sqlite-worker-broker-settlement.js";
import type {
  Actor,
  EnqueueOptions,
  Job,
  Slot,
  SqliteWorkerExecution,
  SqliteWorkerRetainedOutcome,
  SqliteWorkerRetainedResult,
  StoreClient,
} from "./sqlite-worker-broker.types.js";
import { SqliteWorkerError } from "./sqlite-worker-contract.js";
import type { SqliteWorkerInputAdmission } from "./sqlite-worker-input-admission.js";
import type { SqliteWorkerOperationSettlement } from "./sqlite-worker-operation-settlement.js";

type SqliteWorkerActorCloseOwner = {
  explicitSqliteCloseReleasesNativeResources: boolean;
  forget(actor: Actor): void;
  failExecution(slot: Slot, execution: SqliteWorkerExecution, error: unknown): void;
  enqueueClose(
    actor: Actor,
    maintenanceScope: EnqueueOptions["maintenanceScope"],
    settled: Job["settled"],
  ): Promise<unknown>;
  serviceSlot(slot: Slot): void;
  retireRetained(slot: Slot): SqliteWorkerRetainedResult<void>;
  retireExecutionRetained(
    slot: Slot,
    execution: SqliteWorkerExecution,
  ): SqliteWorkerRetainedResult<void>;
};

export function closeSqliteWorkerActorRetained(
  owner: SqliteWorkerActorCloseOwner,
  actor: Actor,
  maintenanceScope?: EnqueueOptions["maintenanceScope"],
): SqliteWorkerRetainedResult<void> {
  if (actor.closingRetained && actor.closingRetained.read().status !== "rejected") {
    return actor.closingRetained;
  }
  if (actor.cleanupState === "complete") {
    // Recorded cleanup survives pruning of the original physical execution.
    const recorded = createRetainedOperation<void>(() => {});
    recorded.resolve(undefined);
    actor.closingRetained = recorded.operation;
    return recorded.operation;
  }
  const execution = [...actor.slot.executions].find(
    (entry) => entry.worker === actor.executionWorker,
  );
  if (!execution) {
    throw new Error("SQLite actor lost its original execution owner");
  }
  const completed = createDeferredCore();
  void completed.promise.catch(() => {});
  let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
  let closeOutcome: SqliteWorkerRetainedOutcome<unknown> = { status: "pending" };
  let closeStarted = false;
  let stop: SqliteWorkerRetainedResult<void> | undefined;
  let observing = false;
  const errors: unknown[] = [];
  const finish = (release: boolean) => {
    if (outcome.status !== "pending") {
      return;
    }
    if (release) {
      owner.forget(actor);
    }
    const error =
      errors.length > 1
        ? createSqliteLifecycleAggregateError(
            errors,
            "SQLite worker actor cleanup failed",
            errors[0],
          )
        : errors[0];
    outcome = errors.length
      ? { status: "rejected", error }
      : { status: "fulfilled", value: undefined };
    if (actor.closingRetained === retained) {
      actor.closing = undefined;
    }
    if (outcome.status === "fulfilled") {
      completed.resolve();
    } else {
      completed.reject(outcome.error);
    }
  };
  const advance = () => {
    if (observing || outcome.status !== "pending") {
      return;
    }
    observing = true;
    try {
      if (actor.cleanupState === "complete") {
        finish(false);
        return;
      }
      if (
        !actor.initialized &&
        !actor.openingError &&
        !actor.slot.failed &&
        !execution.failed &&
        !actor.backendClosed
      ) {
        return;
      }
      if (!closeStarted) {
        closeStarted = true;
        if (actor.openingError && !actor.initialized) {
          if (!actor.openDispatch.dispatched || actor.openDispatch.openNotEntered) {
            actor.backendClosed = true;
            actor.markNativeStopped();
            closeOutcome = { status: "fulfilled", value: undefined };
          } else {
            // A failed native factory cannot certify disposal of partial handles.
            owner.failExecution(actor.slot, execution, actor.openingError.error);
            closeOutcome = { status: "fulfilled", value: undefined };
          }
        } else if (actor.backendClosed) {
          closeOutcome = { status: "fulfilled", value: undefined };
        } else {
          const closing = owner.enqueueClose(actor, maintenanceScope, (settled) => {
            closeOutcome = settled;
            advance();
          });
          void closing.then(
            () => {
              if (closeOutcome.status === "pending") {
                closeOutcome = { status: "fulfilled", value: undefined };
              }
              advance();
            },
            (error: unknown) => {
              if (closeOutcome.status === "pending") {
                closeOutcome = { status: "rejected", error };
              }
              advance();
            },
          );
        }
      }
      if (closeOutcome.status === "pending") {
        return;
      }
      if (closeOutcome.status === "rejected" && errors.length === 0) {
        errors.push(closeOutcome.error);
        if (!actor.physicalJoined) {
          owner.failExecution(actor.slot, execution, closeOutcome.error);
        }
      }
      if (closeOutcome.status === "fulfilled") {
        actor.backendClosed = true;
        if (
          !actor.slot.failed &&
          !execution.failed &&
          owner.explicitSqliteCloseReleasesNativeResources
        ) {
          actor.markNativeStopped();
        }
      }
      const stopSlot =
        actor.slot.failed ||
        (!actor.slot.pendingOpens && [...actor.slot.actors].every((entry) => entry.backendClosed));
      const stopExecution =
        (!owner.explicitSqliteCloseReleasesNativeResources && !actor.physicalJoined) ||
        execution.failed;
      if (stopSlot || stopExecution) {
        if (!stop) {
          stop = stopSlot
            ? owner.retireRetained(actor.slot)
            : owner.retireExecutionRetained(actor.slot, execution);
          void stop.result.then(advance, advance);
        }
        const stopped = stop.read();
        if (stopped.status === "pending") {
          return;
        }
        if (stopped.status === "rejected") {
          errors.push(stopped.error);
          finish(false);
          return;
        }
      }
      finish(true);
    } catch (error) {
      errors.push(error);
      finish(false);
    } finally {
      observing = false;
    }
  };
  const retained: SqliteWorkerRetainedResult<void> = {
    result: completed.promise,
    read: () => outcome,
    service() {
      owner.serviceSlot(actor.slot);
      stop?.service();
      advance();
    },
  };
  actor.cleanupState = "pending";
  actor.closingRetained = retained;
  actor.closing = retained.result;
  void actor.opened.then(advance, advance);
  advance();
  return retained;
}

/** Broker teardown borrows its existing queue, registries, and native lifetime owner. */
export function createSqliteWorkerBrokerRetirement(owner: {
  clients: Set<object>;
  stores: Map<object, StoreClient>;
  actors: Map<string, Actor>;
  slots: Set<Slot>;
  operations: Map<Promise<void>, SqliteWorkerRetainedResult<void> | undefined>;
  waiters: Map<Slot, Set<(error?: unknown) => void>>;
  inputAdmission: Pick<
    SqliteWorkerInputAdmission,
    | "opensPending"
    | "preparationsPending"
    | "joinOpens"
    | "joinPreparations"
    | "serviceOpens"
    | "invalidatePreparations"
  >;
  lifecycle: {
    closeActorRetained(actor: Actor): SqliteWorkerRetainedResult<void>;
    retireOwnedActorRetained(actor: Actor): SqliteWorkerRetainedResult<void>;
    retireRetained(slot: Slot): SqliteWorkerRetainedResult<void>;
    serviceSlot(slot: Slot): void;
    retireExecutionRetained(
      slot: Slot,
      execution: SqliteWorkerExecution,
    ): SqliteWorkerRetainedResult<void>;
  };
  dispatch(slot: Slot): void;
  finish(
    slot: Slot,
    job: Job,
    error?: unknown,
    value?: unknown,
    settlement?: SqliteWorkerOperationSettlement,
  ): void;
}) {
  let draining: Promise<void> | undefined;
  let drainingRetained: SqliteWorkerRetainedResult<void> | undefined;
  let advanceDrain: (() => void) | undefined;
  let drainWakeQueued = false;

  function operationsChanged(): void {
    if (!advanceDrain || drainWakeQueued) {
      return;
    }
    drainWakeQueued = true;
    // Ordinary awaited drainage wakes after the current settlement observer returns.
    // Retained service advances the same facts directly, without running this callback.
    queueMicrotask(() => {
      drainWakeQueued = false;
      advanceDrain?.();
    });
  }

  function closeUnclaimedSharedStateRetained(
    databasePath: string,
  ): SqliteWorkerRetainedResult<void> {
    const completed = createDeferredCore();
    void completed.promise.catch(() => {});
    let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
    let cleanup: SqliteWorkerRetainedResult<void> | undefined;
    let observedOpen: Promise<void> | undefined;
    const advance = () => {
      if (outcome.status !== "pending") {
        return;
      }
      if (owner.inputAdmission.opensPending) {
        const opening = owner.inputAdmission.joinOpens();
        if (observedOpen !== opening) {
          observedOpen = opening;
          void opening.then(advance, advance);
        }
        return;
      }
      if (!cleanup) {
        cleanup = closeUnclaimedSharedStateActors(owner.actors.values(), databasePath, (actor) =>
          owner.lifecycle.closeActorRetained(actor),
        );
        void cleanup.result.then(advance, advance);
      }
      const result = cleanup.read();
      if (result.status === "pending") {
        return;
      }
      outcome = result;
      if (result.status === "fulfilled") {
        completed.resolve();
      } else {
        completed.reject(result.error);
      }
    };
    advance();
    return {
      result: completed.promise,
      read: () => outcome,
      service: () => {
        owner.inputAdmission.serviceOpens();
        cleanup?.service();
        advance();
      },
    };
  }

  function fail(
    slot: Slot,
    reason: unknown,
    currentError?: Error,
    openOutcome?: "refused-before-agent-open",
    completed?: CompletedSqliteWorkerOutcome,
  ): void {
    if (slot.failed) {
      return;
    }
    const error = toErrorObject(reason, "SQLite worker failed");
    slot.failed = new SqliteWorkerError(error.message, "unavailable");
    for (const resume of owner.waiters.get(slot) ?? []) {
      resume(slot.failed);
    }
    const current = slot.current;
    slot.current = undefined;
    if (current) {
      current.inputTransfer?.producer.cancel();
      current.inputTransfer = undefined;
      current.transfer = undefined;
    }
    const queued = slot.queue.splice(0);
    slot.serviceFailureSettlement = settleFailedSqliteWorkerJobs({
      queuedError: slot.failed,
      current,
      queued,
      suspended: (() => {
        const retained = new Set<Job>();
        const seen = new Set<Job>(current ? [current] : []);
        for (let parent = current?.parent; parent; parent = parent.parent) {
          seen.add(parent);
          retained.add(parent);
        }
        const collect = (job: Job) => {
          for (const child of job.provisionalChildren?.values() ?? []) {
            if (seen.has(child)) {
              continue;
            }
            seen.add(child);
            retained.add(child);
            collect(child);
          }
        };
        if (current) {
          collect(current);
        }
        for (const parent of retained) {
          collect(parent);
        }
        return [...retained];
      })(),
      error,
      currentError,
      completed,
      openOutcome,
      retire: () => owner.lifecycle.retireRetained(slot),
      onFinished: () => {
        slot.serviceFailureSettlement = undefined;
      },
      finish: (job, failure, value, settlement) =>
        owner.finish(slot, job, failure, value, settlement),
    });
  }

  function failExecution(
    slot: Slot,
    execution: SqliteWorkerExecution,
    error: unknown,
    currentError?: Error,
    openOutcome?: "refused-before-agent-open",
    completed?: CompletedSqliteWorkerOutcome,
  ): void {
    failSqliteWorkerExecution(
      { slot, execution, error, currentError, openOutcome, completed },
      {
        retireExecutionRetained: (selectedSlot, selectedExecution) =>
          owner.lifecycle.retireExecutionRetained(selectedSlot, selectedExecution),
        serviceSlot: (selectedSlot) => owner.lifecycle.serviceSlot(selectedSlot),
        finish: (job, failure, value, settlement) =>
          owner.finish(slot, job, failure, value, settlement),
        dispatch: (selectedSlot) => owner.dispatch(selectedSlot),
      },
    );
  }

  function closeRetained(): SqliteWorkerRetainedResult<void> {
    if (drainingRetained) {
      return drainingRetained;
    }
    const completed = createDeferredCore();
    void completed.promise.catch(() => {});
    let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
    let actors: SqliteWorkerRetainedResult<void>[] | undefined;
    let slots: SqliteWorkerRetainedResult<void>[] | undefined;
    let advancing = false;
    const finish = (errors: unknown[]) => {
      owner.clients.clear();
      owner.stores.clear();
      draining = undefined;
      drainingRetained = undefined;
      advanceDrain = undefined;
      if (errors.length) {
        const error = new AggregateError(errors, "SQLite worker host cleanup failed");
        outcome = { status: "rejected", error };
        completed.reject(error);
      } else {
        outcome = { status: "fulfilled", value: undefined };
        completed.resolve();
      }
    };
    const advance = () => {
      if (advancing || outcome.status !== "pending") {
        return;
      }
      advancing = true;
      try {
        if (owner.inputAdmission.opensPending) {
          return;
        }
        for (const [promise, operation] of owner.operations) {
          if (operation && operation.read().status !== "pending") {
            owner.operations.delete(promise);
          }
        }
        if (owner.operations.size) {
          return;
        }
        if (!actors) {
          const ownedActors = new Set(owner.actors.values());
          for (const slot of owner.slots) {
            for (const actor of slot.actors) {
              ownedActors.add(actor);
            }
          }
          actors = [...ownedActors].map((actor) => owner.lifecycle.closeActorRetained(actor));
          for (const actor of actors) {
            void actor.result.then(advance, advance);
          }
        }
        const results = actors.map((actor) => actor.read());
        if (
          results.some((result) => result.status === "pending") ||
          owner.inputAdmission.preparationsPending
        ) {
          return;
        }
        if (!slots) {
          slots = [...owner.slots].map((slot) => owner.lifecycle.retireRetained(slot));
          for (const slot of slots) {
            void slot.result.then(advance, advance);
          }
        }
        const stopped = slots.map((slot) => slot.read());
        if (stopped.some((result) => result.status === "pending")) {
          return;
        }
        finish(
          [...results, ...stopped].flatMap((result) =>
            result.status === "rejected" ? [result.error] : [],
          ),
        );
      } catch (error) {
        finish([error]);
      } finally {
        advancing = false;
      }
    };
    const retained: SqliteWorkerRetainedResult<void> = {
      result: completed.promise,
      read: () => outcome,
      service: () => {
        owner.inputAdmission.serviceOpens();
        for (const slot of Array.from(owner.slots)) {
          owner.lifecycle.serviceSlot(slot);
        }
        for (const operation of owner.operations.values()) {
          operation?.service();
        }
        for (const actor of actors ?? []) {
          actor.service();
        }
        for (const slot of slots ?? []) {
          slot.service();
        }
        advance();
      },
    };
    drainingRetained = retained;
    draining = retained.result;
    advanceDrain = advance;
    owner.inputAdmission.invalidatePreparations();
    for (const waiters of owner.waiters.values()) {
      for (const resume of waiters) {
        resume(new SqliteWorkerError("SQLite worker host is closing", "overloaded"));
      }
    }
    for (const client of owner.stores.values()) {
      client.sealed = true;
    }
    void owner.inputAdmission.joinOpens().then(advance, advance);
    void Promise.allSettled(Array.from(owner.operations.keys())).then(advance);
    void owner.inputAdmission.joinPreparations().then(advance, advance);
    advance();
    return retained;
  }

  return {
    get closing() {
      return draining;
    },
    get closingRetained() {
      return drainingRetained;
    },
    operationsChanged,
    closeUnclaimedSharedStateRetained,
    fail,
    failExecution,
    closeRetained,
  };
}
