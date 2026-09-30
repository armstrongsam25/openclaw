import { AsyncLocalStorage } from "node:async_hooks";
import { createDeferredCore } from "../shared/deferred.js";
import { assertStateDatabaseAccessAllowed } from "./gateway-state-owner.js";
import { runtimeNeedsTypeScriptLoader } from "./runtime-worker-url.js";
import {
  assertSqliteWorkerActorReusable,
  assertSqliteWorkerActorExecution,
  captureSqliteWorkerOpen,
  captureSqliteWorkerAdmissionPaths,
  prepareSqliteWorkerDatabaseAdmissionSync,
  resolveSqliteWorkerModuleUrlSync,
  resolveOpenedSqliteWorkerIdentitySync,
  retainSqliteWorkerAdmissionCleanup,
  retainSqliteWorkerAdmissionPathReferences,
  validateSqliteWorkerDatabaseLocator,
} from "./sqlite-worker-broker-admission.js";
import type { createSqliteWorkerLifecycle } from "./sqlite-worker-broker-lifecycle.js";
import type {
  Actor,
  Job,
  EnqueueOptions,
  PreparedSqliteWorkerOpen,
  RequestBody,
  Slot,
  SqliteWorkerSlotReservation,
  SqliteWorkerStoreOptions,
  SqliteWorkerOpenCustody,
  SqliteWorkerRetainedResult,
  SqliteWorkerRetainedOutcome,
} from "./sqlite-worker-broker.types.js";
import {
  SqliteWorkerError,
  type SqliteWorkerOperations,
  type SqliteWorkerStore,
} from "./sqlite-worker-contract.js";
import type { DatabasePathIdentity } from "./sqlite-worker-identity.js";
import type { SqliteWorkerInputAdmission } from "./sqlite-worker-input-admission.js";
import type { SqliteWorkerStateContext } from "./sqlite-worker-state-context.js";
import { sqliteWorkerRequestBytes } from "./sqlite-worker-state-context.js";

export type SqliteWorkerFileOpenDependency = {
  actor: Actor;
  run(operation: () => void): void;
} & ({ kind: "open" } | { kind: "close"; barrier: SqliteWorkerRetainedResult<void> });

function isPendingOrdinaryFileClose(
  actor: Actor,
  barrier: SqliteWorkerRetainedResult<void>,
): boolean {
  const execution = [...actor.slot.executions].find(
    (entry) => entry.worker === actor.executionWorker,
  );
  return (
    execution !== undefined &&
    !execution.failed &&
    !execution.exited &&
    !execution.retiringRetained &&
    actor.kind === "file" &&
    actor.initialized &&
    !actor.backendClosed &&
    !actor.openingError &&
    actor.references === 0 &&
    !actor.retirementRequested &&
    actor.retirementRetained === undefined &&
    actor.cleanupState === "pending" &&
    actor.closingRetained === barrier &&
    barrier.read().status === "pending" &&
    actor.slot.actors.has(actor) &&
    !actor.slot.failed &&
    !actor.slot.retiring &&
    !actor.slot.exited
  );
}

/** Both awaited and retained admission install the same file actor and native open. */
function createSqliteWorkerFileActor(
  options: PreparedSqliteWorkerOpen,
  captured: {
    databasePath: string;
    inputHash: string;
    identity: DatabasePathIdentity;
    admittedPaths: Set<string>;
    modulePath: string;
    moduleUrl: string;
  },
  reserved: SqliteWorkerSlotReservation,
  owner: {
    actors: Map<string, Actor>;
    nextActor(): number;
    enqueue(
      slot: Slot,
      body: RequestBody,
      bytes: number,
      options: EnqueueOptions,
    ): Promise<unknown>;
    reconcileFilePlacement: (
      slot: Slot,
      placement: Actor["placement"],
      identity: DatabasePathIdentity,
    ) => void;
  },
): Extract<Actor, { kind: "file" }> {
  const { slot, executionWorker } = reserved;
  const { databasePath, inputHash, identity, admittedPaths, modulePath, moduleUrl } = captured;
  const nativeStopped = createDeferredCore();
  const opened = createDeferredCore();
  void opened.promise.catch(() => {});
  const actor: Extract<Actor, { kind: "file" }> = {
    kind: "file",
    executionWorker,
    placement: reserved.placement,
    runtimeGeneration: options.runtimeGeneration,
    nativeStopped: nativeStopped.promise,
    nativeStoppedRecorded: false,
    markNativeStopped() {
      actor.nativeStoppedRecorded = true;
      nativeStopped.resolve();
    },
    id: owner.nextActor(),
    key: identity.key,
    pathReferences: new Map([...admittedPaths].map((pathname) => [pathname, 1])),
    moduleUrl,
    inputHash,
    slot,
    references: 1,
    opened: opened.promise,
    openDispatch: { dispatched: false },
    initialized: false,
    backendClosed: false,
    databasePath,
    stateContext: options.stateContext,
    stateDatabasePath: options.stateDatabasePath,
  };
  owner.actors.set(actor.key, actor);
  slot.actors.add(actor);
  slot.pendingOpens -= 1;
  const refuse = (error: unknown) => {
    actor.openingError ??= { error };
    opened.reject(error);
  };
  try {
    const pending = owner.enqueue(
      slot,
      {
        type: "open",
        actor: actor.id,
        moduleUrl,
        databasePath,
        ...(options.createAdmission
          ? { openAdmission: "input" as const }
          : options.createOpenAdmission
            ? { openAdmission: "identity" as const }
            : {}),
        ...(options.existingOnly ? { existingIdentity: identity.key } : {}),
        input: options.input,
        ...(options.preparation ? { preparation: options.preparation } : {}),
        ...(runtimeNeedsTypeScriptLoader(modulePath)
          ? { sourceLoaderUrl: import.meta.resolve("tsx/esm/api") }
          : {}),
      },
      sqliteWorkerRequestBytes(options.input, options.stateContext, options.preparation),
      {
        dispatchState: actor.openDispatch,
        signal: options.signal,
        assertCurrent: options.assertCurrent,
        maintenanceScope: options.maintenanceScope,
        createAdmission: options.createAdmission ?? options.createOpenAdmission,
        settled(outcome) {
          if (outcome.status === "rejected") {
            refuse(outcome.error);
            return;
          }
          if (outcome.status !== "fulfilled") {
            return;
          }
          try {
            const physical = resolveOpenedSqliteWorkerIdentitySync(
              databasePath,
              identity,
              (key) => {
                const existing = owner.actors.get(key);
                return existing !== undefined && existing !== actor;
              },
            );
            owner.reconcileFilePlacement(slot, actor.placement, physical);
            if (physical.key !== actor.key) {
              owner.actors.delete(actor.key);
              actor.key = physical.key;
              owner.actors.set(physical.key, actor);
            }
            actor.initialized = true;
            opened.resolve();
          } catch (error) {
            refuse(error);
          }
        },
      },
    );
    void pending.catch(refuse);
  } catch (error) {
    refuse(error);
  }
  return actor;
}

/** File admission borrows the broker's existing registries, lifecycle, and retained-input owner. */
export function reserveSqliteWorkerFile<Operations extends SqliteWorkerOperations>(
  options: SqliteWorkerStoreOptions,
  stateContext: SqliteWorkerStateContext | undefined,
  assertCurrent: () => void,
  custody: SqliteWorkerOpenCustody,
  owner: {
    identity: object;
    isClosing(): boolean;
    maxStores: number;
    actors: Map<string, Actor>;
    slots: Set<Slot>;
    clients: Set<object>;
    inputAdmission: Pick<
      SqliteWorkerInputAdmission,
      "reserveOpenInput" | "joinOpens" | "startOpen" | "serviceOpens"
    >;
    lifecycle: Pick<
      ReturnType<typeof createSqliteWorkerLifecycle>,
      | "settleGeneration"
      | "releaseActorReference"
      | "closeActorRetained"
      | "tryReserveSlot"
      | "reconcileFilePlacement"
    >;
    nextActor(this: void): number;
    enqueue(
      slot: Slot,
      body: RequestBody,
      bytes: number,
      options: EnqueueOptions,
    ): Promise<unknown>;
    bindClient(
      actor: Actor,
      client: object,
      scope: EnqueueOptions["maintenanceScope"],
      releasePaths: () => void,
      opening: () => boolean,
    ): SqliteWorkerStore<Operations>;
    captureStoreClose(store: object): () => SqliteWorkerRetainedResult<void>;
    captureOpenDependency(): (() => void) | undefined;
    serviceCapturedSlot(slot?: Slot, openDependency?: SqliteWorkerFileOpenDependency): void;
  },
): SqliteWorkerRetainedResult<SqliteWorkerStore<Operations> | undefined> {
  const inCaller = AsyncLocalStorage.snapshot();
  const assertDependency = owner.captureOpenDependency();
  validateSqliteWorkerDatabaseLocator(options.databasePath);
  if (owner.isClosing()) {
    throw new SqliteWorkerError("SQLite worker host is closing", "closed");
  }
  if (owner.clients.size >= owner.maxStores) {
    throw new SqliteWorkerError("SQLite worker store capacity reached", "overloaded");
  }
  const captured = captureSqliteWorkerOpen(options, stateContext, assertCurrent, custody);
  const generation = options.runtimeGeneration;
  captured.nativeWorkerSource.retain(owner.identity, async () => {
    await owner.inputAdmission.joinOpens();
    return await owner.lifecycle.settleGeneration(generation);
  });
  const input = owner.inputAdmission.reserveOpenInput(
    sqliteWorkerRequestBytes(captured.input, captured.stateContext, captured.preparation),
  );
  const client = {};
  owner.clients.add(client);
  const completion = createDeferredCore<SqliteWorkerStore<Operations> | undefined>();
  void completion.promise.catch(() => {});
  let outcome: SqliteWorkerRetainedOutcome<SqliteWorkerStore<Operations> | undefined> = {
    status: "pending",
  };
  let actor: Actor | undefined;
  let store: SqliteWorkerStore<Operations> | undefined;
  let closeStore: (() => SqliteWorkerRetainedResult<void>) | undefined;
  let refusal: { error: unknown; cleanup?: SqliteWorkerRetainedResult<void> } | undefined;
  let retainedBarrier: SqliteWorkerRetainedResult<void> | undefined;
  let barrier: Promise<unknown> | undefined;
  let waitingActor: Actor | undefined;
  let advancing = false;
  let finishOpen = () => {};
  const finish = (value: SqliteWorkerStore<Operations> | undefined) => {
    if (outcome.status !== "pending") {
      return;
    }
    outcome = { status: "fulfilled", value };
    input.release();
    if (!value) {
      owner.clients.delete(client);
    }
    completion.resolve(value);
    finishOpen();
  };
  const finishRefusal = () => {
    if (!refusal || outcome.status !== "pending") {
      return;
    }
    const cleaned = refusal.cleanup?.read();
    if (cleaned?.status === "pending") {
      return;
    }
    const error =
      cleaned?.status === "rejected"
        ? new AggregateError(
            [refusal.error, cleaned.error],
            "SQLite worker admission and cleanup failed",
            { cause: refusal.error },
          )
        : refusal.error;
    outcome = { status: "rejected", error };
    completion.reject(error);
    finishOpen();
  };
  const refuse = (error: unknown) => {
    if (outcome.status !== "pending" || refusal) {
      return;
    }
    refusal = { error };
    input.release();
    try {
      if (closeStore) {
        refusal.cleanup = closeStore();
      } else {
        owner.clients.delete(client);
        if (actor) {
          owner.lifecycle.releaseActorReference(actor);
          if (!actor.references) {
            actor.openingError ??= { error };
            refusal.cleanup = owner.lifecycle.closeActorRetained(actor, captured.maintenanceScope);
          }
        }
      }
      if (refusal.cleanup) {
        void refusal.cleanup.result.then(finishRefusal, finishRefusal);
      }
    } catch (cleanupError) {
      refusal.error = new AggregateError(
        [error, cleanupError],
        "SQLite worker admission and cleanup failed",
        { cause: error },
      );
    }
    finishRefusal();
  };
  const waitFor = (pending: Promise<unknown>) => {
    if (barrier === pending) {
      return;
    }
    barrier = pending;
    void pending.then(() => {
      if (barrier === pending) {
        barrier = undefined;
        advance();
      }
    }, refuse);
  };
  const advance = () => inCaller(advanceInCaller);
  const advanceInCaller = () => {
    if (outcome.status !== "pending" || advancing) {
      return;
    }
    advancing = true;
    try {
      if (refusal) {
        finishRefusal();
        return;
      }
      if (retainedBarrier) {
        const closed = retainedBarrier.read();
        if (closed.status === "pending") {
          return;
        }
        if (closed.status === "rejected") {
          throw closed.error;
        }
        retainedBarrier = undefined;
      }
      captured.assertCurrent?.();
      if (!actor) {
        input.assertCurrent();
        const prepared = prepareSqliteWorkerDatabaseAdmissionSync(captured);
        assertStateDatabaseAccessAllowed(captured.stateDatabasePath ?? prepared.databasePath, {
          maintenanceScope: captured.maintenanceScope,
        });
        const admittedPaths = captureSqliteWorkerAdmissionPaths(
          prepared.databasePath,
          prepared.identity,
          owner.actors.values(),
        );
        if (captured.existingOnly && !prepared.identity.key.startsWith("file:")) {
          finish(undefined);
          return;
        }
        const module = resolveSqliteWorkerModuleUrlSync(captured.moduleUrl);
        captured.assertCurrent?.();
        const existing = owner.actors.get(prepared.identity.key);
        if (existing?.retirementRequested || existing?.cleanupState === "pending") {
          waitingActor = existing;
          retainedBarrier = existing.retirementRetained ?? existing.closingRetained;
          const pending = retainedBarrier?.result ?? existing.retirement ?? existing.closing;
          if (!pending) {
            throw new SqliteWorkerError(
              "SQLite worker cleanup is pending; retry close before reopening",
              "closed",
            );
          }
          waitFor(pending);
          return;
        }
        waitingActor = undefined;
        if (existing) {
          assertSqliteWorkerActorReusable(
            existing,
            module.moduleUrl,
            prepared.inputHash,
            captured.stateContext,
          );
          existing.references += 1;
          actor = existing;
          input.release();
        } else {
          const reserved = owner.lifecycle.tryReserveSlot({
            ...captured,
            placement: prepared.placement,
          });
          if (!reserved) {
            if (!barrier) {
              waitFor(Promise.race([...owner.slots].map((slot) => slot.exit)));
            }
            return;
          }
          const opening = input.handoff(() => {
            actor = createSqliteWorkerFileActor(
              captured,
              { ...prepared, ...module, admittedPaths },
              reserved,
              {
                actors: owner.actors,
                nextActor: owner.nextActor,
                enqueue: (slot, body, bytes, requestOptions) =>
                  owner.enqueue(slot, body, bytes, requestOptions),
                reconcileFilePlacement: owner.lifecycle.reconcileFilePlacement,
              },
            );
            return actor.opened;
          });
          void opening.catch(refuse);
        }
        if (!actor) {
          throw new Error("File reservation lost its admitted actor");
        }
        const admittedActor = actor;
        retainSqliteWorkerAdmissionCleanup(admittedActor, captured.retainCleanup, () =>
          owner.lifecycle.closeActorRetained(admittedActor, captured.maintenanceScope),
        );
        captured.onNativeStopped?.(
          admittedActor.nativeStopped,
          () => admittedActor.closeReceipt,
          () => admittedActor.nativeStoppedRecorded,
        );
        store = owner.bindClient(
          admittedActor,
          client,
          captured.maintenanceScope,
          retainSqliteWorkerAdmissionPathReferences(admittedActor, admittedPaths),
          () => outcome.status === "pending",
        );
        closeStore = owner.captureStoreClose(store);
        void admittedActor.opened.then(advance, refuse);
      }
      if (actor.openingError) {
        throw actor.openingError.error;
      }
      assertSqliteWorkerActorExecution(actor);
      if (actor.retirementRequested || actor.cleanupState) {
        throw new SqliteWorkerError("SQLite actor retired during client admission", "closed");
      }
      if (actor.initialized) {
        finish(store);
      }
    } catch (error) {
      refuse(error);
    } finally {
      advancing = false;
    }
  };
  const serviceOpen = owner.inputAdmission.startOpen(
    (settle) => {
      finishOpen = settle;
      if (outcome.status !== "pending") {
        settle();
        return;
      }
      advance();
    },
    (dependent) => {
      const current = actor ?? waitingActor;
      let dependency: SqliteWorkerFileOpenDependency | undefined;
      if (dependent && actor) {
        dependency = { kind: "open", actor, run: (operation) => inCaller(operation) };
      } else if (
        dependent &&
        !refusal &&
        waitingActor &&
        retainedBarrier &&
        owner.actors.get(waitingActor.key) === waitingActor &&
        isPendingOrdinaryFileClose(waitingActor, retainedBarrier)
      ) {
        dependency = {
          kind: "close",
          actor: waitingActor,
          barrier: retainedBarrier,
          run: (operation) => inCaller(operation),
        };
      }
      owner.serviceCapturedSlot(current?.slot, dependency);
      retainedBarrier?.service();
      refusal?.cleanup?.service();
      for (const slot of Array.from(owner.slots)) {
        if (slot.retiringRetained) {
          slot.retiringRetained.service();
        } else {
          for (const execution of Array.from(slot.executions)) {
            execution.retiringRetained?.service();
          }
        }
      }
      advance();
    },
    assertDependency
      ? (operation) =>
          inCaller(() => {
            assertDependency();
            captured.assertCurrent?.();
            operation();
          })
      : undefined,
  );
  return {
    result: completion.promise,
    read: () => outcome,
    service: () => {
      if (outcome.status === "pending") {
        try {
          serviceOpen();
        } catch (error) {
          // Refuse this dependent invocation without withdrawing the earlier open.
          // Its queued marker still joins the original identity turn in FIFO order.
          refuse(error);
        }
      }
    },
  };
}

/** Service only the exact open or ordinary-close prerequisite of the active global open. */
export function serviceSqliteWorkerFileOpenDependency(
  slot: Slot,
  dependency: SqliteWorkerFileOpenDependency,
  callback: Job,
  dispatch: () => void,
): void {
  let parent: Job | undefined = callback;
  while (parent && (parent.callback?.slot !== slot || parent.completed)) {
    parent = parent.callbackParent;
  }
  if (!parent || parent !== slot.current) {
    return;
  }
  if (dependency.kind === "close") {
    if (
      dependency.actor.slot !== slot ||
      !isPendingOrdinaryFileClose(dependency.actor, dependency.barrier)
    ) {
      return;
    }
    for (let ancestor: Job | undefined = callback; ancestor; ancestor = ancestor.callbackParent) {
      if (!ancestor.completed && ancestor.request.actor === dependency.actor.id) {
        return;
      }
    }
  }
  const prerequisite = slot.queue.find(
    (job) =>
      job.request.type === dependency.kind &&
      job.request.actor === dependency.actor.id &&
      job.executionWorker === dependency.actor.executionWorker &&
      !job.nativeDispatched &&
      !job.completed &&
      job.parent === undefined &&
      job.callbackParent === undefined &&
      (dependency.kind === "open" ||
        (job.callbackScope === undefined && job.callbackPredecessor === undefined)),
  );
  if (prerequisite) {
    // Borrow only the return stack; the accepted request keeps its original owner and guards.
    prerequisite.parent = parent;
    let nativeParent: Job | undefined = parent;
    while (nativeParent && nativeParent.executionWorker !== prerequisite.executionWorker) {
      nativeParent = nativeParent.parent;
    }
    prerequisite.nativeParent = nativeParent;
    dependency.run(dispatch);
  }
}
