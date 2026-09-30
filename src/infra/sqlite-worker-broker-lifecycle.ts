import { createDeferredCore } from "../shared/deferred.js";
import type { RuntimeWorkerGeneration } from "./runtime-worker-generation.js";
import {
  createSqliteLifecycleAggregateError,
  throwSqliteLifecycleErrors,
} from "./sqlite-lifecycle-errors.js";
import {
  createSqliteWorkerExecution,
  retireSqliteWorkerSlotRetained,
  retireSqliteWorkerExecutionRetained,
} from "./sqlite-worker-broker-execution.js";
import type { SqliteWorkerReplyOwner } from "./sqlite-worker-broker-reply.js";
import { closeSqliteWorkerActorRetained } from "./sqlite-worker-broker-retirement.js";
import type {
  Actor,
  SqliteWorkerExecution,
  EnqueueOptions,
  Slot,
  StoreClient,
  SqliteWorkerSlotOptions,
  SqliteWorkerSlotReservation,
  SqliteWorkerPlacement,
  SqliteWorkerRetainedResult,
  SqliteWorkerRetainedOutcome,
  Job,
} from "./sqlite-worker-broker.types.js";
import { SqliteWorkerError } from "./sqlite-worker-contract.js";
import type { DatabasePathIdentity } from "./sqlite-worker-identity.js";

function sharesPlacement(left: SqliteWorkerPlacement, right: SqliteWorkerPlacement): boolean {
  if (
    left.identity.key.startsWith("file:") &&
    left.identity.birthtime !== undefined &&
    left.identity.key === right.identity.key &&
    left.identity.birthtime === right.identity.birthtime
  ) {
    return true;
  }
  const paths = new Set([left.requestedPath, left.canonicalPath]);
  return [right.requestedPath, right.canonicalPath].some((pathname) => paths.has(pathname));
}

/** The broker retains these maps; this owner drains clients before native close custody. */
export function createSqliteWorkerLifecycle({
  explicitSqliteCloseReleasesNativeResources,
  actors,
  slots,
  stores,
  enqueueClose,
  failExecution,
  maxWorkers,
  maxStores,
  createReplyOwner,
}: {
  explicitSqliteCloseReleasesNativeResources: boolean;
  actors: Map<string, Actor>;
  slots: Set<Slot>;
  stores: Map<object, StoreClient>;
  enqueueClose: (
    actor: Actor,
    maintenanceScope?: EnqueueOptions["maintenanceScope"],
    settled?: Job["settled"],
  ) => Promise<unknown>;
  failExecution: (slot: Slot, execution: SqliteWorkerExecution, error: unknown) => void;
  maxWorkers: number;
  maxStores: number;
  createReplyOwner: (
    slot: Slot,
    execution: SqliteWorkerExecution,
  ) => Omit<SqliteWorkerReplyOwner, "resumeReply">;
}) {
  function serviceSlot(slot: Slot): void {
    slot.serviceReplies();
    for (const execution of slot.executions) {
      execution.worker.service();
    }
    const retained = new Set<Job>();
    const collect = (job: Job) => {
      if (retained.has(job)) {
        return;
      }
      retained.add(job);
      for (const child of job.provisionalChildren?.values() ?? []) {
        collect(child);
      }
    };
    for (let job = slot.current; job; job = job.parent) {
      collect(job);
    }
    for (const job of retained) {
      job.callback?.service();
      job.operationAdmission?.admission.service();
    }
    slot.serviceReplies();
    for (const execution of slot.executions) {
      execution.serviceFailureSettlement?.();
    }
    slot.serviceFailureSettlement?.();
    for (const execution of slot.executions) {
      if (
        execution.exited &&
        !execution.serviceFailureSettlement &&
        ![...slot.actors].some((actor) => actor.executionWorker === execution.worker) &&
        ![...retained, ...slot.queue].some((job) => job.executionWorker === execution.worker)
      ) {
        slot.executions.delete(execution);
      }
    }
  }

  /** Service only the retained slot for a deprecated synchronous domain operation. */
  function serviceRetainedReplies(store: object): boolean {
    const client = stores.get(store);
    if (!client) {
      throw new SqliteWorkerError("SQLite worker compatibility owner is unavailable", "closed");
    }
    const slot = client.actor.slot;
    serviceSlot(slot);
    const execution = [...slot.executions].find(
      (entry) => entry.worker === client.actor.executionWorker,
    );
    const failure = slot.failed ?? execution?.failed;
    if (failure) {
      throw failure;
    }
    return !slot.current && slot.queue.length === 0;
  }

  function ensureExecution(slot: Slot, options: SqliteWorkerSlotOptions): SqliteWorkerExecution {
    const current = [...slot.executions].find((entry) => !entry.exited);
    if (current) {
      if (current.failed || current.retiringRetained) {
        throw current.failed ?? new SqliteWorkerError("SQLite execution is retiring", "closed");
      }
      return current;
    }
    return createSqliteWorkerExecution(slot, "file", options, {
      explicitSqliteCloseReleasesNativeResources,
      createReplyOwner,
      failExecution,
    });
  }

  function createSlot(options: SqliteWorkerSlotOptions, borrowedGenerationSlot: boolean): Slot {
    const exited = createDeferredCore();
    const slot: Slot = {
      runtimeGeneration: options.runtimeGeneration,
      ...(borrowedGenerationSlot ? { borrowedGenerationSlot: true as const } : {}),
      executions: new Set(),
      serviceReplies() {
        for (const execution of slot.executions) {
          execution.serviceReplies();
        }
      },
      actors: new Set(),
      queue: [],
      exit: exited.promise,
      exited: false,
      recordJoinedExit() {
        if (slot.exited || [...slot.executions].some((execution) => !execution.exited)) {
          return;
        }
        slot.exited = true;
        slots.delete(slot);
        exited.resolve();
      },
      pendingOpens: 0,
      placements: new Set(),
    };
    ensureExecution(slot, options);
    slots.add(slot);
    return slot;
  }

  function unavailable(slot: Slot): boolean {
    const execution = [...slot.executions].find((entry) => !entry.exited);
    return Boolean(
      slot.failed ||
      slot.retiring ||
      slot.exited ||
      execution?.failed ||
      execution?.retiringRetained,
    );
  }

  function selectSlot(options: SqliteWorkerSlotOptions): SqliteWorkerSlotReservation | undefined {
    options.assertCurrent?.();
    const placement = options.placement
      ? { ...options.placement, identity: { ...options.placement.identity } }
      : undefined;
    const matching = placement
      ? [...slots].filter(
          (slot) =>
            !unavailable(slot) &&
            [...slot.placements].some((retained) => sharesPlacement(retained, placement)),
        )
      : [];
    if (matching.length > 1) {
      throw new SqliteWorkerError(
        "SQLite placement conflicts with existing workers",
        "unavailable",
      );
    }
    const matched = matching[0];
    if (!explicitSqliteCloseReleasesNativeResources && matched) {
      throw new SqliteWorkerError(
        "SQLite placement already retains a physical file actor",
        "closed",
      );
    }
    const available = [...slots].filter(
      (slot) => !unavailable(slot) && slot.runtimeGeneration === options.runtimeGeneration,
    );
    const placed = matched?.runtimeGeneration === options.runtimeGeneration ? matched : undefined;
    // A retained updater cannot borrow another generation's carrier or evict its actors.
    // One extra slot belongs to the broker, not to each generation requesting one.
    const borrowedGenerationSlot =
      explicitSqliteCloseReleasesNativeResources &&
      options.runtimeGeneration !== undefined &&
      available.length === 0 &&
      slots.size >= maxWorkers &&
      ![...slots].some((slot) => slot.borrowedGenerationSlot);
    let selected = placed;
    if (
      !selected &&
      !borrowedGenerationSlot &&
      slots.size >= (explicitSqliteCloseReleasesNativeResources ? maxWorkers : maxStores)
    ) {
      if (!available.length || !explicitSqliteCloseReleasesNativeResources) {
        if ([...slots].some(unavailable)) {
          return undefined;
        }
        throw new SqliteWorkerError(
          explicitSqliteCloseReleasesNativeResources
            ? "SQLite worker runtime capacity reached"
            : "SQLite worker store capacity reached",
          "overloaded",
        );
      }
      const idle = available.filter((slot) => {
        if (slot.current || slot.queue.length) {
          return false;
        }
        const execution = [...slot.executions].find((entry) => !entry.exited);
        return execution !== undefined && !execution.failed && !execution.retiringRetained;
      });
      const candidates = idle.length ? idle : available;
      selected = candidates.reduce((left, right) =>
        left.actors.size <= right.actors.size ? left : right,
      );
    }
    if (selected && placement) {
      reconcileFilePlacement(selected, placement, placement.identity);
    }
    selected ??= createSlot(options, borrowedGenerationSlot);
    const execution = ensureExecution(selected, options);
    selected.pendingOpens += 1;
    if (placement) {
      selected.placements.add(placement);
    }
    return { slot: selected, placement, executionWorker: execution.worker };
  }

  function reconcileFilePlacement(
    slot: Slot,
    placement: SqliteWorkerPlacement | undefined,
    identity: DatabasePathIdentity,
  ): void {
    if (!placement) {
      return;
    }
    const next = { ...placement, identity };
    if (
      [...slots].some(
        (other) =>
          other !== slot &&
          !other.failed &&
          !other.retiring &&
          [...other.placements].some((retained) => sharesPlacement(retained, next)),
      )
    ) {
      throw new SqliteWorkerError(
        "SQLite placement conflicts with an opened file identity",
        "unavailable",
      );
    }
    placement.identity = { ...identity };
  }

  function releasePlacement(slot: Slot, placement: SqliteWorkerPlacement | undefined): void {
    if (placement) {
      slot.placements.delete(placement);
    }
  }

  async function settleGeneration(
    generation: RuntimeWorkerGeneration | undefined,
  ): Promise<() => Promise<void>> {
    const retained = [...actors.values()].filter((actor) => actor.runtimeGeneration === generation);
    const retirement = Promise.allSettled(retained.map((actor) => retireActor(actor)));
    await Promise.all(retained.map(async (actor) => await actor.settlement));
    return async () => {
      const results = await retirement;
      const errors = results.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      throwSqliteLifecycleErrors(errors, "Retained SQLite worker cleanup failed");
      await Promise.all(
        [...slots]
          .filter((slot) => slot.runtimeGeneration === generation)
          .map((slot) => retireEmpty(slot)),
      );
    };
  }

  function cleanupFailedAdmission(
    actor: Actor,
    error: unknown,
    maintenanceScope?: EnqueueOptions["maintenanceScope"],
  ): Promise<void> {
    actor.openingError ??= { error };
    return closeActorRetained(actor, maintenanceScope).result;
  }

  function releaseActorReference(actor: Actor): void {
    actor.references -= 1;
    if (!actor.references) {
      actor.onReferencesDrained?.();
    }
  }

  async function rejectSlotAdmission(
    slot: Slot,
    error: unknown,
    placement?: SqliteWorkerPlacement,
  ): Promise<never> {
    slot.pendingOpens -= 1;
    releasePlacement(slot, placement);
    try {
      await retireEmpty(slot);
    } catch (cleanupError) {
      throw createSqliteLifecycleAggregateError(
        [error, cleanupError],
        "SQLite slot admission and cleanup failed",
        error,
      );
    }
    throw error;
  }

  function retireActorRetained(identity: object): SqliteWorkerRetainedResult<void> {
    const candidates = [...actors.values()];
    for (const slot of slots) {
      candidates.push(...slot.actors);
    }
    const actor = candidates.find((entry) => entry === identity);
    return retireOwnedActorRetained(actor);
  }

  function retireOwnedActorRetained(actor: Actor | undefined): SqliteWorkerRetainedResult<void> {
    if (actor?.retirementRetained && actor.retirementRetained.read().status !== "rejected") {
      return actor.retirementRetained;
    }
    const completed = createDeferredCore();
    void completed.promise.catch(() => {});
    let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
    let native: SqliteWorkerRetainedResult<void> | undefined;
    const clients: SqliteWorkerRetainedResult<void>[] = [];
    let initialized = false;
    let advancing = false;
    function advance() {
      if (!initialized || advancing || outcome.status !== "pending") {
        return;
      }
      advancing = true;
      try {
        const results = clients.map((client) => client.read());
        if (results.some((result) => result.status === "pending") || actor?.references) {
          return;
        }
        const errors = results.flatMap((result) =>
          result.status === "rejected" ? [result.error] : [],
        );
        if (!errors.length && actor) {
          if (!native) {
            native = closeActorRetained(actor);
            void native.result.then(advance, advance);
          }
          const closed = native.read();
          if (closed.status === "pending") {
            return;
          }
          if (closed.status === "rejected") {
            errors.push(closed.error);
          }
        }
        if (actor) {
          actor.retirement = undefined;
          actor.onReferencesDrained = undefined;
        }
        if (errors.length) {
          throw new AggregateError(errors, "SQLite actor retirement failed", { cause: errors[0] });
        }
        outcome = { status: "fulfilled", value: undefined };
        completed.resolve();
      } catch (error) {
        outcome = { status: "rejected", error };
        completed.reject(error);
      } finally {
        advancing = false;
      }
    }
    const retained: SqliteWorkerRetainedResult<void> = {
      result: completed.promise,
      read: () => outcome,
      service() {
        for (const client of clients) {
          client.service();
        }
        native?.service();
        advance();
      },
    };
    if (actor) {
      actor.retirementRequested = true;
      actor.retirementRetained = retained;
      actor.retirement = retained.result;
      const drained = createDeferredCore();
      actor.settlement = drained.promise;
      actor.onReferencesDrained = () => {
        drained.resolve();
        advance();
      };
      if (!actor.references) {
        drained.resolve();
      }
      for (const client of [...stores.values()].filter((entry) => entry.actor === actor)) {
        const closing = client.closeRetained();
        clients.push(closing);
        void closing.result.then(advance, advance);
      }
    }
    initialized = true;
    advance();
    return retained;
  }

  function retireActor(identity: object): Promise<void> {
    return retireActorRetained(identity).result;
  }

  function closeActorRetained(
    actor: Actor,
    maintenanceScope?: EnqueueOptions["maintenanceScope"],
  ): SqliteWorkerRetainedResult<void> {
    return closeSqliteWorkerActorRetained(
      {
        explicitSqliteCloseReleasesNativeResources,
        forget,
        failExecution,
        enqueueClose,
        serviceSlot,
        retireRetained,
        retireExecutionRetained,
      },
      actor,
      maintenanceScope,
    );
  }

  function closeActor(
    actor: Actor,
    maintenanceScope?: EnqueueOptions["maintenanceScope"],
  ): Promise<void> {
    return closeActorRetained(actor, maintenanceScope).result;
  }

  function forget(actor: Actor): void {
    if (actor.cleanupState === "complete") {
      return;
    }
    if (actors.get(actor.key) === actor) {
      actors.delete(actor.key);
    }
    actor.slot.actors.delete(actor);
    releasePlacement(actor.slot, actor.placement);
    actor.cleanupState = "complete";
  }

  async function retireEmpty(slot: Slot): Promise<void> {
    if (!slot.actors.size && !slot.pendingOpens) {
      await retire(slot);
    }
  }

  function retireExecutionRetained(
    slot: Slot,
    execution: SqliteWorkerExecution,
  ): SqliteWorkerRetainedResult<void> {
    return retireSqliteWorkerExecutionRetained(slot, execution, serviceSlot);
  }

  function retireRetained(slot: Slot): SqliteWorkerRetainedResult<void> {
    return retireSqliteWorkerSlotRetained(slot, serviceSlot, retireExecutionRetained);
  }

  function retire(slot: Slot): Promise<void> {
    return retireRetained(slot).result;
  }

  return {
    serviceRetainedReplies,
    serviceSlot,
    tryReserveSlot: selectSlot,
    reconcileFilePlacement,
    settleGeneration,
    cleanupFailedAdmission,
    releaseActorReference,
    rejectSlotAdmission,
    retireActor,
    retireActorRetained,
    retireOwnedActorRetained,
    closeActor,
    closeActorRetained,
    forget,
    retireEmpty,
    retire,
    retireRetained,
    retireExecutionRetained,
    ensureExecution,
  };
}
