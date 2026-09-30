import { availableParallelism } from "node:os";
import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import { getChildLogger } from "../logging/logger.js";
import { createDeferredCore } from "../shared/deferred.js";
import { captureSqliteWorkerClosePolicy } from "./bun-sqlite-library.js";
import { findUnclaimedSharedStateActors } from "./sqlite-worker-broker-admission.js";
import { SqliteWorkerCallbacks } from "./sqlite-worker-broker-callbacks.js";
import {
  reserveSqliteWorkerFile,
  serviceSqliteWorkerFileOpenDependency,
  type SqliteWorkerFileOpenDependency,
} from "./sqlite-worker-broker-file.js";
import { createSqliteWorkerLifecycle } from "./sqlite-worker-broker-lifecycle.js";
import { dispatchSqliteWorkerJob } from "./sqlite-worker-broker-reply.js";
import { createSqliteWorkerBrokerRetirement } from "./sqlite-worker-broker-retirement.js";
import { settleSqliteWorkerJob } from "./sqlite-worker-broker-settlement.js";
import type {
  Actor,
  EnqueueOptions,
  Job,
  RequestBody,
  Slot,
  SqliteWorkerStoreOptions,
  StoreClient,
  SqliteWorkerOpenCustody,
  SqliteWorkerInputPreparation,
  SqliteWorkerInputRetention,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";
import {
  bindSqliteWorkerClient,
  getSqliteWorkerClientActorIdentity,
  runSqliteWorkerClientOperation,
} from "./sqlite-worker-client.js";
import {
  SQLITE_WORKER_MAX_MESSAGE_BYTES,
  SqliteWorkerError,
  type SqliteWorkerOperations,
  type SqliteWorkerStore,
} from "./sqlite-worker-contract.js";
import {
  SqliteWorkerInputAdmission,
  waitForSqliteWorkerCapacity,
} from "./sqlite-worker-input-admission.js";
import type { SqliteWorkerAdmissionFactory } from "./sqlite-worker-operation-admission.js";
import type { SqliteWorkerOperationSettlement } from "./sqlite-worker-operation-settlement.js";
import type { SqliteWorkerStateContext } from "./sqlite-worker-state-context.js";

const ADMISSION_TIMEOUT_MS = 10_000;
const MAX_STORES = 64;
const MAX_QUEUED_COMMAND_BYTES = 64 * 1024 * 1024;
export const SQLITE_WORKER_MAX_REQUESTS_PER_WORKER = 128;
const SQLITE_WORKER_MAX_QUEUED_BYTES = 256 * 1024 * 1024;

export class SqliteWorkerBroker {
  private readonly callbacks = new SqliteWorkerCallbacks({
    retainReturn: (bytes) => {
      if (this.bytes + this.inputAdmission.retainedBytes + bytes > SQLITE_WORKER_MAX_QUEUED_BYTES) {
        throw new SqliteWorkerError("SQLite callback return capacity reached", "overloaded");
      }
      return this.inputAdmission.retain(bytes);
    },
    dispatch: (slot) => this.dispatch(slot),
    serviceSlot: (slot) => this.lifecycle.serviceSlot(slot),
    fail: (slot, parent, error) => this.failExecutionForJob(slot, parent, error),
  });
  // WAL readers on independent actors can progress on separate threads without blocking writers.
  private readonly maxWorkers = Math.min(8, Math.max(2, Math.floor(availableParallelism() / 8)));
  private readonly waiters = new Map<Slot, Set<(error?: unknown) => void>>();
  private readonly resuming = new Set<Slot>();
  private nextAdmissionWarning = 0;
  private readonly actors = new Map<string, Actor>();
  private readonly slots = new Set<Slot>();
  private readonly clients = new Set<object>();
  private readonly stores = new Map<object, StoreClient>();
  private readonly operations = new Map<
    Promise<void>,
    SqliteWorkerRetainedResult<void> | undefined
  >();
  private readonly lifecycle = createSqliteWorkerLifecycle({
    explicitSqliteCloseReleasesNativeResources: captureSqliteWorkerClosePolicy(),
    actors: this.actors,
    slots: this.slots,
    stores: this.stores,
    enqueueClose: (actor, maintenanceScope, settled) =>
      this.enqueue(actor.slot, { type: "close", actor: actor.id }, 0, {
        maintenanceScope,
        // Ordinary close retains its own queue position until a live dependency lends the route.
        parent: null,
        settled,
      }),
    failExecution: (slot, execution, error) =>
      this.retirement.failExecution(slot, execution, error),
    maxWorkers: this.maxWorkers,
    maxStores: MAX_STORES,
    createReplyOwner: (slot, execution) => ({
      fail: (reason, currentError, openOutcome, completed) =>
        this.retirement.failExecution(
          slot,
          execution,
          reason,
          currentError,
          openOutcome,
          completed,
        ),
      finish: (job, error, value, settlement) => this.finish(slot, job, error, value, settlement),
      dispatch: () => this.dispatch(slot),
      returnProvisional: (job, outcome, receipt) => {
        const callbackScope = job.callbackScope;
        this.releaseTransport(slot, job);
        job.returned?.(outcome, receipt);
        this.resumeWaiters(slot);
        this.callbacks.advance(job.callbackParent, callbackScope);
      },
    }),
  });
  private nextActor = 0;
  private nextRequest = 0;
  // A slow worker cannot consume another worker's admission; retained input still shares one budget.
  private readonly requests = new WeakMap<Slot, number>();
  private bytes = 0;
  private readonly inputAdmission = new SqliteWorkerInputAdmission({
    queuedBytes: () => this.bytes,
    isClosing: () => this.retirement.closing !== undefined,
    maxQueuedBytes: SQLITE_WORKER_MAX_QUEUED_BYTES,
    maxQueuedInputBytes: MAX_QUEUED_COMMAND_BYTES,
    maxMessageBytes: SQLITE_WORKER_MAX_MESSAGE_BYTES,
  });

  private readonly retirement = createSqliteWorkerBrokerRetirement({
    clients: this.clients,
    stores: this.stores,
    actors: this.actors,
    slots: this.slots,
    operations: this.operations,
    waiters: this.waiters,
    inputAdmission: this.inputAdmission,
    lifecycle: this.lifecycle,
    dispatch: (slot) => this.dispatch(slot),
    finish: (slot, job, error, value, settlement) =>
      this.finish(slot, job, error, value, settlement),
  });

  reserveInputPreparation(
    inputBytes: number,
    retention: SqliteWorkerInputRetention = "stream",
  ): SqliteWorkerInputPreparation {
    return this.inputAdmission.reserveInputPreparation(inputBytes, retention);
  }

  /** Reserve the existing file opener now; its recorded native outcome drives either consumer. */
  reserveFile<Operations extends SqliteWorkerOperations>(
    options: SqliteWorkerStoreOptions,
    stateContext: SqliteWorkerStateContext | undefined,
    assertCurrent: () => void,
    custody: SqliteWorkerOpenCustody,
  ): SqliteWorkerRetainedResult<SqliteWorkerStore<Operations> | undefined> {
    return reserveSqliteWorkerFile<Operations>(options, stateContext, assertCurrent, custody, {
      identity: this,
      isClosing: () => this.retirement.closing !== undefined,
      maxStores: MAX_STORES,
      actors: this.actors,
      slots: this.slots,
      clients: this.clients,
      inputAdmission: this.inputAdmission,
      lifecycle: this.lifecycle,
      nextActor: () => ++this.nextActor,
      enqueue: (slot, body, bytes, requestOptions) =>
        this.enqueue(slot, body, bytes, requestOptions),
      bindClient: (actor, client, scope, releasePaths, opening) =>
        this.bindClient<Operations>(actor, client, scope, releasePaths, opening),
      captureStoreClose: (store) => this.captureStoreClose(store),
      captureOpenDependency: () => this.callbacks.captureOpenDependency(),
      serviceCapturedSlot: (slot, dependency) => this.serviceCapturedSlot(slot, dependency),
    });
  }

  private captureStoreClose(store: object): () => SqliteWorkerRetainedResult<void> {
    const client = this.stores.get(store);
    if (!client) {
      throw new SqliteWorkerError("SQLite worker store is closed", "closed");
    }
    return () => client.closeRetained();
  }

  open<Operations extends SqliteWorkerOperations>(
    options: SqliteWorkerStoreOptions,
    stateContext?: SqliteWorkerStateContext,
    assertCurrent?: () => void,
    custody: SqliteWorkerOpenCustody = {},
  ): Promise<SqliteWorkerStore<Operations> | undefined> {
    try {
      return this.reserveFile<Operations>(
        options,
        stateContext,
        assertCurrent ?? (() => {}),
        custody,
      ).result;
    } catch (error) {
      return Promise.reject(toErrorObject(error, "SQLite worker input could not be admitted"));
    }
  }

  private bindClient<Operations extends SqliteWorkerOperations>(
    owned: Actor,
    client: object,
    maintenanceScope?: EnqueueOptions["maintenanceScope"],
    releasePaths: () => void = () => {},
    opening: () => boolean = () => false,
  ): SqliteWorkerStore<Operations> {
    return bindSqliteWorkerClient<Operations>(owned, client, {
      stores: this.stores,
      clients: this.clients,
      draining: () => this.retirement.closingRetained,
      opening,
      enqueue: (slot, body, bytes, options) => this.enqueue(slot, body, bytes, options),
      releaseReference: () => this.lifecycle.releaseActorReference(owned),
      releasePaths,
      closeActorRetained: () => this.lifecycle.closeActorRetained(owned, maintenanceScope),
      retireSlotRetained: () => this.lifecycle.retireRetained(owned.slot),
      retireExecutionRetained: (execution) =>
        this.lifecycle.retireExecutionRetained(owned.slot, execution),
      service: () => this.lifecycle.serviceSlot(owned.slot),
    });
  }

  runOperation<Operations extends SqliteWorkerOperations, T>(
    store: SqliteWorkerStore<Operations>,
    operation: (scope: Pick<SqliteWorkerStore<Operations>, "execute">) => T | Promise<T>,
    stateContext?: SqliteWorkerStateContext,
    assertCurrent?: (commandType: PropertyKey) => void,
    createAdmission?: SqliteWorkerAdmissionFactory,
    settled?: Job["settled"],
  ): Promise<T> {
    return runSqliteWorkerClientOperation(
      this.retirement.closing ? undefined : this.stores.get(store),
      operation,
      stateContext,
      (pending) => {
        this.operations.set(pending, undefined);
        this.retirement.operationsChanged();
        return () => {
          this.operations.delete(pending);
          this.retirement.operationsChanged();
        };
      },
      assertCurrent,
      createAdmission,
      settled,
    );
  }

  isAvailable(store: object): boolean {
    return this.stores.get(store)?.isAvailable() ?? false;
  }

  private serviceCapturedSlot(slot?: Slot, dependency?: SqliteWorkerFileOpenDependency): void {
    const callback = this.callbacks.active();
    if (callback?.callback) {
      if (slot && dependency) {
        serviceSqliteWorkerFileOpenDependency(slot, dependency, callback, () =>
          this.dispatch(slot),
        );
      }
      this.callbacks.service(callback);
    }
    if (slot) {
      this.lifecycle.serviceSlot(slot);
    }
  }

  serviceRetainedReplies(store: object): boolean {
    const callback = this.callbacks.active();
    if (callback?.callback) {
      this.callbacks.service(callback);
    }
    return this.lifecycle.serviceRetainedReplies(store);
  }

  retireActor(identity: object): Promise<void> {
    return this.lifecycle.retireActor(identity);
  }

  getActorIdentity(
    store: object,
  ): Readonly<Pick<Extract<Actor, { kind: "file" }>, "key" | "databasePath">> {
    return getSqliteWorkerClientActorIdentity(this.stores.get(store));
  }

  hasUnclaimedSharedStateCleanup(databasePath: string): boolean {
    return findUnclaimedSharedStateActors(this.actors.values(), databasePath).length > 0;
  }

  closeUnclaimedSharedStateRetained(databasePath: string): SqliteWorkerRetainedResult<void> {
    return this.retirement.closeUnclaimedSharedStateRetained(databasePath);
  }

  closeUnclaimedSharedState(databasePath: string): Promise<void> {
    return this.closeUnclaimedSharedStateRetained(databasePath).result;
  }

  private enqueue(
    slot: Slot,
    body: RequestBody,
    bytes: number,
    options: EnqueueOptions = {},
    admitted = false,
  ): Promise<unknown> {
    const { signal, dispatchState, scope, assertCurrent, createAdmission, maintenanceScope } =
      options;
    const activeCallback = this.callbacks.active();
    const callbackParent =
      options.parent !== undefined ? (options.parent ?? undefined) : activeCallback;
    const callbackScope = callbackParent?.callback;
    const predecessor = callbackScope?.lastChild;
    let parent = callbackParent;
    while (parent && (!parent.callback || parent.callback.slot !== slot || parent.completed)) {
      parent = parent.callbackParent;
    }
    const reject = (error: Error) => {
      options.settled?.(
        { status: "rejected", error },
        { settlement: { kind: "not-entered", error } },
      );
      return Promise.reject(error);
    };
    if (this.retirement.closing && body.type !== "close" && !scope?.active) {
      return reject(new SqliteWorkerError("SQLite worker host is closing", "closed"));
    }
    if (slot.failed) {
      return reject(slot.failed);
    }
    const actor = [...slot.actors].find((candidate) => candidate.id === body.actor);
    const execution =
      actor && [...slot.executions].find((entry) => entry.worker === actor.executionWorker);
    if (
      !actor ||
      !execution ||
      execution.failed ||
      execution.exited ||
      execution.retiringRetained
    ) {
      return reject(
        execution?.failed ??
          new SqliteWorkerError("SQLite actor execution is unavailable", "closed"),
      );
    }
    let nativeParent = parent;
    while (nativeParent && nativeParent.executionWorker !== execution.worker) {
      nativeParent = nativeParent.parent;
    }
    for (let ancestor = parent; ancestor; ancestor = ancestor.parent) {
      if (ancestor.executionFailure) {
        return reject(ancestor.executionFailure);
      }
    }
    const activeInput = body.type === "execute" && bytes > MAX_QUEUED_COMMAND_BYTES;
    const reservedBytes = activeInput ? SQLITE_WORKER_MAX_MESSAGE_BYTES : bytes;
    if (
      body.type !== "close" &&
      ((body.type !== "execute" && bytes > SQLITE_WORKER_MAX_MESSAGE_BYTES) ||
        (activeInput &&
          ((predecessor && !predecessor.transportReleased) ||
            (slot.current && slot.current !== parent) ||
            slot.queue.some((queued) => queued.parent === parent) ||
            slot.pendingOpens > 0)) ||
        this.bytes + this.inputAdmission.retainedBytes + reservedBytes >
          SQLITE_WORKER_MAX_QUEUED_BYTES)
    ) {
      return reject(new SqliteWorkerError("SQLite worker queue capacity reached", "overloaded"));
    }
    if (
      body.type !== "close" &&
      ((this.requests.get(slot) ?? 0) >= SQLITE_WORKER_MAX_REQUESTS_PER_WORKER ||
        (!admitted && this.waiters.has(slot)))
    ) {
      // Oversized active inputs cannot be retained in an admission queue.
      if (activeInput || this.retirement.closing || callbackParent) {
        return reject(new SqliteWorkerError("SQLite worker queue capacity reached", "overloaded"));
      }
      return waitForSqliteWorkerCapacity(
        slot,
        body,
        bytes,
        { ...options, parent: parent ?? null },
        {
          waiters: this.waiters,
          admissionTimeoutMs: ADMISSION_TIMEOUT_MS,
          maxRequestsPerWorker: SQLITE_WORKER_MAX_REQUESTS_PER_WORKER,
          retain: (retainedBytes) => this.inputAdmission.retain(retainedBytes),
          warnAdmission: (selectedSlot, waitMs) => this.warnAdmission(selectedSlot, waitMs),
          enqueue: (selectedSlot, queuedBody, queuedBytes, queuedOptions, wasAdmitted) =>
            this.enqueue(selectedSlot, queuedBody, queuedBytes, queuedOptions, wasAdmitted),
        },
      );
    }
    const result = createDeferredCore<unknown>();
    const job: Job = {
      executionWorker: execution.worker,
      nativeParent,
      runCallback: (request, grant, callback, callbackContext) =>
        this.callbacks.run(slot, job, request, grant, callback, callbackContext),
      runRetainedCallback: (request, grant, start, callbackContext) =>
        this.callbacks.runRetained(slot, job, request, grant, start, callbackContext),
      callbackParent,
      callbackScope,
      callbackPredecessor: predecessor,
      parent,
      returned: options.returned,
      signal,
      settled: options.settled,
      maintenanceScope,
      createAdmission,
      assertCurrent: parent
        ? () => {
            let ancestor: Job | undefined = parent;
            while (ancestor?.completed) {
              ancestor = ancestor.parent;
            }
            if (ancestor?.executionFailure) {
              throw ancestor.executionFailure;
            }
            ancestor?.assertCurrent?.();
            assertCurrent?.();
          }
        : assertCurrent,
      dispatchState,
      request: { ...body, id: ++this.nextRequest },
      bytes: reservedBytes,
      resolve: result.resolve,
      reject: result.reject,
      detach: () => signal?.removeEventListener("abort", abort),
    };
    if (callbackScope) {
      callbackScope.pending.set(job, slot);
      callbackScope.lastChild = job;
    }
    const abort = () => {
      const index = slot.queue.indexOf(job);
      if (index >= 0) {
        slot.queue.splice(index, 1);
        this.finish(slot, job, signal?.reason ?? new Error("SQLite worker operation canceled"));
      }
      // Once dispatched, retain the Promise until the database outcome is known.
    };
    this.requests.set(slot, (this.requests.get(slot) ?? 0) + 1);
    this.bytes += reservedBytes;
    if (activeInput) {
      signal?.addEventListener("abort", abort, { once: true });
      // A complete oversized value is active-operation memory, never retained in the bounded queue.
      if (signal?.aborted) {
        this.finish(slot, job, signal.reason ?? new Error("SQLite worker operation canceled"));
      } else {
        this.dispatchJob(slot, job);
      }
      return result.promise;
    }
    slot.queue.push(job);
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) {
      abort();
    }
    this.dispatch(slot);
    return result.promise;
  }

  private warnAdmission(slot: Slot, waitMs: number): void {
    const now = Date.now();
    if (now >= this.nextAdmissionWarning) {
      this.nextAdmissionWarning = now + ADMISSION_TIMEOUT_MS;
      getChildLogger({ subsystem: "infra/sqlite-worker" }).warn("SQLite worker admission delayed", {
        queueDepth: this.waiters.get(slot)?.size ?? 0,
        waitMs,
      });
    }
  }

  private dispatch(slot: Slot): void {
    if (slot.exited || slot.failed || (slot.current && !slot.current.callback)) {
      return;
    }
    const parent = slot.current;
    const index = slot.queue.findIndex(
      (job) =>
        job.parent === parent &&
        (!job.callbackPredecessor || job.callbackPredecessor.transportReleased),
    );
    if (index < 0) {
      if (!parent) {
        for (const execution of slot.executions) {
          execution.worker.unref();
        }
      }
      return;
    }
    const job = slot.queue.splice(index, 1)[0]!;
    this.dispatchJob(slot, job);
  }

  private dispatchJob(slot: Slot, job: Job): void {
    job.callbackPredecessor = undefined;
    slot.current = job;
    job.executionWorker.ref();
    dispatchSqliteWorkerJob(slot, job, (error, retire) => {
      // Slot failure owns settlement after joining preparation and native retirement.
      if (slot.current !== job) {
        return;
      }
      if (retire) {
        // Retire uncertain transfers or failed prepared-custody cleanup before settlement.
        this.failExecutionForJob(
          slot,
          job,
          error,
          toErrorObject(error, "SQLite worker transfer failed"),
        );
      } else {
        slot.current = undefined;
        this.finish(slot, job, error, undefined, { kind: "not-entered", error }, { error });
        this.dispatch(slot);
      }
    });
  }

  private failExecutionForJob(slot: Slot, job: Job, error: unknown, currentError?: Error): void {
    const execution = [...slot.executions].find((entry) => entry.worker === job.executionWorker);
    if (!execution) {
      this.retirement.fail(
        slot,
        new Error("SQLite failed Job lost its execution owner", { cause: error }),
      );
      return;
    }
    this.retirement.failExecution(slot, execution, error, currentError);
  }

  private finish(
    slot: Slot,
    job: Job,
    error?: unknown,
    value?: unknown,
    settlement?: SqliteWorkerOperationSettlement,
    rejected?: NonNullable<Job["completedError"]>,
  ): void {
    const callbackScope = job.callbackScope;
    let parent = job.parent;
    while (parent?.completed) {
      parent = parent.parent;
    }
    if (parent && !slot.current && !slot.failed) {
      slot.current = parent;
    }
    job.provisionalOwner?.provisionalChildren?.delete(job.request.id);
    this.releaseTransport(slot, job);
    job.completed = true;
    if (rejected || error !== undefined) {
      job.completedError = rejected ?? { error };
    }
    settleSqliteWorkerJob(job, error, value, settlement, rejected);
    job.readCallbackDeliveryFailure = undefined;
    job.provisionalOutcome = undefined;
    this.resumeWaiters(slot);
    this.callbacks.advance(job.callbackParent, callbackScope);
  }

  private releaseTransport(slot: Slot, job: Job): void {
    if (job.transportReleased) {
      return;
    }
    job.transportReleased = true;
    job.callbackScope?.pending.delete(job);
    job.callbackScope = undefined;
    this.requests.set(slot, (this.requests.get(slot) ?? 0) - 1);
    this.bytes -= job.bytes;
    job.bytes = 0;
    if (job.request.type === "execute") {
      job.request.input = Buffer.alloc(0);
    }
  }

  private resumeWaiters(slot: Slot): void {
    if (!this.resuming.has(slot)) {
      this.resuming.add(slot);
      while ((this.requests.get(slot) ?? 0) < SQLITE_WORKER_MAX_REQUESTS_PER_WORKER) {
        const resume = this.waiters.get(slot)?.values().next().value;
        if (!resume) {
          break;
        }
        resume();
      }
      this.resuming.delete(slot);
    }
  }

  closeRetained(): SqliteWorkerRetainedResult<void> {
    return this.retirement.closeRetained();
  }

  close(): Promise<void> {
    return this.closeRetained().result;
  }
}
