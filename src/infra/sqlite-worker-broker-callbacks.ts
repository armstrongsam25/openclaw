import { AsyncLocalStorage } from "node:async_hooks";
import { serialize } from "node:v8";
import { createRetainedOperation, type RetainedOperation } from "./retained-operation.js";
import type {
  Job,
  Slot,
  SqliteWorkerCallbackContext,
  SqliteWorkerCallbackScope,
} from "./sqlite-worker-broker.types.js";
import { SQLITE_WORKER_MAX_MESSAGE_BYTES, SqliteWorkerError } from "./sqlite-worker-contract.js";
import type { SqliteWorkerCallbackRequest } from "./sqlite-worker-operation-settlement.js";
import { ownedWorkerBytes } from "./worker-transfer-bytes.js";

type CallbackQueueOwner = {
  retainReturn(bytes: number): () => void;
  dispatch(slot: Slot): void;
  serviceSlot(slot: Slot): void;
  fail(slot: Slot, parent: Job, error: unknown): void;
};

/** Callback ancestry retains existing broker Jobs; queue custody stays with the broker. */
export class SqliteWorkerCallbacks {
  private readonly callbacks = new AsyncLocalStorage<SqliteWorkerCallbackContext>();
  constructor(private readonly owner: CallbackQueueOwner) {}

  get context(): SqliteWorkerCallbackContext | undefined {
    const context = this.callbacks.getStore();
    return context && !context.job.completed && context.job.callback === context.scope
      ? context
      : undefined;
  }

  /** A synchronous callback extends its accepted job; delivery joins descendants independently. */
  run<T>(
    slot: Slot,
    parent: Job,
    request: SqliteWorkerCallbackRequest,
    grant: () => boolean,
    callback: () => T,
    context?: object,
  ): T {
    const scope = this.beginScope(slot, parent, request, grant, context);
    let value: T;
    try {
      value = this.callbacks.run({ job: parent, scope }, () => {
        const returned = callback();
        try {
          scope.completion = this.prepareReturn(scope, returned);
        } catch (error) {
          scope.deliveryFailure = { error };
          throw error;
        }
        return returned;
      });
    } catch (error) {
      scope.accepting = false;
      this.refuse(scope, error);
      this.advance(parent, scope);
      throw error;
    }
    scope.accepting = false;
    this.advance(parent, scope);
    if (scope.deliveryFailure) {
      throw scope.deliveryFailure.error;
    }
    return value;
  }

  /** Observe the producer's available value without adopting its final settlement or cleanup. */
  runRetained<T>(
    slot: Slot,
    parent: Job,
    request: SqliteWorkerCallbackRequest,
    grant: () => boolean,
    start: () => RetainedOperation<T>,
    context?: object,
  ): RetainedOperation<T> {
    const inCaller = AsyncLocalStorage.snapshot();
    const scope = this.beginScope(slot, parent, request, grant, context);
    let child: RetainedOperation<T> | undefined;
    scope.observeReturn = () =>
      inCaller(() =>
        this.callbacks.run({ job: parent, scope }, () => {
          if (scope.accepting || !child || scope.completion || parent.callback !== scope) {
            return;
          }
          let outcome = child.read();
          if (outcome.status === "pending") {
            child.service();
            outcome = child.read();
            if (outcome.status === "pending") {
              return;
            }
          }
          scope.observeReturn = undefined;
          if (outcome.status === "rejected") {
            this.refuse(scope, outcome.error);
            return;
          }
          try {
            scope.completion = this.prepareReturn(scope, outcome.value);
          } catch (error) {
            scope.deliveryFailure = { error };
            this.refuse(scope, error);
          }
        }),
      );
    try {
      child = this.callbacks.run({ job: parent, scope }, start);
    } catch (error) {
      const rejected = createRetainedOperation<T>(() => {});
      rejected.reject(error);
      child = rejected.operation;
    }
    scope.accepting = false;
    const retained = child;
    // Final Promise notification is a wakeup; provisional delivery comes from read/Job progress.
    void retained.result.then(
      () => this.advance(parent, scope),
      () => this.advance(parent, scope),
    );
    this.advance(parent, scope);
    return retained;
  }

  private beginScope(
    slot: Slot,
    parent: Job,
    request: SqliteWorkerCallbackRequest,
    grant: () => boolean,
    context?: object,
  ): SqliteWorkerCallbackScope {
    const { port, acknowledgment, transaction } = request;
    if (typeof transaction !== "boolean") {
      throw new Error("SQLite callback requires its actual transaction mode");
    }
    if (
      slot.failed ||
      parent.executionFailure ||
      slot.current !== parent ||
      !parent.nativeDispatched ||
      parent.callback ||
      parent.completed
    ) {
      throw new SqliteWorkerError("SQLite callback lost its accepted operation", "closed");
    }
    parent.assertCurrent?.();
    parent.readCallbackDeliveryFailure = undefined;
    if (acknowledgment.byteLength !== Int32Array.BYTES_PER_ELEMENT) {
      throw new Error("SQLite callback acknowledgement is invalid");
    }
    Atomics.store(new Int32Array(acknowledgment), 0, 1);
    if (!grant()) {
      throw new SqliteWorkerError("SQLite callback admission was refused", "closed");
    }
    parent.callbackContext = context;
    const scope: SqliteWorkerCallbackScope = {
      transaction,
      port,
      slot,
      accepting: true,
      service: () => {
        if (parent.callback === scope) {
          this.service(parent);
        }
      },
      cancel: () => this.cancelScope(parent, scope),
      pending: new Map(),
    };
    parent.callback = scope;
    parent.readCallbackDeliveryFailure = () => scope.deliveryFailure;
    return scope;
  }

  private cancelScope(parent: Job, scope: SqliteWorkerCallbackScope): void {
    scope.releaseReturn?.();
    scope.releaseReturn = undefined;
    scope.observeReturn = undefined;
    scope.completion = undefined;
    scope.accepting = false;
    if (parent.callback === scope) {
      parent.callback = undefined;
    }
    scope.port.close();
  }

  private refuse(scope: SqliteWorkerCallbackScope, error: unknown): void {
    scope.releaseReturn?.();
    scope.releaseReturn = undefined;
    scope.observeReturn = undefined;
    scope.refusal ??= { error };
    // Already accepted descendants keep their original producers and drain before refusal.
    scope.completion = undefined;
  }

  private prepareReturn(
    scope: SqliteWorkerCallbackScope,
    value: unknown,
  ): NonNullable<SqliteWorkerCallbackScope["completion"]> {
    const encoded = serialize(value);
    if (encoded.byteLength > SQLITE_WORKER_MAX_MESSAGE_BYTES) {
      throw new SqliteWorkerError(
        "SQLite callback result exceeds the transport limit",
        "overloaded",
      );
    }
    scope.releaseReturn = this.owner.retainReturn(encoded.byteLength);
    return { accepted: true, value: ownedWorkerBytes(encoded) };
  }

  active(): Job | undefined {
    const context = this.context;
    if (!context) {
      return undefined;
    }
    return context.scope.accepting ? context.job : undefined;
  }

  /** Pin the exact accepted scope before joining global file-open identity ordering. */
  captureOpenDependency(): (() => void) | undefined {
    const context = this.context;
    const parent = this.active();
    const scope = context?.scope;
    if (!parent || !scope) {
      return undefined;
    }
    return () => {
      if (
        parent.completed ||
        parent.executionFailure ||
        parent.callback !== scope ||
        !scope.accepting
      ) {
        throw new SqliteWorkerError("SQLite callback open dependency expired", "closed");
      }
      parent.assertCurrent?.();
    };
  }

  service(parent: Job): void {
    const selected = new Set<Slot>();
    const entered: SqliteWorkerCallbackScope[] = [];
    try {
      for (let current: Job | undefined = parent; current; current = current.callbackParent) {
        const scope = current.callback;
        if (!scope || scope.servicing) {
          continue;
        }
        scope.servicing = true;
        entered.push(scope);
        this.advance(current, scope);
        for (const slot of scope.pending.values()) {
          selected.add(slot);
        }
      }
      for (const slot of selected) {
        this.owner.dispatch(slot);
        this.owner.serviceSlot(slot);
      }
    } finally {
      for (const scope of entered) {
        scope.servicing = undefined;
      }
    }
  }

  complete(parent: Job, scope: SqliteWorkerCallbackScope): { error: unknown } | undefined {
    if (
      parent.callback !== scope ||
      parent.executionFailure ||
      !scope.completion ||
      scope.pending.size
    ) {
      return undefined;
    }
    try {
      if (!scope.slot.failed && !parent.completed) {
        const completion = scope.completion;
        scope.port.postMessage(
          { type: "return", ...completion },
          completion.accepted ? [completion.value.buffer] : [],
        );
      }
    } catch (error) {
      scope.deliveryFailure ??= { error };
      this.owner.fail(scope.slot, parent, error);
      return { error };
    } finally {
      if (!parent.executionFailure) {
        this.cancelScope(parent, scope);
      }
    }
    return undefined;
  }

  advance(parent: Job | undefined, scope: SqliteWorkerCallbackScope | undefined): void {
    if (!parent || !scope || parent.callback !== scope || scope.advancing) {
      return;
    }
    if (parent.completed || scope.slot.failed) {
      this.cancelScope(parent, scope);
      return;
    }
    scope.advancing = true;
    try {
      scope.observeReturn?.();
      if (scope.refusal && !scope.pending.size) {
        scope.completion = { accepted: false };
      }
      if (scope.completion?.accepted !== false) {
        for (const slot of new Set(scope.pending.values())) {
          this.owner.dispatch(slot);
        }
      }
      this.complete(parent, scope);
    } catch (error) {
      this.owner.fail(scope.slot, parent, error);
    } finally {
      scope.advancing = undefined;
      if (parent.executionFailure) {
        // Native stop may predate this host producer's last asynchronous notification.
        for (const execution of scope.slot.executions) {
          if (execution.worker === parent.executionWorker) {
            execution.serviceFailureSettlement?.();
            break;
          }
        }
      }
    }
  }
}
