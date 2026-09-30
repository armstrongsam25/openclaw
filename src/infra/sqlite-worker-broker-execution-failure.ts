import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import { createDeferredCore } from "../shared/deferred.js";
import { createSqliteLifecycleAggregateError } from "./sqlite-lifecycle-errors.js";
import {
  settleFailedSqliteWorkerJobs,
  type CompletedSqliteWorkerOutcome,
} from "./sqlite-worker-broker-settlement.js";
import type {
  Job,
  Slot,
  SqliteWorkerExecution,
  SqliteWorkerRetainedOutcome,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";
import { SqliteWorkerError } from "./sqlite-worker-contract.js";

type ExecutionFailureOwner = {
  retireExecutionRetained(
    slot: Slot,
    execution: SqliteWorkerExecution,
  ): SqliteWorkerRetainedResult<void>;
  serviceSlot(slot: Slot): void;
  finish: Parameters<typeof settleFailedSqliteWorkerJobs>[0]["finish"];
  dispatch(slot: Slot): void;
};

/** A physical join cannot release an ancestor's accepted host producers. */
function joinCallbackDrain(
  job: Job,
  native: SqliteWorkerRetainedResult<void>,
): SqliteWorkerRetainedResult<void> {
  const scope = job.callback;
  const completed = createDeferredCore();
  void completed.promise.catch(() => {});
  let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
  const advance = () => {
    if (outcome.status !== "pending") {
      return;
    }
    const stopped = native.read();
    if (
      stopped.status === "pending" ||
      (scope && (scope.accepting || scope.pending.size || scope.observeReturn))
    ) {
      return;
    }
    try {
      scope?.cancel();
      outcome = stopped;
    } catch (cleanupError) {
      outcome = {
        status: "rejected",
        error:
          stopped.status === "rejected"
            ? createSqliteLifecycleAggregateError(
                [stopped.error, cleanupError],
                "SQLite execution and callback cleanup failed",
                stopped.error,
              )
            : cleanupError,
      };
    }
    if (outcome.status === "fulfilled") {
      completed.resolve();
    } else {
      completed.reject(outcome.error);
    }
  };
  void native.result.then(advance, advance);
  advance();
  return {
    result: completed.promise,
    read: () => outcome,
    service() {
      native.service();
      scope?.service();
      advance();
    },
  };
}

/** Borrow accepted Jobs; only their captured physical execution becomes unavailable. */
export function failSqliteWorkerExecution(
  {
    slot,
    execution,
    error: reason,
    currentError,
    openOutcome,
    completed,
  }: {
    slot: Slot;
    execution: SqliteWorkerExecution;
    error: unknown;
    currentError?: Error;
    openOutcome?: "refused-before-agent-open";
    completed?: CompletedSqliteWorkerOutcome;
  },
  owner: ExecutionFailureOwner,
): void {
  if (execution.failed) {
    return;
  }
  const error = toErrorObject(reason, "SQLite worker failed");
  execution.failed = new SqliteWorkerError(error.message, "unavailable");
  const visited = new Set<Job>();
  const selected: Job[] = [];
  const collect = (job: Job) => {
    if (visited.has(job)) {
      return;
    }
    visited.add(job);
    for (const child of job.provisionalChildren?.values() ?? []) {
      collect(child);
    }
    if (!job.completed && job.executionWorker === execution.worker) {
      selected.push(job);
    }
  };
  let current: Job | undefined;
  for (let job = slot.current; job; job = job.parent) {
    if (!current && !job.completed && job.executionWorker === execution.worker) {
      current = job;
    }
    collect(job);
  }
  const queued = slot.queue.filter((job) => job.executionWorker === execution.worker);
  slot.queue = slot.queue.filter((job) => job.executionWorker !== execution.worker);
  for (const job of [...selected, ...queued]) {
    job.executionFailure = execution.failed;
  }

  // All selected settlements borrow this captured stop; other executions keep their custody.
  const native = owner.retireExecutionRetained(slot, execution);
  const observers: Array<() => void> = [];
  let remaining = selected.length + (queued.length ? 1 : 0);
  let initializing = true;
  let servicing = false;
  const finish: ExecutionFailureOwner["finish"] = (job, failure, value, settlement) => {
    if (job.completed) {
      return;
    }
    if (slot.current === job) {
      slot.current = undefined;
    }
    owner.finish(job, failure, value, settlement);
  };
  const onFinished = () => {
    remaining -= 1;
    if (!initializing && remaining === 0) {
      execution.serviceFailureSettlement = undefined;
      owner.dispatch(slot);
    }
  };
  const service = () => {
    if (initializing || servicing) {
      return;
    }
    servicing = true;
    try {
      native.service();
      owner.serviceSlot(slot);
      for (const observe of observers) {
        observe();
      }
    } finally {
      servicing = false;
    }
  };
  execution.serviceFailureSettlement = service;

  if (queued.length) {
    const observer = settleFailedSqliteWorkerJobs({
      queuedError: execution.failed,
      current: undefined,
      queued,
      error,
      retire: () => native,
      finish,
      onFinished,
    });
    if (observer) {
      observers.push(observer);
    }
  }
  // Settle provisional leaves before cancelling their ancestors' callback scopes.
  for (const job of selected) {
    const observer = settleFailedSqliteWorkerJobs({
      queuedError: execution.failed,
      current: job === current ? job : undefined,
      queued: [],
      suspended: job === current ? [] : [job],
      error,
      ...(job === current ? { currentError, openOutcome, completed } : {}),
      retire: () => joinCallbackDrain(job, native),
      finish,
      onFinished,
    });
    if (observer) {
      observers.push(observer);
    }
  }
  initializing = false;
  if (remaining === 0) {
    execution.serviceFailureSettlement = undefined;
    owner.dispatch(slot);
  } else {
    service();
  }
}
