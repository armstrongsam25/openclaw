import { isNativeError } from "node:util/types";
import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import type { Job, SqliteWorkerRetainedResult } from "./sqlite-worker-broker.types.js";
import {
  hasSqliteWorkerOutcomeUnknown,
  retainSqliteWorkerErrorCode,
  SqliteWorkerError,
} from "./sqlite-worker-contract.js";
import type {
  SqliteWorkerOperationSettlement,
  SqliteWorkerRetainedReceipt,
} from "./sqlite-worker-operation-settlement.js";

export type CompletedSqliteWorkerOutcome = { value: unknown } | { error: unknown };

export function recordSqliteWorkerHostRefusal(job: Job, error: unknown): Error {
  const previous = job.refusal?.pending ?? job.refusal?.admissionFailure;
  const transportError =
    previous && Object.is(previous.original, error)
      ? previous.transportError
      : isNativeError(error)
        ? error
        : new Error("SQLite host callback refused", { cause: error });
  job.refusal = Object.freeze({
    ...job.refusal,
    pending: Object.freeze({ original: error, transportError }),
  });
  return transportError;
}

/** Keep the original failure and outcome classification when retirement also fails. */
export function withSqliteWorkerCleanupFailure<T>(
  failure: T,
  cleanup: { error: unknown } | undefined,
): T | Error {
  if (cleanup === undefined) {
    return failure;
  }
  const combined = new AggregateError(
    [failure, cleanup.error],
    "SQLite worker failure and cleanup failed",
    { cause: failure },
  );
  return retainSqliteWorkerErrorCode(combined, failure);
}

export function settleFailedSqliteWorkerJobs({
  queuedError,
  current,
  queued,
  suspended = [],
  error,
  currentError,
  completed,
  openOutcome,
  retire,
  finish,
  onFinished,
}: {
  queuedError: Error;
  current: Job | undefined;
  queued: Job[];
  suspended?: Job[];
  error: Error;
  currentError?: Error;
  completed?: CompletedSqliteWorkerOutcome;
  openOutcome?: "refused-before-agent-open";
  retire: () => SqliteWorkerRetainedResult<void>;
  finish: typeof settleSqliteWorkerJob;
  onFinished?: () => void;
}): (() => void) | undefined {
  const retirement = retire();
  // Join native exit before releasing any operation that might have touched SQLite.
  const finishFailed = (retired: boolean, cleanup?: { error: unknown }) => {
    if (current && completed) {
      process.emitWarning(
        new Error("SQLite worker operation completed before native cleanup failed", {
          cause: withSqliteWorkerCleanupFailure(error, cleanup),
        }),
      );
      finish(
        current,
        "error" in completed ? completed.error : undefined,
        "value" in completed ? completed.value : undefined,
        retired
          ? { kind: "completed" }
          : { kind: "unknown", error: cleanup ? cleanup.error : error },
      );
    } else if (current) {
      const failure =
        currentError ??
        new SqliteWorkerError(
          `SQLite worker stopped before its result was received: ${error.message}`,
          current.request.type === "execute" && current.nativeDispatched
            ? "outcome-unknown"
            : "unavailable",
        );
      if (!currentError) {
        failure.cause = error;
      }
      finish(
        current,
        withSqliteWorkerCleanupFailure(failure, cleanup),
        undefined,
        current.nativeDispatched
          ? retired && openOutcome === "refused-before-agent-open"
            ? { kind: "completed" }
            : { kind: "unknown", error: currentError ?? error }
          : { kind: "not-entered", error },
      );
    }
    for (const job of suspended) {
      const failure = hasSqliteWorkerOutcomeUnknown(error)
        ? error
        : new SqliteWorkerError(
            `SQLite worker stopped before a suspended operation settled: ${error.message}`,
            "outcome-unknown",
          );
      if (failure !== error) {
        failure.cause = error;
      }
      finish(job, withSqliteWorkerCleanupFailure(failure, cleanup), undefined, {
        kind: "unknown",
        error,
      });
    }
    for (const job of queued) {
      finish(job, withSqliteWorkerCleanupFailure(queuedError, cleanup));
    }
  };
  let finished = false;
  let servicing = false;
  const observe = () => {
    if (finished) {
      return;
    }
    const outcome = retirement.read();
    if (outcome.status === "pending") {
      return;
    }
    finished = true;
    try {
      if (outcome.status === "fulfilled") {
        finishFailed(!current?.operationAdmission?.admission.cleanupFailures.length);
      } else {
        finishFailed(false, { error: outcome.error });
      }
    } finally {
      onFinished?.();
    }
  };
  const service = () => {
    if (finished || servicing) {
      return;
    }
    servicing = true;
    try {
      retirement.service();
      observe();
    } finally {
      servicing = false;
    }
  };
  void retirement.result.then(observe, observe);
  observe();
  return finished ? undefined : service;
}

export function settleSqliteWorkerJob(
  job: Job,
  error?: unknown,
  value?: unknown,
  settlement?: SqliteWorkerOperationSettlement,
  rejected?: NonNullable<Job["completedError"]>,
): void {
  const nativeSettlement: SqliteWorkerOperationSettlement =
    settlement ?? (job.nativeDispatched ? { kind: "completed" } : { kind: "not-entered", error });
  const committed = job.operationAdmission?.admission.committed;
  const refusal = job.refusal;
  const occurrence = refusal?.admissionFailure;
  job.settleNative?.(nativeSettlement);
  job.operationAdmission?.admission.finish();
  job.operationAdmission?.releaseService();
  let failure = rejected ?? (error !== undefined ? { error } : undefined);
  const admissionCleanupFailures = job.operationAdmission?.admission.cleanupFailures ?? [];
  if (admissionCleanupFailures.length > 0) {
    const cleanupError = new AggregateError(
      admissionCleanupFailures,
      "SQLite worker admission cleanup failed",
    );
    if (!failure && job.request.type === "execute") {
      process.emitWarning(cleanupError);
    } else {
      failure = {
        error: failure
          ? withSqliteWorkerCleanupFailure(
              rejected ? failure.error : toErrorObject(failure.error, "SQLite worker failed"),
              { error: cleanupError },
            )
          : cleanupError,
      };
    }
  }
  job.callback?.cancel();
  job.inputTransfer?.producer.cancel();
  job.inputTransfer = undefined;
  job.transfer = undefined;
  job.detach();
  const receipt: SqliteWorkerRetainedReceipt = {
    settlement: nativeSettlement,
    ...(refusal?.nativeConfirmed && occurrence
      ? { refusal: { original: occurrence.original, transportError: occurrence.transportError } }
      : {}),
    ...(committed ? { committed } : {}),
    ...(job.rollbackSource ? { rollbackSource: job.rollbackSource } : {}),
  };
  job.finalReceipt = receipt;
  job.settled?.(
    failure ? { status: "rejected", error: failure.error } : { status: "fulfilled", value },
    receipt,
  );
  if (failure) {
    job.reject(failure.error);
  } else {
    job.resolve(value);
  }
}
