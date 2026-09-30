import { AsyncLocalStorage } from "node:async_hooks";
import type { DatabaseSync } from "node:sqlite";
import { isPromise } from "node:util/types";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { encodeOpenClawStateWorkerError } from "../state/openclaw-state-worker-error.js";
import type { SqliteWorkerReply, SqliteWorkerRequest } from "./sqlite-worker-contract.js";
import { bindSqliteWorkerOperationDatabase } from "./sqlite-worker-operation-admission.js";

type CallbackTransactionOutcome =
  | { committed: true }
  | { committed: false; error: unknown; rollbackId?: number; admissionRefused?: true };
export type SqliteWorkerCallbackFrame = {
  id: number;
  actor: number;
  parent?: SqliteWorkerCallbackFrame;
  active: boolean;
  dispatch(request: SqliteWorkerRequest): void;
  children: Set<SqliteWorkerCallbackFrame>;
  database?: { connection: DatabaseSync; entryScope?: object };
  held?: { parent: SqliteWorkerCallbackFrame; scope: object; outcome?: CallbackTransactionOutcome };
  finalize?: (outcome: CallbackTransactionOutcome) => void;
  rolledBack?: (outcome: Extract<CallbackTransactionOutcome, { committed: false }>) => void;
};
const frames = resolveGlobalSingleton(
  Symbol.for("openclaw.sqliteWorkerCallbackFrame"),
  () => new AsyncLocalStorage<SqliteWorkerCallbackFrame>(),
);

export function createSqliteWorkerCallbackFrame(
  id: number,
  actor: number,
  dispatch: (request: SqliteWorkerRequest) => void,
): SqliteWorkerCallbackFrame {
  return { id, actor, dispatch, parent: frames.getStore(), active: false, children: new Set() };
}

export function withSqliteWorkerCallbackFrame<T>(
  frame: SqliteWorkerCallbackFrame,
  operation: () => T,
): T {
  frame.active = true;
  try {
    if (frame.database) {
      bindSqliteWorkerOperationDatabase(frame.database.connection);
    }
    return frames.run(frame, operation);
  } finally {
    frame.active = false;
  }
}

/** Code preparation cannot yield while any executing ancestor holds a native transaction. */
export function hasSqliteWorkerActiveTransaction(): boolean {
  for (let frame = frames.getStore(); frame; frame = frame.parent) {
    if (frame.active && frame.database?.connection.isTransaction) {
      return true;
    }
  }
  return false;
}

export function settleSqliteWorkerCallbackChildren(frame: SqliteWorkerCallbackFrame): void {
  for (const child of frame.children) {
    const outcome = child.held?.outcome;
    if (!outcome || !child.finalize) {
      throw new Error("Nested SQLite work did not reach native settlement");
    }
    child.finalize(outcome);
    frame.children.delete(child);
  }
}

export function createSqliteWorkerCallbackFailure(
  id: number,
  error: unknown,
  rollbackId?: number,
  phase: "final" | "rollback" = "final",
  admissionRefused = false,
): Extract<SqliteWorkerReply, { ok: false }> {
  const failure = error instanceof Error ? error : new Error(String(error));
  const code = "code" in failure ? failure.code : undefined;
  const sharedState = encodeOpenClawStateWorkerError(failure, { includeOrdinary: true });
  return {
    id,
    ok: false,
    ...(phase === "final"
      ? { provisionalFinal: true as const }
      : { provisionalRollback: true as const }),
    ...(rollbackId !== undefined ? { rollbackId } : {}),
    ...(admissionRefused ? { admissionRefused: true as const } : {}),
    error: {
      name: failure.name,
      message: failure.message,
      ...(typeof code === "string" || typeof code === "number" ? { code } : {}),
      ...(sharedState ? { sharedState } : {}),
    },
  };
}

export type SqliteWorkerKernel<T> = Generator<unknown, T, unknown>;
export async function driveSqliteWorkerKernel<T>(
  kernel: SqliteWorkerKernel<T>,
  enter: <R>(operation: () => R) => R,
): Promise<T> {
  let step = enter(() => kernel.next());
  while (!step.done) {
    let value: unknown;
    try {
      value = await step.value;
    } catch (error) {
      step = enter(() => kernel.throw(error));
      continue;
    }
    step = enter(() => kernel.next(value));
  }
  return step.value;
}
export function driveSqliteWorkerKernelSync<T>(
  kernel: SqliteWorkerKernel<T>,
  enter: <R>(operation: () => R) => R,
): T {
  let step = enter(() => kernel.next());
  while (!step.done) {
    const value = step.value;
    if (isPromise(value) || (isRecord(value) && typeof value.then === "function")) {
      if (isPromise(value)) {
        void value.catch(() => {});
      }
      step = enter(() =>
        kernel.throw(
          new Error("SQLite callback prerequisites must be loaded before its native transaction"),
        ),
      );
    } else {
      step = enter(() => kernel.next(value));
    }
  }
  return step.value;
}
