import { AsyncLocalStorage } from "node:async_hooks";
import type { DatabaseSync } from "node:sqlite";
import { isPromise } from "node:util/types";
import { deserialize } from "node:v8";
import { MessageChannel, receiveMessageOnPort } from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { encodeOpenClawStateWorkerError } from "../state/openclaw-state-worker-error.js";
import { getSqliteTransactionScope, stageSqliteTransactionState } from "./sqlite-post-commit.js";
import {
  SQLITE_WORKER_MAX_MESSAGE_BYTES,
  SqliteWorkerError,
  type SqliteWorkerReply,
  type SqliteWorkerRequest,
} from "./sqlite-worker-contract.js";
import {
  bindSqliteWorkerOperationDatabase,
  isSqliteWorkerOperationRefusal,
  requestSqliteWorkerOperationAdmission,
  type SqliteWorkerAdmissionRequest,
} from "./sqlite-worker-operation-admission.js";

export type CallbackTransactionOutcome =
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

/** Facts from the active accepted frame; request IDs pair lineage and never grant authority. */
export function captureSqliteWorkerCallbackExecution():
  | { requestId: number; actorId: number; database: DatabaseSync }
  | undefined {
  const frame = frames.getStore();
  if (!frame?.active || !frame.database) {
    return undefined;
  }
  return { requestId: frame.id, actorId: frame.actor, database: frame.database.connection };
}

/** Retain native ancestry only; the domain owns its binding and callback interval. */
export function captureSqliteWorkerCallbackFrameAssertion(): {
  requestId: number;
  actorId: number;
  assertCurrent(this: void): void;
} {
  const frame = frames.getStore();
  if (!frame?.active) {
    throw new SqliteWorkerError(
      "SQLite callback continuation requires an active native frame",
      "closed",
    );
  }
  return {
    requestId: frame.id,
    actorId: frame.actor,
    assertCurrent() {
      let current = frames.getStore();
      if (!current?.active || !frame.active) {
        throw new SqliteWorkerError("SQLite callback continuation lost its native frame", "closed");
      }
      for (; current; current = current.parent) {
        if (current === frame) {
          return;
        }
      }
      throw new SqliteWorkerError(
        "SQLite callback continuation lost its native ancestry",
        "closed",
      );
    },
  };
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

function transactionOwner(
  frame: SqliteWorkerCallbackFrame,
  database: DatabaseSync,
  scope: object,
): SqliteWorkerCallbackFrame {
  for (let owner: SqliteWorkerCallbackFrame | undefined = frame; owner; owner = owner.parent) {
    if (owner.database?.connection === database && owner.database.entryScope !== scope) {
      if (!owner.active) {
        break;
      }
      return owner;
    }
  }
  throw new Error("SQLite transaction has no executing connection owner");
}

/** Record connection facts before this command can enter a transaction or savepoint. */
export function bindSqliteWorkerCallbackDatabase(database: DatabaseSync): void {
  const frame = frames.getStore();
  if (!frame?.active) {
    throw new Error("SQLite connection binding requires an executing carrier command");
  }
  bindSqliteWorkerOperationDatabase(database);
  if (frame.database) {
    if (frame.database.connection !== database) {
      throw new Error("SQLite command changed its bound connection");
    }
    return;
  }
  const scope = database.isTransaction ? getSqliteTransactionScope(database) : undefined;
  frame.database = { connection: database, entryScope: scope };
  if (!database.isTransaction) {
    return;
  }
  if (!scope) {
    throw new Error("A nested SQLite result requires a managed transaction owner");
  }
  const parent = transactionOwner(frame, database, scope);
  const held: NonNullable<SqliteWorkerCallbackFrame["held"]> = { parent, scope };
  if (
    !stageSqliteTransactionState(database, {
      stage() {},
      commit() {
        held.outcome = { committed: true };
      },
      rollback(error) {
        const outcome = {
          committed: false as const,
          error,
          rollbackId: frames.getStore()?.id,
          ...(isSqliteWorkerOperationRefusal(error) ? { admissionRefused: true as const } : {}),
        };
        held.outcome = outcome;
        frame.rolledBack?.(outcome);
      },
    })
  ) {
    throw new Error("A nested SQLite result requires a managed transaction owner");
  }
  frame.held = held;
  parent.children.add(frame);
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

/** The grant enters only this typed request loop; callback return obtains fresh commit authority. */
export function requestSqliteWorkerCallback(facts: unknown): unknown {
  const frame = frames.getStore();
  if (!frame?.active) {
    throw new Error("SQLite callbacks require an executing carrier command");
  }
  const database = frame.database?.connection;
  const scope = database && getSqliteTransactionScope(database);
  if (!database?.isTransaction || !scope) {
    throw new Error("SQLite callbacks require their bound managed transaction");
  }
  transactionOwner(frame, database, scope);
  return requestSqliteWorkerCallbackAtFrame(
    frame,
    facts,
    true,
    "commit",
    requestSqliteWorkerOperationAdmission,
  );
}

/** Accepted native preparation can obtain a finite host plan before its connection enters a transaction. */
export function requestSqliteWorkerPreparationCallback(facts: unknown): unknown {
  const frame = frames.getStore();
  if (!frame?.active) {
    throw new Error("SQLite preparation callbacks require an executing carrier command");
  }
  if (frame.database?.connection.isTransaction) {
    throw new Error("SQLite preparation callbacks must precede their native transaction");
  }
  return requestSqliteWorkerCallbackAtFrame(
    frame,
    facts,
    false,
    "prepare",
    requestSqliteWorkerOperationAdmission,
  );
}

/** Complete a restriction callback through its original same-stage admission dispatcher. */
export function requestSqliteWorkerRestrictionCallback(
  facts: unknown,
  returnStage: "transaction" | "commit",
  dispatch: (request: SqliteWorkerAdmissionRequest) => void,
): unknown {
  const frame = frames.getStore();
  if (!frame?.active) {
    throw new Error("SQLite callbacks require an executing carrier command");
  }
  const database = frame.database?.connection;
  const transaction = database?.isTransaction === true;
  const scope = database && getSqliteTransactionScope(database);
  if (!database || !transaction || !scope) {
    throw new Error("SQLite callbacks require their bound managed transaction");
  }
  transactionOwner(frame, database, scope);
  return requestSqliteWorkerCallbackAtFrame(frame, facts, transaction, returnStage, dispatch);
}

function requestSqliteWorkerCallbackAtFrame(
  frame: SqliteWorkerCallbackFrame,
  facts: unknown,
  transaction: boolean,
  returnStage: "prepare" | "transaction" | "commit",
  dispatch: (request: SqliteWorkerAdmissionRequest) => void,
): unknown {
  const { port1, port2 } = new MessageChannel();
  const waiting = new Int32Array(new SharedArrayBuffer(4));
  const acknowledgment = new Int32Array(new SharedArrayBuffer(4));
  try {
    requestSqliteWorkerOperationAdmission(
      {
        stage: "prepare",
        facts: {
          kind: "sqlite-worker-callback",
          transaction,
          facts,
          port: port2,
          parentActor: frame.actor,
          acknowledgment: acknowledgment.buffer,
        },
      },
      [port2],
    );
    if (Atomics.load(acknowledgment, 0) !== 1) {
      throw new SqliteWorkerError(
        "SQLite host did not acknowledge the callback protocol",
        "unavailable",
      );
    }
    while (true) {
      const queued = receiveMessageOnPort(port1);
      if (!queued) {
        Atomics.wait(waiting, 0, 0, 5);
        continue;
      }
      const message: unknown = queued.message;
      if (!isRecord(message)) {
        throw new Error("SQLite callback returned an invalid request");
      }
      if (message.type === "return" && typeof message.accepted === "boolean") {
        let value: unknown;
        if (message.accepted) {
          if (
            !(message.value instanceof Uint8Array) ||
            message.value.byteLength > SQLITE_WORKER_MAX_MESSAGE_BYTES
          ) {
            throw new Error("SQLite callback returned an invalid bounded result");
          }
          value = deserialize(message.value);
        }
        dispatch({
          stage: returnStage,
          facts: {
            kind: "sqlite-worker-callback-return",
            accepted: message.accepted,
          },
        });
        if (!message.accepted) {
          throw new SqliteWorkerError("SQLite callback refused its return", "closed");
        }
        return value;
      }
      if (
        message.type !== "request" ||
        !isRecord(message.request) ||
        typeof message.request.id !== "number" ||
        typeof message.request.actor !== "number" ||
        !["open", "execute", "execute-start", "execute-frame", "result-next", "close"].includes(
          String(message.request.type),
        )
      ) {
        throw new Error("SQLite callback returned an invalid carrier request");
      }
      // SAFETY: The host sends only canonical accepted Job requests over this private rendezvous.
      frame.dispatch(message.request as SqliteWorkerRequest);
    }
  } finally {
    port1.close();
    port2.close();
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
