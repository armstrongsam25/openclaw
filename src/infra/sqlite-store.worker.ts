import { isPromise } from "node:util/types";
import { deserialize, serialize } from "node:v8";
import { MessagePort, parentPort, workerData } from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { routeLogsToStderr } from "../logging/console.js";
import { drainProcessOutput } from "../process/output-drain.js";
import {
  encodeOpenClawStateWorkerError,
  type OpenClawStateWorkerErrorPayload,
} from "../state/openclaw-state-worker-error.js";
import { withSqliteReaderOwner } from "./sqlite-reader-lifecycle.js";
import {
  createSqliteWorkerCallbackFrame,
  createSqliteWorkerCallbackFailure,
  withSqliteWorkerCallbackFrame,
  settleSqliteWorkerCallbackChildren,
  driveSqliteWorkerKernel,
  driveSqliteWorkerKernelSync,
  type SqliteWorkerCallbackFrame,
  type SqliteWorkerKernel,
} from "./sqlite-worker-callback.worker.js";
import { encodeSqliteWorkerResult } from "./sqlite-worker-carrier-result.js";
import {
  SQLITE_WORKER_PREPARE_COMMAND,
  SQLITE_WORKER_PREPARE_ADMITTED,
  SQLITE_WORKER_OPERATION_CLEANUP,
  SQLITE_WORKER_CLOSE_RECEIPT,
  type SqliteWorkerCloseReceipt,
  type SqliteWorkerPreparedBackend,
  type SqliteWorkerCommand,
  type SqliteWorkerOperations,
  type SqliteWorkerReply,
  type SqliteWorkerRequest,
} from "./sqlite-worker-contract.js";
import { assertExistingDatabaseIdentity } from "./sqlite-worker-identity.js";
import {
  getLoadedSqliteWorkerModule,
  loadSqliteWorkerModule,
} from "./sqlite-worker-module.worker.js";
import {
  SqliteWorkerOpenRefusedError,
  withSqliteWorkerOperationAdmission,
  withoutSqliteWorkerOperationAdmission,
  requestSqliteWorkerOperationAdmission,
  settleSqliteWorkerOperationContext,
  type SqliteWorkerOperationContext,
} from "./sqlite-worker-operation-admission.js";
import {
  runWithSqliteWorkerStateContext,
  type SqliteWorkerStateContext,
} from "./sqlite-worker-state-context.js";
import {
  createSqliteWorkerTransferOwner,
  createSqliteWorkerTransferReceiver,
  type SqliteWorkerTransferFrame,
} from "./sqlite-worker-transfer.js";
import { cancelWorkerIdleGc, scheduleWorkerIdleGc } from "./worker-idle-gc.js";
import { ownedWorkerBytes } from "./worker-transfer-bytes.js";

if (!parentPort) {
  throw new Error("SQLite store worker requires its host port");
}
const port = parentPort;
const legacyHost = workerData === null || workerData === undefined;
if (!legacyHost && (!isRecord(workerData) || !(workerData.replyPort instanceof MessagePort))) {
  throw new Error("SQLite store worker requires its retained reply port");
}
// Installed updater hosts predate the retained reply channel.
const replyPort = legacyHost ? port : workerData.replyPort;
// Diagnostics must preserve the caller's structured stdout.
routeLogsToStderr();
type Backend = SqliteWorkerPreparedBackend<SqliteWorkerOperations>;

function assertSqliteWorkerBackend(backend: unknown): asserts backend is Backend {
  if (
    !isRecord(backend) ||
    typeof backend.execute !== "function" ||
    typeof backend.close !== "function" ||
    (SQLITE_WORKER_PREPARE_COMMAND in backend &&
      backend[SQLITE_WORKER_PREPARE_COMMAND] !== undefined &&
      typeof backend[SQLITE_WORKER_PREPARE_COMMAND] !== "function") ||
    (SQLITE_WORKER_PREPARE_ADMITTED in backend &&
      backend[SQLITE_WORKER_PREPARE_ADMITTED] !== undefined &&
      (typeof backend[SQLITE_WORKER_PREPARE_ADMITTED] !== "function" ||
        typeof backend.assertSettled !== "function")) ||
    (SQLITE_WORKER_OPERATION_CLEANUP in backend &&
      backend[SQLITE_WORKER_OPERATION_CLEANUP] !== undefined &&
      (typeof backend[SQLITE_WORKER_OPERATION_CLEANUP] !== "function" ||
        typeof backend.assertSettled !== "function")) ||
    (SQLITE_WORKER_CLOSE_RECEIPT in backend &&
      backend[SQLITE_WORKER_CLOSE_RECEIPT] !== undefined &&
      typeof backend[SQLITE_WORKER_CLOSE_RECEIPT] !== "function") ||
    (backend.assertSettled !== undefined && typeof backend.assertSettled !== "function") ||
    (backend.prepare !== undefined && typeof backend.prepare !== "function")
  ) {
    throw new Error("SQLite worker module returned an invalid backend");
  }
}

const actors = new Map<number, SqliteWorkerPreparedBackend<SqliteWorkerOperations>>();
type StagedInput = {
  requestId: number;
  actor: number;
  receiver: ReturnType<typeof createSqliteWorkerTransferReceiver>;
  command: unknown;
};
const stateContexts = new Map<number, SqliteWorkerStateContext>();
type OperationState = {
  actor: number;
  context?: SqliteWorkerStateContext;
  transfers: ReturnType<typeof createSqliteWorkerTransferOwner>;
  pendingResult?: { requestId: number; actor: number; transferId: number };
  pendingInput?: StagedInput;
  nativeCleanupFailure?: OpenClawStateWorkerErrorPayload;
  operationAdmission?: { actor: number; context: SqliteWorkerOperationContext };
  callback?: SqliteWorkerCallbackFrame;
  provisional?: { parentActor: number; parentId: number };
};
function newOperationState(actor: number): OperationState {
  return { actor, context: stateContexts.get(actor), transfers: createSqliteWorkerTransferOwner() };
}
let state = newOperationState(0);
const operations = new Map<number, OperationState>();
let callbackDepth = 0;
function enterOperation<T>(next: OperationState, operation: () => T): T {
  const previous = state;
  const previousContext = stateContexts.get(previous.actor);
  state = next;
  if (next.context) {
    stateContexts.set(next.actor, next.context);
  } else {
    stateContexts.delete(next.actor);
  }
  try {
    return operation();
  } finally {
    next.context = stateContexts.get(next.actor);
    state = previous;
    if (previous.actor) {
      if (previousContext) {
        stateContexts.set(previous.actor, previousContext);
      } else {
        stateContexts.delete(previous.actor);
      }
    }
  }
}
function operationFor(request: SqliteWorkerRequest): OperationState {
  let current = operations.get(request.id);
  if (!current) {
    current = newOperationState(request.actor);
    operations.set(request.id, current);
  } else if (current.actor !== request.actor) {
    throw new Error("SQLite continuation changed its actor");
  }
  return current;
}

function runWithActorFacts<T>(actor: number, operation: () => T): T {
  const context = stateContexts.get(actor);
  return context ? runWithSqliteWorkerStateContext(context, operation) : operation();
}

function runInActorContext<T>(actor: number, operation: () => T): T {
  return runWithActorFacts(actor, () =>
    state.operationAdmission?.actor === actor
      ? withSqliteWorkerOperationAdmission(state.operationAdmission.context, operation)
      : operation(),
  );
}

function receive(request: SqliteWorkerRequest): Promise<SqliteWorkerReply> {
  const current = operationFor(request);
  return driveSqliteWorkerKernel(receiveOperation(request), (operation) =>
    enterOperation(current, operation),
  );
}

function* receiveOperation(request: SqliteWorkerRequest): SqliteWorkerKernel<SqliteWorkerReply> {
  let reply: SqliteWorkerReply;
  let executed = state.pendingResult !== undefined;
  let retire = false;
  let completeResult = false;
  let inputNext = false;
  let openNotEntered = false;
  let commandAdmissionRefused = false;
  let settleCurrentCommand: ((failure?: { error: unknown }) => void) | undefined;
  let commandFailure: { error: unknown } | undefined;
  try {
    let value: unknown;
    let closeReceipt: SqliteWorkerCloseReceipt | undefined;
    if (request.type !== "result-next" && request.type !== "execute-frame") {
      if (request.operationAdmission) {
        if (state.operationAdmission) {
          throw new Error("SQLite operation admission still belongs to the preceding operation");
        }
        state.operationAdmission = {
          actor: request.actor,
          context: {
            port: request.operationAdmission,
            address: { requestId: request.id, actorId: request.actor },
          },
        };
      }
      if (request.stateContext) {
        stateContexts.set(request.actor, request.stateContext);
      }
    }
    const executeCommand = function* (command: unknown): SqliteWorkerKernel<void> {
      const backend = actors.get(request.actor);
      if (!backend) {
        throw new Error("SQLite worker actor is closed");
      }
      // SAFETY: The broker serialized a command from this actor's typed store contract.
      const typedCommand = command as SqliteWorkerCommand<SqliteWorkerOperations>;
      const assertSettled = () => {
        const settlement: unknown = runInActorContext(request.actor, () => ({
          settlement: backend.assertSettled?.(),
        })).settlement;
        if (
          isPromise(settlement) ||
          (isRecord(settlement) && typeof settlement.then === "function")
        ) {
          if (isPromise(settlement)) {
            void settlement.catch(() => {});
          }
          throw new Error("SQLite worker settlement checks must remain synchronous");
        }
        return backend.assertSettled !== undefined;
      };
      const settleCommand = (failure?: { error: unknown }) => {
        let verified: boolean;
        try {
          if (state.callback) {
            settleSqliteWorkerCallbackChildren(state.callback);
          }
          verified = assertSettled();
        } catch (error) {
          if (state.operationAdmission) {
            settleSqliteWorkerOperationContext(state.operationAdmission.context, "unknown");
          }
          // The broker joins native exit before settling this operation's admission.
          retire = true;
          if (failure && failure.error !== error) {
            throw new AggregateError(
              [failure.error, error],
              `${String(failure.error)}; SQLite worker settlement failed: ${String(error)}`,
              { cause: error },
            );
          }
          throw error;
        }
        try {
          if (verified && backend[SQLITE_WORKER_OPERATION_CLEANUP]) {
            const cleanup: unknown = runInActorContext(request.actor, () => ({
              cleanup: backend[SQLITE_WORKER_OPERATION_CLEANUP]?.(typedCommand),
            })).cleanup;
            if (isPromise(cleanup) || (isRecord(cleanup) && typeof cleanup.then === "function")) {
              if (isPromise(cleanup)) {
                void cleanup.catch(() => {});
              }
              throw new Error("SQLite worker operation cleanup must remain synchronous");
            }
            assertSettled();
          }
        } catch (error) {
          const cleanupError = error instanceof Error ? error : new Error(String(error));
          state.nativeCleanupFailure =
            encodeOpenClawStateWorkerError(cleanupError, { includeOrdinary: true }) ??
            encodeOpenClawStateWorkerError(new Error("SQLite worker operation cleanup failed"), {
              includeOrdinary: true,
            });
        } finally {
          // Cleanup can still request live source authority; preserve the prior native outcome.
          if (state.operationAdmission) {
            settleSqliteWorkerOperationContext(
              state.operationAdmission.context,
              verified ? "completed" : "unknown",
            );
          }
        }
      };
      settleCurrentCommand = settleCommand;
      const callback = createSqliteWorkerCallbackFrame(
        request.id,
        request.actor,
        dispatchCallbackRequest,
      );
      state.callback = callback;
      try {
        const loading = withoutSqliteWorkerOperationAdmission(() =>
          withSqliteWorkerCallbackFrame(callback, () =>
            backend[SQLITE_WORKER_PREPARE_COMMAND]?.(typedCommand.type),
          ),
        );
        if (loading) {
          yield loading;
        }
        // Preparation carries captured facts without retaining synchronous admission authority.
        const preparation = withoutSqliteWorkerOperationAdmission(() =>
          withSqliteWorkerCallbackFrame(callback, () =>
            runWithActorFacts(request.actor, () => backend.prepare?.(typedCommand)),
          ),
        );
        if (preparation !== undefined) {
          yield preparation;
        }
        if (backend[SQLITE_WORKER_PREPARE_ADMITTED]) {
          // Only the synchronous prefix inherits authority; deferred preparation does not.
          const admitted = runInActorContext(request.actor, () => ({
            preparation: withSqliteWorkerCallbackFrame(callback, () =>
              backend[SQLITE_WORKER_PREPARE_ADMITTED]?.(typedCommand),
            ),
          })).preparation;
          if (admitted !== undefined) {
            yield admitted;
          }
        }
        value = runInActorContext(request.actor, () =>
          withSqliteReaderOwner(
            {
              operation: typedCommand.type,
              ownerKind: "worker",
              actorId: request.actor,
            },
            () => ({
              // SAFETY: The typed host command is serialized once; framing validates complete reconstruction.
              result: withSqliteWorkerCallbackFrame(callback, () => backend.execute(typedCommand)),
            }),
          ),
        ).result;
      } catch (error) {
        // Cleanup can replace the refusal; only settled command failures retain its provenance.
        const admissionRefused =
          state.operationAdmission?.actor === request.actor &&
          state.operationAdmission.context.refusal !== undefined &&
          state.operationAdmission.context.refusal === error;
        commandFailure = { error };
        if (!state.callback?.held) {
          settleCommand(commandFailure);
        }
        commandAdmissionRefused = admissionRefused;
        throw error;
      }
      executed = true;
      completeResult = true;
      if (isPromise(value) || (isRecord(value) && typeof value.then === "function")) {
        retire = true;
        if (state.operationAdmission) {
          settleSqliteWorkerOperationContext(state.operationAdmission.context, "unknown");
        }
        if (isPromise(value)) {
          // Retirement owns the failure; consume rejection while native exit is joined.
          void value.catch(() => {});
        }
        throw new Error("SQLite worker operations must remain synchronous");
      }
      if (!state.callback?.held) {
        settleCommand();
      }
    };
    if (request.type === "result-next") {
      if (
        state.pendingResult?.requestId !== request.id ||
        state.pendingResult.actor !== request.actor ||
        state.pendingResult.transferId !== request.transferId
      ) {
        throw new Error("SQLite worker result transfer is no longer current");
      }
      executed = true;
      const frame = state.transfers.next(request.transferId);
      if (frame.done) {
        state.transfers.end(request.transferId);
        state.pendingResult = undefined;
      }
      value = frame;
    } else if (state.pendingResult) {
      throw new Error("SQLite worker result transfer has not finished");
    } else if (request.type === "execute-start") {
      retire = true;
      if (
        state.pendingInput ||
        !actors.has(request.actor) ||
        request.transfer.kinds.length !== 1 ||
        request.transfer.kinds[0] !== "command"
      ) {
        throw new Error("SQLite worker received unexpected command staging");
      }
      const input: StagedInput = {
        requestId: request.id,
        actor: request.actor,
        command: undefined,
        receiver: createSqliteWorkerTransferReceiver(request.transfer, (record) => {
          input.command = record.value;
        }),
      };
      state.pendingInput = input;
      inputNext = true;
      retire = false;
    } else if (request.type === "execute-frame") {
      retire = true;
      const input = state.pendingInput;
      if (!input || input.requestId !== request.id || input.actor !== request.actor) {
        throw new Error("SQLite worker command staging is no longer current");
      }
      // SAFETY: The matching host emits frames; the shared receiver validates sequence and bounds.
      const frame = deserialize(request.input) as SqliteWorkerTransferFrame;
      const counts = input.receiver.accept(frame);
      if (counts) {
        if (counts.length !== 1 || counts[0]?.[1] !== 1) {
          throw new Error("SQLite worker received an incomplete command");
        }
        state.pendingInput = undefined;
        retire = false;
        yield* executeCommand(input.command);
      } else {
        inputNext = true;
        retire = false;
      }
    } else if (state.pendingInput) {
      retire = true;
      throw new Error("SQLite worker command staging has not finished");
    } else if (request.type === "open") {
      if (actors.has(request.actor)) {
        throw new Error("SQLite worker actor is already open");
      }
      if (request.existingIdentity) {
        assertExistingDatabaseIdentity(request.databasePath, request.existingIdentity);
      }
      if (callbackDepth && !getLoadedSqliteWorkerModule(request.moduleUrl)) {
        openNotEntered = true;
        throw new SqliteWorkerOpenRefusedError(
          new Error("SQLite callback backend module was not preloaded"),
        );
      }
      const module = yield loadSqliteWorkerModule(request.moduleUrl, request.sourceLoaderUrl);
      const factoryName = request.existingIdentity
        ? "openExistingSqliteWorkerBackend"
        : "createSqliteWorkerBackend";
      if (!isRecord(module) || typeof module[factoryName] !== "function") {
        throw new Error(`SQLite worker module must export ${factoryName}`);
      }
      const factory = module[factoryName];
      // Module loading can yield before the factory opens native state.
      if (request.existingIdentity) {
        assertExistingDatabaseIdentity(request.databasePath, request.existingIdentity);
      }
      const backend: unknown = yield runInActorContext(request.actor, () => {
        const input = deserialize(request.input);
        if (request.openAdmission) {
          try {
            requestSqliteWorkerOperationAdmission({
              stage: "open",
              facts: request.openAdmission === "input" ? input : undefined,
            });
          } catch (error) {
            openNotEntered = true;
            throw error;
          }
        }
        return factory(input, {
          databasePath: request.databasePath,
          ...(request.preparation ? { preparation: deserialize(request.preparation) } : {}),
          ...(request.existingIdentity ? { existingIdentity: request.existingIdentity } : {}),
        });
      });
      assertSqliteWorkerBackend(backend);
      actors.set(request.actor, backend);
    } else if (request.type === "close") {
      const backend = actors.get(request.actor);
      if (!backend) {
        throw new Error("SQLite worker actor is closed");
      }
      try {
        yield runInActorContext(request.actor, () => backend.close());
        closeReceipt = runInActorContext(request.actor, () =>
          backend[SQLITE_WORKER_CLOSE_RECEIPT]?.(),
        );
      } catch (error) {
        retire = true;
        throw error;
      }
      actors.delete(request.actor);
      stateContexts.delete(request.actor);
    } else {
      yield* executeCommand(deserialize(request.input));
    }
    let serialized: Uint8Array | undefined;
    try {
      serialized = serialize(value);
      const readFacts = state.operationAdmission?.context.readFacts?.result;
      const reservedReplyBytes =
        !state.callback?.held && !state.pendingInput && !state.pendingResult && readFacts
          ? serialize(readFacts).byteLength
          : 0;
      reply = encodeSqliteWorkerResult(
        state,
        request,
        serialized,
        completeResult,
        closeReceipt,
        inputNext,
        reservedReplyBytes,
      );
    } finally {
      // Deferred settlement retains command custody, never the returned payload.
      value = undefined;
      serialized = undefined;
    }
  } catch (error) {
    const opening = request.type === "open";
    if (openNotEntered && opening) {
      stateContexts.delete(request.actor);
    }
    state.transfers.cancel();
    state.pendingResult = undefined;
    state.pendingInput = undefined;
    const refusedOpen = opening && error instanceof SqliteWorkerOpenRefusedError;
    const originalError = refusedOpen ? error.originalError : error;
    commandFailure ??= { error: originalError };
    const admissionRefused =
      commandAdmissionRefused ||
      (opening &&
        state.operationAdmission?.actor === request.actor &&
        state.operationAdmission.context.refusal !== undefined &&
        state.operationAdmission.context.refusal === originalError);
    const failure =
      originalError instanceof Error ? originalError : new Error(String(originalError));
    const code = executed ? "outcome-unknown" : "code" in failure ? failure.code : undefined;
    const errorContext =
      request.stateContext ??
      (request.type === "execute-frame" ? stateContexts.get(request.actor) : undefined);
    const sharedState =
      errorContext && !executed ? encodeOpenClawStateWorkerError(failure) : undefined;
    reply = {
      id: request.id,
      ok: false,
      ...(retire || (state.nativeCleanupFailure && executed) ? { retire: true } : {}),
      ...(refusedOpen ? { openOutcome: "refused-before-agent-open" } : {}),
      ...(openNotEntered ? { openNotEntered: true } : {}),
      ...(admissionRefused ? { admissionRefused: true } : {}),
      error: {
        name: executed ? "SqliteWorkerError" : failure.name,
        message: failure.message,
        ...(typeof code === "string" || typeof code === "number" ? { code } : {}),
        ...(sharedState ? { sharedState } : {}),
      },
    };
  }
  if (state.callback?.held && settleCurrentCommand && !retire) {
    retainProvisionalSettlement(
      request.id,
      state,
      state.callback,
      settleCurrentCommand,
      commandFailure,
    );
  }
  if (state.provisional) {
    reply.provisional = state.provisional;
  }
  finishReplyTransport(request.id, reply);
  if (request.type === "close" && reply.ok && actors.size === 0) {
    yield new Promise<void>((resolve) => {
      drainProcessOutput(resolve);
    });
  }
  return reply;
}

function retainProvisionalSettlement(
  id: number,
  current: OperationState,
  callback: SqliteWorkerCallbackFrame,
  settle: (failure?: { error: unknown }) => void,
  failure?: { error: unknown },
): void {
  const held = callback.held;
  if (!held) {
    throw new Error("Nested SQLite result has no transaction owner");
  }
  current.provisional = { parentActor: held.parent.actor, parentId: held.parent.id };
  callback.rolledBack = (outcome) => {
    // The active frame may be an ancestor; these facts belong to this retained operation.
    const reply = createSqliteWorkerCallbackFailure(
      id,
      outcome.error,
      outcome.rollbackId,
      "rollback",
      outcome.admissionRefused,
    );
    const receipt = current.operationAdmission?.context.rollbackCheckpoint?.receipt;
    if (receipt) {
      reply.rollbackSource = receipt;
    }
    postReply(reply);
  };
  callback.finalize = (outcome) =>
    enterOperation(current, () => {
      state.provisional = undefined;
      let finalReply: SqliteWorkerReply;
      try {
        settle(failure ?? (outcome.committed ? undefined : { error: outcome.error }));
        finalReply = outcome.committed
          ? { id, ok: true, value: serialize(undefined), provisionalFinal: true }
          : createSqliteWorkerCallbackFailure(
              id,
              outcome.error,
              outcome.rollbackId,
              "final",
              outcome.admissionRefused,
            );
      } catch (error) {
        finalReply = { ...createSqliteWorkerCallbackFailure(id, error), retire: true };
      }
      finishReplyTransport(id, finalReply);
      postReply(finalReply);
    });
}

function finishReplyTransport(id: number, reply: SqliteWorkerReply): void {
  const receipt = state.operationAdmission?.context.rollbackCheckpoint?.receipt;
  if (receipt) {
    reply.rollbackSource = receipt;
  }
  const complete = !reply.ok || (!state.pendingInput && !state.pendingResult);
  if (complete && !state.provisional) {
    const readFacts = state.operationAdmission?.context.readFacts?.result;
    if (readFacts) {
      reply.readFacts = readFacts;
    }
    state.operationAdmission?.context.port.close();
    state.operationAdmission = undefined;
    operations.delete(id);
  }
  if (complete && state.nativeCleanupFailure) {
    reply.cleanupFailure = state.nativeCleanupFailure;
    state.nativeCleanupFailure = undefined;
  }
  if (complete) {
    scheduleWorkerIdleGc();
  }
}
function postReply(reply: SqliteWorkerReply): void {
  if (reply.ok) {
    const bytes = ownedWorkerBytes(reply.value);
    replyPort.postMessage({ ...reply, value: bytes }, [bytes.buffer]);
  } else {
    replyPort.postMessage(reply, []);
  }
}
function dispatchCallbackRequest(request: SqliteWorkerRequest): void {
  const current = operationFor(request);
  callbackDepth += 1;
  try {
    const reply = driveSqliteWorkerKernelSync(receiveOperation(request), (operation) =>
      enterOperation(current, operation),
    );
    postReply(reply);
  } finally {
    callbackDepth -= 1;
  }
}

async function dispatch(request: SqliteWorkerRequest): Promise<void> {
  cancelWorkerIdleGc();
  postReply(await receive(request));
}

// The broker sends one request at a time, including module initialization.
port.on("message", (request: SqliteWorkerRequest) => {
  void dispatch(request);
});
