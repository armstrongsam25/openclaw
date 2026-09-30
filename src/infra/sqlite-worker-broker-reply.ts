import { deserialize, serialize } from "node:v8";
import { toErrorObject } from "@openclaw/normalization-core/error-coercion";
import {
  retainOpenClawStateWorkerErrorPayload,
  hydrateOpenClawStateWorkerError,
  type OpenClawStateWorkerErrorPayload,
} from "../state/openclaw-state-worker-error.js";
import {
  prepareSqliteWorkerActorContext,
  prepareSqliteWorkerOperationAdmission,
} from "./sqlite-worker-broker-admission.js";
import type { CompletedSqliteWorkerOutcome } from "./sqlite-worker-broker-settlement.js";
import type { Job, Slot, SqliteWorkerProvisionalReceipt } from "./sqlite-worker-broker.types.js";
import {
  SQLITE_WORKER_MAX_MESSAGE_BYTES,
  SqliteWorkerError,
  type SqliteWorkerReply,
  type SqliteWorkerCloseReceipt,
  type SqliteWorkerRequest,
} from "./sqlite-worker-contract.js";
import {
  parseSqliteWorkerSourceReceipt,
  type SqliteWorkerOperationSettlement,
} from "./sqlite-worker-operation-settlement.js";
import {
  createSqliteWorkerTransferOwner,
  createSqliteWorkerTransferReceiver,
  type SqliteWorkerTransferFrame,
  type SqliteWorkerTransferHandle,
} from "./sqlite-worker-transfer.js";
import type { RetainedNativeWorker } from "./worker-native-lifecycle.types.js";

function postJobRequest(
  job: Job,
  request: SqliteWorkerRequest,
  transfer: Parameters<RetainedNativeWorker["postMessage"]>[1],
): void {
  const callback = job.nativeParent?.callback;
  if (
    job.nativeParent &&
    (!callback ||
      job.nativeParent.completed ||
      job.nativeParent.executionWorker !== job.executionWorker)
  ) {
    throw new SqliteWorkerError("SQLite callback lost its actual native execution owner", "closed");
  }
  if (callback) {
    callback.port.postMessage({ type: "request", request }, [...(transfer ?? [])]);
  } else {
    job.executionWorker.postMessage(request, transfer ?? []);
  }
}

export function dispatchSqliteWorkerJob(
  slot: Slot,
  job: Job,
  onRejected: (error: unknown, retire: boolean) => void,
): void {
  const actor = [...slot.actors].find((candidate) => candidate.id === job.request.actor);
  const assertCurrentJob = () => {
    let ancestor = slot.current;
    while (ancestor && ancestor !== job.provisionalOwner) {
      ancestor = ancestor.parent;
    }
    const retainedChild =
      ancestor !== undefined && ancestor.provisionalChildren?.get(job.request.id) === job;
    if (slot.failed || job.executionFailure || (slot.current !== job && !retainedChild)) {
      throw (
        slot.failed ??
        job.executionFailure ??
        new SqliteWorkerError("SQLite worker job is no longer current", "closed")
      );
    }
  };
  const assertDispatchable = () => {
    if (!job.nativeDispatched) {
      job.signal?.throwIfAborted();
    }
    job.assertCurrent?.();
    assertCurrentJob();
  };
  try {
    assertDispatchable();
    prepareSqliteWorkerActorContext(actor, job);
    job.request.operationAdmission = prepareSqliteWorkerOperationAdmission(
      job,
      actor,
      assertDispatchable,
      assertCurrentJob,
    );
    const request = prepareSqliteWorkerRequest(job);
    assertDispatchable();
    job.nativeDispatched = true;
    job.detach();
    if (job.dispatchState) {
      job.dispatchState.dispatched = true;
    }
    job.requestPosted = true;
    postJobRequest(job, request, request.operationAdmission ? [request.operationAdmission] : []);
  } catch (error) {
    onRejected(error, job.requestPosted === true);
  }
}

function prepareSqliteWorkerRequest(job: Job): SqliteWorkerRequest {
  if (
    job.request.type !== "execute" ||
    job.request.input.byteLength <= SQLITE_WORKER_MAX_MESSAGE_BYTES
  ) {
    return job.request;
  }
  const { input, ...request } = job.request;
  const producer = createSqliteWorkerTransferOwner();
  const transfer = producer.start([{ kind: "command", serialized: input }].values(), {
    kinds: ["command"],
  });
  job.inputTransfer = { id: transfer.id, producer };
  job.request.input = new Uint8Array();
  return { ...request, type: "execute-start", transfer };
}

function decodeSqliteWorkerReplyValue(
  job: Job,
  reply: Extract<SqliteWorkerReply, { ok: true }>,
):
  | { type: "complete"; value: unknown }
  | {
      type: "continue";
      request: Extract<SqliteWorkerRequest, { type: "result-next" | "execute-frame" }>;
    } {
  if (reply.input === "next") {
    const transfer = job.inputTransfer;
    if (!transfer || reply.transfer) {
      throw new Error("SQLite worker requested unexpected command input");
    }
    const frame = transfer.producer.next(transfer.id);
    const input = serialize(frame);
    if (input.byteLength > SQLITE_WORKER_MAX_MESSAGE_BYTES) {
      throw new Error("SQLite worker input frame exceeds the transport byte limit");
    }
    if (frame.done) {
      transfer.producer.end(transfer.id);
      job.inputTransfer = undefined;
    }
    return {
      type: "continue",
      request: { type: "execute-frame", id: job.request.id, actor: job.request.actor, input },
    };
  }
  if (job.inputTransfer) {
    throw new Error("SQLite worker completed before receiving its command input");
  }
  let value: unknown;
  if (reply.transfer === "start") {
    // SAFETY: The matching worker emits this private handle; framing validates its records.
    const handle = deserialize(reply.value) as SqliteWorkerTransferHandle;
    if (
      job.request.type !== "execute" ||
      job.transfer ||
      handle.kinds.length !== 1 ||
      handle.kinds[0] !== "result"
    ) {
      throw new Error("SQLite worker returned an unexpected result transfer");
    }
    const transfer: NonNullable<Job["transfer"]> = {
      id: handle.id,
      value: undefined,
      receiver: createSqliteWorkerTransferReceiver(handle, (record) => {
        transfer.value = record.value;
      }),
    };
    job.transfer = transfer;
  } else if (reply.transfer === "frame") {
    const transfer = job.transfer;
    if (!transfer) {
      throw new Error("SQLite worker returned an unexpected result frame");
    }
    // SAFETY: The matching worker emits frames; the shared receiver validates their sequence and bounds.
    const frame = deserialize(reply.value) as SqliteWorkerTransferFrame;
    const counts = transfer.receiver.accept(frame);
    if (counts) {
      if (counts.length !== 1 || counts[0]?.[1] !== 1) {
        throw new Error("SQLite worker returned an incomplete result transfer");
      }
      value = transfer.value;
      job.transfer = undefined;
    }
  } else {
    if (job.transfer) {
      throw new Error("SQLite worker ended its result transfer without completion");
    }
    value = deserialize(reply.value);
  }
  return job.transfer
    ? {
        type: "continue",
        request: {
          type: "result-next",
          id: job.request.id,
          actor: job.request.actor,
          transferId: job.transfer.id,
        },
      }
    : { type: "complete", value };
}

export function decodeSqliteWorkerReplyError(
  job: Job,
  error: Extract<SqliteWorkerReply, { ok: false }>["error"],
): Error {
  const failure = Object.assign(new Error(error.message), {
    name: error.name,
    ...(error.code === undefined ? {} : { code: error.code }),
  });
  if (job.request.stateContext && error.code !== "outcome-unknown" && error.sharedState) {
    retainOpenClawStateWorkerErrorPayload(failure, error.sharedState);
  }
  return failure;
}

export function decodeSqliteWorkerCleanupError(payload: OpenClawStateWorkerErrorPayload): Error {
  const failure = new Error("SQLite worker native cleanup failed");
  retainOpenClawStateWorkerErrorPayload(failure, payload);
  return hydrateOpenClawStateWorkerError(failure, { includeOrdinary: true });
}

function confirmSqliteWorkerRefusal(
  job: Job,
  reply: Extract<SqliteWorkerReply, { ok: false }>,
  original: Job = job,
): void {
  const occurrence = original.operationAdmission?.admission.failure;
  if (reply.admissionRefused && occurrence && original.refusal?.admissionFailure === occurrence) {
    job.refusal = Object.freeze({ admissionFailure: occurrence, nativeConfirmed: true });
  }
}

function provisionalSqliteWorkerReceipt(
  job: Job,
  kind: "returned" | "rolled-back",
): SqliteWorkerProvisionalReceipt {
  const refusal = job.refusal;
  const occurrence = refusal?.admissionFailure;
  return {
    kind,
    ...(job.rollbackSource ? { rollbackSource: job.rollbackSource } : {}),
    ...(refusal?.nativeConfirmed && occurrence
      ? { refusal: { original: occurrence.original, transportError: occurrence.transportError } }
      : {}),
  };
}

function resolveSqliteWorkerCommandFailure(
  job: Job,
  reply: Extract<SqliteWorkerReply, { ok: false }>,
): unknown {
  confirmSqliteWorkerRefusal(job, reply);
  const admission = job.operationAdmission?.admission;
  const failure =
    !reply.admissionRefused &&
    (admission?.failure?.source === "domain" ||
      (job.refusal !== undefined &&
        Object.is(
          admission?.failure?.transportError,
          job.refusal.admissionFailure?.transportError,
        )))
      ? undefined
      : admission?.failure?.transportError;
  return failure ?? decodeSqliteWorkerReplyError(job, reply.error);
}

export type SqliteWorkerReplyOwner = {
  resumeReply(reply: SqliteWorkerReply, executionWorker: RetainedNativeWorker): void;
  fail(
    reason: unknown,
    currentError?: Error,
    openOutcome?: "refused-before-agent-open",
    completed?: CompletedSqliteWorkerOutcome,
  ): void;
  finish(
    job: Job,
    error?: unknown,
    value?: unknown,
    settlement?: SqliteWorkerOperationSettlement,
    closeReceipt?: SqliteWorkerCloseReceipt,
  ): void;
  dispatch(): void;
  returnProvisional(
    job: Job,
    outcome: NonNullable<Job["provisionalOutcome"]>,
    receipt: SqliteWorkerProvisionalReceipt,
  ): void;
};

export function receiveSqliteWorkerReply(
  slot: Pick<Slot, "current" | "failed">,
  reply: SqliteWorkerReply,
  owner: SqliteWorkerReplyOwner,
  executionWorker: RetainedNativeWorker,
): void {
  const held = slot.current;
  const find = (job: Job): Job | undefined => {
    if (job.request.id === reply.id) {
      return job;
    }
    for (const child of job.provisionalChildren?.values() ?? []) {
      const found = find(child);
      if (found) {
        return found;
      }
    }
    return undefined;
  };
  let selected = held ? find(held) : undefined;
  for (let parent = held?.parent; !selected && parent; parent = parent.parent) {
    selected = find(parent);
  }
  if (!selected) {
    owner.fail(new Error("SQLite callback reply lost its accepted operation"));
    return;
  }
  if (selected.executionWorker !== executionWorker) {
    owner.fail(new Error("SQLite reply came from another native execution owner"));
    return;
  }
  if (selected.executionFailure) {
    return;
  }
  if (selected.provisionalChildren?.size && !reply.provisional) {
    selected.deferredReply = { reply, executionWorker };
    return;
  }
  slot.current = selected;
  try {
    receiveCurrentSqliteWorkerReply(slot, reply, owner);
  } finally {
    if (held !== selected && !slot.failed) {
      slot.current = held;
    }
  }
  if (held?.deferredReply && !held.provisionalChildren?.size && !slot.failed) {
    const deferred = held.deferredReply;
    held.deferredReply = undefined;
    owner.resumeReply(deferred.reply, deferred.executionWorker);
  }
}

function returnProvisionalSqliteWorkerReply(
  slot: Pick<Slot, "current">,
  job: Job,
  descriptor: NonNullable<SqliteWorkerReply["provisional"]>,
  outcome: NonNullable<Job["provisionalOutcome"]>,
  owner: SqliteWorkerReplyOwner,
): void {
  let parent = job.nativeParent;
  while (parent && parent.request.id !== descriptor.parentId) {
    parent = parent.nativeParent;
  }
  if (!parent || parent.request.actor !== descriptor.parentActor || job.provisionalOutcome) {
    owner.fail(new Error("SQLite provisional return lost its actual transaction owner"));
    return;
  }
  parent.provisionalChildren ??= new Map();
  parent.provisionalChildren.set(job.request.id, job);
  job.provisionalOwner = parent;
  job.provisionalOutcome = outcome;
  slot.current = job.parent;
  owner.returnProvisional(job, outcome, provisionalSqliteWorkerReceipt(job, "returned"));
  owner.dispatch();
}

function callbackRollbackError(
  job: Job,
  reply: Extract<SqliteWorkerReply, { ok: false }>,
): unknown {
  if (reply.rollbackId !== undefined) {
    for (let ancestor: Job | undefined = job; ancestor; ancestor = ancestor.nativeParent) {
      if (ancestor.request.id !== reply.rollbackId) {
        continue;
      }
      confirmSqliteWorkerRefusal(job, reply, ancestor);
      return (
        ancestor.completedError?.error ??
        (ancestor.provisionalOutcome?.status === "rejected"
          ? ancestor.provisionalOutcome.error
          : undefined) ??
        resolveSqliteWorkerCommandFailure(ancestor, reply)
      );
    }
    return decodeSqliteWorkerReplyError(job, reply.error);
  }
  return resolveSqliteWorkerCommandFailure(job, reply);
}

function receiveCurrentSqliteWorkerReply(
  slot: Pick<Slot, "current" | "failed">,
  reply: SqliteWorkerReply,
  owner: SqliteWorkerReplyOwner,
): void {
  const job = slot.current;
  if (!job || reply.id !== job.request.id) {
    owner.fail(new Error("SQLite worker returned an unexpected response"));
    return;
  }
  if (reply.rollbackSource !== undefined) {
    const receipt = parseSqliteWorkerSourceReceipt(reply.rollbackSource);
    if (!receipt) {
      owner.fail(
        new SqliteWorkerError("SQLite rollback source receipt is invalid", "outcome-unknown"),
      );
      return;
    }
    job.rollbackSource ??= receipt;
  }
  if (reply.readFacts !== undefined) {
    const admission = job.operationAdmission?.admission;
    if (!admission) {
      owner.fail(
        new SqliteWorkerError("SQLite read facts lost their operation owner", "outcome-unknown"),
      );
      return;
    }
    admission.deliverReadFacts(reply.readFacts);
  }
  if (reply.provisionalRollback) {
    if (
      reply.ok ||
      !job.provisionalOutcome ||
      job.provisionalOwner?.provisionalChildren?.get(job.request.id) !== job
    ) {
      owner.fail(new Error("SQLite rollback lost its returned native operation"));
      return;
    }
    const outcome = { status: "rejected" as const, error: callbackRollbackError(job, reply) };
    job.provisionalOutcome = outcome;
    owner.returnProvisional(job, outcome, provisionalSqliteWorkerReceipt(job, "rolled-back"));
    return;
  }
  if (reply.provisionalFinal) {
    const initial = job.provisionalOutcome;
    if (!initial || job.provisionalOwner?.provisionalChildren?.get(job.request.id) !== job) {
      owner.fail(new Error("SQLite final callback receipt has no returned native operation"));
      return;
    }
    const outcome =
      initial.status === "rejected" || reply.ok
        ? initial
        : { status: "rejected" as const, error: callbackRollbackError(job, reply) };
    const cleanup = reply.cleanupFailure
      ? { error: decodeSqliteWorkerCleanupError(reply.cleanupFailure) }
      : !reply.ok && reply.retire
        ? { error: callbackRollbackError(job, reply) }
        : undefined;
    if (cleanup) {
      owner.fail(
        cleanup.error,
        undefined,
        undefined,
        outcome.status === "fulfilled" ? { value: outcome.value } : { error: outcome.error },
      );
      return;
    }
    slot.current = undefined;
    const admission = job.operationAdmission?.admission;
    const failure =
      admission?.failure?.source === "domain" ? undefined : admission?.failure?.transportError;
    owner.finish(
      job,
      outcome.status === "rejected" ? outcome.error : failure,
      outcome.status === "fulfilled" ? outcome.value : undefined,
      { kind: "completed" },
    );
    owner.dispatch();
    return;
  }
  if (!reply.ok && reply.provisional) {
    if (reply.retire || reply.cleanupFailure) {
      owner.fail(decodeSqliteWorkerReplyError(job, reply.error));
      return;
    }
    returnProvisionalSqliteWorkerReply(
      slot,
      job,
      reply.provisional,
      {
        status: "rejected",
        error: resolveSqliteWorkerCommandFailure(job, reply),
      },
      owner,
    );
    return;
  }
  if (!reply.ok) {
    if (reply.cleanupFailure && job.nativeDispatched && !reply.retire) {
      const original = resolveSqliteWorkerCommandFailure(job, reply);
      owner.fail(decodeSqliteWorkerCleanupError(reply.cleanupFailure), undefined, undefined, {
        error: original,
      });
      return;
    }
    const opening = job.request.type === "open";
    if (reply.openNotEntered && opening && job.dispatchState) {
      job.dispatchState.openNotEntered = true;
    }
    const error = decodeSqliteWorkerReplyError(job, reply.error);
    if (opening && reply.openNotEntered && !reply.retire) {
      slot.current = undefined;
      const refusal = job.operationAdmission?.admission.failure?.transportError ?? error;
      owner.finish(job, refusal, undefined, { kind: "not-entered", error: refusal });
      owner.dispatch();
      return;
    }
    if (job.request.type !== "execute" || reply.retire) {
      const refusedOpen = opening && reply.openOutcome === "refused-before-agent-open";
      const failure =
        refusedOpen || (opening && reply.admissionRefused)
          ? toErrorObject(
              job.operationAdmission?.admission.failure?.transportError ?? error,
              "SQLite callback open failed",
            )
          : toErrorObject(error, "SQLite worker operation failed");
      owner.fail(
        failure,
        job.request.type !== "execute" ? failure : undefined,
        refusedOpen ? "refused-before-agent-open" : undefined,
      );
      return;
    }
    slot.current = undefined;
    owner.finish(job, resolveSqliteWorkerCommandFailure(job, reply));
    owner.dispatch();
    return;
  }
  let value: unknown;
  try {
    const result = decodeSqliteWorkerReplyValue(job, reply);
    if (result.type === "continue") {
      // Continuations retain the current job and its reserved transport credits through drain.
      postJobRequest(job, result.request, []);
      return;
    }
    value = result.value;
  } catch (error) {
    owner.fail(error);
    return;
  }
  if (reply.provisional) {
    returnProvisionalSqliteWorkerReply(
      slot,
      job,
      reply.provisional,
      { status: "fulfilled", value },
      owner,
    );
    return;
  }
  if (reply.cleanupFailure) {
    const admission = job.operationAdmission?.admission;
    const failure =
      admission?.failure?.source === "domain" ? undefined : admission?.failure?.transportError;
    owner.fail(
      decodeSqliteWorkerCleanupError(reply.cleanupFailure),
      undefined,
      undefined,
      failure === undefined ? { value } : { error: failure },
    );
    return;
  }
  slot.current = undefined;
  if (job.request.type === "close") {
    owner.finish(job, undefined, value, undefined, reply.closeReceipt);
  } else {
    // Domains own handled refusal results; physical and request authority still fence delivery.
    const admission = job.operationAdmission?.admission;
    owner.finish(
      job,
      admission?.failure?.source === "domain" ? undefined : admission?.failure?.transportError,
      value,
    );
  }
  owner.dispatch();
}
