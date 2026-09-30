import { AsyncLocalStorage } from "node:async_hooks";
import { MessageChannel, receiveMessageOnPort } from "node:worker_threads";
import { createDeferredCore } from "../shared/deferred.js";
import { ensureSqliteLibrarySelected } from "./bun-sqlite-library.js";
import { resolveNodeCompileCacheEnv } from "./node-compile-cache-env.js";
import { resolveRuntimeWorkerThreadExecArgv } from "./runtime-worker-url.js";
import { createSqliteLifecycleAggregateError } from "./sqlite-lifecycle-errors.js";
import {
  receiveSqliteWorkerReply,
  type SqliteWorkerReplyOwner,
} from "./sqlite-worker-broker-reply.js";
import type {
  Slot,
  SqliteWorkerExecution,
  SqliteWorkerSlotOptions,
  SqliteWorkerRetainedOutcome,
  SqliteWorkerRetainedResult,
} from "./sqlite-worker-broker.types.js";
import type { SqliteWorkerReply } from "./sqlite-worker-contract.js";
import { createRetainedNativeWorker } from "./worker-native-lifecycle.js";

const runOutsideCaller = AsyncLocalStorage.snapshot();

type SqliteWorkerExecutionOwner = {
  explicitSqliteCloseReleasesNativeResources: boolean;
  createReplyOwner(
    slot: Slot,
    execution: SqliteWorkerExecution,
  ): Omit<SqliteWorkerReplyOwner, "resumeReply">;
  failExecution(slot: Slot, execution: SqliteWorkerExecution, error: unknown): void;
};

/** Physical custody stays separate from the slot's one admission queue. */
export function createSqliteWorkerExecution(
  slot: Slot,
  kind: SqliteWorkerExecution["kind"],
  options: SqliteWorkerSlotOptions,
  owner: SqliteWorkerExecutionOwner,
): SqliteWorkerExecution {
  ensureSqliteLibrarySelected();
  options.assertCurrent?.();
  const { port1: replyPort, port2: workerReplyPort } = new MessageChannel();
  let worker: SqliteWorkerExecution["worker"];
  try {
    worker = runOutsideCaller(() =>
      createRetainedNativeWorker(
        options.carrierUrl,
        {
          resourceLimits: { maxOldGenerationSizeMb: 512 },
          env: resolveNodeCompileCacheEnv(),
          execArgv: resolveRuntimeWorkerThreadExecArgv(options.carrierUrl),
          workerData: { replyPort: workerReplyPort },
          transferList: [workerReplyPort],
        },
        options.nativeWorkerSource,
      ),
    );
  } catch (error) {
    replyPort.close();
    workerReplyPort.close();
    throw error;
  }
  const exited = createDeferredCore();
  const execution: SqliteWorkerExecution = {
    kind,
    worker,
    replyPort,
    exit: exited.promise,
    exited: false,
    serviceReplies() {
      for (
        let queued = receiveMessageOnPort(replyPort);
        queued;
        queued = receiveMessageOnPort(replyPort)
      ) {
        execution.receiveReply(queued.message);
      }
    },
    receiveReply(reply) {
      receiveSqliteWorkerReply(slot, reply, replyOwner, worker);
      for (const retained of slot.executions) {
        retained.serviceFailureSettlement?.();
      }
    },
    recordJoinedExit(code) {
      if (execution.exited) {
        return;
      }
      execution.exited = true;
      execution.serviceReplies();
      replyPort.close();
      for (const actor of slot.actors) {
        if (actor.executionWorker !== worker) {
          continue;
        }
        actor.backendClosed = true;
        actor.physicalJoined = true;
        actor.markNativeStopped();
      }
      if (!execution.retiringRetained) {
        owner.failExecution(
          slot,
          execution,
          new Error(
            code === undefined
              ? "SQLite worker native lifetime ended"
              : `SQLite worker exited with code ${code}`,
          ),
        );
      }
      exited.resolve();
      slot.recordJoinedExit();
    },
  };
  const brokerReplyOwner = owner.createReplyOwner(slot, execution);
  const replyOwner: SqliteWorkerReplyOwner = {
    ...brokerReplyOwner,
    resumeReply(reply, executionWorker) {
      const original = [...slot.executions].find((entry) => entry.worker === executionWorker);
      if (!original) {
        throw new Error("SQLite deferred reply lost its original execution owner");
      }
      original.receiveReply(reply);
    },
    finish(job, error, value, settlement, closeReceipt) {
      if (job.request.type === "close") {
        const actor = [...slot.actors].find((entry) => entry.id === job.request.actor);
        if (actor?.executionWorker === worker) {
          if (closeReceipt) {
            actor.closeReceipt = closeReceipt;
          }
          if (error === undefined) {
            actor.backendClosed = true;
            if (owner.explicitSqliteCloseReleasesNativeResources) {
              actor.markNativeStopped();
            }
          }
        }
      }
      brokerReplyOwner.finish(job, error, value, settlement, closeReceipt);
    },
  };
  slot.executions.add(execution);
  replyPort.on("message", (reply: SqliteWorkerReply) => execution.receiveReply(reply));
  replyPort.on("messageerror", (error) => owner.failExecution(slot, execution, error));
  replyPort.unref();
  // An updater can retain a carrier from the installed generation.
  // SAFETY: This captured SQLite carrier sends the broker's typed reply protocol.
  worker.on("message", (reply) => execution.receiveReply(reply as SqliteWorkerReply));
  worker.on("error", (error) => owner.failExecution(slot, execution, error));
  worker.on("messageerror", (error) => owner.failExecution(slot, execution, error));
  // RetainedNativeWorker emits exit only after its resource owner has also joined.
  worker.once("exit", (code) => execution.recordJoinedExit(code));
  worker.unref();
  return execution;
}

export function retireSqliteWorkerExecutionRetained(
  slot: Slot,
  execution: SqliteWorkerExecution,
  serviceSlot: (slot: Slot) => void,
): SqliteWorkerRetainedResult<void> {
  if (execution.retiringRetained && execution.retiringRetained.read().status !== "rejected") {
    return execution.retiringRetained;
  }
  const completed = createDeferredCore();
  void completed.promise.catch(() => {});
  let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
  let native: ReturnType<SqliteWorkerExecution["worker"]["stop"]> | undefined;
  const finish = (settled: Exclude<SqliteWorkerRetainedOutcome<void>, { status: "pending" }>) => {
    if (outcome.status !== "pending") {
      return;
    }
    outcome = settled;
    if (settled.status === "fulfilled") {
      completed.resolve();
    } else {
      completed.reject(settled.error);
    }
  };
  const advance = () => {
    if (outcome.status !== "pending") {
      return;
    }
    try {
      if (execution.exited && !native) {
        finish({ status: "fulfilled", value: undefined });
        return;
      }
      if (!native) {
        native = execution.worker.stop();
        void native.result.then(advance, advance);
      }
      const stopped = native.read();
      if (stopped.status === "pending") {
        return;
      }
      if (stopped.status === "rejected") {
        throw stopped.error;
      }
      // This also covers constructor refusal: the retained owner proved no child survived.
      execution.recordJoinedExit();
      finish({ status: "fulfilled", value: undefined });
    } catch (error) {
      finish({ status: "rejected", error });
    }
  };
  const retained: SqliteWorkerRetainedResult<void> = {
    result: completed.promise,
    read: () => outcome,
    service() {
      native?.service();
      serviceSlot(slot);
      advance();
    },
  };
  execution.retiringRetained = retained;
  advance();
  return retained;
}

/** A full slot drain waits for every captured native owner, including failed ones. */
export function retireSqliteWorkerSlotRetained(
  slot: Slot,
  serviceSlot: (slot: Slot) => void,
  retireExecution: (
    slot: Slot,
    execution: SqliteWorkerExecution,
  ) => SqliteWorkerRetainedResult<void>,
): SqliteWorkerRetainedResult<void> {
  if (slot.retiringRetained && slot.retiringRetained.read().status !== "rejected") {
    return slot.retiringRetained;
  }
  const completed = createDeferredCore();
  void completed.promise.catch(() => {});
  let outcome: SqliteWorkerRetainedOutcome<void> = { status: "pending" };
  const stops: SqliteWorkerRetainedResult<void>[] = [];
  let initialized = false;
  const advance = () => {
    if (!initialized || outcome.status !== "pending") {
      return;
    }
    const results = stops.map((stop) => stop.read());
    if (results.some((result) => result.status === "pending")) {
      return;
    }
    const errors = results.flatMap((result) =>
      result.status === "rejected" ? [result.error] : [],
    );
    if (errors.length) {
      const error =
        errors.length === 1
          ? errors[0]
          : createSqliteLifecycleAggregateError(
              errors,
              "SQLite worker slot cleanup failed",
              errors[0],
            );
      outcome = { status: "rejected", error };
      completed.reject(error);
      return;
    }
    slot.recordJoinedExit();
    outcome = { status: "fulfilled", value: undefined };
    if (slot.retiringRetained === retained) {
      slot.retiringRetained = undefined;
      slot.retiring = undefined;
    }
    completed.resolve();
  };
  const retained: SqliteWorkerRetainedResult<void> = {
    result: completed.promise,
    read: () => outcome,
    service() {
      for (const stop of stops) {
        stop.service();
      }
      serviceSlot(slot);
      advance();
    },
  };
  slot.retiringRetained = retained;
  slot.retiring = retained.result;
  const executions = [...slot.executions];
  for (const execution of executions) {
    const stop = retireExecution(slot, execution);
    stops.push(stop);
    void stop.result.then(advance, advance);
  }
  initialized = true;
  advance();
  return retained;
}
