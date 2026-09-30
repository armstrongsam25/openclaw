import { expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { createRetainedOperation } from "./retained-operation.js";
import type { Actor } from "./sqlite-worker-broker.types.js";
import {
  createSqliteWorkerClient,
  runSqliteWorkerClientOperation,
} from "./sqlite-worker-client.js";

type Operations = { write: { input: string; output: string } };
const closedError = { code: "closed", message: "SQLite worker store is closed" };

function createActor(): Actor {
  return {
    kind: "file",
    get executionWorker(): never {
      throw new Error("Client scope must not access the native execution handle");
    },
    get nativeStopped(): never {
      throw new Error("Client scope must not observe native termination");
    },
    get nativeStoppedRecorded(): never {
      throw new Error("Client scope must not inspect native termination");
    },
    markNativeStopped() {
      throw new Error("Client scope must not record native termination");
    },
    id: 1,
    key: "client-fixture",
    databasePath: "/fixture/state.sqlite",
    pathReferences: new Map([["/fixture/state.sqlite", 1]]),
    moduleUrl: "file:///fixture/sqlite-backend.js",
    inputHash: "client-fixture",
    get slot(): never {
      throw new Error("Client scope must not access the broker's native Worker slot");
    },
    references: 1,
    opened: Promise.resolve(),
    openDispatch: { dispatched: true },
    initialized: true,
    backendClosed: false,
  };
}

it.each(["missing", "sealed"] as const)(
  "refuses a %s client before entering an operation or dispatching work",
  async (boundary) => {
    const dispatch = vi.fn(async () => "committed");
    const { client, store } = createSqliteWorkerClient<Operations>({
      actor: createActor(),
      isDraining: () => boundary === "sealed",
      isAvailable: () => true,
      dispatch,
      releaseRetained: () => {
        const released = createRetainedOperation<void>(() => {
          if (released.operation.read().status === "pending") {
            throw new Error("Logical client release must settle synchronously");
          }
        });
        released.resolve(undefined);
        return released.operation;
      },
      service() {
        throw new Error("Refused operation must not service native work");
      },
    });
    const operation = vi.fn(() => store.execute({ type: "write", input: "must not enter" }));
    const track = vi.fn(() => () => {});
    const assertCurrent = vi.fn();
    const createAdmission = vi.fn(() => {
      throw new Error("Refused operation must not acquire admission");
    });

    await expect(
      runSqliteWorkerClientOperation(
        boundary === "missing" ? undefined : client,
        operation,
        undefined,
        track,
        assertCurrent,
        createAdmission,
      ),
    ).rejects.toMatchObject(closedError);
    expect(operation).not.toHaveBeenCalled();
    expect(track).not.toHaveBeenCalled();
    expect(assertCurrent).not.toHaveBeenCalled();
    expect(createAdmission).not.toHaveBeenCalled();
    expect(dispatch).not.toHaveBeenCalled();
    await store.close();
  },
);

it("lets an admitted scope finish through close before releasing its owner", async () => {
  const resume = createDeferred();
  const dispatched = createDeferred();
  const committed = createDeferred<string>();
  const events: string[] = [];
  let draining = false;
  const release = vi.fn(() => {
    const released = createRetainedOperation<void>(() => {
      if (released.operation.read().status === "pending") {
        throw new Error("Logical client release must settle synchronously");
      }
    });
    events.push("released");
    released.resolve(undefined);
    return released.operation;
  });
  const { client, store } = createSqliteWorkerClient<Operations>({
    actor: createActor(),
    isDraining: () => draining,
    isAvailable: () => true,
    dispatch: () => {
      events.push("dispatched");
      dispatched.resolve();
      return committed.promise;
    },
    releaseRetained: release,
    service() {
      throw new Error("Awaited scope must not service native work");
    },
  });
  const accepted = runSqliteWorkerClientOperation<Operations, string>(
    client,
    async (scope) => {
      await resume.promise;
      const result = await scope.execute({ type: "write", input: "accepted before close" });
      events.push("completed");
      return result;
    },
    undefined,
    () => () => {},
  );
  draining = true;
  const closing = store.close();
  const lateOperation = vi.fn(async () => "must not enter");
  try {
    await expect(
      runSqliteWorkerClientOperation(client, lateOperation, undefined, () => () => {}),
    ).rejects.toMatchObject(closedError);
    await expect(store.execute({ type: "write", input: "after close" })).rejects.toMatchObject(
      closedError,
    );
    expect(lateOperation).not.toHaveBeenCalled();
    expect(release).not.toHaveBeenCalled();
    resume.resolve();
    await Promise.race([dispatched.promise, accepted]);
    expect(release).not.toHaveBeenCalled();
    committed.resolve("committed");
    await expect(accepted).resolves.toBe("committed");
    await closing;
    expect(events).toEqual(["dispatched", "completed", "released"]);
    expect(release).toHaveBeenCalledOnce();
  } finally {
    resume.resolve();
    committed.resolve("committed");
    await Promise.allSettled([accepted, closing]);
  }
});

it.each(["fulfilled", "rejected"] as const)(
  "public close reentered during dispatch waits for the %s command promise",
  async (outcome) => {
    const commandResult = createDeferred<string>();
    const closeEntered = createDeferred<{ closing: Promise<void> }>();
    const failure = new Error("Command promise rejected after native settlement");
    let closeSettled = false;
    let closing: Promise<void> | undefined;
    const releaseLogicalClient = () => {
      const released = createRetainedOperation<void>(() => {
        throw new Error("Logical client release must not service native work");
      });
      released.resolve(undefined);
      return released.operation;
    };
    const release = vi.fn(releaseLogicalClient);
    const { client, store } = createSqliteWorkerClient<Operations>({
      actor: createActor(),
      isDraining: () => false,
      isAvailable: () => true,
      dispatch: (_payload, _signal, _scope, _assertCurrent, _createAdmission, settled) => {
        if (!settled) {
          throw new Error("Client dispatch must retain its original settlement callback");
        }
        settled(
          outcome === "fulfilled"
            ? { status: "fulfilled", value: "committed" }
            : { status: "rejected", error: failure },
          { settlement: { kind: "completed" } },
        );
        return commandResult.promise;
      },
      releaseRetained: release,
      service() {
        throw new Error("Public close must not service native work in this logical fixture");
      },
    });
    const command = client.execute(
      { type: "write", input: "accepted before reentrant close" },
      {},
      undefined,
      () => {
        closing = store.close();
        void closing.then(
          () => {
            closeSettled = true;
          },
          () => {
            closeSettled = true;
          },
        );
        closeEntered.resolve({ closing });
      },
    );
    void command.catch(() => undefined);
    try {
      const reentered = await closeEntered.promise;
      expect(release).toHaveBeenCalledOnce();

      // A later independent public close can finish while this accepted command remains pending.
      const independentRelease = vi.fn(releaseLogicalClient);
      const independent = createSqliteWorkerClient<Operations>({
        actor: createActor(),
        isDraining: () => false,
        isAvailable: () => true,
        dispatch() {
          throw new Error("Independent client must not dispatch work");
        },
        releaseRetained: independentRelease,
        service() {
          throw new Error("Independent logical close must not service native work");
        },
      });
      await independent.store.close();
      expect(independentRelease).toHaveBeenCalledOnce();
      expect(closeSettled).toBe(false);

      if (outcome === "fulfilled") {
        commandResult.resolve("committed");
        await expect(command).resolves.toBe("committed");
      } else {
        commandResult.reject(failure);
        await expect(command).rejects.toBe(failure);
      }
      await expect(reentered.closing).resolves.toBeUndefined();
      expect(closeSettled).toBe(true);
      expect(release).toHaveBeenCalledOnce();
    } finally {
      commandResult.resolve("committed");
      await Promise.allSettled([command, closing]);
    }
  },
);
