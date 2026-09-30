import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { afterEach, expect, it, vi, type MockInstance } from "vitest";
import { waitForFixtureFile } from "../../test/helpers/process-wait.js";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { createDeferredCore } from "../shared/deferred.js";
import { initializeSqliteRuntimeCapabilities } from "./bun-sqlite-library.js";
import { createRetainedOperation, flatMapRetainedOperation } from "./retained-operation.js";
import { SqliteWorkerBroker } from "./sqlite-worker-broker.js";
import { createSqliteWorkerOperationAdmission } from "./sqlite-worker-operation-admission.js";
import type { FixtureOpenInput, FixtureOperations } from "./sqlite-worker-store.test-support.js";
import { NativeWorker } from "./worker-native-handle.js";

vi.mock("node:os", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:os")>()),
  availableParallelism: () => 16,
}));

const { explicitSqliteCloseReleasesNativeResources } = await initializeSqliteRuntimeCapabilities();
const poolIt = explicitSqliteCloseReleasesNativeResources ? it : it.skip;

const dirs = useAutoCleanupTempDirTracker((cleanup) => afterEach(cleanup));
afterEach(() => vi.restoreAllMocks());

it("preserves a native commit and its queued follower when admission cleanup fails", async () => {
  const root = dirs.make("sqlite-worker-admission-cleanup-");
  const databasePath = path.join(root, "store.sqlite");
  const gatePath = path.join(root, "release");
  await writeFile(gatePath, "preparation already released");
  const broker = new SqliteWorkerBroker();
  const cleanupFailure = new Error("Synthetic post-grant cleanup failed");
  const warnings = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
  try {
    const store = await broker.open<FixtureOperations>({
      moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
      databasePath,
      input: { type: "prepare", markerPath: path.join(root, "prepared"), gatePath, guarded: true },
    });
    assert.ok(store);
    const write = broker.runOperation(
      store,
      (scope) => scope.execute({ type: "append", input: { value: "committed once" } }),
      undefined,
      undefined,
      () => ({
        nativeLocations: [databasePath],
        admission: createSqliteWorkerOperationAdmission((request, grant) => {
          grant();
          if (request.stage === "commit") {
            throw cleanupFailure;
          }
        }),
      }),
    );
    const follower = store.execute({ type: "read", input: undefined });
    const [receipt, values] = await Promise.all([write, follower]);
    expect(receipt).toMatchObject({ writes: 1 });
    expect(values).toEqual(["committed once"]);
    expect(warnings).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ errors: [cleanupFailure] }),
    );
    await store.close();
    const reopened = await broker.open<FixtureOperations>({
      moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
      databasePath,
      input: undefined,
    });
    assert.ok(reopened);
    expect(await reopened.execute({ type: "read", input: undefined })).toEqual(["committed once"]);
  } finally {
    await broker.close();
  }
});

it("settles ordinary unclaimed cleanup after an open queued later finishes", async () => {
  const root = dirs.make("sqlite-worker-unclaimed-open-wakeup-");
  const broker = new SqliteWorkerBroker();
  const target = path.join(root, "unclaimed.sqlite");
  const firstMarker = path.join(root, "first-preparing");
  const secondMarker = path.join(root, "second-preparing");
  const firstGate = path.join(root, "release-first");
  const secondGate = path.join(root, "release-second");
  const open = (name: string, markerPath: string, gatePath: string) =>
    broker.open<FixtureOperations>({
      moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
      databasePath: path.join(root, `${name}.sqlite`),
      input: { type: "prepareOpen", markerPath, gatePath },
    });
  const first = open("first", firstMarker, firstGate);
  let second: ReturnType<typeof open> | undefined;
  try {
    await waitForFixtureFile(firstMarker, first, "preparing open");
    // No actor exists for this target; this exercises the real broker's OPEN join.
    expect(broker.hasUnclaimedSharedStateCleanup(target)).toBe(false);
    let cleanupSettled = false;
    const cleanup = broker.closeUnclaimedSharedState(target);
    const observeCleanup = () => {
      cleanupSettled = true;
    };
    void cleanup.then(observeCleanup, observeCleanup);
    second = open("second", secondMarker, secondGate);
    await expect(readFile(secondMarker)).rejects.toMatchObject({ code: "ENOENT" });
    expect(cleanupSettled).toBe(false);

    await writeFile(firstGate, "release first open");
    const firstStore = await first;
    assert.ok(firstStore);
    await waitForFixtureFile(secondMarker, second, "preparing open");
    expect(cleanupSettled).toBe(false);

    await writeFile(secondGate, "release second open");
    const secondStore = await second;
    assert.ok(secondStore);
    expect(await secondStore.execute({ type: "read", input: undefined })).toEqual([]);
    await broker.closeUnclaimedSharedState(target);
    // A real later read and cleanup completed; the earlier ordinary join must also finish.
    expect(cleanupSettled).toBe(true);
    await cleanup;
    expect(await firstStore.execute({ type: "read", input: undefined })).toEqual([]);
    await Promise.all([firstStore.close(), secondStore.close()]);
  } finally {
    await Promise.all([
      writeFile(firstGate, "release for cleanup"),
      writeFile(secondGate, "release for cleanup"),
    ]);
    await Promise.allSettled([first, second]);
    await broker.close();
  }
});

poolIt(
  "opens an unrelated file on a busy healthy carrier while a failed carrier awaits native stop",
  async () => {
    const root = dirs.make("sqlite-worker-failed-capacity-");
    const failedPath = path.join(root, "failed.sqlite");
    const healthyPath = path.join(root, "healthy.sqlite");
    const unrelatedPath = path.join(root, "unrelated.sqlite");
    const markerPath = path.join(root, "preparing");
    const gatePath = path.join(root, "release-prepare");
    const broker = new SqliteWorkerBroker();
    const opened = new Map<string, { worker: NativeWorker; actor: number }>();
    const executed: { worker: NativeWorker; actor: number }[] = [];
    // oxlint-disable-next-line typescript/unbound-method -- apply preserves the original retained owner.
    const postMessage = NativeWorker.prototype.postMessage;
    const messages = vi.spyOn(NativeWorker.prototype, "postMessage").mockImplementation(function (
      this: NativeWorker,
      ...args: Parameters<NativeWorker["postMessage"]>
    ) {
      const [request] = args;
      if (isRecord(request) && typeof request.actor === "number") {
        if (request.type === "open" && typeof request.databasePath === "string") {
          opened.set(request.databasePath, { worker: this, actor: request.actor });
        } else if (request.type === "execute") {
          executed.push({ worker: this, actor: request.actor });
        }
      }
      return postMessage.apply(this, args);
    });
    const open = (databasePath: string, input?: FixtureOpenInput) =>
      broker.open<FixtureOperations>({
        moduleUrl: new URL("./sqlite-worker-store.test-support.ts", import.meta.url),
        databasePath,
        input,
      });
    const permitStop = createRetainedOperation<void>(() => {});
    const stopping = createDeferredCore();
    let nativeExited = false;
    let heldStop: ReturnType<NativeWorker["stop"]> | undefined;
    let stopObserver: MockInstance<NativeWorker["stop"]> | undefined;
    let write: Promise<unknown> | undefined;
    let unrelated: ReturnType<typeof open> | undefined;
    try {
      const failed = await open(failedPath);
      const healthy = await open(healthyPath, { type: "prepare", markerPath, gatePath });
      assert.ok(failed);
      assert.ok(healthy);
      const failedWorker = opened.get(failedPath)?.worker;
      const healthyWorker = opened.get(healthyPath)?.worker;
      assert.ok(failedWorker);
      assert.ok(healthyWorker);
      expect(failedWorker === healthyWorker).toBe(false);
      failedWorker.once("exit", () => {
        nativeExited = true;
      });
      const originalStop = failedWorker.stop.bind(failedWorker);
      const gatedStop = flatMapRetainedOperation(permitStop.operation, originalStop);
      heldStop = gatedStop;
      stopObserver = vi.spyOn(failedWorker, "stop").mockImplementation(() => {
        stopping.resolve();
        return gatedStop;
      });

      write = healthy.execute({ type: "append", input: { value: "healthy write" } });
      await waitForFixtureFile(markerPath, write, "preparing");
      const original = new Error("Synthetic first carrier failure at pool capacity");
      failedWorker.emit("error", original);
      await stopping.promise;
      await expect(failed.execute({ type: "read", input: undefined })).rejects.toMatchObject({
        code: "unavailable",
        message: original.message,
      });

      // Both OPENs finished; this reservation selects synchronously while the healthy Job is busy.
      unrelated = open(unrelatedPath);
      void unrelated.catch(() => {});
      expect(opened.has(unrelatedPath)).toBe(false);
      expect(gatedStop.read().status).toBe("pending");
      await writeFile(gatePath, "release healthy command preparation");
      expect(await write).toMatchObject({ writes: 1 });
      const store = await unrelated;
      assert.ok(store);
      const unrelatedOpen = opened.get(unrelatedPath);
      assert.ok(unrelatedOpen);
      expect(unrelatedOpen.worker === healthyWorker).toBe(true);
      expect(
        await store.execute({ type: "append", input: { value: "unrelated write" } }),
      ).toMatchObject({ writes: 1 });
      const unrelatedExecutions = executed.filter(({ actor }) => actor === unrelatedOpen.actor);
      expect(unrelatedExecutions.length).toBeGreaterThan(0);
      expect(unrelatedExecutions.every(({ worker }) => worker === healthyWorker)).toBe(true);
      expect(await store.execute({ type: "read", input: undefined })).toEqual(["unrelated write"]);
      expect(await healthy.execute({ type: "read", input: undefined })).toEqual(["healthy write"]);
      expect(gatedStop.read().status).toBe("pending");
      expect(nativeExited).toBe(false);
      expect(failedWorker.threadId).not.toBe(-1);
    } finally {
      permitStop.resolve(undefined);
      try {
        await writeFile(gatePath, "release for cleanup");
        await Promise.allSettled([write, unrelated, heldStop?.result]);
      } finally {
        stopObserver?.mockRestore();
        messages.mockRestore();
        await broker.close();
      }
    }
    expect(heldStop?.read()).toEqual({ status: "fulfilled", value: undefined });
    expect(nativeExited).toBe(true);
  },
);
