import { describe, expect, it, vi, type MockInstance } from "vitest";
import { createDeferredCore } from "../shared/deferred.js";
import { createRetainedOperation, flatMapRetainedOperation } from "./retained-operation.js";
import type { RuntimeWorkerGeneration } from "./runtime-worker-generation.js";
import { failSqliteWorkerExecution } from "./sqlite-worker-broker-execution-failure.js";
import { createSqliteWorkerLifecycle } from "./sqlite-worker-broker-lifecycle.js";
import type { Actor } from "./sqlite-worker-broker.types.js";
import {
  captureRetainedNativeWorkerSource,
  type RetainedNativeWorkerSource,
} from "./worker-native-lifecycle.js";
import type { RetainedNativeWorker } from "./worker-native-lifecycle.types.js";

vi.mock("./bun-sqlite-library.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./bun-sqlite-library.js")>()),
  ensureSqliteLibrarySelected: () => {},
}));

const forbidden = () => {
  throw new Error("Fixture unexpectedly entered another lifecycle capability");
};

describe("SQLite worker slots", () => {
  // Bun resolves a `file:` preload by stripping "file://", so tsx's URL breaks on Windows.
  it.each([
    { runtime: "Node", bun: undefined, execArgv: ["--import", import.meta.resolve("tsx/esm")] },
    { runtime: "Bun", bun: "1.4.3", execArgv: [] },
  ])("gives $runtime source workers only the TypeScript loader they need", ({ bun, execArgv }) => {
    const versions = Object.getOwnPropertyDescriptor(process, "versions");
    Object.defineProperty(process, "versions", {
      configurable: true,
      value: { ...process.versions, bun },
    });
    try {
      const creationBoundary = new Error("Fixture stops at native worker creation");
      const creations: Array<{ filename: string | URL; execArgv: string[] | undefined }> = [];
      const nativeWorkerSource: RetainedNativeWorkerSource = {
        create(filename, options) {
          creations.push({ filename, execArgv: options?.execArgv });
          throw creationBoundary;
        },
        captureResource: forbidden,
        retain: forbidden,
      };
      const lifecycle = createSqliteWorkerLifecycle({
        explicitSqliteCloseReleasesNativeResources: true,
        actors: new Map(),
        slots: new Set(),
        stores: new Map(),
        enqueueClose: forbidden,
        failExecution: forbidden,
        maxWorkers: 1,
        maxStores: 1,
        createReplyOwner: forbidden,
      });
      expect(() =>
        lifecycle.tryReserveSlot({
          carrierUrl: new URL("file:///openclaw/src/infra/sqlite-store.worker.ts"),
          nativeWorkerSource,
        }),
      ).toThrow(creationBoundary);
      expect(creations).toEqual([{ filename: expect.any(URL), execArgv }]);
    } finally {
      if (versions) {
        Object.defineProperty(process, "versions", versions);
      }
    }
  });

  it.each([
    { capable: true, closeFails: false },
    { capable: false, closeFails: false },
    { capable: true, closeFails: true },
    { capable: false, closeFails: true },
  ])(
    "settles native custody after close or required exit (capable: $capable, failed: $closeFails)",
    async ({ capable, closeFails }) => {
      const generationFinals = new Map<object, Parameters<RuntimeWorkerGeneration["retain"]>[1]>();
      const generation: RuntimeWorkerGeneration = {
        resolve: (url) => url,
        retain: (owner, settle) => {
          generationFinals.set(owner, settle);
        },
      };
      const source = captureRetainedNativeWorkerSource({ runtimeGeneration: generation });
      let worker: RetainedNativeWorker | undefined;
      source.retain({}, async () => async () => {
        await worker?.stop().result;
      });
      const terminating = createDeferredCore();
      const permitStop = createRetainedOperation<void>(() => {});
      let stopObserver: MockInstance<RetainedNativeWorker["stop"]> | undefined;
      let closing: Promise<void> | undefined;
      try {
        const actors = new Map<string, Actor>();
        const error = new Error("native close failed");
        const lifecycle: ReturnType<typeof createSqliteWorkerLifecycle> =
          createSqliteWorkerLifecycle({
            explicitSqliteCloseReleasesNativeResources: capable,
            actors,
            slots: new Set(),
            stores: new Map(),
            enqueueClose: closeFails
              ? vi.fn().mockRejectedValue(error)
              : vi.fn().mockResolvedValue(undefined),
            failExecution(slot, execution, failure) {
              failSqliteWorkerExecution(
                { slot, execution, error: failure },
                {
                  retireExecutionRetained: lifecycle.retireExecutionRetained,
                  serviceSlot: lifecycle.serviceSlot,
                  finish: forbidden,
                  dispatch(current) {
                    if (current.current || current.queue.length) {
                      throw new Error("Fixture unexpectedly dispatched a SQLite command");
                    }
                  },
                },
              );
            },
            maxWorkers: 1,
            maxStores: 1,
            createReplyOwner: () => ({
              fail: forbidden,
              finish: forbidden,
              dispatch: forbidden,
              returnProvisional: forbidden,
            }),
          });
        const reservation = lifecycle.tryReserveSlot({
          carrierUrl: new URL("data:text/javascript,setInterval(() => {}, 1000)"),
          nativeWorkerSource: source,
          runtimeGeneration: generation,
        });
        if (!reservation) {
          throw new Error("Expected the initial FILE slot reservation");
        }
        const { slot, executionWorker } = reservation;
        worker = executionWorker;
        const originalStop = worker.stop.bind(worker);
        const gatedStop = flatMapRetainedOperation(permitStop.operation, originalStop);
        stopObserver = vi.spyOn(worker, "stop").mockImplementation(() => {
          terminating.resolve();
          return gatedStop;
        });
        const nativeStopped = createDeferredCore();
        const markNativeStopped = vi.fn(() => {
          actor.nativeStoppedRecorded = true;
          nativeStopped.resolve();
        });
        const actor: Actor = {
          kind: "file",
          id: 1,
          key: "fixture",
          databasePath: "/state/openclaw.sqlite",
          pathReferences: new Map(),
          moduleUrl: "file:///openclaw/dist/device-auth-store.sqlite.js",
          inputHash: "fixture",
          slot,
          executionWorker,
          runtimeGeneration: generation,
          references: 0,
          opened: Promise.resolve(),
          openDispatch: { dispatched: true },
          initialized: true,
          backendClosed: false,
          nativeStopped: nativeStopped.promise,
          nativeStoppedRecorded: false,
          markNativeStopped,
        };
        actors.set(actor.key, actor);
        slot.actors.add(actor);
        // The retained pending open prevents the ordinary empty-slot retirement path.
        let settled = false;
        closing = lifecycle.closeActor(actor).finally(() => {
          settled = true;
        });
        const outcome = Promise.allSettled([closing]);
        if (!capable || closeFails) {
          await terminating.promise;
          expect(settled).toBe(false);
          expect(markNativeStopped).not.toHaveBeenCalled();
          permitStop.resolve(undefined);
        } else {
          await closing;
          expect(stopObserver).not.toHaveBeenCalled();
        }
        expect(await outcome).toEqual([
          closeFails
            ? { status: "rejected", reason: error }
            : { status: "fulfilled", value: undefined },
        ]);
        expect(markNativeStopped).toHaveBeenCalledOnce();
        expect(actors.size).toBe(0);
      } finally {
        permitStop.resolve(undefined);
        try {
          await Promise.allSettled(closing ? [closing] : []);
          for (const settle of generationFinals.values()) {
            const nativeFinal = await settle();
            await nativeFinal?.();
          }
        } finally {
          stopObserver?.mockRestore();
        }
      }
    },
  );
});
