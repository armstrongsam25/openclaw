import { writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import type { Worker } from "node:worker_threads";
import { expect, it, vi } from "vitest";
import { createDeferredCore } from "../shared/deferred.js";
import { initializeSqliteRuntimeCapabilities } from "./bun-sqlite-library.js";
import {
  captureRuntimeWorkerSource,
  withRuntimeWorkerGeneration,
} from "./runtime-worker-generation.js";
import {
  useSqliteWorkerStoreFixture,
  appendWorkerRow as append,
  readWorkerRows as read,
} from "./sqlite-worker-fixture.test-support.js";
import {
  openSqliteWorkerStore,
  runSqliteWorkerStoreOperation,
  type SqliteWorkerStore,
} from "./sqlite-worker-store.js";
import type { FixtureOperations } from "./sqlite-worker-store.test-support.js";
import { getTrackedWorkerCpuSources } from "./worker-cpu.js";
import * as workerCpu from "./worker-cpu.js";

vi.mock("node:os", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:os")>()),
  availableParallelism: () => 32,
}));

const { stores, tempDirs, databasePath, open } = useSqliteWorkerStoreFixture(
  "openclaw-sqlite-worker-generation-",
);

const { explicitSqliteCloseReleasesNativeResources } = await initializeSqliteRuntimeCapabilities();
const poolIt = explicitSqliteCloseReleasesNativeResources ? it : it.skip;

function observeCpuRegistrations() {
  type CpuSource = ReturnType<typeof getTrackedWorkerCpuSources>["workers"][number];
  type CpuWorker = Parameters<typeof workerCpu.trackNativeWorkerForCpu>[0];
  const carriers: Array<{ worker: CpuWorker; threadId: number; source: CpuSource }> = [];
  const supervisors: Array<{ worker: Worker; source: CpuSource }> = [];
  const addedSource = (before: CpuSource[]) => {
    const added = getTrackedWorkerCpuSources().workers.filter((source) => !before.includes(source));
    expect(added).toHaveLength(1);
    const source = added[0];
    if (!source) {
      throw new Error("Native worker CPU registration was not recorded");
    }
    return source;
  };
  const create = workerCpu.createCpuTrackedWorker;
  const track = workerCpu.trackNativeWorkerForCpu;
  const created = vi.spyOn(workerCpu, "createCpuTrackedWorker").mockImplementation((...args) => {
    const before = getTrackedWorkerCpuSources().workers;
    const worker = create(...args);
    supervisors.push({ worker, source: addedSource(before) });
    return worker;
  });
  const tracked = vi.spyOn(workerCpu, "trackNativeWorkerForCpu").mockImplementation((...args) => {
    const before = getTrackedWorkerCpuSources().workers;
    track(...args);
    const worker = args[0];
    carriers.push({ worker, threadId: worker.threadId, source: addedSource(before) });
  });
  return {
    carriers,
    supervisors,
    restore() {
      tracked.mockRestore();
      created.mockRestore();
    },
  };
}

poolIt("borrows only one carrier at capacity and never crosses retained generations", async () => {
  const ordinary = await Promise.all(Array.from({ length: 4 }, () => open(databasePath())));
  const ordinaryThreads = new Set(
    await Promise.all(ordinary.map(async (store) => (await append(store, "ordinary")).threadId)),
  );
  expect(ordinaryThreads.size).toBe(4);
  const moduleUrl = new URL("./sqlite-worker-store.test-support.ts", import.meta.url);
  const directory = tempDirs.make("openclaw-retained-sqlite-generation-");
  const generation = async (name: string, run: () => Promise<void>) => {
    const retained = pathToFileURL(path.join(directory, `${name}.mts`));
    await writeFile(retained, `export * from ${JSON.stringify(moduleUrl.href)};\n`);
    return await withRuntimeWorkerGeneration(
      async (bind) => {
        bind((url) => (url.href === moduleUrl.href ? retained : url));
        await run();
      },
      async () => {},
    );
  };
  const openRetained = async () => {
    const source = captureRuntimeWorkerSource(moduleUrl);
    const store = await openSqliteWorkerStore<FixtureOperations>({
      ...source,
      databasePath: databasePath(),
      input: undefined,
    });
    stores.add(store);
    return store;
  };
  let retained: SqliteWorkerStore<FixtureOperations> | undefined;
  const before = getTrackedWorkerCpuSources().workers;
  const observed = observeCpuRegistrations();
  const releaseWriter = createDeferredCore();
  const phaseOne = createDeferredCore();
  let nativeFinal = false;
  let writer: ReturnType<typeof append> | undefined;
  let closing: Promise<void> | undefined;
  try {
    closing = generation("first", async () => {
      retained = await openRetained();
      const first = await append(retained, "first");
      expect(ordinaryThreads.has(first.threadId)).toBe(false);
      expect(observed.carriers.map(({ threadId }) => threadId)).toEqual([first.threadId]);
      expect(getTrackedWorkerCpuSources().workers).toEqual([
        ...before,
        ...observed.supervisors.map(({ source }) => source),
        ...observed.carriers.map(({ source }) => source),
      ]);
      const sibling = await openRetained();
      expect((await append(sibling, "same generation")).threadId).toBe(first.threadId);
      await generation("second", async () => {
        await expect(openRetained()).rejects.toMatchObject({ code: "overloaded" });
        expect(observed.carriers.map(({ threadId }) => threadId)).toEqual([first.threadId]);
        expect(getTrackedWorkerCpuSources().workers).toEqual([
          ...before,
          ...observed.supervisors.map(({ source }) => source),
          ...observed.carriers.map(({ source }) => source),
        ]);
      });
      await Promise.all([append(retained, "second"), append(retained, "third")]);
      expect(await read(retained)).toEqual(["first", "second", "third"]);
      let entered = false;
      writer = runSqliteWorkerStoreOperation(retained, async (scope) => {
        entered = true;
        await releaseWriter.promise;
        return await scope.execute({
          type: "append",
          input: { value: "accepted through generation close" },
        });
      });
      void writer.catch(() => {});
      expect(entered).toBe(true);
      const source = captureRuntimeWorkerSource(moduleUrl);
      if (!source.runtimeGeneration) {
        throw new Error("Expected the actual retained generation for settlement observation");
      }
      source.runtimeGeneration.retain({}, async () => {
        phaseOne.resolve();
        return async () => {
          nativeFinal = true;
        };
      });
    });
    await Promise.race([phaseOne.promise, closing]);
    const reader = ordinary[0];
    if (!reader || !writer) {
      throw new Error("Expected the original reader and accepted generation writer");
    }
    expect(await read(reader)).toEqual(["ordinary"]);
    expect(nativeFinal).toBe(false);
    releaseWriter.resolve();
    expect(await writer).toMatchObject({ writes: 4 });
    await closing;
    expect(nativeFinal).toBe(true);
    expect(getTrackedWorkerCpuSources().workers).toEqual(before);
    for (const { worker } of [...observed.carriers, ...observed.supervisors]) {
      expect(worker.threadId).toBe(-1);
    }
    await expect(read(retained!)).rejects.toMatchObject({ code: "closed" });
    for (const store of ordinary) {
      await expect(append(store, "preserved")).resolves.toMatchObject({ writes: 2 });
    }
  } finally {
    releaseWriter.resolve();
    await Promise.allSettled([writer, closing]);
    observed.restore();
  }
});
