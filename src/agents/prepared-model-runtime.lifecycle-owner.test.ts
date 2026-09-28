import { expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import {
  recoverPreparedModelCatalogBorrowers,
  registerPreparedModelRuntimeClose,
} from "./prepared-model-runtime.lifecycle.js";

it("recovers all live owners without allowing stale deregistration to erase a replacement", async () => {
  const first = vi.fn(async () => {});
  const second = vi.fn(async () => {});
  const closeFirst = async () => {};
  const releaseFirst = registerPreparedModelRuntimeClose(closeFirst, first);
  const releaseSecond = registerPreparedModelRuntimeClose(async () => {}, second);
  let releaseReplacement: (() => void) | undefined;
  const borrowers = [{ agentDir: "/synthetic/agent", isCurrent: () => true }];
  try {
    await recoverPreparedModelCatalogBorrowers(borrowers);
    expect(first).toHaveBeenCalledExactlyOnceWith(borrowers);
    expect(second).toHaveBeenCalledExactlyOnceWith(borrowers);

    releaseFirst();
    await recoverPreparedModelCatalogBorrowers(borrowers);
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(2);

    releaseReplacement = registerPreparedModelRuntimeClose(closeFirst, first);
    releaseFirst();
    await recoverPreparedModelCatalogBorrowers(borrowers);
    expect(first).toHaveBeenCalledTimes(2);
    expect(second).toHaveBeenCalledTimes(3);

    releaseReplacement();
    releaseSecond();
    await recoverPreparedModelCatalogBorrowers(borrowers);
    expect(first).toHaveBeenCalledTimes(2);
    expect(second).toHaveBeenCalledTimes(3);
  } finally {
    releaseFirst();
    releaseReplacement?.();
    releaseSecond();
  }
});

it("settles every owner's recovery before propagating failures", async () => {
  const ready = createDeferred();
  const release = createDeferred();
  const firstFailure = new Error("first recovery failed");
  const secondFailure = new Error("second recovery failed");
  let secondFinished = false;
  const releaseFirst = registerPreparedModelRuntimeClose(
    async () => {},
    async () => {
      throw firstFailure;
    },
  );
  const releaseSecond = registerPreparedModelRuntimeClose(
    async () => {},
    async () => {
      ready.resolve();
      await release.promise;
      secondFinished = true;
      throw secondFailure;
    },
  );
  const recovery = recoverPreparedModelCatalogBorrowers([]).catch((error: unknown) => {
    expect(secondFinished).toBe(true);
    return error;
  });
  try {
    await ready.promise;
    expect(secondFinished).toBe(false);
    release.resolve();
    const error = await recovery;
    expect(error).toBeInstanceOf(AggregateError);
    if (!(error instanceof AggregateError)) {
      throw new Error("Recovery did not report its failures");
    }
    expect(error.errors).toEqual([firstFailure, secondFailure]);
  } finally {
    release.resolve();
    await recovery.catch(() => undefined);
    releaseFirst();
    releaseSecond();
  }
});
