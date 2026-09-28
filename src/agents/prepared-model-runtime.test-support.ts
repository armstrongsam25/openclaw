import type {
  PreparedModelRuntimeInput,
  PreparedModelRuntimePublicationOptions,
  PreparedModelRuntimeSnapshot,
} from "./prepared-model-runtime.types.js";

type PreparedModelRuntimeTestApi = {
  publishPreparedModelRuntimeSnapshot(
    input: PreparedModelRuntimeInput,
    options?: PreparedModelRuntimePublicationOptions,
  ): Promise<PreparedModelRuntimeSnapshot>;
  resetPreparedModelRuntimeSnapshotsForTest(): Promise<void>;
};

function getPreparedModelRuntimeTestApi(): PreparedModelRuntimeTestApi | undefined {
  return (globalThis as Record<PropertyKey, unknown>)[
    Symbol.for("openclaw.preparedModelRuntimeTestApi")
  ] as PreparedModelRuntimeTestApi | undefined;
}

export function publishPreparedModelRuntimeSnapshot(
  input: PreparedModelRuntimeInput,
  options?: PreparedModelRuntimePublicationOptions,
): Promise<PreparedModelRuntimeSnapshot> {
  const api = getPreparedModelRuntimeTestApi();
  if (!api) {
    throw new Error("Prepared model runtime must be loaded in a test worker before publication");
  }
  return api.publishPreparedModelRuntimeSnapshot(input, options);
}

/** Clears prepared model owners when the production module is loaded in this test worker. */
export async function resetPreparedModelRuntimeSnapshotsForTest(): Promise<void> {
  await getPreparedModelRuntimeTestApi()?.resetPreparedModelRuntimeSnapshotsForTest();
}

export async function resetPreparedModelCatalogStateForTest(): Promise<void> {
  const { resetModelCatalogBuilderCacheForTest } = await import("./model-catalog.js");
  await resetPreparedModelRuntimeSnapshotsForTest();
  resetModelCatalogBuilderCacheForTest();
}

export async function resetPreparedGatewayModelCatalogForTest(): Promise<void> {
  // Gateway fixtures retain the same model-owner initialization before resetting it.
  await import("../gateway/server-start.js");
  await import("../gateway/server-model-catalog.js");
  await resetPreparedModelCatalogStateForTest();
}
