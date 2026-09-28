/** Process close owns every admitted model runtime and native catalog worker. */
import { createDeferredCore } from "../shared/deferred.js";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import type {
  PreparedModelRuntimeOwner,
  PreparedModelRuntimeReplacement,
} from "./prepared-model-runtime.types.js";

/** Notifications supplement the owner's generation and registration checks. */
export function capturePreparedModelRuntimeGeneration(
  owner: Pick<PreparedModelRuntimeOwner, "generationRetirement">,
): AbortSignal {
  return (owner.generationRetirement ??= new AbortController()).signal;
}

export function retirePreparedModelRuntimeGeneration(
  owner: Pick<PreparedModelRuntimeOwner, "generationRetirement">,
): void {
  const retirement = owner.generationRetirement;
  owner.generationRetirement = undefined;
  retirement?.abort();
}

type ModelRuntimeClose = (error: Error) => Promise<void>;
type ModelCatalogRecovery = (
  borrowers: readonly { agentDir: string; isCurrent: () => boolean }[],
) => Promise<void>;
class ProcessModelRuntimeLifetimes {
  readonly closeCallbacks = new Set<ModelRuntimeClose>();
  // Retained runtime copies can predate this additive recovery metadata.
  catalogRecoveries?: Map<ModelRuntimeClose, { recover?: ModelCatalogRecovery }>;
  retirePlugins?: () => Promise<void>;
  epoch = 0;
  closing?: Promise<void>;
}

const lifetimes = resolveGlobalSingleton(
  Symbol.for("openclaw.preparedModelRuntimeLifetimes"),
  () => new ProcessModelRuntimeLifetimes(),
  () => closePreparedModelRuntimeSnapshots(),
);

export function capturePreparedModelRuntimeLifetime(): () => void {
  const epoch = lifetimes.epoch;
  const assertCurrent = () => {
    if (lifetimes.closing || epoch !== lifetimes.epoch) {
      throw new Error("prepared model runtime process lifetime closed");
    }
  };
  assertCurrent();
  return assertCurrent;
}

export function registerPreparedModelRuntimeClose(
  close: ModelRuntimeClose,
  recoverCatalog?: ModelCatalogRecovery,
): () => void {
  capturePreparedModelRuntimeLifetime();
  lifetimes.closeCallbacks.add(close);
  const registration = { recover: recoverCatalog };
  (lifetimes.catalogRecoveries ??= new Map()).set(close, registration);
  return () => {
    if (lifetimes.catalogRecoveries?.get(close) !== registration) {
      return;
    }
    lifetimes.closeCallbacks.delete(close);
    lifetimes.catalogRecoveries.delete(close);
  };
}

/** Worker failures return to the live model owner after all failed borrowers settle. */
export async function recoverPreparedModelCatalogBorrowers(
  borrowers: Parameters<ModelCatalogRecovery>[0],
): Promise<void> {
  const results = await Promise.allSettled(
    [...(lifetimes.catalogRecoveries?.entries() ?? [])].map(async ([close, registration]) => {
      if (lifetimes.catalogRecoveries?.get(close) === registration) {
        await registration.recover?.(borrowers);
      }
    }),
  );
  const failures = results.flatMap((result) =>
    result.status === "rejected" ? [result.reason] : [],
  );
  if (failures.length) {
    throw new AggregateError(failures, "Prepared model catalog recovery failed");
  }
}

/** Install the shared plugin resource owner only when a real generation acquires it. */
export function registerPreparedPluginRetirement(retire: () => Promise<void>): void {
  capturePreparedModelRuntimeLifetime();
  lifetimes.retirePlugins ??= retire;
}

/** Fence admission before abort callbacks run; old publications cannot enter the next lifetime. */
export function closePreparedModelRuntimeSnapshots(): Promise<void> {
  if (lifetimes.closing) {
    return lifetimes.closing;
  }
  const closed = createDeferredCore();
  lifetimes.closing = closed.promise;
  lifetimes.epoch += 1;
  const error = new Error("prepared model runtime process lifetime closed");
  void Promise.allSettled(
    [...lifetimes.closeCallbacks].map(async (close) => await close(error)),
  ).then(async (results) => {
    try {
      await lifetimes.retirePlugins?.();
      lifetimes.retirePlugins = undefined;
    } catch (reason) {
      results.push({ status: "rejected", reason });
    }
    const failures = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    );
    if (failures.length) {
      closed.reject(new AggregateError(failures, "Prepared model runtime failed to close"));
    } else {
      lifetimes.closing = undefined;
      closed.resolve();
    }
  });
  return closed.promise;
}

export function createPreparedModelRuntimeReplacement(): PreparedModelRuntimeReplacement {
  const { promise, resolve, reject } = createDeferredCore();
  // Readers await the original promise. This handler only prevents an unobserved rejected gate
  // when a reload fails before any request reaches the stale generation.
  void promise.catch(() => undefined);
  return { gateId: Symbol("prepared-model-runtime-replacement"), promise, resolve, reject };
}
