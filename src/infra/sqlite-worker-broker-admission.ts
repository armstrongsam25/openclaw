import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { realpathSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { serialize } from "node:v8";
import { createDeferredCore } from "../shared/deferred.js";
import { INCOGNITO_AGENT_SQLITE_BASENAME } from "../state/openclaw-agent-db.paths.js";
import { getOpenClawDatabaseMaintenanceScope } from "../state/openclaw-state-db-async-lifecycle.js";
import {
  acquireStateDatabaseSchemaLease,
  assertStateDatabaseAccessAllowed,
  type StateDatabaseSchemaLease,
} from "./gateway-state-owner.js";
import { createRetainedOperation, type RetainedOperation } from "./retained-operation.js";
import { resolveRuntimeProcessEntrypointUrl } from "./runtime-process-url.js";
import { retainSqliteWriteAdmissionService } from "./sqlite-transaction.js";
import { recordSqliteWorkerHostRefusal } from "./sqlite-worker-broker-settlement.js";
import type {
  PreparedSqliteWorkerOpen,
  SqliteWorkerStoreOptions,
  Actor,
  SqliteWorkerExecution,
  Job,
  SqliteWorkerOpenCustody,
  SqliteWorkerPlacement,
} from "./sqlite-worker-broker.types.js";
import { SqliteWorkerError } from "./sqlite-worker-contract.js";
import {
  readDatabasePathIdentitySync,
  type DatabasePathIdentity,
} from "./sqlite-worker-identity.js";
import {
  createSqliteWorkerOperationAdmission,
  type SqliteWorkerAdmissionFactory,
} from "./sqlite-worker-operation-admission.js";
import type { SqliteWorkerOperationSettlement } from "./sqlite-worker-operation-settlement.js";
import {
  captureSqliteWorkerStateContext,
  type SqliteWorkerStateContext,
} from "./sqlite-worker-state-context.js";
import { captureRetainedNativeWorkerSource } from "./worker-native-lifecycle.js";

export function validateSqliteWorkerDatabaseLocator(databasePath: string): void {
  const basename = path.basename(databasePath);
  if (
    !databasePath ||
    databasePath.startsWith("file:") ||
    basename === ":memory:" ||
    basename === INCOGNITO_AGENT_SQLITE_BASENAME
  ) {
    throw new Error(
      "SQLite worker stores require a file-backed filesystem path; in-memory and incognito databases are not supported",
    );
  }
}

export function captureSqliteWorkerOpen(
  options: SqliteWorkerStoreOptions,
  stateContext?: SqliteWorkerStateContext,
  assertCurrent?: () => void,
  custody: SqliteWorkerOpenCustody = {},
): PreparedSqliteWorkerOpen {
  const { createAdmission, preparation, ...native } = custody;
  const inCaller = createAdmission ? AsyncLocalStorage.snapshot() : undefined;
  const ownedAdmission = options.admission;
  const assertOpening = ownedAdmission
    ? () => {
        assertCurrent?.();
        ownedAdmission.assertCurrent();
      }
    : assertCurrent;
  const databasePath = path.resolve(options.databasePath);
  if (
    options.admission &&
    (!options.existingOnly || !options.admission.identity.startsWith("file:"))
  ) {
    throw new Error("Owned SQLite Worker admission requires an existing physical identity");
  }
  assertOpening?.();
  const carrier = resolveRuntimeProcessEntrypointUrl("sqliteStore");
  const carrierUrl = options.runtimeGeneration?.resolve(carrier) ?? carrier;
  return {
    ...native,
    maintenanceScope: custody.maintenanceScope ?? getOpenClawDatabaseMaintenanceScope(),
    ...(preparation !== undefined ? { preparation: serialize(preparation) } : {}),
    runtimeGeneration: options.runtimeGeneration,
    nativeWorkerSource: captureRetainedNativeWorkerSource({
      runtimeGeneration: options.runtimeGeneration,
    }),
    carrierUrl,
    createAdmission:
      createAdmission && inCaller ? (operation) => inCaller(createAdmission, operation) : undefined,
    assertCurrent: assertOpening,
    ...(options.admission
      ? {
          expectedIdentity: options.admission.identity,
          createOpenAdmission: () => {
            let granted = false;
            return {
              nativeLocations: [databasePath],
              admission: createSqliteWorkerOperationAdmission((request, grant) => {
                if (granted || request.stage !== "open") {
                  throw new Error("SQLite Worker open admission requested out of order");
                }
                assertOpening!();
                if (!grant()) {
                  throw new Error("SQLite Worker open admission expired");
                }
                granted = true;
              }),
            };
          },
        }
      : {}),
    moduleUrl: new URL(options.moduleUrl),
    databasePath,
    input: serialize(options.input),
    existingOnly: options.existingOnly === true,
    ...(stateContext ? { stateContext: captureSqliteWorkerStateContext(stateContext) } : {}),
  };
}

function validateSqliteWorkerModuleUrl(moduleUrl: URL): void {
  if (moduleUrl.protocol !== "file:" || moduleUrl.search || moduleUrl.hash) {
    throw new Error("SQLite worker backend must be a static local module URL");
  }
}

/** Resolve one captured locator before the broker's retained native open. */
export function prepareSqliteWorkerDatabaseAdmissionSync(options: PreparedSqliteWorkerOpen) {
  validateSqliteWorkerModuleUrl(options.moduleUrl);
  const identity = readDatabasePathIdentitySync(path.resolve(options.databasePath));
  const databasePath = path.resolve(options.databasePath);
  const inputHash = createHash("sha256").update(options.input).digest("hex");
  options.assertCurrent?.();
  if (options.expectedIdentity && identity.key !== options.expectedIdentity) {
    throw new Error("SQLite Worker path no longer matches its borrowed native owner");
  }
  const placement: SqliteWorkerPlacement | undefined =
    options.stateContext && options.stateDatabasePath === undefined
      ? {
          kind: "file",
          requestedPath: databasePath,
          canonicalPath: identity.canonicalPath,
          identity: { ...identity },
        }
      : undefined;
  return { databasePath, inputHash, identity, placement };
}

export function captureSqliteWorkerAdmissionPaths(
  databasePath: string,
  identity: DatabasePathIdentity,
  actors: Iterable<Actor>,
): Set<string> {
  const admittedPaths = new Set([databasePath, identity.canonicalPath]);
  if (
    [...actors].some(
      (entry) =>
        entry.key !== identity.key &&
        [...admittedPaths].some((pathname) => entry.pathReferences.has(pathname)),
    )
  ) {
    throw new Error(
      "SQLite database pathname changed while its worker owner is active; close the existing store first",
    );
  }
  return admittedPaths;
}

export function retainSqliteWorkerAdmissionCleanup(
  actor: Actor,
  retain: PreparedSqliteWorkerOpen["retainCleanup"],
  close: () => RetainedOperation<void>,
): void {
  const closeRetained = () => {
    if (actor.references === 0) {
      return close();
    }
    const completion = createRetainedOperation<void>(() => {});
    completion.resolve(undefined);
    return completion.operation;
  };
  retain?.({
    get pending() {
      return actor.references === 0 && actor.cleanupState === "pending";
    },
    closeRetained,
    close: () => closeRetained().result,
  });
}

export function retainSqliteWorkerAdmissionPathReferences(actor: Actor, paths: Set<string>) {
  for (const pathname of paths) {
    actor.pathReferences.set(pathname, (actor.pathReferences.get(pathname) ?? 0) + 1);
  }
  return () => {
    for (const pathname of paths) {
      const references = actor.pathReferences.get(pathname) ?? 0;
      if (references > 1) {
        actor.pathReferences.set(pathname, references - 1);
      } else {
        actor.pathReferences.delete(pathname);
      }
    }
  };
}

export function resolveOpenedSqliteWorkerIdentitySync(
  databasePath: string,
  previous: DatabasePathIdentity,
  isOwnedElsewhere: (key: string) => boolean,
): DatabasePathIdentity {
  return validateOpenedIdentity(
    readDatabasePathIdentitySync(databasePath),
    previous,
    isOwnedElsewhere,
  );
}

function validateOpenedIdentity(
  openedIdentity: DatabasePathIdentity,
  previous: DatabasePathIdentity,
  isOwnedElsewhere: (key: string) => boolean,
): DatabasePathIdentity {
  const physical = openedIdentity.key;
  if (openedIdentity.canonicalPath !== previous.canonicalPath) {
    throw new Error("SQLite database canonical pathname changed during open");
  }
  if (!physical.startsWith("file:")) {
    throw new Error("SQLite worker backend did not establish its database file");
  }
  if (isOwnedElsewhere(physical)) {
    throw new Error("SQLite database identity collided with an existing worker owner during open");
  }
  if (previous.key.startsWith("file:") && physical !== previous.key) {
    throw new Error("SQLite database file identity changed during open");
  }
  return openedIdentity;
}

export function findUnclaimedSharedStateActors(
  actors: Iterable<Actor>,
  databasePath: string,
): Actor[] {
  const pathname = path.resolve(databasePath);
  return [...actors].filter(
    (actor) =>
      actor.stateContext !== undefined &&
      actor.references === 0 &&
      actor.cleanupState === "pending" &&
      actor.databasePath === pathname,
  );
}

export function closeUnclaimedSharedStateActors(
  actors: Iterable<Actor>,
  databasePath: string,
  close: (actor: Actor) => RetainedOperation<void>,
): RetainedOperation<void> {
  const pending = findUnclaimedSharedStateActors(actors, databasePath).map(close);
  const completion = createRetainedOperation<void>(() => {
    for (const operation of pending) {
      operation.service();
    }
    advance();
  });
  function advance() {
    if (completion.operation.read().status !== "pending") {
      return;
    }
    const results = pending.map((operation) => operation.read());
    if (results.some((result) => result.status === "pending")) {
      return;
    }
    const errors = results.flatMap((result) =>
      result.status === "rejected" ? [result.error] : [],
    );
    if (errors.length) {
      completion.reject(
        new AggregateError(errors, "SQLite worker unclaimed cleanup failed", { cause: errors[0] }),
      );
    } else {
      completion.resolve(undefined);
    }
  }
  for (const operation of pending) {
    void operation.result.then(advance, advance);
  }
  advance();
  return completion.operation;
}

export function resolveSqliteWorkerModuleUrlSync(sourceUrl: URL) {
  const modulePath = realpathSync(fileURLToPath(sourceUrl));
  const isFile = statSync(modulePath).isFile();
  const moduleUrl = pathToFileURL(modulePath).href;
  if (!/\.[cm]?[jt]s$/.test(modulePath) || !isFile) {
    throw new Error("SQLite worker backend must identify a JavaScript or TypeScript file");
  }
  return { modulePath, moduleUrl };
}

function assertSqliteWorkerActorStateContext(
  actor: Actor,
  stateContext: SqliteWorkerStateContext | undefined,
): void {
  if (actor.stateContext?.existingSchemaPath !== stateContext?.existingSchemaPath) {
    throw new Error("Shared-state worker schema policy changed; close its actor first");
  }
}

export function assertSqliteWorkerActorExecution(actor: Actor): SqliteWorkerExecution {
  const execution = [...actor.slot.executions].find(
    (entry) => entry.worker === actor.executionWorker,
  );
  const failure = actor.slot.failed ?? execution?.failed;
  if (failure) {
    throw failure;
  }
  if (!execution || execution.exited || execution.retiringRetained || actor.backendClosed) {
    throw new SqliteWorkerError("SQLite actor execution is no longer available", "closed");
  }
  return execution;
}

export function assertSqliteWorkerActorReusable(
  actor: Actor,
  moduleUrl: string,
  inputHash: string,
  stateContext: SqliteWorkerStateContext | undefined,
): void {
  assertSqliteWorkerActorExecution(actor);
  if (actor.moduleUrl !== moduleUrl || actor.inputHash !== inputHash) {
    throw new Error("SQLite database already belongs to another worker backend");
  }
  assertSqliteWorkerActorStateContext(actor, stateContext);
}

export function prepareSqliteWorkerActorContext(actor: Actor | undefined, job: Job): void {
  const { request } = job;
  const stateContext = request.stateContext ?? actor?.stateContext;
  // A drained actor retains native disposal custody after its caller loses admission.
  if (actor && request.type !== "close") {
    assertStateDatabaseAccessAllowed(actor.stateDatabasePath ?? actor.databasePath, {
      maintenanceScope: job.maintenanceScope,
    });
  }
  if (actor && stateContext) {
    assertSqliteWorkerActorStateContext(actor, stateContext);
    request.stateDatabasePath = actor.stateDatabasePath ?? actor.databasePath;
    request.stateContext = stateContext;
  }
}

export function prepareSqliteWorkerOperationAdmission(
  job: Job,
  actor: Actor | undefined,
  assertDispatchable: () => void,
  assertCurrentJob: () => void,
) {
  const databasePath = job.request.stateDatabasePath ?? actor?.databasePath;
  if (!job.createAdmission && !databasePath) {
    return undefined;
  }
  const settlement = createDeferredCore<SqliteWorkerOperationSettlement>();
  job.settleNative = settlement.resolve;
  job.nativeSettlement = settlement.promise;
  let registeringConsumerCompletion = true;
  let retained: ReturnType<SqliteWorkerAdmissionFactory>;
  try {
    retained = job.createAdmission
      ? job.createAdmission({
          settled: settlement.promise,
          readFinalReceipt: () => job.finalReceipt,
          retainConsumerCompletion(completion) {
            if (!registeringConsumerCompletion || job.nativeDispatched || job.completed) {
              throw new SqliteWorkerError(
                "SQLite consumer completion registration is closed",
                "closed",
              );
            }
            if (job.consumerCompletion) {
              if (job.consumerCompletion !== completion) {
                throw new SqliteWorkerError(
                  "SQLite consumer completion is already registered",
                  "closed",
                );
              }
              return;
            }
            job.consumerCompletion = completion;
            job.consumerSettlement = settlement.promise.then<SqliteWorkerOperationSettlement>(
              (native) => {
                if (native.kind === "unknown") {
                  return native;
                }
                return completion.then(
                  () => native,
                  () => native,
                );
              },
            );
          },
          runCallback: job.runCallback,
          runRetainedCallback: job.runRetainedCallback,
          readCallbackDeliveryFailure: () => job.readCallbackDeliveryFailure?.(),
          refuseCallback(error) {
            if (job.completed || !job.nativeDispatched) {
              throw new SqliteWorkerError(
                "SQLite callback refusal lost its accepted operation",
                "closed",
              );
            }
            throw recordSqliteWorkerHostRefusal(job, error);
          },
          retainCommitAuthority: (assertCurrent) => {
            if (!job.operationAdmission) {
              throw new Error("SQLite operation admission is not installed");
            }
            job.operationAdmission.admission.retainCommitAuthority(assertCurrent);
          },
        })
      : {
          admission: createSqliteWorkerOperationAdmission(() => {
            throw new SqliteWorkerError(
              "SQLite domain operation requires its own admission",
              "closed",
            );
          }),
          nativeLocations: databasePath ? [databasePath] : [],
        };
  } finally {
    registeringConsumerCompletion = false;
  }
  try {
    retained.admission.bindRefusalProvenance({
      pending: () => job.refusal?.pending,
      selected(occurrence) {
        const previous = job.refusal;
        // A child with no local failure can still carry its ancestor's confirmed refusal.
        const selected = occurrence ?? previous?.admissionFailure;
        if (!selected) {
          job.refusal = undefined;
          return;
        }
        job.refusal =
          previous?.admissionFailure === selected && previous.nativeConfirmed
            ? Object.freeze({ admissionFailure: selected, nativeConfirmed: true })
            : Object.freeze({ admissionFailure: selected });
      },
    });
    retained.admission.bindCommitAuthority((references) => {
      const candidates = new Map<number, Job>();
      const collect = (candidate: Job) => {
        if (candidates.has(candidate.request.id)) {
          return;
        }
        candidates.set(candidate.request.id, candidate);
        for (const child of candidate.provisionalChildren?.values() ?? []) {
          collect(child);
        }
      };
      for (let ancestor: Job | undefined = job; ancestor; ancestor = ancestor.parent) {
        collect(ancestor);
      }
      for (const reference of references) {
        const candidate = candidates.get(reference.requestId);
        if (
          !candidate ||
          candidate.completed ||
          candidate.request.actor !== reference.actorId ||
          candidate.executionWorker !== job.executionWorker ||
          !candidate.operationAdmission
        ) {
          throw new SqliteWorkerError(
            "SQLite commit authority lost its accepted native lineage",
            "closed",
          );
        }
        try {
          candidate.operationAdmission.admission.assertCommitAuthority(reference.authorityId);
        } catch (error) {
          throw recordSqliteWorkerHostRefusal(job, error);
        }
      }
    });
    if (databasePath) {
      let schemaLease: StateDatabaseSchemaLease | undefined;
      const assertAccess = () => {
        assertCurrentJob();
        job.maintenanceScope?.assertAdmission();
        assertStateDatabaseAccessAllowed(databasePath, {
          maintenanceScope: job.maintenanceScope,
          schemaLease,
        });
      };
      retained.admission.bindDatabaseAuthority({
        databasePath,
        assertRequest: assertDispatchable,
        assertAccess,
        acquireSchema() {
          assertAccess();
          const acquire = () => acquireStateDatabaseSchemaLease(databasePath);
          const lease = job.maintenanceScope ? job.maintenanceScope.run(acquire) : acquire();
          schemaLease = lease;
          job.maintenanceScope?.own(lease, "shared-resources", () => lease.release());
          return {
            assertCurrent() {
              assertAccess();
              lease.assertCurrent();
            },
            release: () => lease.release(),
          };
        },
      });
    }
  } catch (error) {
    retained.admission.finish();
    throw error;
  }
  job.operationAdmission = {
    admission: retained.admission,
    // Native BEGIN services the live job's grants at the actual admitted database paths.
    releaseService: retainSqliteWriteAdmissionService(
      [
        ...retained.nativeLocations,
        ...(databasePath ? [databasePath] : []),
        ...(actor?.pathReferences.keys() ?? []),
      ],
      () => retained.admission.service(),
    ),
  };
  return retained.admission.port;
}
