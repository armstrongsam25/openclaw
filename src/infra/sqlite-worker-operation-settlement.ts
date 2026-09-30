import type { DatabaseSync } from "node:sqlite";
import type { MessagePort } from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import type { Result } from "@openclaw/normalization-core/result";
import type { RetainedOperation } from "./retained-operation.js";
import {
  deferSqlitePostCommitPublication,
  getSqliteTransactionScope,
  stageSqliteTransactionState,
} from "./sqlite-post-commit.js";

export type SqliteWorkerCallbackRequest = {
  port: MessagePort;
  acknowledgment: SharedArrayBuffer;
  transaction: boolean;
};

export type SqliteWorkerCallbackAdmission = <T>(
  request: SqliteWorkerCallbackRequest,
  grant: () => boolean,
  callback: () => T,
  context?: object,
) => T;

export type SqliteWorkerRetainedCallbackAdmission = <T>(
  request: SqliteWorkerCallbackRequest,
  grant: () => boolean,
  start: () => RetainedOperation<T>,
  context?: object,
) => RetainedOperation<T>;

/** This presence cell distinguishes a delivery throw of undefined from no delivery failure. */
export type SqliteWorkerCallbackDeliveryFailure = Readonly<{ error: unknown }>;

export type SqliteWorkerCommitAuthorityReference = {
  requestId: number;
  actorId: number;
  authorityId: number;
};

/** The original host throw is restored only for this exact definite native refusal. */
export type SqliteWorkerRefusalReceipt = Readonly<{
  original: unknown;
  transportError: Error;
}>;

/** Native settlement is independent of whether delivery of the result succeeded. */
export type SqliteWorkerOperationSettlement =
  | { kind: "completed" }
  | { kind: "not-entered"; error: unknown }
  | { kind: "unknown"; error: unknown };

/** Source facts describe their domain checkpoint; they never grant write authority. */
export type SqliteWorkerSourceReceipt = { facts: unknown };

export type SqliteWorkerNativeSettlement =
  | { kind: "completed"; committed?: SqliteWorkerSourceReceipt }
  | { kind: "unknown"; committed?: SqliteWorkerSourceReceipt };

export type SqliteWorkerNativeSettlementOwner = {
  readonly committed: SqliteWorkerSourceReceipt | undefined;
  readonly settlement: SqliteWorkerNativeSettlement | undefined;
  waitForSettlement(
    deadlineMs: number,
  ): Extract<SqliteWorkerNativeSettlement, { kind: "completed" }>;
};

/** The broker resolves this only from the executing owner's settlement evidence. */
export type RetainedWorkerTransactionAdmission = {
  readonly settled: Promise<SqliteWorkerOperationSettlement>;
  /** This Job's final receipt is available only after its settlement owner records final settlement. */
  readonly readFinalReceipt?: (this: void) => SqliteWorkerRetainedReceipt | undefined;
  /** Register the actual finite async consumer result during this Job's admission factory. */
  readonly retainConsumerCompletion?: (completion: Promise<unknown>) => void;
  /** Present only on the canonical broker's accepted operation, never a reconstructed receipt. */
  readonly runCallback?: SqliteWorkerCallbackAdmission;
  readonly runRetainedCallback?: SqliteWorkerRetainedCallbackAdmission;
  readonly readCallbackDeliveryFailure?: () => SqliteWorkerCallbackDeliveryFailure | undefined;
  readonly refuseCallback?: (error: unknown) => never;
  readonly retainCommitAuthority?: (assertCurrent: () => void) => void;
};

/** Actual settlement and commit facts retained even when result delivery refuses. */
export type SqliteWorkerRetainedReceipt = {
  settlement: SqliteWorkerOperationSettlement;
  refusal?: SqliteWorkerRefusalReceipt;
  committed?: SqliteWorkerSourceReceipt;
  rollbackSource?: SqliteWorkerSourceReceipt;
};

export function parseSqliteWorkerSourceReceipt(
  value: unknown,
): SqliteWorkerSourceReceipt | undefined {
  return isRecord(value) && Object.hasOwn(value, "facts") ? { facts: value.facts } : undefined;
}

export function deferSqliteWorkerOwnedCommitReceipt(
  database: DatabaseSync,
  owner: { port: MessagePort; committed?: SqliteWorkerSourceReceipt },
  prepare: () => unknown,
): void {
  if (
    !deferSqlitePostCommitPublication(database, () => {
      owner.committed = { facts: prepare() };
      owner.port.postMessage({ kind: "native-commit", committed: owner.committed }, []);
    })
  ) {
    throw new Error("SQLite worker receipt requires a transaction publication owner");
  }
}

export function parseSqliteWorkerNativeSettlement(
  value: unknown,
  committed: SqliteWorkerSourceReceipt | undefined,
): SqliteWorkerNativeSettlement | undefined {
  const receipt = isRecord(value) ? parseSqliteWorkerSourceReceipt(value.committed) : undefined;
  if (
    !isRecord(value) ||
    (value.kind !== "completed" && value.kind !== "unknown") ||
    (value.committed !== undefined && !receipt)
  ) {
    return undefined;
  }
  const current = receipt ?? committed;
  return { kind: value.kind, ...(current ? { committed: current } : {}) };
}

type SqliteWorkerRollbackCheckpoint = { receipt?: SqliteWorkerSourceReceipt };

export function deferSqliteWorkerOwnedRollbackReceipt(
  database: DatabaseSync,
  owner: {
    database?: DatabaseSync;
    rollbackCheckpoint?: SqliteWorkerRollbackCheckpoint;
  },
  prepare: () => SqliteWorkerSourceReceipt | undefined,
): void {
  if (owner.database !== database || !database.isTransaction || owner.rollbackCheckpoint) {
    throw new Error("SQLite rollback checkpoint requires its original unregistered transaction");
  }
  const checkpoint: SqliteWorkerRollbackCheckpoint = {};
  if (
    !stageSqliteTransactionState(database, {
      stage() {
        owner.rollbackCheckpoint = checkpoint;
      },
      commit() {},
      rollback() {
        checkpoint.receipt = prepare();
      },
    })
  ) {
    throw new Error("SQLite rollback checkpoint requires a managed transaction owner");
  }
}

export type SqliteWorkerReadFacts = Result<unknown[], unknown>;
export type SqliteWorkerReadFactsCollection = {
  current?: { scope: object; preparations: Set<{ prepare: () => unknown }> };
  result?: SqliteWorkerReadFacts;
};

/** One captured operation owns finite preparations; rollback removes only its affected entries. */
export function deferSqliteWorkerOwnedReadFacts(
  database: DatabaseSync,
  owner: { database?: DatabaseSync; readFacts?: SqliteWorkerReadFactsCollection },
  prepare: () => unknown,
  capture: (
    preparations: readonly (() => unknown)[],
    previous: SqliteWorkerReadFacts | undefined,
  ) => SqliteWorkerReadFacts,
): void {
  const scope = getSqliteTransactionScope(database);
  if (owner.database !== database || !database.isTransaction || !scope) {
    throw new Error("SQLite read facts require their bound managed transaction");
  }
  const facts = (owner.readFacts ??= {});
  if (facts.current && facts.current.scope !== scope) {
    throw new Error("SQLite read facts changed their active transaction scope");
  }
  if (!facts.current) {
    const collection: NonNullable<SqliteWorkerReadFactsCollection["current"]> = {
      scope,
      preparations: new Set(),
    };
    const release = () => {
      if (facts.current === collection) {
        facts.current = undefined;
      }
    };
    stageSqliteTransactionState(database, {
      stage() {
        facts.current = collection;
      },
      commit: release,
      prepareObservers() {
        const preparations = Array.from(collection.preparations, (entry) => entry.prepare);
        collection.preparations.clear();
        if (preparations.length) {
          facts.result = capture(preparations, facts.result);
        }
      },
      rollback: release,
    });
  }
  const collection = facts.current;
  if (!collection) {
    throw new Error("SQLite read facts lost their managed transaction owner");
  }
  const preparation = { prepare };
  stageSqliteTransactionState(database, {
    stage() {
      collection.preparations.add(preparation);
    },
    commit() {},
    rollback() {
      collection.preparations.delete(preparation);
    },
  });
}

export function parseSqliteWorkerCommitAuthorities(message: Record<string, unknown>): Result<
  {
    references: SqliteWorkerCommitAuthorityReference[];
    guard: { id: number; acknowledgment: SharedArrayBuffer } | undefined;
  },
  string
> {
  const references: SqliteWorkerCommitAuthorityReference[] = [];
  if (message.commitAuthorities !== undefined) {
    if (!Array.isArray(message.commitAuthorities)) {
      return { ok: false, error: "SQLite commit authority references are invalid" };
    }
    for (const reference of message.commitAuthorities) {
      if (
        !isRecord(reference) ||
        typeof reference.requestId !== "number" ||
        typeof reference.actorId !== "number" ||
        typeof reference.authorityId !== "number" ||
        !Number.isSafeInteger(reference.requestId) ||
        !Number.isSafeInteger(reference.actorId) ||
        !Number.isSafeInteger(reference.authorityId)
      ) {
        return { ok: false, error: "SQLite commit authority reference is invalid" };
      }
      references.push({
        requestId: reference.requestId,
        actorId: reference.actorId,
        authorityId: reference.authorityId,
      });
    }
  }
  const guard = message.commitAuthority;
  if (guard === undefined) {
    return { ok: true, value: { references, guard } };
  }
  if (
    !isRecord(guard) ||
    typeof guard.id !== "number" ||
    !Number.isSafeInteger(guard.id) ||
    !(guard.acknowledgment instanceof SharedArrayBuffer) ||
    guard.acknowledgment.byteLength !== Int32Array.BYTES_PER_ELEMENT
  ) {
    return { ok: false, error: "SQLite retained authority request is invalid" };
  }
  return {
    ok: true,
    value: { references, guard: { id: guard.id, acknowledgment: guard.acknowledgment } },
  };
}
