import { AsyncLocalStorage } from "node:async_hooks";
import type { DatabaseSync } from "node:sqlite";
import { isNativeError, isPromise } from "node:util/types";
import {
  MessageChannel,
  receiveMessageOnPort,
  type MessagePort,
  type Transferable,
} from "node:worker_threads";
import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { resolveIdentityPathViaExistingAncestorSync } from "./boundary-path.js";
import { getSqliteTransactionScope, stageSqliteTransactionState } from "./sqlite-post-commit.js";
import {
  captureSqliteWorkerSourceFacts,
  parseSqliteWorkerReadFacts,
  SqliteWorkerError,
} from "./sqlite-worker-contract.js";
import {
  deferSqliteWorkerOwnedCommitReceipt,
  parseSqliteWorkerCommitAuthorities,
  parseSqliteWorkerNativeSettlement,
  parseSqliteWorkerSourceReceipt,
  type RetainedWorkerTransactionAdmission,
  type SqliteWorkerReadFacts,
  type SqliteWorkerReadFactsCollection,
  type SqliteWorkerCommitAuthorityReference,
  type SqliteWorkerSourceReceipt,
  type SqliteWorkerNativeSettlement,
  type SqliteWorkerNativeSettlementOwner,
  type SqliteWorkerRefusalReceipt,
} from "./sqlite-worker-operation-settlement.js";

const REQUESTED = 0;
const GRANTED = 1;
const REFUSED = 2;

/** Only the factory's admission before agent open may certify this refusal. */
export const SqliteWorkerOpenRefusedError = resolveGlobalSingleton(
  Symbol.for("openclaw.sqliteWorkerOpenRefusedError"),
  () =>
    class OpenRefusedError extends Error {
      constructor(readonly originalError: unknown) {
        super("SQLite worker admission was refused before agent open", { cause: originalError });
        this.name = "SqliteWorkerOpenRefusedError";
      }
    },
);

export type SqliteWorkerAdmissionRequest = {
  stage: "open" | "prepare" | "transaction" | "commit";
  facts: unknown;
};

type AdmissionFailureSource = "authority" | "domain" | "protocol";
type SqliteWorkerAdmissionFailure = SqliteWorkerRefusalReceipt &
  Readonly<{ source: AdmissionFailureSource }>;

type SqliteWorkerReadObserver = {
  pending(request: SqliteWorkerAdmissionRequest): void;
  committed(facts: SqliteWorkerReadFacts): void;
};

export type SqliteWorkerOperationAdmission = SqliteWorkerNativeSettlementOwner & {
  readonly port: MessagePort;
  readonly failure: SqliteWorkerAdmissionFailure | undefined;
  readonly cleanupFailures: readonly unknown[];
  bindReadObserver(observer: SqliteWorkerReadObserver): void;
  deliverReadFacts(value: unknown): void;
  bindRefusalProvenance(provenance: {
    pending(): SqliteWorkerRefusalReceipt | undefined;
    selected(occurrence: SqliteWorkerAdmissionFailure | undefined): void;
  }): void;
  retainCommitAuthority(assertCurrent: () => void): void;
  assertCommitAuthority(authorityId: number): void;
  bindCommitAuthority(
    assertCurrent: (references: readonly SqliteWorkerCommitAuthorityReference[]) => void,
  ): void;
  service(): void;
  finish(): void;
  bindRequestAuthority(assertCurrent: () => void): void;
  bindDatabaseAuthority(authority: {
    databasePath: string;
    assertRequest?(): void;
    assertAccess(): void;
    acquireSchema(): { assertCurrent(): void; release(): void };
  }): void;
};

export type SqliteWorkerAdmissionFactory = (operation: RetainedWorkerTransactionAdmission) => {
  admission: SqliteWorkerOperationAdmission;
  nativeLocations: readonly string[];
};

/** The caller retains real source custody before invoking the synchronous grant. */
export function createSqliteWorkerOperationAdmission(
  admit: (request: SqliteWorkerAdmissionRequest, grant: () => boolean) => void,
  attachment?: unknown,
  onCommitted?: (facts: unknown) => void,
): SqliteWorkerOperationAdmission {
  const { port1, port2 } = new MessageChannel();
  if (attachment !== undefined) {
    try {
      // This message moves with port2; command payloads retain their v8 encoding.
      port1.postMessage({ kind: "sqlite-operation-attachment", value: attachment }, []);
    } catch (error) {
      port1.close();
      port2.close();
      throw error;
    }
  }
  const inOwnerContext = AsyncLocalStorage.snapshot();
  const decisions = new Set<Int32Array>();
  const cleanupFailures: unknown[] = [];
  let closed = false;
  let failure: SqliteWorkerAdmissionFailure | undefined;
  let refusalProvenance:
    | Parameters<SqliteWorkerOperationAdmission["bindRefusalProvenance"]>[0]
    | undefined;
  let commitObserverFailure: { error: unknown } | undefined;
  let admissionSealed = false;
  let readObserver: SqliteWorkerReadObserver | undefined;
  let committed: SqliteWorkerNativeSettlementOwner["committed"];
  let settlement: SqliteWorkerNativeSettlement | undefined;
  let requestAuthority: (() => void) | undefined;
  let commitAuthority:
    | ((references: readonly SqliteWorkerCommitAuthorityReference[]) => void)
    | undefined;
  const retainedAuthorities = new Map<number, () => unknown>();
  let retaining: { id: number; acknowledgment: Int32Array; decision: Int32Array } | undefined;
  let databaseAuthority:
    | (Parameters<SqliteWorkerOperationAdmission["bindDatabaseAuthority"]>[0] & {
        lease?: { assertCurrent(): void; release(): void };
      })
    | undefined;
  const waiting = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
  const recordFailure = (error: unknown, source: AdmissionFailureSource) => {
    // A handled domain refusal cannot hide a later loss of physical custody or protocol failure.
    if (!failure || (failure.source === "domain" && source !== "domain")) {
      const pending = refusalProvenance?.pending();
      const original =
        pending && Object.is(pending.transportError, error) ? pending.original : error;
      failure = Object.freeze({
        original,
        transportError: isNativeError(error)
          ? error
          : new Error("SQLite worker admission refused", { cause: error }),
        source,
      });
    }
    // Ignored domain failures still consume their pending boxing without replacing selection.
    refusalProvenance?.selected(failure);
  };
  const refuse = (decision: Int32Array, error: unknown, source: AdmissionFailureSource) => {
    if (Atomics.compareExchange(decision, 0, REQUESTED, REFUSED) === REQUESTED) {
      recordFailure(error, source);
      Atomics.notify(decision, 0);
    } else {
      if (Atomics.load(decision, 0) === GRANTED) {
        cleanupFailures.push(error);
      }
      refusalProvenance?.selected(failure);
    }
  };
  const assertRetainedAuthority = (authorityId: number) => {
    const predicate = retainedAuthorities.get(authorityId);
    if (closed || !predicate) {
      throw new SqliteWorkerError("SQLite retained commit authority is unavailable", "closed");
    }
    // A provisional return can release its local transport; the original owner predicate survives.
    const result = predicate();
    if (isPromise(result)) {
      void result.catch(() => undefined);
      throw new Error("SQLite commit authority must remain synchronous");
    }
  };
  const notify = (observer: () => unknown) => {
    try {
      const result: unknown = inOwnerContext(observer);
      if (isPromise(result)) {
        void result.catch(() => undefined);
        throw new Error("SQLite committed-fact observers must remain synchronous");
      }
    } catch (error) {
      if (commitObserverFailure) {
        cleanupFailures.push(error);
      } else {
        commitObserverFailure = { error };
      }
      recordFailure(error, "authority");
    }
  };
  const receive = (message: unknown) => {
    admissionSealed = true;
    if (isRecord(message) && message.kind === "native-commit") {
      const receipt = parseSqliteWorkerSourceReceipt(message.committed);
      if (!receipt || settlement) {
        recordFailure(
          new SqliteWorkerError("SQLite worker commit receipt is invalid", "outcome-unknown"),
          "protocol",
        );
        return;
      }
      committed = receipt;
      if (onCommitted) {
        notify(() => onCommitted(receipt.facts));
      }
      return;
    }
    if (isRecord(message) && message.kind === "native-settlement") {
      const received = parseSqliteWorkerNativeSettlement(message.settlement, committed);
      if (!received || settlement) {
        recordFailure(
          new SqliteWorkerError("SQLite worker native settlement is invalid", "outcome-unknown"),
          "protocol",
        );
        return;
      }
      committed = received.committed;
      settlement = received;
      return;
    }
    if (
      !isRecord(message) ||
      !(message.decision instanceof SharedArrayBuffer) ||
      message.decision.byteLength !== Int32Array.BYTES_PER_ELEMENT ||
      (message.stage !== "open" &&
        message.stage !== "prepare" &&
        message.stage !== "transaction" &&
        message.stage !== "commit")
    ) {
      recordFailure(
        new SqliteWorkerError("SQLite worker admission request is invalid", "unavailable"),
        "protocol",
      );
      return;
    }
    const decision = new Int32Array(message.decision);
    const parsed = parseSqliteWorkerCommitAuthorities(message);
    if (!parsed.ok) {
      refuse(decision, new SqliteWorkerError(parsed.error, "closed"), "protocol");
      return;
    }
    const { references, guard } = parsed.value;
    const assertCommitAuthorities = () => {
      if (!references.length) {
        return;
      }
      if (message.stage !== "commit" || !commitAuthority) {
        throw new SqliteWorkerError("SQLite commit authority owner is unavailable", "closed");
      }
      commitAuthority(references);
    };
    decisions.add(decision);
    if (closed || commitObserverFailure) {
      refuse(
        decision,
        commitObserverFailure
          ? commitObserverFailure.error
          : new SqliteWorkerError("SQLite worker admission is closed", "closed"),
        "authority",
      );
      return;
    }
    const request: SqliteWorkerAdmissionRequest = { stage: message.stage, facts: message.facts };
    const grant = () => {
      if (commitObserverFailure) {
        refuse(decision, commitObserverFailure.error, "authority");
        return false;
      }
      if (closed || Atomics.load(decision, 0) !== REQUESTED) {
        return false;
      }
      // Domain admission can reenter owner lifecycle before handing the native writer its grant.
      try {
        inOwnerContext(() => {
          requestAuthority?.();
          databaseAuthority?.assertAccess();
          const pending: unknown = readObserver?.pending(request);
          if (isPromise(pending)) {
            void pending.catch(() => undefined);
            throw new Error("SQLite read observation must remain synchronous");
          }
          assertCommitAuthorities();
          if (retaining?.decision === decision && Atomics.load(retaining.acknowledgment, 0) === 1) {
            assertRetainedAuthority(retaining.id);
          }
        });
      } catch (error) {
        refuse(decision, error, "authority");
        return false;
      }
      const granted = Atomics.compareExchange(decision, 0, REQUESTED, GRANTED) === REQUESTED;
      if (granted) {
        Atomics.notify(decision, 0);
      }
      return granted;
    };
    const previousRetaining = retaining;
    retaining =
      isRecord(guard) &&
      typeof guard.id === "number" &&
      guard.acknowledgment instanceof SharedArrayBuffer
        ? { id: guard.id, acknowledgment: new Int32Array(guard.acknowledgment), decision }
        : undefined;
    let source: AdmissionFailureSource = "authority";
    try {
      inOwnerContext(() => {
        requestAuthority?.();
        databaseAuthority?.assertRequest?.();
        databaseAuthority?.assertAccess();
        assertCommitAuthorities();
      });
      if (
        request.stage === "prepare" &&
        isRecord(request.facts) &&
        request.facts.kind === "schema-maintenance"
      ) {
        const authority = databaseAuthority;
        if (
          !authority ||
          typeof request.facts.databasePath !== "string" ||
          resolveIdentityPathViaExistingAncestorSync(request.facts.databasePath) !==
            authority.databasePath
        ) {
          throw new SqliteWorkerError(
            "SQLite schema maintenance target differs from its admitted database",
            "closed",
          );
        }
        inOwnerContext(() => {
          authority.lease ??= authority.acquireSchema();
          authority.lease.assertCurrent();
          grant();
        });
      } else {
        source = "domain";
        inOwnerContext(admit, request, grant);
      }
    } catch (error) {
      refuse(decision, error, source);
      return;
    } finally {
      // Repeated preparation requests must not retain every settled decision.
      if (retaining && Atomics.load(decision, 0) !== GRANTED) {
        retainedAuthorities.delete(retaining.id);
      }
      retaining = previousRetaining;
      decisions.delete(decision);
    }
    if (Atomics.load(decision, 0) === REQUESTED) {
      refuse(
        decision,
        new SqliteWorkerError("SQLite worker admission was not granted", "closed"),
        "domain",
      );
    }
  };
  port1.on("message", receive);
  port1.unref();
  const service = () => {
    for (let queued = receiveMessageOnPort(port1); queued; queued = receiveMessageOnPort(port1)) {
      receive(queued.message);
    }
  };
  return {
    port: port2,
    bindRefusalProvenance(provenance) {
      if (closed || refusalProvenance || admissionSealed) {
        throw new SqliteWorkerError(
          "SQLite refusal provenance must bind once before dispatch",
          "closed",
        );
      }
      refusalProvenance = {
        pending: provenance.pending.bind(provenance),
        selected: provenance.selected.bind(provenance),
      };
      refusalProvenance.selected(failure);
    },
    retainCommitAuthority(assertCurrent) {
      if (
        !retaining ||
        Atomics.load(retaining.decision, 0) !== REQUESTED ||
        retainedAuthorities.has(retaining.id)
      ) {
        throw new SqliteWorkerError(
          "SQLite commit authority requires its pending native mutation request",
          "closed",
        );
      }
      const inRetainedContext = AsyncLocalStorage.snapshot();
      retainedAuthorities.set(retaining.id, () => inRetainedContext(assertCurrent));
      Atomics.store(retaining.acknowledgment, 0, 1);
    },
    assertCommitAuthority: assertRetainedAuthority,
    bindCommitAuthority(assertCurrent) {
      if (closed || commitAuthority) {
        throw new SqliteWorkerError("SQLite commit authority is already bound or closed", "closed");
      }
      commitAuthority = assertCurrent;
    },
    bindReadObserver(observer) {
      if (closed || readObserver || admissionSealed) {
        throw new SqliteWorkerError(
          "SQLite read observation must bind once before dispatch",
          "closed",
        );
      }
      readObserver = {
        pending: observer.pending.bind(observer),
        committed: observer.committed.bind(observer),
      };
    },
    deliverReadFacts(value) {
      const reader = readObserver;
      if (reader) {
        notify(() => reader.committed(parseSqliteWorkerReadFacts(value)));
      }
    },
    bindRequestAuthority(assertCurrent) {
      if (closed || requestAuthority) {
        throw new SqliteWorkerError(
          "SQLite request authority is already bound or closed",
          "closed",
        );
      }
      admissionSealed = true;
      requestAuthority = assertCurrent;
    },
    bindDatabaseAuthority(authority) {
      if (closed || databaseAuthority) {
        throw new SqliteWorkerError(
          "SQLite database authority is already bound or closed",
          "closed",
        );
      }
      admissionSealed = true;
      databaseAuthority = {
        ...authority,
        databasePath: resolveIdentityPathViaExistingAncestorSync(authority.databasePath),
      };
    },
    get failure() {
      return failure;
    },
    get cleanupFailures() {
      return cleanupFailures;
    },
    get committed() {
      // Event callbacks can precede delivery of already queued commit facts.
      service();
      return committed;
    },
    get settlement() {
      return settlement;
    },
    waitForSettlement(deadlineMs) {
      while (true) {
        service();
        if (failure !== undefined) {
          throw failure.transportError;
        }
        if (settlement?.kind === "completed") {
          return settlement;
        }
        const remaining = deadlineMs - performance.now();
        if (settlement?.kind === "unknown" || closed || remaining <= 0) {
          throw new SqliteWorkerError(
            "SQLite worker native settlement is unknown",
            "outcome-unknown",
          );
        }
        Atomics.wait(waiting, 0, 0, Math.min(5, remaining));
      }
    },
    service,
    finish() {
      closed = true;
      // Receipts remain observable; late requests can no longer obtain authority.
      service();
      for (const decision of decisions) {
        if (Atomics.load(decision, 0) === REQUESTED) {
          refuse(
            decision,
            new SqliteWorkerError("SQLite worker admission is closed", "closed"),
            "authority",
          );
        }
      }
      port1.close();
      port2.close();
      retainedAuthorities.clear();
      if (databaseAuthority?.lease) {
        try {
          databaseAuthority.lease.release();
          databaseAuthority.lease = undefined;
        } catch (error) {
          cleanupFailures.push(error);
        }
      }
    },
  };
}

export type SqliteWorkerOperationContext = {
  port: MessagePort;
  address?: { requestId: number; actorId: number };
  database?: DatabaseSync;
  nextAuthorityId?: number;
  refusal?: SqliteWorkerError;
  committed?: SqliteWorkerSourceReceipt;
  rollbackCheckpoint?: { receipt?: SqliteWorkerSourceReceipt };
  readFacts?: SqliteWorkerReadFactsCollection;
  settled?: true;
};

type WorkerAdmissionScope = {
  // Published SDK request helpers share these port/active carrier fields.
  port: MessagePort;
  owner: SqliteWorkerOperationContext;
  active: boolean;
};
// Source brokers and built plugin backends can load separate module copies in
// one Worker. Share the carrier, while each operation still owns its private port.
const currentAdmission = resolveGlobalSingleton(
  Symbol.for("openclaw.sqliteWorkerOperationAdmission"),
  () => new AsyncLocalStorage<WorkerAdmissionScope | undefined>(),
);

const transactionAuthorities = resolveGlobalSingleton(
  Symbol.for("openclaw.sqliteWorkerTransactionAuthorities"),
  () => new WeakMap<object, Set<SqliteWorkerCommitAuthorityReference>>(),
);

/** Connection facts carry no authority and preparation may intentionally have no admission. */
export function bindSqliteWorkerOperationDatabase(database: DatabaseSync): void {
  const scope = currentAdmission.getStore();
  if (!scope?.active) {
    return;
  }
  if (scope.owner.database && scope.owner.database !== database) {
    throw new Error("SQLite operation changed its bound native connection");
  }
  scope.owner.database = database;
}

/** Preparation must not inherit an enclosing callback's native admission. */
export function withoutSqliteWorkerOperationAdmission<T>(operation: () => T): T {
  return currentAdmission.run(undefined, operation);
}

/** Install only the private port belonging to the broker's currently executing operation. */
export function withSqliteWorkerOperationAdmission<T>(
  owner: SqliteWorkerOperationContext,
  operation: () => T,
): T {
  const scope = { owner, port: owner.port, active: true };
  try {
    return currentAdmission.run(scope, operation);
  } finally {
    scope.active = false;
  }
}

function requireSqliteReceiptOwner(): SqliteWorkerOperationContext {
  const scope = currentAdmission.getStore();
  if (!scope?.active) {
    throw new SqliteWorkerError("SQLite receipt requires its retained admission", "unavailable");
  }
  return scope.owner;
}

/** Capture now, publish only after the real transaction commits. */
export function deferSqliteWorkerCommitReceipt(database: DatabaseSync, facts: unknown): void {
  const owner = requireSqliteReceiptOwner();
  const captured = captureSqliteWorkerSourceFacts(facts);
  deferSqliteWorkerOwnedCommitReceipt(database, owner, () => captured);
}

/** The executing worker calls this only after its backend's native settlement check. */
export function settleSqliteWorkerOperationContext(
  owner: SqliteWorkerOperationContext,
  kind: "completed" | "unknown",
): void {
  if (owner.settled) {
    return;
  }
  owner.settled = true;
  owner.port.postMessage(
    {
      kind: "native-settlement",
      settlement: { kind, ...(owner.committed ? { committed: owner.committed } : {}) },
    },
    [],
  );
}

/** Called on the SQLite worker, after transaction entry and before its row mutation. */
export function requestSqliteWorkerOperationAdmission(
  request: SqliteWorkerAdmissionRequest,
  transferList: Transferable[] = [],
): void {
  const scope = currentAdmission.getStore();
  if (!scope?.active) {
    throw new SqliteWorkerError("SQLite operation requires its retained admission", "unavailable");
  }
  const decision = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
  const database = scope.owner.database;
  const transaction = database?.isTransaction ? getSqliteTransactionScope(database) : undefined;
  const address = scope.owner.address;
  const authorityId = (scope.owner.nextAuthorityId ?? 0) + 1;
  scope.owner.nextAuthorityId = authorityId;
  const acknowledgment =
    transaction && address
      ? new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT))
      : undefined;
  const commitAuthorities =
    request.stage === "commit" && transaction
      ? [...(transactionAuthorities.get(transaction) ?? [])]
      : [];
  scope.port.postMessage(
    {
      ...request,
      decision: decision.buffer,
      ...(acknowledgment
        ? { commitAuthority: { id: authorityId, acknowledgment: acknowledgment.buffer } }
        : {}),
      ...(commitAuthorities.length ? { commitAuthorities } : {}),
    },
    transferList,
  );
  // Host scheduling delay does not revoke the retained owner's authority. The
  // broker keeps this port through settlement and joins worker exit on failure;
  // only the live host owner can grant or refuse the pending request.
  while (Atomics.load(decision, 0) === REQUESTED) {
    Atomics.wait(decision, 0, REQUESTED);
  }
  if (Atomics.load(decision, 0) !== GRANTED) {
    const refusal = new SqliteWorkerError("SQLite transaction admission was refused", "closed");
    scope.owner.refusal = refusal;
    throw refusal;
  }
  if (acknowledgment && Atomics.load(acknowledgment, 0) === 1) {
    if (
      !database?.isTransaction ||
      !address ||
      !transaction ||
      getSqliteTransactionScope(database) !== transaction
    ) {
      throw new Error("SQLite retained authority lost its actual transaction scope");
    }
    let authorities = transactionAuthorities.get(transaction);
    if (!authorities) {
      authorities = new Set();
      transactionAuthorities.set(transaction, authorities);
    }
    const ownedAuthorities = authorities;
    const reference = { ...address, authorityId };
    const release = () => {
      ownedAuthorities.delete(reference);
      if (!ownedAuthorities.size) {
        transactionAuthorities.delete(transaction);
      }
    };
    if (
      !stageSqliteTransactionState(database, {
        stage() {
          ownedAuthorities.add(reference);
        },
        commit: release,
        rollback: release,
      })
    ) {
      release();
      throw new Error("SQLite retained authority requires its actual transaction owner");
    }
  }
}

/** Schema work borrows live host authority through the same retained job port. */
export function requestSqliteWorkerSchemaMaintenance(databasePath: string): boolean {
  if (!currentAdmission.getStore()) {
    return false;
  }
  requestSqliteWorkerOperationAdmission({
    stage: "prepare",
    facts: { kind: "schema-maintenance", databasePath },
  });
  return true;
}

/** Consume owner-prepared data from this executing operation's private port. */
export function takeSqliteWorkerOperationAdmissionAttachment(): unknown {
  const scope = currentAdmission.getStore();
  if (!scope?.active) {
    throw new SqliteWorkerError("SQLite operation requires its retained admission", "unavailable");
  }
  const message: unknown = receiveMessageOnPort(scope.port)?.message;
  if (!isRecord(message) || message.kind !== "sqlite-operation-attachment") {
    throw new SqliteWorkerError("SQLite operation attachment is unavailable", "unavailable");
  }
  return message.value;
}
