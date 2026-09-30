import type { MessagePort } from "node:worker_threads";
import type { OpenClawDatabaseMaintenanceScope } from "../state/openclaw-state-db-async-lifecycle.js";
import type { RuntimeWorkerGeneration } from "./runtime-worker-generation.js";
import type {
  SqliteWorkerRequest,
  SqliteWorkerReply,
  SqliteWorkerCloseReceipt,
} from "./sqlite-worker-contract.js";
import type { DatabasePathIdentity } from "./sqlite-worker-identity.js";
import type {
  SqliteWorkerAdmissionFactory,
  SqliteWorkerOperationAdmission,
} from "./sqlite-worker-operation-admission.js";
import type {
  SqliteWorkerOperationSettlement,
  SqliteWorkerRetainedReceipt,
  SqliteWorkerRefusalReceipt,
  SqliteWorkerSourceReceipt,
  SqliteWorkerCallbackAdmission,
  SqliteWorkerRetainedCallbackAdmission,
  SqliteWorkerCallbackDeliveryFailure,
} from "./sqlite-worker-operation-settlement.js";
import type { SqliteWorkerStateContext } from "./sqlite-worker-state-context.js";
import type {
  createSqliteWorkerTransferOwner,
  createSqliteWorkerTransferReceiver,
} from "./sqlite-worker-transfer.js";
import type { RetainedNativeWorkerSource } from "./worker-native-lifecycle.js";
import type { RetainedNativeWorker } from "./worker-native-lifecycle.types.js";
export type RequestBody = SqliteWorkerRequest extends infer Request
  ? Request extends SqliteWorkerRequest
    ? Omit<Request, "id">
    : never
  : never;
type DispatchState = { dispatched: boolean; openNotEntered?: boolean };
export type Job = {
  executionWorker: RetainedNativeWorker;
  nativeParent?: Job;
  executionFailure?: Error;
  rollbackSource?: SqliteWorkerSourceReceipt;
  refusal?: {
    pending?: SqliteWorkerRefusalReceipt;
    nativeConfirmed?: true;
    admissionFailure?: NonNullable<SqliteWorkerOperationAdmission["failure"]>;
  };
  nativeSettlement?: Promise<SqliteWorkerOperationSettlement>;
  finalReceipt?: SqliteWorkerRetainedReceipt;
  /** Explicitly registered by this Job's admission factory before dispatch. */
  consumerCompletion?: Promise<unknown>;
  consumerSettlement?: Promise<SqliteWorkerOperationSettlement>;
  runCallback?: SqliteWorkerCallbackAdmission;
  runRetainedCallback?: SqliteWorkerRetainedCallbackAdmission;
  readCallbackDeliveryFailure?: () => SqliteWorkerCallbackDeliveryFailure | undefined;
  callbackParent?: Job;
  callbackScope?: SqliteWorkerCallbackScope;
  callbackPredecessor?: SqliteWorkerCallbackDependency;
  /** The paused ancestor in the logical admission slot. */
  parent?: Job;
  callbackContext?: object;
  provisionalOutcome?: Exclude<SqliteWorkerRetainedOutcome<unknown>, { status: "pending" }>;
  completed?: true;
  completedError?: { error: unknown };
  transportReleased?: true;
  provisionalOwner?: Job;
  deferredReply?: { reply: SqliteWorkerReply; executionWorker: RetainedNativeWorker };
  callback?: SqliteWorkerCallbackScope;
  provisionalChildren?: Map<number, Job>;
  returned?: (
    outcome: Exclude<SqliteWorkerRetainedOutcome<unknown>, { status: "pending" }>,
    receipt: SqliteWorkerProvisionalReceipt,
  ) => void;
  settled?: (
    outcome: SqliteWorkerRetainedOutcome<unknown>,
    receipt: SqliteWorkerRetainedReceipt,
  ) => void;
  signal?: AbortSignal;
  maintenanceScope?: OpenClawDatabaseMaintenanceScope;
  createAdmission?: SqliteWorkerAdmissionFactory;
  operationAdmission?: { admission: SqliteWorkerOperationAdmission; releaseService(): void };
  settleNative?: (settlement: SqliteWorkerOperationSettlement) => void;
  nativeDispatched?: boolean;
  requestPosted?: boolean;
  assertCurrent?: () => void;
  inputTransfer?: {
    id: number;
    producer: ReturnType<typeof createSqliteWorkerTransferOwner>;
  };
  transfer?: {
    id: number;
    receiver: ReturnType<typeof createSqliteWorkerTransferReceiver>;
    value: unknown;
  };
  dispatchState?: DispatchState;
  request: SqliteWorkerRequest;
  bytes: number;
  resolve(value: unknown): void;
  reject(error: unknown): void;
  detach(): void;
};
export type SqliteWorkerCallbackScope = {
  transaction: boolean;
  port: MessagePort;
  slot: Slot;
  accepting: boolean;
  service(): void;
  cancel(): void;
  servicing?: true;
  pending: Map<Job, Slot>;
  advancing?: true;
  observeReturn?: () => void;
  deliveryFailure?: SqliteWorkerCallbackDeliveryFailure;
  refusal?: { error: unknown };
  lastChild?: SqliteWorkerCallbackDependency;
  releaseReturn?: () => void;
  completion?: { accepted: false } | { accepted: true; value: Uint8Array<ArrayBuffer> };
};
export type SqliteWorkerCallbackDependency = { transportReleased?: true };
export type SqliteWorkerCallbackContext = {
  job: Job;
  scope: SqliteWorkerCallbackScope;
};
export type SqliteWorkerExecution = {
  kind: "file";
  worker: RetainedNativeWorker;
  replyPort: MessagePort;
  serviceReplies(): void;
  receiveReply(reply: SqliteWorkerReply): void;
  serviceFailureSettlement?: () => void;
  failed?: Error;
  retiringRetained?: SqliteWorkerRetainedResult<void>;
  exit: Promise<void>;
  exited: boolean;
  recordJoinedExit(code?: number): void;
};

export type Slot = {
  runtimeGeneration?: RuntimeWorkerGeneration;
  borrowedGenerationSlot?: true;
  executions: Set<SqliteWorkerExecution>;
  serviceReplies(): void;
  serviceFailureSettlement?: () => void;
  actors: Set<Actor>;
  queue: Job[];
  current?: Job;
  failed?: Error;
  retiring?: Promise<void>;
  retiringRetained?: SqliteWorkerRetainedResult<void>;
  exit: Promise<void>;
  exited: boolean;
  recordJoinedExit(): void;
  pendingOpens: number;
  placements: Set<SqliteWorkerPlacement>;
};
export type Actor = {
  executionWorker: RetainedNativeWorker;
  runtimeGeneration?: RuntimeWorkerGeneration;
  nativeStopped: Promise<void>;
  nativeStoppedRecorded: boolean;
  markNativeStopped(): void;
  closeReceipt?: SqliteWorkerCloseReceipt;
  stateDatabasePath?: string;
  id: number;
  key: string;
  placement?: SqliteWorkerPlacement;
  pathReferences: Map<string, number>;
  moduleUrl: string;
  inputHash: string;
  slot: Slot;
  references: number;
  opened: Promise<unknown>;
  openDispatch: DispatchState;
  initialized: boolean;
  openingError?: { error: unknown };
  backendClosed: boolean;
  cleanupState?: "pending" | "complete";
  closing?: Promise<void>;
  closingRetained?: SqliteWorkerRetainedResult<void>;
  retirementRequested?: boolean;
  settlement?: Promise<void>;
  retirement?: Promise<void>;
  retirementRetained?: SqliteWorkerRetainedResult<void>;
  onReferencesDrained?: () => void;
  stateContext?: SqliteWorkerStateContext;
} & { kind: "file"; databasePath: string; physicalJoined?: true };
export type OperationScope = {
  returned?: Job["returned"];
  maintenanceScope?: OpenClawDatabaseMaintenanceScope;
  createAdmission?: SqliteWorkerAdmissionFactory;
  assertCurrent?: (commandType: PropertyKey) => void;
  active: boolean;
  pending: Set<Promise<unknown>>;
  stateContext?: SqliteWorkerStateContext;
};
export type EnqueueOptions = {
  parent?: Job | null;
  returned?: Job["returned"];
  settled?: Job["settled"];
  maintenanceScope?: OpenClawDatabaseMaintenanceScope;
  createAdmission?: SqliteWorkerAdmissionFactory;
  signal?: AbortSignal;
  dispatchState?: DispatchState;
  scope?: OperationScope;
  assertCurrent?: () => void;
};
export type StoreClient = {
  actor: Actor;
  close(): Promise<void>;
  closeRetained(): SqliteWorkerRetainedResult<void>;
  sealed: boolean;
  isAvailable(): boolean;
  scopes: Set<Promise<void>>;
  execute(
    command: { type: PropertyKey; input: unknown },
    options: { signal?: AbortSignal },
    scope?: OperationScope,
    settled?: Job["settled"],
  ): Promise<unknown>;
};

export type SqliteWorkerStoreOptions = {
  runtimeGeneration?: RuntimeWorkerGeneration;
  moduleUrl: URL;
  databasePath: string;
  input: unknown;
  existingOnly?: boolean;
  admission?: { identity: string; assertCurrent(): void };
};

export type PreparedSqliteWorkerOpen = {
  nativeWorkerSource: RetainedNativeWorkerSource;
  signal?: AbortSignal;
  preparation?: Buffer;
  runtimeGeneration?: RuntimeWorkerGeneration;
  carrierUrl: URL;
  expectedIdentity?: string;
  createOpenAdmission?: SqliteWorkerAdmissionFactory;
  maintenanceScope?: OpenClawDatabaseMaintenanceScope;
  retainCleanup?: (cleanup: SqliteWorkerAdmissionCleanup) => void;
  onNativeStopped?: (
    stopped: Promise<void>,
    readCloseReceipt: () => SqliteWorkerCloseReceipt | undefined,
    readNativeStopped: () => boolean,
  ) => void;
  stateDatabasePath?: string;
  createAdmission?: SqliteWorkerAdmissionFactory;
  assertCurrent?: () => void;
  moduleUrl: URL;
  databasePath: string;
  input: Buffer;
  existingOnly: boolean;
  stateContext?: SqliteWorkerStateContext;
};

export type SqliteWorkerSlotOptions = Pick<
  PreparedSqliteWorkerOpen,
  "carrierUrl" | "runtimeGeneration" | "assertCurrent" | "nativeWorkerSource"
> & { placement?: SqliteWorkerPlacement };

/** Bounded co-location facts only; native source admission remains with its existing owner. */
export type SqliteWorkerPlacement = {
  kind: "file";
  readonly requestedPath: string;
  readonly canonicalPath: string;
  identity: DatabasePathIdentity;
};
export type SqliteWorkerSlotReservation = {
  slot: Slot;
  executionWorker: RetainedNativeWorker;
  placement?: SqliteWorkerPlacement;
};

export type SqliteWorkerRetainedOutcome<T> =
  | { status: "pending" }
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; error: unknown };
export type SqliteWorkerProvisionalReceipt = {
  kind: "returned" | "rolled-back";
  rollbackSource?: SqliteWorkerSourceReceipt;
  refusal?: SqliteWorkerRefusalReceipt;
};
export type SqliteWorkerRetainedResult<T> = {
  result: Promise<T>;
  read(): SqliteWorkerRetainedOutcome<T>;
  service(): void;
};

/** Exact failed-admission custody; pathname cleanup can include unrelated actors. */
export type SqliteWorkerAdmissionCleanup = {
  readonly pending: boolean;
  closeRetained(): SqliteWorkerRetainedResult<void>;
  close(): Promise<void>;
};

export type SqliteWorkerOpenCustody = Pick<
  PreparedSqliteWorkerOpen,
  | "maintenanceScope"
  | "retainCleanup"
  | "createAdmission"
  | "stateDatabasePath"
  | "onNativeStopped"
  | "signal"
> & { preparation?: unknown };
export type SqliteWorkerInputRetention = "snapshot" | "stream";
export type SqliteWorkerInputPreparation = {
  assertCurrent: () => void;
  /** Transfer to a dispatch that reaches enqueue synchronously before returning its handle. */
  handoff<T>(dispatch: () => T): T;
  release(): void;
};
