import type { SqliteWalHealth } from "../../infra/sqlite-wal-checkpoint.js";
import type { SessionTranscriptWatermark } from "./session-accessor.sqlite-transcript-watermark-read.js";
import type {
  SessionBranchSummary,
  SessionEntrySummary,
  TranscriptEvent,
} from "./session-accessor.types.js";
import type { InternalSessionEntry as SessionEntry } from "./types.js";
export type {
  DeletedAgentSessionEntryPurgeParams,
  DeleteSessionEntryLifecycleParams,
  DeleteSessionEntryLifecycleResult,
  ResetSessionEntryLifecycleParams,
  ResetSessionEntryLifecycleResult,
  SessionEntryLifecycleMutationResult,
  SessionEntryLifecycleRemoval,
  SessionEntryLifecycleUpsert,
  SessionLifecycleArchivedTranscript,
  SessionLifecycleArtifactCleanupParams,
  SessionLifecycleArtifactCleanupResult,
} from "./session-accessor.lifecycle-types.js";

export type SessionEntryStatus = NonNullable<SessionEntry["status"]>;

export type SessionTranscriptContextVersion = {
  generation: string | null;
  rawSeq: number | null;
  updatedAt: number | null;
};

export type ResolvedSqliteScope = {
  agentId: string;
  databaseAgentId?: string;
  env?: NodeJS.ProcessEnv;
  ownerStorePath?: string;
  path?: string;
  sessionKey: string;
};

export type ResolvedSqliteReadScope = Omit<ResolvedSqliteScope, "sessionKey"> & {
  sessionKey?: string;
};

export type ResolvedTranscriptScope = ResolvedSqliteScope & {
  sessionId: string;
};

export type ResolvedTranscriptReadScope = ResolvedSqliteReadScope & {
  sessionId: string;
};

export type SessionModelContextLimits = {
  maxBytes: number;
  maxEvents: number;
  /** Detached model views may omit result bodies; evidence and fork readers remain strict. */
  toolResultOverflow?: "omit";
};

export type SessionTranscriptModelContext = {
  events: TranscriptEvent[];
  version?: SessionTranscriptContextVersion;
};

export type SessionTranscriptReadSnapshot = {
  events: TranscriptEvent[];
  version: SessionTranscriptContextVersion;
};

export type SessionPendingInputReceipt =
  | { runId: string; state: "pending"; cancelled?: true }
  | { runId: string; state: "consumed"; consumedByEventId: string };

export type SessionIdentityEvidenceIdentity = {
  sessionId: string;
  sessionKey?: string;
};

export type SessionIdentityEvidenceResult =
  | { status: "current"; sessionKey: string }
  | { status: "absent" }
  | {
      status: "unknown";
      reason: "ambiguous" | "read-failed" | "row-invalid" | "schema-missing";
    };

export type SessionTranscriptBoundedActiveContext = {
  activeLeafEntryId: string | null;
  version: SessionTranscriptContextVersion;
  opaqueParents: Map<string, string | null>;
  parents: Map<string, string | null>;
  firstKeptRanges: Map<string, { startIndex: number; endIndex: number }>;
  persistedSuffixStartSeq: number;
  boundaryCount: number;
  events: TranscriptEvent[];
  serializedBytes: number;
  totalEvents: number;
  transcriptMutationAt: number | null;
  truncated: boolean;
};

export type SessionBranchSummaryReadRequest = {
  database: { agentId: string; path: string };
  databaseIdentity: string;
  sessionKey: string;
  sessionId: string;
  lifecycleRevision?: string;
};
export type SessionBranchSummaryReadResult =
  | ({ status: "ok"; branches: SessionBranchSummary[] } & SessionTranscriptWatermark)
  | { status: "missing-session" | "failed" };

/** SQLite database target resolved from a legacy session store path. */
export type ResolvedSqliteStoreTarget = {
  agentId?: string;
  ownerSource?:
    | "database-registry"
    | "database-path"
    | "registered-suffixed"
    | "occupied-unsuffixed"
    | "configured-default"
    | "ambiguous-registry";
  path: string;
  shared?: boolean;
  unsuffixedOwnerAgentId?: string;
};

export type CanonicalSessionValidationResult = {
  validatedRows: number;
  certifiedRows: number;
  hasMore: boolean;
  oversizedRows: number;
};

/** Worker operation facts; no Worker object or plan payload is retained. */
export type SqliteSessionReclamationDiagnostics = {
  kind?:
    | "archive-publish-prepare"
    | "archive-publish-record"
    | "deletion-plan"
    | "entry"
    | "lifecycle-artifacts"
    | "lifecycle-projection-plan"
    | "lifecycle-projection-commit"
    | "lifecycle-projection-count"
    | "history-eviction"
    | "historical-generation"
    | "maintenance-plan"
    | "maintenance-finalize"
    | "maintenance-statistics"
    | "maintenance-pages"
    | "cold-batch"
    | "cold-maintain"
    | "cold-restore";
  workerThreadId?: number;
};

/** One validated request owns this record until its observed release event. */
export type SqliteSessionReclamationAdmissionDiagnostics = {
  admissionId: number;
  releaseCause?: "worker-release" | "worker-exit";
};

export type SqliteSessionDatabaseAdmissionDiagnostics = {
  admissionMode?: "cached" | "async";
  admissionMs?: number;
};

/** One cleanup attempt owns these numeric observations; no row or transcript is retained. */
export type SqliteSessionArtifactPreparationDiagnostics =
  SqliteSessionDatabaseAdmissionDiagnostics & {
    nodeInventoryMs?: number;
    referencePlanningMs?: number;
    orphanPlanningMs?: number;
    markerScanMs?: number;
    nodeRows?: number;
    windowRows?: number;
    referenceIds?: number;
    selectedEntries?: number;
    markerWindows?: number;
    markerRows?: number;
    deletePlans?: number;
    completed?: boolean;
  };

/** One pruning attempt retains only aggregate stage observations. */
export type SqliteSessionArchivePruningDiagnostics = {
  trigger: "initial" | "after-eviction" | "final";
  checkpointCalls?: number;
  checkpointIncomplete?: number;
  checkpoint?: SqliteWalHealth;
  totalBytesBefore?: number;
  totalBytesAfter?: number;
  walBytesBefore?: number;
  walBytesAfter?: number;
  checkpointMs?: number;
  checkpointMaxMs?: number;
  vacuumMs?: number;
  vacuumPasses?: number;
  vacuumPagesRequested?: number;
  queryMs?: number;
  rowDeletionMs?: number;
  fileRemovalMs?: number;
  removedFiles?: number;
  missingFiles?: number;
  failedRemovals?: number;
  measurementMs?: number;
  measurements?: number;
  legacyInventoryMs?: number;
  completed?: boolean;
};

export type SqliteSessionWriteDiagnostics = SqliteSessionReclamationDiagnostics & {
  artifactPreparation?: SqliteSessionArtifactPreparationDiagnostics;
  reclamationAdmission?: SqliteSessionReclamationAdmissionDiagnostics;
};

export type SessionTranscriptInstance = SessionEntrySummary & {
  agentId: string;
  /** Stable transcript identity, including rotated history for one logical session key. */
  sessionId: string;
  /** True when this transcript instance was owned by an ACP runtime. */
  acpOwned: boolean;
  /** True when exclusion-sensitive session ownership was captured for this transcript id. */
  provenanceKnown: boolean;
  /** Activity timestamp for this transcript instance, not the current logical session row. */
  updatedAtMs: number;
  /** Recorded source facts; coarse historical trust classes cannot identify an exact hook source. */
  sourceMetadata: {
    createdAt: number;
    channel: string | null;
    accountId: string | null;
    chatType: NonNullable<SessionEntry["chatType"]> | null;
    hookExternalContentSource: NonNullable<SessionEntry["hookExternalContentSource"]> | null;
  };
};

export type SessionTranscriptInstanceListOptions = {
  /** Include empty and internal windows when inspecting recorded source metadata. */
  includeAllWindows?: boolean;
  sessionId?: string;
};

export type TranscriptEventAppendOptions = {
  appendIntent?: "active-branch";
  /** Synchronous authority check run inside the append transaction. */
  beforeCommitInTransaction?: () => void;
  /** Reject the append when the transcript changed since the caller loaded it. */
  expectedMutationAt?: number | null;
};

export type TranscriptAppendRefusal =
  | {
      actualSessionIdHash: string;
      agentIdHash: string;
      code: "session-rebound";
      expectedSessionIdHash: string;
      sessionKeyHash: string;
    }
  | {
      agentIdHash: string;
      code: "session-entry-missing";
      expectedSessionIdHash: string;
      sessionKeyHash: string;
    };

export type {
  ForkSessionEntryFromParentTargetParams,
  ForkSessionEntryFromParentTargetResult,
  ForkSessionFromParentTranscriptParams,
  ForkSessionFromParentTranscriptResult,
  SessionParentForkDecision,
  SessionTranscriptRawDeltaLimits,
  SessionTranscriptRawDeltaResult,
  SessionTranscriptVisibleMessageDeltaLimits,
  SessionTranscriptVisibleMessageDeltaResult,
  TranscriptEvent,
} from "./session-accessor.types.js";

export type LatestTranscriptAssistantMessage = {
  id?: string;
  message: unknown;
};

type SessionEntryBatchProjectionMutation = {
  entry: SessionEntry;
  previousSessionKeys?: readonly string[];
  sessionKey: string;
};

export type SessionEntryBatchProjectionUpdate<T> = {
  mutations?: Iterable<SessionEntryBatchProjectionMutation>;
  result: T;
};

export type {
  ExactSessionEntry,
  LatestTranscriptAssistantText,
  SessionAccessScope,
  SessionEntryPatchContext,
  SessionEntryPatchOptions,
  SessionEntryReplacementSnapshot,
  SessionEntryReplacementUpdate,
  SessionEntrySummary,
  SessionEntryTargetPatchScope,
  SessionTranscriptAccessScope,
  SessionTranscriptEventRow,
  SessionTranscriptReadScope,
  SessionTranscriptStats,
  SessionTranscriptTurnMessageAppend,
  SessionTranscriptTurnWriteContext,
  SessionTranscriptWriteScope,
  TranscriptMessageAppendOptions,
  TranscriptMessageAppendResult,
  TranscriptUpdatePayload,
} from "./session-accessor.types.js";
