import type {
  ArtifactSummary,
  ArtifactsListParams,
} from "../../../packages/gateway-protocol/src/schema/artifacts.js";
import type { TranscriptDisplayPosition } from "../../chat/transcript-display-position.js";
import type {
  ArtifactDownloadResponse,
  ArtifactDownloadResponseRequest,
  PreparedArtifactDownload,
} from "../../gateway/artifact-download-projection.js";
import type { ArtifactRecord } from "../../gateway/server-methods/artifacts-content.js";
import type { AgentHistoryActivity } from "../../infra/agent-activity-events.js";
import type {
  TranscriptAnchorPageOptions,
  TranscriptRecentReadLimits,
} from "../../sessions/transcript-anchor-page.js";
import type {
  TranscriptReadWindow,
  TranscriptReadWindowOptions,
} from "../../sessions/transcript-read-window.js";
import type { ConversationRecord } from "./session-accessor.sqlite-conversation-read.js";
import type {
  SessionTranscriptRawDeltaLimits,
  SessionTranscriptRawDeltaResult,
  SessionTranscriptReadScope,
  TranscriptEvent,
} from "./session-accessor.types.js";
import type { StoredMessageReactionSummary } from "./session-reaction-store.types.js";
import type { SessionTranscriptWorkerReadError } from "./session-transcript-worker-error.types.js";
import type { InternalSessionEntry, SessionEntry } from "./types.js";

export type ReadRecentSessionMessagesOptions = {
  maxMessages: number;
  maxBytes?: number;
  maxLines?: number;
};

export type ReadSessionMessagesAsyncOptions =
  | { mode: "full"; reason: string }
  | ({ mode: "recent" } & ReadRecentSessionMessagesOptions);

export type SessionTranscriptReadOptions = {
  allowResetArchiveFallback?: boolean;
  readOnly?: boolean;
};

export type SessionTranscriptPageOptions = TranscriptReadWindowOptions &
  SessionTranscriptReadOptions & {
    offset: number;
    maxMessages: number;
    beforeSeq?: number;
    recentAtHead?: TranscriptRecentReadLimits;
    maxBytes?: number;
    allowOversizedFirst?: boolean;
  };

export type ReadRecentSessionMessagesResult = {
  olderOffset?: number;
  omittedOversized?: boolean;
  activeLeafEntryId?: string | null;
  deltaCursor?: string;
  displaySource?: string;
  readWindow?: TranscriptReadWindow;
  windowReset?: boolean;
  messages: unknown[];
  transcriptEvents?: TranscriptEvent[];
  transcriptPath?: string;
  transcriptSource?: "active" | "reset-archive";
  totalMessages: number;
};

export type ReadSessionMessagesResult = {
  messages: unknown[];
  transcriptPath?: string;
};

export type ReadSessionMessageByIdResult = {
  message?: unknown;
  seq?: number;
  oversized: boolean;
  found: boolean;
  serializedBytes?: number;
};

export type ReadSessionMessagesAroundIdResult = ReadRecentSessionMessagesResult & {
  found: boolean;
  hasOverreadContext: boolean;
  offset: number;
};

export type SessionTranscriptMessageByIdOptions =
  | { currentOnly?: false; maxBytes?: never }
  | { currentOnly: true; maxBytes: number };

type SessionTranscriptRawDeltaPage = Extract<SessionTranscriptRawDeltaResult, { kind: "page" }>;

export type SessionTranscriptDisplayDeltaResult =
  | (Omit<SessionTranscriptRawDeltaPage, "events"> & {
      activeLeafEntryId: string | null;
      events: Array<
        SessionTranscriptRawDeltaPage["events"][number] & {
          messageSeq?: number;
          displayPosition?: TranscriptDisplayPosition;
        }
      >;
    })
  | Exclude<SessionTranscriptRawDeltaResult, { kind: "page" }>;

export type SessionArtifactReadQuery = Pick<ArtifactsListParams, "runId" | "messageRole"> &
  (
    | {
        kind: "list";
        sessionKey: string;
        includeDownloadData?: boolean;
        downloadArtifactIds?: string[];
      }
    | {
        kind: "image-page";
        sessionKey: string;
        limit: number;
        beforeSeq?: number;
        imageOffset?: number;
        readWindow?: TranscriptReadWindow;
      }
    | {
        kind: "image";
        sessionKey: string;
        artifactId: string;
        includeData: boolean;
      }
    | { kind: "download-grant"; sessionKey: string; artifactId: string }
    | {
        kind: "download-response";
        sessionKey: string;
        artifactId: string;
        response: ArtifactDownloadResponseRequest;
      }
  );

export type SessionArtifactReadResult =
  | { kind: "list"; artifacts: ArtifactRecord[] }
  | {
      kind: "image-page";
      artifacts: ArtifactSummary[];
      next?: { beforeSeq: number; imageOffset: number; readWindow: TranscriptReadWindow };
      omittedOversized?: boolean;
    }
  | { kind: "image"; artifact?: ArtifactRecord }
  | {
      kind: "download-grant";
      selection?:
        | { kind: "prepared"; download: PreparedArtifactDownload }
        | { kind: "raw"; artifact: ArtifactRecord };
    }
  | { kind: "download-response"; response?: ArtifactDownloadResponse };

export type ChatHistoryResponsePage<Messages extends unknown[] | Uint8Array = unknown[]> = {
  messages: Messages;
  activity?: AgentHistoryActivity[];
  messagesBytes: number;
  responseHistoryBytes: number;
  omission?: { omittedCount: number; normalizedBytes: number };
  nextOffset?: number;
  hasMore?: boolean;
  totalMessages?: number;
  completeSnapshot?: true;
};

export type ChatHistoryPage = {
  encodedResponse?: ChatHistoryResponsePage<Uint8Array>;
  windowReset?: boolean;
  activeLeafEntryId?: string | null;
  deltaCursor?: string;
  messages: unknown[];
  activity?: AgentHistoryActivity[];
  responseOffset?: number;
  completeCliImport?: true;
  // Absent only for anchored (messageId) reads: the anchor may resolve a
  // reset-archive transcript that numeric offset cursors cannot address, so
  // anchored responses expose no paging metadata.
  pagination?: {
    offset: number;
    totalMessages: number;
    rawPageMessages: number;
    exhausted?: true;
  };
};

export type ChatHistoryPageParams = {
  encodeResponse?: boolean;
  entry: InternalSessionEntry | undefined;
  provider: string | undefined;
  sessionId: string | undefined;
  storePath: string | undefined;
  sessionAgentId: string;
  canonicalKey: string;
  max: number;
  maxHistoryBytes: number;
  effectiveMaxChars: number;
  offset: number | undefined;
  messageId: string | undefined;
  ignoreCliSessionImports?: boolean;
};

type SessionHistoryTranscriptMeta = {
  idempotencyKey?: string;
  seq?: number;
  turnBoundary?: boolean;
};

export type SessionHistoryMessage = Record<string, unknown> & {
  __openclaw?: SessionHistoryTranscriptMeta;
};

export type PaginatedSessionHistory = {
  windowReset?: boolean;
  items: SessionHistoryMessage[];
  messages: SessionHistoryMessage[];
  nextCursor?: string;
  hasMore: boolean;
};

export type SessionHistorySnapshot = {
  history: PaginatedSessionHistory;
  rawTranscriptSeq: number;
  turnBoundaryPending: boolean;
  assistantErrorPending: boolean;
  transcriptPath?: string;
};

export type SessionHistoryTranscriptTarget = Pick<
  SessionTranscriptReadScope,
  "agentId" | "env" | "sessionId" | "storePath"
> & {
  sessionEntry?: SessionEntry;
  sessionKey: string;
};

export type SessionHistoryReadParams = {
  target: SessionHistoryTranscriptTarget;
  maxChars?: number;
  limit?: number;
  cursor?: string;
};

export type SessionHistorySubagentLookup =
  | { kind: "session"; sessionKey: string }
  | { kind: "run"; runId: string; messageSeq: number | undefined };

export type SessionHistorySubagentFacts = {
  sessions: Array<[sessionKey: string, hidden: boolean]>;
  runMessages: Array<[runId: string, messageSeq: number | undefined, hidden: boolean]>;
  failure?: { lookup: SessionHistorySubagentLookup; error: SessionTranscriptWorkerReadError };
};

export type SessionHistoryDelta = {
  delta: SessionTranscriptDisplayDeltaResult;
  subagentCoordination: SessionHistorySubagentFacts;
};

export type SessionHistoryTranscriptBinding = { sessionKey: string; sessionId: string };

export type SessionConversationBinding = Pick<
  ConversationRecord,
  "channel" | "accountId" | "target" | "threadId" | "nativeChannelId"
>;

export type SessionHistoryWorkerRequest =
  | { kind: "reactions"; params: { target: SessionTranscriptReadScope } }
  | {
      kind: "conversation-binding";
      params: { target: SessionTranscriptReadScope; conversationRef: string };
    }
  | {
      kind: "artifacts";
      params: { target: SessionTranscriptReadScope; query: SessionArtifactReadQuery };
    }
  | {
      kind: "message-page";
      params: {
        target: SessionTranscriptReadScope;
        options: SessionTranscriptPageOptions;
      };
    }
  | {
      kind: "around-id";
      params: {
        target: SessionTranscriptReadScope;
        options: TranscriptAnchorPageOptions & SessionTranscriptReadOptions;
      };
    }
  | {
      kind: "source-messages";
      params: {
        target: SessionTranscriptReadScope;
        options: ReadSessionMessagesAsyncOptions & SessionTranscriptReadOptions;
      };
    }
  | {
      kind: "recent-page";
      params: {
        target: SessionTranscriptReadScope;
        exactArchivePath?: string;
        options: ReadRecentSessionMessagesOptions &
          TranscriptReadWindowOptions &
          SessionTranscriptReadOptions;
      };
    }
  | {
      kind: "transcript-binding";
      params: { target: SessionTranscriptReadScope };
    }
  | { kind: "rpc"; params: ChatHistoryPageParams & { sessionId: string; storePath: string } }
  | { kind: "message-lookup"; params: { target: SessionTranscriptReadScope; messageId: string } }
  | {
      kind: "message-by-id";
      params: {
        target: SessionTranscriptReadScope;
        messageId: string;
        options?: SessionTranscriptMessageByIdOptions & { allowResetArchiveFallback?: boolean };
      };
    }
  | { kind: "message-count"; params: { target: SessionTranscriptReadScope } }
  | {
      kind: "recent";
      params: {
        target: SessionTranscriptReadScope;
        maxMessages: number;
        maxLines: number;
        allowResetArchiveFallback?: boolean;
      };
    }
  | {
      kind: "delta";
      params: { target: SessionTranscriptReadScope; limits: SessionTranscriptRawDeltaLimits };
    }
  | { kind: "http"; params: SessionHistoryReadParams };

export type SessionHistoryWorkerResult =
  | { kind: "reactions"; result: Record<string, StoredMessageReactionSummary[]> }
  | { kind: "conversation-binding"; result: SessionConversationBinding | null }
  | { kind: "artifacts"; result: SessionArtifactReadResult }
  | { kind: "message-page" | "recent-page"; result: ReadRecentSessionMessagesResult }
  | { kind: "around-id"; result: ReadSessionMessagesAroundIdResult }
  | { kind: "source-messages"; result: ReadSessionMessagesResult }
  | { kind: "transcript-binding"; binding: SessionHistoryTranscriptBinding | undefined }
  | { kind: "rpc"; page: ChatHistoryPage }
  | { kind: "message-lookup"; messages: unknown[] }
  | { kind: "message-by-id"; result: ReadSessionMessageByIdResult }
  | { kind: "message-count"; count: number }
  | { kind: "recent"; messages: unknown[] }
  | ({ kind: "delta" } & SessionHistoryDelta)
  | { kind: "http"; snapshot: SessionHistorySnapshot };
