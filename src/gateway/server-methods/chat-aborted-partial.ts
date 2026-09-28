import type { Result } from "@openclaw/normalization-core/result";
import type { ErrorShape } from "../../../packages/gateway-protocol/src/index.js";
import type { ChatAbortControllerEntry } from "../chat-abort.types.js";
import { loadSessionEntry } from "../session-utils.js";

export type ChatAbortOrigin = "rpc" | "stop-command" | "placement-abandon";

export type ChatAbortSessionSnapshot = Result<
  Pick<
    ReturnType<typeof loadSessionEntry>,
    "cfg" | "storePath" | "entry" | "canonicalKey" | "agentId"
  >,
  unknown
>;

export type AbortedPartialSnapshot = ReturnType<typeof captureAbortedPartial>;

export function withAbortedPartialPersistenceWarning(
  error: ErrorShape,
  warning: string | undefined,
): ErrorShape {
  return warning ? { ...error, message: `${error.message} ${warning}` } : error;
}

export type QueuedCollectorAbortOutcome = Result<
  { aborted: boolean; runIds: string[]; warning?: string },
  ErrorShape
>;

export function withQueuedCollectorWarning(
  outcome: QueuedCollectorAbortOutcome,
  warning: string,
): QueuedCollectorAbortOutcome {
  return outcome.ok
    ? { ok: true, value: { ...outcome.value, warning } }
    : { ok: false, error: withAbortedPartialPersistenceWarning(outcome.error, warning) };
}

/** Capture before signaling cancellation; persistence waits for producer settlement. */
export function captureAbortedPartial(params: {
  runId: string;
  sessionKey: string;
  sessionId: string;
  agentId?: string;
  text: string;
  abortOrigin: ChatAbortOrigin;
  session?: ChatAbortSessionSnapshot;
  resolveTerminalProducer?: ChatAbortControllerEntry["resolveTerminalProducer"];
}) {
  const { runId, abortOrigin } = params;
  try {
    const session = params.session ?? {
      ok: true,
      value: loadSessionEntry(
        params.sessionKey,
        params.agentId ? { agentId: params.agentId } : undefined,
      ),
    };
    if (!session.ok) {
      throw session.error;
    }
    const { cfg, storePath, entry, canonicalKey, agentId } = session.value;
    if (entry?.sessionId !== params.sessionId) {
      throw new Error("Aborted partial transcript session changed before persistence");
    }
    const producer = params.resolveTerminalProducer?.();
    const settlement = {
      deferred: false,
      producer:
        producer?.sessionId === params.sessionId && producer.sessionKey === params.sessionKey
          ? producer
          : undefined,
    };
    // Snapshot the incarnation before signaling. Reset can keep the SID, and
    // the guarded writer rechecks both facts inside its commit transaction.
    return {
      runId,
      abortOrigin,
      ok: true,
      settlement,
      value: {
        sessionKey: canonicalKey,
        sessionId: params.sessionId,
        expectedSessionId: params.sessionId,
        expectedLifecycleRevision: entry.lifecycleRevision ?? null,
        agentId,
        storePath,
        cfg,
        message: params.text,
        createIfMissing: true,
        idempotencyKey: `${runId}:assistant`,
        abortMeta: { aborted: true, origin: abortOrigin, runId },
      },
    } as const;
  } catch (error) {
    // Preparation is fallible metadata I/O, never a prerequisite for cancellation.
    return { runId, abortOrigin, ok: false, error } as const;
  }
}
