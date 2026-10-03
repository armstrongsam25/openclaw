import {
  closeAdmittedRunDelegatedAuthority,
  resolveAdmittedRunActiveAssertion,
  type AdmittedRunContext,
} from "../agents/admitted-run-context.js";
import {
  classifyAgentRunTerminalOutcome,
  type AgentRunTerminalOutcome,
} from "../agents/agent-run-terminal-outcome.js";
import { hasCurrentAcpSourceTurn } from "../config/sessions/acp-source-turn-state.js";
import { patchSessionEntryCore } from "../config/sessions/session-accessor.js";
import type { TranscriptTurnAdmission } from "../config/sessions/transcript-entry-anchor.js";
import { hasLiveAgentRunContext } from "../infra/agent-run-registry.js";
import { formatErrorMessage } from "../infra/errors.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { UserTurnTranscriptRecorder } from "./user-turn-transcript.types.js";

const log = createSubsystemLogger("sessions/acp");
const ownedInputCustody = new WeakSet<UserTurnTranscriptRecorder>();

export type AcpSourceTurnInputIdentity = {
  agentId: string;
  sessionKey: string;
  sessionId: string;
  lifecycleRevision: string | undefined;
};

export async function prepareAcpSourceTurnInput(
  recorder: UserTurnTranscriptRecorder | undefined,
  target: { agentId: string; sessionKey: string; entry?: { sessionId: string } },
  runId: string,
  assertCurrent: () => void,
  assertRouteCurrent: () => Promise<void>,
  expectedSource?: AcpSourceTurnInputIdentity,
  assertCustodyCurrent?: () => void,
): Promise<void> {
  if (!recorder) {
    await assertRouteCurrent();
    return;
  }
  assertCurrent();
  if (expectedSource) {
    if (!assertCustodyCurrent) {
      throw new Error("ACP source input custody requires live runtime authority.");
    }
    ownedInputCustody.add(recorder);
    if (
      !(await recorder.stageApproved?.({
        runId,
        assertCurrent,
        assertAdmittedCurrent: assertCustodyCurrent,
      }))
    ) {
      throw new Error("ACP source input custody could not be admitted before dispatch.");
    }
    assertCurrent();
  }
  const persisted = await recorder.persistApproved();
  assertCurrent();
  if (!recorder.hasPersisted()) {
    throw new Error("ACP input must be durably committed before dispatch.");
  }
  await assertRouteCurrent();
  const source = recorder.getAdmissionReceipt();
  if (source) {
    if (!persisted?.sessionEntry) {
      // A successful transcript-only recorder has no canonical execution owner to claim.
      if (persisted && !expectedSource) {
        return;
      }
      throw new Error("ACP source session identity is required before dispatch.");
    }
    if (
      expectedSource &&
      (source.agentId !== expectedSource.agentId ||
        source.sessionKey !== expectedSource.sessionKey ||
        source.sessionId !== expectedSource.sessionId ||
        persisted.sessionEntry.lifecycleRevision !== expectedSource.lifecycleRevision)
    ) {
      throw new Error("ACP source changed before input admission completed.");
    }
    await claimAcpSourceTurn({
      source,
      runId,
      expectedLifecycleRevision: expectedSource
        ? expectedSource.lifecycleRevision
        : persisted.sessionEntry.lifecycleRevision,
      targetAgentId: target.agentId,
      targetSessionKey: target.sessionKey,
      targetSessionId: target.entry?.sessionId ?? null,
      assertCurrent,
    });
    await assertRouteCurrent();
    assertCurrent();
  }
}

/** Commit before submitting input to ACP, while the source admission still owns the turn. */
async function claimAcpSourceTurn(params: {
  source: TranscriptTurnAdmission;
  runId: string;
  expectedLifecycleRevision: string | undefined;
  targetAgentId: string;
  targetSessionKey: string;
  targetSessionId: string | null;
  assertCurrent: () => void;
}): Promise<void> {
  let incumbentRunIds: string[] = [];
  const assertClaimCurrent = () => {
    params.assertCurrent();
    if (incumbentRunIds.some((runId) => hasLiveAgentRunContext(runId))) {
      throw new Error("Another live run still owns the ACP source.");
    }
  };
  const committed = await patchSessionEntryCore(
    params.source,
    (entry) => {
      incumbentRunIds = [entry.activeWriterRunId, entry.lifecycleRunId].filter(
        (runId): runId is string => runId !== undefined && runId !== params.runId,
      );
      assertClaimCurrent();
      if (
        entry.sessionId !== params.source.sessionId ||
        entry.lifecycleRevision !== params.expectedLifecycleRevision
      ) {
        throw new Error("ACP source changed before execution ownership was committed.");
      }
      return {
        acpSourceTurn: {
          sourceSessionId: entry.sessionId,
          sourceLifecycleRevision: entry.lifecycleRevision,
          runId: params.runId,
          targetAgentId: params.targetAgentId,
          targetSessionKey: params.targetSessionKey,
          targetSessionId: params.targetSessionId,
        },
        activeWriterRunId: params.runId,
        lifecycleRunId: params.runId,
        lastRunId: undefined,
        lastRunError: undefined,
        startedAt: Date.now(),
        endedAt: undefined,
        runtimeMs: undefined,
        status: "running",
        abortedLastRun: false,
      };
    },
    {
      skipMaintenance: true,
      requireWriteSuccess: true,
      workerGuard: { assertCurrent: assertClaimCurrent },
    },
  );
  if (committed?.acpSourceTurn?.runId !== params.runId) {
    throw new Error("ACP source execution ownership was not persisted.");
  }
}

/** ACP target lifecycle events cannot substitute for settlement of the admitted source. */
async function settleAcpSourceTurn(params: {
  source: TranscriptTurnAdmission;
  runId: string;
  outcome: AgentRunTerminalOutcome;
  assertCurrent?: () => void;
}): Promise<void> {
  // Restart cancellation leaves the source for the new process's interruption notice.
  if (params.outcome.reason === "cancelled" && params.outcome.stopReason === "restart") {
    return;
  }
  const status = {
    success: "done",
    failure: "failed",
    cancellation: "killed",
    timeout: "timeout",
  } as const;
  const endedAt = params.outcome.endedAt ?? Date.now();
  await patchSessionEntryCore(
    params.source,
    (entry) => {
      if (
        entry.sessionId !== params.source.sessionId ||
        !hasCurrentAcpSourceTurn(entry) ||
        entry.acpSourceTurn?.runId !== params.runId ||
        entry.activeWriterRunId !== params.runId
      ) {
        return null;
      }
      return {
        acpSourceTurn: undefined,
        activeWriterRunId: undefined,
        abortedLastRun: false,
        endedAt,
        lastRunId: params.runId,
        lastRunError: params.outcome.error,
        lifecycleRunId: undefined,
        status: status[classifyAgentRunTerminalOutcome(params.outcome)],
        runtimeMs: Math.max(0, endedAt - (entry.startedAt ?? endedAt)),
      };
    },
    {
      skipMaintenance: true,
      requireWriteSuccess: true,
      workerGuard: { assertCurrent: params.assertCurrent },
    },
  );
}

export async function finishAcpSourceTurn(
  recorder: UserTurnTranscriptRecorder | undefined,
  runId: string,
  outcome: AgentRunTerminalOutcome | undefined,
  admittedRunContext: AdmittedRunContext | undefined,
): Promise<void> {
  let source: TranscriptTurnAdmission | undefined;
  try {
    source = recorder?.getAdmissionReceipt();
    if (source && outcome) {
      const assertCurrent = admittedRunContext
        ? resolveAdmittedRunActiveAssertion(admittedRunContext)
        : undefined;
      if (admittedRunContext && !assertCurrent) {
        throw new Error("ACP source settlement admission is no longer active.");
      }
      await settleAcpSourceTurn({ source, runId, outcome, assertCurrent });
    }
  } catch (error) {
    // Cleanup cannot reopen consumed input after ACP execution or reply delivery.
    // An uncommitted source fact remains recoverable; uncertain commits are logged.
    log.warn("ACP source settlement failed after dispatch", {
      runId,
      sourceSessionKey: source?.sessionKey,
      error: formatErrorMessage(error),
    });
  } finally {
    try {
      if (recorder && ownedInputCustody.delete(recorder)) {
        recorder.finishPendingInput?.("interrupted");
      }
    } catch (error) {
      log.warn("ACP source input custody release failed", {
        runId,
        error: formatErrorMessage(error),
      });
    } finally {
      if (admittedRunContext) {
        closeAdmittedRunDelegatedAuthority(admittedRunContext);
      }
    }
  }
}
