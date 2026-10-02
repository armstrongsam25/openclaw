import { afterAll, expect, test } from "vitest";
import { buildAgentRunTerminalOutcomeFromLifecycleEvent } from "../agents/agent-run-terminal-outcome.js";
import {
  loadSessionEntryReadOnly,
  replaceSessionEntry,
} from "../config/sessions/session-accessor.js";
import type { InternalSessionEntry } from "../config/sessions/types.js";
import { useSessionStoreTempDirs } from "../test-utils/session-state-cleanup.js";
import { finishAcpSourceTurn, prepareAcpSourceTurnInput } from "./acp-source-turn.js";
import { createUserTurnTranscriptRecorder } from "./user-turn-transcript.js";
import { createSqliteTranscriptTarget } from "./user-turn-transcript.test-support.js";

const sessionDirs = useSessionStoreTempDirs(afterAll, "openclaw-acp-source-turn-");

test("channel ACP admission replaces a dead native writer and starts fresh lifecycle timing", async () => {
  const target = createSqliteTranscriptTarget({ dir: sessionDirs.make() });
  const prior: InternalSessionEntry = {
    sessionId: target.sessionId,
    updatedAt: 2,
    activeWriterRunId: "previous-native-run",
    lastRunId: "previous-native-run",
    lastRunError: "previous native failure",
    status: "failed",
    startedAt: 1,
    endedAt: 2,
    runtimeMs: 1,
  };
  await replaceSessionEntry(target, prior);
  const recorder = createUserTurnTranscriptRecorder({
    target: { ...target, sessionEntry: prior },
    input: { text: "Run this request through ACP" },
  });
  const beforeAdmission = Date.now();
  // Channel ACP can be audit-only; no Gateway lifecycle event supplies this state.
  await prepareAcpSourceTurnInput(
    recorder,
    { agentId: "main", sessionKey: "agent:main:acp:target", entry: { sessionId: "acp-target" } },
    "channel-acp-run",
    () => {},
    async () => {},
  );
  const running = loadSessionEntryReadOnly(target);
  expect(running).toMatchObject({
    status: "running",
    activeWriterRunId: "channel-acp-run",
    lifecycleRunId: "channel-acp-run",
    acpSourceTurn: { sourceSessionId: target.sessionId, runId: "channel-acp-run" },
  });
  expect(running?.startedAt).toBeGreaterThanOrEqual(beforeAdmission);
  expect(running?.endedAt).toBeUndefined();
  expect(running?.runtimeMs).toBeUndefined();
  expect(running?.lastRunError).toBeUndefined();
  expect(running?.lastRunId).toBeUndefined();
  if (!running?.startedAt) {
    throw new Error("missing ACP source start");
  }
  await finishAcpSourceTurn(
    recorder,
    "channel-acp-run",
    buildAgentRunTerminalOutcomeFromLifecycleEvent({
      phase: "end",
      data: { status: "completed", startedAt: running.startedAt, endedAt: running.startedAt + 25 },
      endedAt: running.startedAt + 25,
    }),
    undefined,
  );
  const completed = loadSessionEntryReadOnly(target);
  expect(completed).toMatchObject({ status: "done", runtimeMs: 25, lastRunId: "channel-acp-run" });
  expect(completed?.acpSourceTurn).toBeUndefined();
  expect(completed?.activeWriterRunId).toBeUndefined();
});
