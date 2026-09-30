import { afterEach, beforeEach, expect, it, onTestFinished, vi } from "vitest";
import type { BoardWidgetPutResult } from "../../packages/gateway-protocol/src/index.js";
import { useAutoCleanupTempDirTracker } from "../../test/helpers/temp-dir.js";
import { replaceSessionEntrySync } from "../config/sessions/session-accessor.entry.js";
import { createRetainedOperation } from "../infra/retained-operation.js";
import { receiveSqliteWorkerReply } from "../infra/sqlite-worker-broker-reply.js";
import {
  settleFailedSqliteWorkerJobs,
  settleSqliteWorkerJob,
  withSqliteWorkerCleanupFailure,
} from "../infra/sqlite-worker-broker-settlement.js";
import type { Job } from "../infra/sqlite-worker-broker.types.js";
import {
  isSqliteWorkerError,
  SqliteWorkerError,
  type SqliteWorkerStore,
} from "../infra/sqlite-worker-contract.js";
import { sessionChanges, type SessionRowChange } from "../sessions/session-row-changes.js";
import { createDeferredCore } from "../shared/deferred.js";
import {
  closeOpenClawAgentDatabasesAsync,
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../state/openclaw-agent-db.js";
import {
  closeOpenClawStateDatabaseAsync,
  closeOpenClawStateDatabaseForTest,
} from "../state/openclaw-state-db.js";
import { BoardValidationError } from "./board-layout.js";
import type { BoardWriteOperations, BoardWriteOutcome } from "./sqlite-board-operations.js";
import { SqliteBoardStore } from "./sqlite-board-store.js";

const boundary = vi.hoisted(() => ({
  execute: vi.fn(
    async (
      _command: Parameters<SqliteWorkerStore<BoardWriteOperations>["execute"]>[0],
    ): Promise<BoardWriteOutcome<BoardWidgetPutResult>> => {
      throw new Error("Expected the test to supply a worker outcome");
    },
  ),
  close: vi.fn(async () => {}),
}));

vi.mock("../state/openclaw-agent-worker-store.js", () => ({
  openOpenClawAgentSqliteWorkerStore: async () => ({
    async run<T>(
      operation: (scope: Pick<SqliteWorkerStore<BoardWriteOperations>, "execute">) => Promise<T>,
      assertCurrent: () => void,
    ) {
      assertCurrent();
      return operation({ execute: boundary.execute });
    },
    close: () => boundary.close(),
  }),
}));

const tempDirs = useAutoCleanupTempDirTracker((cleanup) =>
  afterEach(async () => {
    await closeOpenClawAgentDatabasesAsync();
    closeOpenClawAgentDatabasesForTest();
    await closeOpenClawStateDatabaseAsync();
    closeOpenClawStateDatabaseForTest();
    cleanup();
  }),
);

beforeEach(() => {
  boundary.execute.mockReset();
  boundary.close.mockReset().mockResolvedValue(undefined);
});

function fixture() {
  const env = { OPENCLAW_STATE_DIR: tempDirs.make("board-publication-outcome-") };
  const sessionKey = "agent:main:original";
  const database = openOpenClawAgentDatabase({ agentId: "main", env });
  replaceSessionEntrySync(
    { agentId: "main", sessionKey, storePath: database.path },
    { sessionId: "board-outcome-session", updatedAt: 1 },
  );
  const store = new SqliteBoardStore({
    resolveSession: (target) => ({
      agentId: "main",
      path: database.path,
      sessionKey: target.sessionKey,
    }),
    env,
  });
  const params = {
    sessionKey,
    name: "status",
    content: { kind: "html" as const, html: "<p>committed</p>" },
  };
  const change = { sessionKey, storePath: database.path };
  const changes: SessionRowChange[] = [];
  onTestFinished(sessionChanges.subscribe((published) => changes.push(published)));
  return { store, params, change, changes };
}

async function receiveExecutedFailure(retire: boolean) {
  const rejected = createDeferredCore<unknown>();
  const events: string[] = [];
  // This fixture uses execution identity only; native operations are forbidden.
  const executionWorker: Job["executionWorker"] = {
    get threadId(): number {
      throw new Error("Board outcome fixture must not inspect a native thread");
    },
    get started(): boolean {
      throw new Error("Board outcome fixture must not inspect native startup");
    },
    get executionStopped(): boolean {
      throw new Error("Board outcome fixture must not inspect native termination");
    },
    postMessage() {
      throw new Error("A failure reply must not request another result frame");
    },
    ref() {
      throw new Error("Board outcome fixture must not reference a native worker");
    },
    unref() {
      throw new Error("Board outcome fixture must not unreference a native worker");
    },
    terminate() {
      throw new Error("Board outcome fixture must not terminate a native worker");
    },
    on() {
      throw new Error("Board outcome fixture must not subscribe to native events");
    },
    once() {
      throw new Error("Board outcome fixture must not subscribe to native exit");
    },
    removeListener() {
      throw new Error("Board outcome fixture must not remove a native listener");
    },
    removeAllListeners() {
      throw new Error("Board outcome fixture must not remove native listeners");
    },
    cpuUsage() {
      throw new Error("Board outcome fixture must not inspect native CPU usage");
    },
    getHeapStatistics() {
      throw new Error("Board outcome fixture must not inspect native heap statistics");
    },
    service() {
      throw new Error("Board outcome fixture must not service a native worker");
    },
    stop() {
      throw new Error("Board outcome fixture must not stop a native worker");
    },
  };
  const job: Job = {
    executionWorker,
    request: { type: "execute", id: 1, actor: 1, input: new Uint8Array() },
    bytes: 0,
    nativeDispatched: true,
    settleNative: (settlement) => events.push(`settled:${settlement.kind}`),
    resolve: () => rejected.reject(new Error("Failure reply unexpectedly resolved")),
    reject: (error) => {
      events.push("rejected");
      rejected.resolve(error);
    },
    detach() {},
  };
  const slot: Parameters<typeof receiveSqliteWorkerReply>[0] = {
    current: job,
  };
  receiveSqliteWorkerReply(
    slot,
    {
      id: 1,
      ok: false,
      ...(retire ? { retire: true } : {}),
      error: {
        name: "SqliteWorkerError",
        code: "outcome-unknown",
        message: retire
          ? "Native settlement failed"
          : "Result serialization failed after execution",
      },
    },
    {
      fail(reason, currentError, openOutcome) {
        if (!(reason instanceof Error)) {
          throw new Error("Expected a decoded worker error");
        }
        const current = slot.current;
        slot.current = undefined;
        slot.failed = new SqliteWorkerError(reason.message, "unavailable");
        settleFailedSqliteWorkerJobs({
          queuedError: slot.failed,
          current,
          queued: [],
          error: reason,
          currentError,
          openOutcome,
          retire: () => {
            const retirement = createRetainedOperation<void>(() => {
              throw new Error("Board outcome fixture must not service native retirement");
            });
            events.push("retired");
            retirement.resolve(undefined);
            return retirement.operation;
          },
          finish: settleSqliteWorkerJob,
        });
      },
      finish(current, error, value, settlement, closeReceipt) {
        if (closeReceipt !== undefined) {
          throw new Error("Board execution failure must not publish a close receipt");
        }
        settleSqliteWorkerJob(current, error, value, settlement);
      },
      dispatch: () => events.push("dispatch"),
      returnProvisional() {
        throw new Error("Ordinary Board failure must not publish a provisional result");
      },
      resumeReply() {
        throw new Error("Ordinary Board failure must not resume a deferred reply");
      },
    },
    executionWorker,
  );
  return { error: await rejected.promise, events };
}

it.each([
  { name: "retired settlement", retire: true },
  { name: "non-retiring result serialization", retire: false },
])(
  "invalidates the original Board after the real receiver settles $name failure",
  async ({ retire }) => {
    const { store, params, change, changes } = fixture();
    // Native settlement failure retires; a later serialization failure need not.
    const received = await receiveExecutedFailure(retire);
    if (retire) {
      expect(isSqliteWorkerError(received.error, "outcome-unknown")).toBe(true);
    }
    expect(received.error).toMatchObject({ name: "SqliteWorkerError", code: "outcome-unknown" });
    expect(received.events).toEqual(
      retire
        ? ["retired", "settled:unknown", "rejected"]
        : ["settled:completed", "rejected", "dispatch"],
    );
    boundary.execute.mockImplementation(async () => {
      params.sessionKey = "agent:main:replacement";
      throw received.error;
    });
    await expect(store.putWidget(params)).rejects.toBe(received.error);
    expect(changes).toEqual([change]);
    expect(boundary.execute).toHaveBeenCalledOnce();
    expect(boundary.close).toHaveBeenCalledOnce();
  },
);

it("relays a committed Board outcome after revocation without failing on client cleanup", async () => {
  const { store, params, change, changes } = fixture();
  const committed: BoardWidgetPutResult = {
    sessionKey: params.sessionKey,
    revision: 17,
    tabs: [],
    widgets: [],
    resolvedWidgetName: params.name,
  };
  let current = true;
  boundary.execute.mockImplementation(async () => {
    current = false;
    params.sessionKey = "agent:main:replacement";
    return { value: committed, changes: [change] };
  });
  boundary.close.mockRejectedValue(new Error("Client cleanup failed after committed publication"));
  await expect(
    store.putWidget(params, {
      assertCurrent() {
        if (!current) {
          throw new Error("Board request revoked during committed delivery");
        }
      },
    }),
  ).resolves.toBe(committed);
  expect(changes).toEqual([change]);
  expect(boundary.execute).toHaveBeenCalledOnce();
  expect(boundary.close).toHaveBeenCalledOnce();
});

it("invalidates the original Board target for a nested cleanup aggregate without replay", async () => {
  const { store, params, change, changes } = fixture();
  const original = new SqliteWorkerError("Native Board outcome is unknown", "outcome-unknown");
  const failure = withSqliteWorkerCleanupFailure(
    withSqliteWorkerCleanupFailure(original, { error: new Error("Native cleanup failed") }),
    { error: new Error("Later cleanup failed") },
  );
  boundary.execute.mockImplementation(async () => {
    params.sessionKey = "agent:main:replacement";
    throw failure;
  });
  await expect(store.putWidget(params)).rejects.toBe(failure);
  expect(changes).toEqual([change]);
  expect(boundary.execute).toHaveBeenCalledOnce();
  expect(boundary.close).toHaveBeenCalledOnce();
});

it("restores a known Board validation rollback without publishing a change", async () => {
  const { store, params, changes } = fixture();
  boundary.execute.mockRejectedValue(
    Object.assign(new Error("Board widget revision changed"), {
      name: "BoardValidationError",
      code: "conflict",
    }),
  );
  const failure: unknown = await store.putWidget(params).catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(BoardValidationError);
  expect(failure).toMatchObject({ code: "conflict", message: "Board widget revision changed" });
  expect(changes).toEqual([]);
  expect(boundary.execute).toHaveBeenCalledOnce();
  expect(boundary.close).toHaveBeenCalledOnce();
});
