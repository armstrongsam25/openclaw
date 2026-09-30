import { serialize } from "node:v8";
import {
  SQLITE_WORKER_MAX_RESULT_BYTES,
  type SqliteWorkerCloseReceipt,
  type SqliteWorkerReply,
  type SqliteWorkerRequest,
} from "./sqlite-worker-contract.js";
import type { createSqliteWorkerTransferOwner } from "./sqlite-worker-transfer.js";

type SqliteWorkerResultState = {
  transfers: ReturnType<typeof createSqliteWorkerTransferOwner>;
  pendingResult?: { requestId: number; actor: number; transferId: number };
  provisional?: { parentActor: number; parentId: number };
};

export function encodeSqliteWorkerResult(
  state: SqliteWorkerResultState,
  request: SqliteWorkerRequest,
  serialized: Uint8Array,
  completeResult: boolean,
  closeReceipt?: SqliteWorkerCloseReceipt,
  inputNext = false,
  reservedReplyBytes = 0,
): SqliteWorkerReply {
  if (serialized.byteLength + reservedReplyBytes > SQLITE_WORKER_MAX_RESULT_BYTES) {
    if (!completeResult) {
      throw new Error("SQLite worker frame exceeds the transport byte limit");
    }
    const handle = state.transfers.start([{ kind: "result", serialized }].values(), {
      kinds: ["result"],
    });
    state.pendingResult = { requestId: request.id, actor: request.actor, transferId: handle.id };
    return {
      id: request.id,
      ok: true,
      value: serialize(handle),
      transfer: "start",
      ...(state.provisional ? { provisional: state.provisional } : {}),
    };
  }
  return {
    id: request.id,
    ok: true,
    value: serialized,
    ...(closeReceipt ? { closeReceipt } : {}),
    ...(request.type === "result-next" ? { transfer: "frame" as const } : {}),
    ...(inputNext ? { input: "next" as const } : {}),
    ...(state.provisional ? { provisional: state.provisional } : {}),
  };
}
