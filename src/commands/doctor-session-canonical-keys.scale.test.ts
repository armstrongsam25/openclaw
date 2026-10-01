import path from "node:path";
import { afterEach, expect, it } from "vitest";
import { resolveSessionStorePathCore } from "../config/sessions/paths.js";
import { resolveSqliteTargetFromSessionStorePath } from "../config/sessions/session-sqlite-target.js";
import {
  closeOpenClawAgentDatabasesForTest,
  openOpenClawAgentDatabase,
} from "../state/openclaw-agent-db.js";
import { withStateDirEnv } from "../test-helpers/state-dir-env.js";
import { repairCanonicalSessionKeys } from "./doctor-session-canonical-keys.js";

afterEach(() => closeOpenClawAgentDatabasesForTest());

it("repairs deep owner aliases without losing a large healthy transcript", async () => {
  await withStateDirEnv("openclaw-doctor-canonical-scale-", async ({ stateDir }) => {
    const env = { ...process.env, OPENCLAW_STATE_DIR: stateDir };
    const storeTemplate = path.join(stateDir, "agents", "{agentId}", "sessions.json");
    const storePath = resolveSessionStorePathCore(storeTemplate, { agentId: "main", env });
    const cfg = {
      agents: { list: [{ id: "main", default: true }] },
      session: { store: storeTemplate },
    };
    const { db } = openOpenClawAgentDatabase({
      agentId: "main",
      env,
      path: resolveSqliteTargetFromSessionStorePath(storePath, { agentId: "main", env }).path,
    });
    const aliasCount = 12_000;
    const eventCount = 200_000;
    const key = (index: number) => `agent:main:dashboard:scale-${String(index).padStart(5, "0")}`;
    const node = db.prepare(
      "INSERT INTO session_nodes (session_key, current_session_id, entry_json, entry_valid, updated_at) VALUES (?, ?, ?, ?, 10)",
    );
    const window = db.prepare(
      "INSERT INTO session_windows (session_id, session_key, reason, session_scope, created_at, updated_at) VALUES (?, ?, 'initial', 'conversation', 10, 10)",
    );
    const event = db.prepare(
      "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?, ?, ?, 10)",
    );
    const identity = db.prepare(
      "INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, parent_id, created_at) VALUES (?, ?, ?, 'message', ?, 10)",
    );
    const sessionId = `scale-${aliasCount}`;
    db.exec("BEGIN");
    try {
      for (let index = 0; index <= aliasCount; index += 1) {
        const currentId = `scale-${Math.min(index + 1, aliasCount)}`;
        node.run(
          key(index),
          currentId,
          index === aliasCount ? JSON.stringify({ sessionId, updatedAt: 10 }) : "{}",
          index === aliasCount ? 1 : 0,
        );
        window.run(`scale-${index}`, key(index));
      }
      for (let seq = 0; seq < eventCount; seq += 1) {
        const id = `event-${seq}`;
        const parentId = seq === 0 ? null : `event-${seq - 1}`;
        event.run(
          sessionId,
          seq,
          JSON.stringify({
            id,
            parentId,
            type: "message",
            message: { role: "user", content: `history ${seq}` },
          }),
        );
        identity.run(sessionId, id, seq, parentId);
      }
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }
    expect(db.prepare("PRAGMA integrity_check").get()).toEqual({ integrity_check: "ok" });
    expect(db.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
    await expect(repairCanonicalSessionKeys({ apply: false, cfg, env })).resolves.toMatchObject({
      foundGroups: 1,
      removedRows: aliasCount,
      scannedStores: 1,
    });
    await expect(repairCanonicalSessionKeys({ apply: true, cfg, env })).resolves.toMatchObject({
      repairedGroups: 1,
      removedRows: aliasCount,
    });
    expect(db.prepare("SELECT count(*) AS count FROM transcript_events").get()).toEqual({
      count: eventCount,
    });
    expect(db.prepare("SELECT count(*) AS count FROM session_nodes").get()).toEqual({ count: 1 });
    expect(db.prepare("SELECT session_key, current_session_id FROM session_nodes").get()).toEqual({
      session_key: key(aliasCount),
      current_session_id: sessionId,
    });
    expect(db.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
  });
});
