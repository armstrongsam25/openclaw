import type { DatabaseSync } from "node:sqlite";
import { assertAgentDatabaseMaintenanceAuthority } from "../state/openclaw-agent-db-lease.js";
import type { DB } from "../state/openclaw-agent-db.generated.js";
import { VERSION } from "../version.js";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
} from "./kysely-sync.js";
import { runSqliteImmediateTransactionSync } from "./sqlite-transaction.js";

const META_KEY = "media-transcript-archive-verification-v1";

/** Derived normalization facts; legacy/restored databases simply verify again. */
export function readTranscriptArchiveVerification(database: DatabaseSync): Set<string> {
  const db = getNodeSqliteKysely<Pick<DB, "schema_meta">>(database);
  const row = executeSqliteQueryTakeFirstSync(
    database,
    db.selectFrom("schema_meta").select("app_version").where("meta_key", "=", META_KEY),
  );
  if (!row?.app_version) {
    return new Set();
  }
  try {
    const parsed: unknown = JSON.parse(row.app_version);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "version" in parsed &&
      parsed.version === VERSION &&
      "fingerprints" in parsed &&
      Array.isArray(parsed.fingerprints) &&
      parsed.fingerprints.every((value: unknown) => typeof value === "string")
    ) {
      return new Set(parsed.fingerprints);
    }
  } catch {
    // A damaged cache cannot exempt any retained archive from verification.
  }
  return new Set();
}

export function recordTranscriptArchiveVerification(
  database: DatabaseSync,
  agentId: string,
  pathname: string,
  previous: ReadonlySet<string>,
  verified: ReadonlySet<string>,
): void {
  if (previous.size === verified.size && [...verified].every((value) => previous.has(value))) {
    return;
  }
  const db = getNodeSqliteKysely<Pick<DB, "schema_meta">>(database);
  const now = Date.now();
  const appVersion = JSON.stringify({ version: VERSION, fingerprints: [...verified].toSorted() });
  // Only the changed receipt is written; archive verification never holds this lock.
  runSqliteImmediateTransactionSync(
    database,
    () => {
      assertAgentDatabaseMaintenanceAuthority();
      executeSqliteQuerySync(
        database,
        db
          .insertInto("schema_meta")
          .values({
            meta_key: META_KEY,
            role: "agent",
            agent_id: agentId,
            schema_version: 1,
            app_version: appVersion,
            created_at: now,
            updated_at: now,
          })
          .onConflict((conflict) =>
            conflict.column("meta_key").doUpdateSet({ app_version: appVersion, updated_at: now }),
          ),
      );
      assertAgentDatabaseMaintenanceAuthority();
    },
    { databaseLabel: pathname, operationLabel: "transcript-archive-verification.record" },
  );
}
