import fs from "node:fs";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { replaceFileAtomicSync } from "@openclaw/fs-safe/atomic";
import {
  decodeSessionArchiveBytes,
  encodeSessionArchiveContent,
  SESSION_ARCHIVE_ZSTD_SUFFIX,
} from "../config/sessions/archive-compression.js";
import { resolveSqliteTranscriptArchiveDirectory } from "../config/sessions/session-accessor.sqlite-scope.js";
import { assertAgentDatabaseMaintenanceAuthority } from "../state/openclaw-agent-db-lease.js";
import type { DB as OpenClawAgentKyselyDatabase } from "../state/openclaw-agent-db.generated.js";
import { SESSION_TRANSCRIPT_ARCHIVES_TABLE } from "../state/openclaw-agent-session-transcript-archive-schema.js";
import { OPENCLAW_SQLITE_BUSY_TIMEOUT_MS } from "../state/openclaw-state-db.js";
import { VERSION } from "../version.js";
import { sha256Hex } from "./crypto-digest.js";
import {
  executeSqliteQuerySync,
  executeSqliteQueryTakeFirstSync,
  getNodeSqliteKysely,
  clearNodeSqliteKyselyCacheForDatabase,
} from "./kysely-sync.js";
import {
  formatMigrationWarningSummary,
  MIGRATION_WARNING_EXAMPLE_LIMIT,
} from "./migration-warning-summary.js";
import { openNodeSqliteDatabase } from "./node-sqlite.js";
import { withPreparedSqliteSnapshot } from "./sqlite-readonly-location-cleanup.js";
import { prepareSqliteReadOnlyLocation } from "./sqlite-snapshot-source.js";
import { runSqliteImmediateTransactionSync } from "./sqlite-transaction.js";
import type { PreparedAgentDatabaseMigrationDiscovery } from "./state-migrations.media-persistence-targets.js";
import { transformMediaArchiveContent } from "./state-migrations.media-persistence-transform.js";
import { readTranscriptArchiveVerification } from "./state-migrations.transcript-archive-verification.js";
import {
  parseDirectiveMigrationTranscriptEvent,
  transformHistoricalTranscriptEvent,
} from "./state-migrations.transcript-directives-transform.js";

export const TRANSCRIPT_DIRECTIVE_MIGRATION_BATCH_SIZE = 32;

type TranscriptArchiveMigrationDatabase = Pick<
  OpenClawAgentKyselyDatabase,
  "session_transcript_archives"
>;

type ArchiveCursor = { generation: string; sessionId: string };

type ArchiveContentTransform = (
  content: string,
  owner: string,
) => { changed: boolean; content: string };

type ArchiveMigrationOptions = {
  agentId: string;
  database: DatabaseSync;
  pathname: string;
  start: ArchiveCursor;
  writeCursor?: (cursor: ArchiveCursor | { phase: "complete" }) => void;
  prepared?: PreparedTranscriptArchives;
  verified?: ReadonlySet<string>;
  onVerified?: (fingerprint: string) => void;
};

type ArchiveMigrationResult = {
  rewrittenArchives: number;
  warnings: string[];
};

type ArchiveRowPlan = {
  archiveName: string;
  archiveSha256: string;
  bytes: Buffer;
  changed: boolean;
  encoding: "identity" | "zstd";
  generation: string;
  nextBytes: Buffer;
  nextSha256: string;
  publishedAt: number | null;
  sessionId: string;
  fileCurrent: boolean;
  fingerprint?: string;
};

export type PreparedTranscriptArchives = Set<string>;

function archivePathFor(archiveDirectory: string, archiveName: string): string {
  const archivePath = path.resolve(archiveDirectory, archiveName);
  if (
    path.dirname(archivePath) !== path.resolve(archiveDirectory) ||
    path.basename(archivePath) !== archiveName
  ) {
    throw new Error(`Cannot migrate transcript archive outside ${archiveDirectory}`);
  }
  return archivePath;
}

function archiveFingerprint(
  archivePath: string,
  sha256: string,
  encoding: string,
): string | undefined {
  let stat: fs.BigIntStats | undefined;
  try {
    stat = fs.statSync(archivePath, { bigint: true, throwIfNoEntry: false });
  } catch {
    // Unobservable attributes cannot certify a copy; its per-row repair still owns IO errors.
    return undefined;
  }
  return sha256Hex(
    JSON.stringify([
      VERSION,
      archivePath,
      sha256,
      encoding,
      stat ? [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].map(String) : null,
    ]),
  );
}

function transformArchiveContent(
  content: string,
  owner: string,
): {
  changed: boolean;
  content: string;
} {
  if (!content) {
    return { changed: false, content };
  }
  const trailingNewline = content.endsWith("\n");
  const lines = trailingNewline ? content.slice(0, -1).split("\n") : content.split("\n");
  let changed = false;
  const rewritten = lines.map((line, index) => {
    if (!line) {
      throw new Error(`${owner} contains a blank JSONL record at line ${index + 1}`);
    }
    const event = parseDirectiveMigrationTranscriptEvent(line, `${owner}:${index + 1}`);
    const transformed = transformHistoricalTranscriptEvent(event);
    changed ||= transformed.changed;
    return transformed.changed ? JSON.stringify(transformed.event) : line;
  });
  return {
    changed,
    content: `${rewritten.join("\n")}${trailingNewline ? "\n" : ""}`,
  };
}

function encodeArchiveContent(
  content: string,
  encoding: "identity" | "zstd",
  owner: string,
): Buffer {
  if (encoding === "identity") {
    return Buffer.from(content, "utf8");
  }
  const encoded = encodeSessionArchiveContent(content);
  if (encoded.suffix !== SESSION_ARCHIVE_ZSTD_SUFFIX) {
    throw new Error(`${owner} could not be re-encoded with its zstd codec`);
  }
  return encoded.bytes;
}

function readArchiveEncoding(value: string, owner: string): "identity" | "zstd" {
  if (value === "identity" || value === "zstd") {
    return value;
  }
  throw new Error(`${owner} has unsupported transcript archive encoding ${value}`);
}

function hasArchiveTable(database: DatabaseSync): boolean {
  // The archive table was added lazily at agent schema v17, so valid v17 databases may omit it.
  return Boolean(
    database
      .prepare("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")
      .get(SESSION_TRANSCRIPT_ARCHIVES_TABLE),
  );
}

function listArchiveBatch(
  database: DatabaseSync,
  cursor: ArchiveCursor,
  transformContent: ArchiveContentTransform = transformArchiveContent,
  verification?: {
    archiveDirectory: string;
    prepared?: PreparedTranscriptArchives;
    verified?: ReadonlySet<string>;
  },
): ArchiveRowPlan[] {
  const db = getNodeSqliteKysely<TranscriptArchiveMigrationDatabase>(database);
  let query = db
    .selectFrom("session_transcript_archives")
    .select([
      "archive_blob",
      "archive_name",
      "archive_sha256",
      "encoding",
      "generation",
      "published_at",
      "session_id",
    ])
    .orderBy("session_id", "asc")
    .orderBy("generation", "asc")
    .limit(TRANSCRIPT_DIRECTIVE_MIGRATION_BATCH_SIZE);
  if (cursor.sessionId) {
    // Seek the composite key; OR branches rescan the visited prefix on every page.
    query = query.where((eb) =>
      eb(
        eb.refTuple("session_id", "generation"),
        ">",
        eb.tuple(cursor.sessionId, cursor.generation),
      ),
    );
  }
  return executeSqliteQuerySync(database, query).rows.map((row) => {
    const owner = `${row.session_id}:${row.generation}`;
    const encoding = readArchiveEncoding(row.encoding, owner);
    const bytes = Buffer.from(row.archive_blob);
    if (sha256Hex(bytes) !== row.archive_sha256) {
      throw new Error(`Canonical SQLite transcript archive is corrupt for ${row.session_id}`);
    }
    const archivePath = verification
      ? archivePathFor(verification.archiveDirectory, row.archive_name)
      : undefined;
    let fingerprint = archivePath
      ? archiveFingerprint(archivePath, row.archive_sha256, encoding)
      : undefined;
    const verified =
      fingerprint !== undefined &&
      (verification?.prepared?.has(fingerprint) || verification?.verified?.has(fingerprint));
    const transformed = verified
      ? undefined
      : transformContent(decodeSessionArchiveBytes(bytes, encoding === "zstd"), owner);
    const nextBytes = transformed?.changed
      ? encodeArchiveContent(transformed.content, encoding, owner)
      : bytes;
    const nextSha256 = sha256Hex(nextBytes);
    let fileCurrent = false;
    try {
      fileCurrent = verified
        ? fs.existsSync(archivePath!)
        : Boolean(
            archivePath &&
            fs.existsSync(archivePath) &&
            sha256Hex(fs.readFileSync(archivePath)) === nextSha256,
          );
      if (
        archivePath &&
        archiveFingerprint(archivePath, row.archive_sha256, encoding) !== fingerprint
      ) {
        fileCurrent = false;
        fingerprint = undefined;
      }
    } catch {
      // Defer copy failures to per-row repair so earlier archives keep their progress.
      fingerprint = undefined;
    }
    return {
      archiveName: row.archive_name,
      archiveSha256: row.archive_sha256,
      bytes,
      changed: transformed?.changed === true,
      encoding,
      generation: row.generation,
      nextBytes,
      nextSha256,
      publishedAt: row.published_at,
      sessionId: row.session_id,
      fileCurrent,
      fingerprint,
    };
  });
}

export function transcriptDirectiveArchivesNeedMigration(
  database: DatabaseSync,
  start: ArchiveCursor,
): boolean {
  if (!hasArchiveTable(database)) {
    return false;
  }
  let cursor = start;
  while (true) {
    const batch = listArchiveBatch(database, cursor);
    const last = batch.at(-1);
    if (!last) {
      return false;
    }
    if (batch.some((planned) => planned.changed)) {
      return true;
    }
    cursor = { generation: last.generation, sessionId: last.sessionId };
  }
}

function assertArchiveSourceUnchanged(database: DatabaseSync, planned: ArchiveRowPlan): boolean {
  const db = getNodeSqliteKysely<TranscriptArchiveMigrationDatabase>(database);
  const current = executeSqliteQueryTakeFirstSync(
    database,
    db
      .selectFrom("session_transcript_archives")
      .select(["archive_blob", "archive_name", "archive_sha256", "encoding"])
      .where("session_id", "=", planned.sessionId)
      .where("generation", "=", planned.generation),
  );
  if (!current) {
    return false;
  }
  if (
    current.archive_name !== planned.archiveName ||
    current.archive_sha256 !== planned.archiveSha256 ||
    current.encoding !== planned.encoding ||
    !Buffer.from(current.archive_blob).equals(planned.bytes)
  ) {
    throw new Error(
      `Transcript archive source changed before migration commit for ${planned.sessionId}`,
    );
  }
  return true;
}

function rewriteArchiveRow(database: DatabaseSync, planned: ArchiveRowPlan): boolean {
  if (!assertArchiveSourceUnchanged(database, planned)) {
    return false;
  }
  const db = getNodeSqliteKysely<TranscriptArchiveMigrationDatabase>(database);
  const result = executeSqliteQuerySync(
    database,
    db
      .updateTable("session_transcript_archives")
      .set({
        archive_blob: planned.nextBytes,
        archive_sha256: planned.nextSha256,
        published_at: null,
      })
      .where("session_id", "=", planned.sessionId)
      .where("generation", "=", planned.generation)
      .where("archive_sha256", "=", planned.archiveSha256),
  );
  if (result.numAffectedRows !== 1n) {
    throw new Error(`Transcript archive changed before rewrite for ${planned.sessionId}`);
  }
  return true;
}

function repairPublishedArchiveFile(params: {
  archiveDirectory: string;
  planned: ArchiveRowPlan;
}): boolean {
  const archiveDirectory = path.resolve(params.archiveDirectory);
  const archivePath = archivePathFor(archiveDirectory, params.planned.archiveName);
  const fingerprint = () =>
    archiveFingerprint(archivePath, params.planned.nextSha256, params.planned.encoding);
  const before = fingerprint();
  if (params.planned.fileCurrent && before !== undefined && params.planned.fingerprint === before) {
    return true;
  }
  if (!fs.existsSync(archivePath)) {
    params.planned.fingerprint = before;
    return false;
  }
  if (sha256Hex(fs.readFileSync(archivePath)) === params.planned.nextSha256) {
    params.planned.fingerprint = fingerprint() === before ? before : undefined;
    return true;
  }
  assertAgentDatabaseMaintenanceAuthority();
  replaceFileAtomicSync({
    beforeRename: ({ tempPath }) => {
      const stagedHash = sha256Hex(fs.readFileSync(tempPath));
      if (stagedHash !== params.planned.nextSha256) {
        throw new Error(`Transcript archive staging verification failed for ${archivePath}`);
      }
      // Staging and fsync can outlive the timer-driven lease heartbeat. Recheck
      // at the atomic publication boundary so an expired owner cannot rename.
      assertAgentDatabaseMaintenanceAuthority();
    },
    content: params.planned.nextBytes,
    filePath: archivePath,
    preserveExistingMode: true,
    syncParentDir: true,
    syncTempFile: true,
    tempPrefix: `${path.basename(archivePath)}.directive-migration`,
  });
  const published = fingerprint();
  if (sha256Hex(fs.readFileSync(archivePath)) !== params.planned.nextSha256) {
    throw new Error(`Transcript archive verification failed for ${archivePath}`);
  }
  params.planned.fingerprint = fingerprint() === published ? published : undefined;
  return true;
}

function finalizeArchiveCursor(params: {
  database: DatabaseSync;
  fileCurrent: boolean;
  planned: ArchiveRowPlan;
  writeCursor?: (cursor: ArchiveCursor | { phase: "complete" }) => void;
}): void {
  const db = getNodeSqliteKysely<TranscriptArchiveMigrationDatabase>(params.database);
  const current = executeSqliteQueryTakeFirstSync(
    params.database,
    db
      .selectFrom("session_transcript_archives")
      .select(["archive_blob", "archive_sha256"])
      .where("session_id", "=", params.planned.sessionId)
      .where("generation", "=", params.planned.generation),
  );
  if (current) {
    if (
      current.archive_sha256 !== params.planned.nextSha256 ||
      !Buffer.from(current.archive_blob).equals(params.planned.nextBytes)
    ) {
      throw new Error(
        `Transcript archive changed before migration commit for ${params.planned.sessionId}`,
      );
    }
    if (params.planned.changed && params.planned.publishedAt !== null && params.fileCurrent) {
      executeSqliteQuerySync(
        params.database,
        db
          .updateTable("session_transcript_archives")
          .set({ published_at: params.planned.publishedAt })
          .where("session_id", "=", params.planned.sessionId)
          .where("generation", "=", params.planned.generation)
          .where("archive_sha256", "=", params.planned.nextSha256),
      );
    }
  }
  params.writeCursor?.({
    generation: params.planned.generation,
    sessionId: params.planned.sessionId,
  });
}

/** Repairs canonical blobs before their reconstructible files under maintenance authority. */
export async function migrateCanonicalTranscriptArchives(
  params: ArchiveMigrationOptions & {
    onArchive?: (archivePath: string) => void;
    transformContent: ArchiveContentTransform;
  },
): Promise<ArchiveMigrationResult> {
  let rewrittenArchives = 0;
  let missingCopies = 0;
  const missingCopyExamples: string[] = [];
  const archivesPresent = hasArchiveTable(params.database);
  let cursor = params.start;
  const archiveDirectory = resolveSqliteTranscriptArchiveDirectory({
    agentId: params.agentId,
    path: params.pathname,
  });
  while (true) {
    const batch = archivesPresent
      ? listArchiveBatch(params.database, cursor, params.transformContent, {
          archiveDirectory,
          prepared: params.prepared,
          verified: params.verified,
        })
      : [];
    if (batch.length === 0) {
      if (params.writeCursor) {
        runSqliteImmediateTransactionSync(
          params.database,
          () => {
            assertAgentDatabaseMaintenanceAuthority();
            params.writeCursor?.({ phase: "complete" });
            assertAgentDatabaseMaintenanceAuthority();
          },
          {
            databaseLabel: params.pathname,
            operationLabel: "historical-transcript-archive.complete",
          },
        );
      }
      return {
        rewrittenArchives,
        warnings:
          missingCopies > 0
            ? [
                formatMigrationWarningSummary({
                  summary: `${params.pathname}: Missing ${missingCopies} canonical transcript archive file(s)`,
                  count: missingCopies,
                  detail:
                    "Canonical SQLite archive blobs remain retained. Migration completed without recreating the missing copies.",
                }),
                ...missingCopyExamples,
              ]
            : [],
      };
    }
    for (const planned of batch) {
      const archivePath = path.resolve(archiveDirectory, planned.archiveName);
      params.onArchive?.(archivePath);
      const rowPresent = planned.changed
        ? runSqliteImmediateTransactionSync(
            params.database,
            () => {
              assertAgentDatabaseMaintenanceAuthority();
              const currentRowPresent = rewriteArchiveRow(params.database, planned);
              assertAgentDatabaseMaintenanceAuthority();
              return currentRowPresent;
            },
            {
              busyTimeoutMs: OPENCLAW_SQLITE_BUSY_TIMEOUT_MS,
              databaseLabel: params.pathname,
              operationLabel: "historical-transcript-archive-directives",
            },
          )
        : assertArchiveSourceUnchanged(params.database, planned);
      const fileCurrent = rowPresent
        ? repairPublishedArchiveFile({ archiveDirectory, planned })
        : false;
      if (rowPresent && !planned.changed) {
        assertArchiveSourceUnchanged(params.database, planned);
      }
      if (rowPresent && !fileCurrent) {
        missingCopies += 1;
        if (missingCopyExamples.length < MIGRATION_WARNING_EXAMPLE_LIMIT) {
          missingCopyExamples.push(`Missing canonical transcript archive copy: ${archivePath}`);
        }
      }
      if (planned.changed) {
        runSqliteImmediateTransactionSync(
          params.database,
          () => {
            assertAgentDatabaseMaintenanceAuthority();
            finalizeArchiveCursor({
              database: params.database,
              fileCurrent,
              planned,
              writeCursor: params.writeCursor,
            });
            assertAgentDatabaseMaintenanceAuthority();
          },
          {
            busyTimeoutMs: OPENCLAW_SQLITE_BUSY_TIMEOUT_MS,
            databaseLabel: params.pathname,
            operationLabel: "historical-transcript-archive-cursor",
          },
        );
      }
      if (rowPresent) {
        if (planned.fingerprint) {
          params.onVerified?.(planned.fingerprint);
        }
      }
      rewrittenArchives += planned.changed && rowPresent ? 1 : 0;
      cursor = { generation: planned.generation, sessionId: planned.sessionId };
    }
    if (params.writeCursor && !batch.at(-1)?.changed) {
      runSqliteImmediateTransactionSync(
        params.database,
        () => {
          assertAgentDatabaseMaintenanceAuthority();
          params.writeCursor?.(cursor);
          assertAgentDatabaseMaintenanceAuthority();
        },
        { databaseLabel: params.pathname, operationLabel: "historical-transcript-archive-cursor" },
      );
    }
    // Archive planning and file publication are synchronous. Give the lease
    // heartbeat a scheduling point before the next bounded batch begins.
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  }
}

/** Read and transform retained archives before Doctor stops the managed writer. */
export async function prepareCanonicalTranscriptArchiveMigrations(
  discovery: PreparedAgentDatabaseMigrationDiscovery,
): Promise<void> {
  const prepared = new Map<string, PreparedTranscriptArchives>();
  discovery.preparedTranscriptArchives = prepared;
  for (const target of discovery.discovery.targets) {
    const snapshot = await prepareSqliteReadOnlyLocation(target.realPath, {
      preserveSourceArtifacts: true,
      allowLiveOwner: true,
    });
    const preparedFacts = await withPreparedSqliteSnapshot(snapshot, async (location) => {
      const database = openNodeSqliteDatabase(location, { readOnly: true });
      try {
        const cache: PreparedTranscriptArchives = new Set();
        if (!hasArchiveTable(database)) {
          return cache;
        }
        const archiveDirectory = resolveSqliteTranscriptArchiveDirectory({
          agentId: target.agentId,
          path: target.path,
        });
        const verified = readTranscriptArchiveVerification(database);
        let cursor = { generation: "", sessionId: "" };
        while (true) {
          const batch = listArchiveBatch(database, cursor, transformMediaArchiveContent, {
            archiveDirectory,
            verified,
          });
          const last = batch.at(-1);
          if (!last) {
            return cache;
          }
          for (const planned of batch) {
            // Retain only unchanged facts; the write set keeps its bounded batch buffers.
            if (
              !planned.changed &&
              planned.fingerprint &&
              (planned.fileCurrent ||
                !fs.existsSync(archivePathFor(archiveDirectory, planned.archiveName)))
            ) {
              cache.add(planned.fingerprint);
            }
          }
          cursor = { generation: last.generation, sessionId: last.sessionId };
          await new Promise<void>((resolve) => {
            setImmediate(resolve);
          });
        }
      } finally {
        clearNodeSqliteKyselyCacheForDatabase(database);
        database.close();
      }
    });
    prepared.set(target.path, preparedFacts);
  }
}

export function migrateTranscriptDirectiveArchives(
  params: ArchiveMigrationOptions,
): Promise<ArchiveMigrationResult> {
  return migrateCanonicalTranscriptArchives({
    ...params,
    transformContent: transformArchiveContent,
  });
}
