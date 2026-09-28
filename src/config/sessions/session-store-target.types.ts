import type { OpenClawRegisteredAgentDatabase } from "../../state/openclaw-agent-db-contract.js";
import type { OpenClawConfig } from "../types.openclaw.js";
import type {
  CapturedSessionStorePaths,
  SessionStoreReadCandidate,
} from "./session-store-read-candidates.js";

/** One session store path paired with its owning agent id. */
export type SessionStoreTarget = {
  agentId: string;
  storePath: string;
};

export type SessionStoreRegistryRead =
  | readonly Pick<OpenClawRegisteredAgentDatabase, "agentId" | "path">[]
  | { status: "deferred" | "unavailable" };

export type SessionStoreTargetsReadResult =
  | { available: true; targets: SessionStoreTarget[] }
  | {
      available: false;
      reason: "database-missing" | "schema-missing" | "read-failed";
    };

export type SessionStoreTargetReadRequest = {
  agentId?: string;
  defaultAgentId?: string;
  storePath: string;
  env: NodeJS.ProcessEnv;
  candidates: SessionStoreReadCandidate[];
  registeredDatabases: SessionStoreRegistryRead;
};

type SessionStoreRegistryRequired = {
  kind: "session-target-registry-required";
  readFailed?: boolean;
};

export type SessionStoreTargetReadResult =
  | SessionStoreRegistryRequired
  | {
      kind: "session-store-target";
      sourcePath: string;
      logicalAgentId: string;
      database: { agentId: string; path: string };
    };

export type SessionStoreTargetInventoryRequest = {
  config: OpenClawConfig;
  legacyDefaultAgentId?: string;
  agentIds: string[];
  env: NodeJS.ProcessEnv;
  paths: CapturedSessionStorePaths;
  candidates: SessionStoreReadCandidate[];
  registeredDatabases: SessionStoreRegistryRead;
};

export type SessionStoreTargetInventoryResult =
  | SessionStoreRegistryRequired
  | {
      kind: "session-target-inventory";
      agents: Array<{
        agentId: string;
        result: SessionStoreTargetsReadResult;
        reads: Array<{ target: SessionStoreTarget; database: { agentId: string; path: string } }>;
      }>;
    };
