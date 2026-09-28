import type { ManagedUpdateLeaseDatabaseIdentity } from "./update-managed-service-handoff-identity.js";
import type { BorrowedLegacyHandoffParent } from "./update-managed-service-handoff-legacy-parent.js";
import type { ManagedHandoffLeasePayload } from "./update-managed-service-handoff-schema.js";

export type ManagedHandoffLease = ManagedHandoffLeasePayload & {
  key: string;
  owner: string;
  payload: string;
  updatedAt: number;
};

export type ManagedHandoffParent = ManagedHandoffLease | BorrowedLegacyHandoffParent;

export type LeaseAcquisition =
  | { kind: "busy"; owner: string }
  | {
      kind: "acquired";
      lease: ManagedHandoffLease;
      originalDatabaseIdentity?: ManagedUpdateLeaseDatabaseIdentity;
    };

export type ManagedHandoffLeaseStoreOptions = {
  databasePath: string;
  serviceManagerEnv: NodeJS.ProcessEnv;
  existingIdentity?: ManagedUpdateLeaseDatabaseIdentity;
  originalUpdateKey?: string;
  onProcessIdentityWarning?: (pid: number, message: string) => void;
};

export type ManagedHandoffRepair = {
  assertCurrent: () => void;
  bindRun(runId: string): void;
  complete(runId: string): void;
  [Symbol.dispose](): void;
};

export type ManagedHandoffLeaseTransition = (
  lease: ManagedHandoffLease,
  action: ManagedHandoffLease["action"],
  executor?: ManagedHandoffLease["executor"],
  recovery?: (next: ManagedHandoffLease) => string,
) => ManagedHandoffLease | null;
