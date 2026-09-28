import type {
  UserProfile as UserProfileListItem,
  UsersMergeResult,
} from "../../packages/gateway-protocol/src/schema/users.js";
import type { UserProfileAvatarInspection } from "./user-profiles-avatar.types.js";
import type { TailscaleProfileIdentity } from "./user-profiles-tailscale-login.js";
import type {
  ProfileDisplayRow,
  UserProfile,
  UserProfileAvatarMime,
  UserProfileDisplay,
  UserProfileGitHubSyncInput,
  UserProfileOwnerErrorCode,
  UserChannelIdentityWorkerOperations,
} from "./user-profiles.types.js";

export type UserProfileWriteResult<T> =
  | { ok: true; value: T }
  | { ok: false; kind: "not-found"; profileId: string }
  | { ok: false; kind: "merge"; message: string }
  | { ok: false; kind: "owner"; code: UserProfileOwnerErrorCode };

export type UserProfileWriteOperations = {
  "userProfiles.setRole": {
    input: { profileId: string; role: string | null };
    output: UserProfileWriteResult<UserProfileListItem>;
  };
  "userProfiles.linkEmail": {
    input: { email: string; targetProfileId: string };
    output: UserProfileWriteResult<{ profile: UserProfileListItem; display: UserProfileDisplay }>;
  };
  "userProfiles.merge": {
    input: { sourceProfileId: string; targetProfileId: string };
    output: UserProfileWriteResult<UsersMergeResult & { display: UserProfileDisplay }>;
  };
  "userProfiles.ensureEmail": {
    input: { email: string; expectedGitHubAccountId?: number };
    output: UserProfileWriteResult<UserProfile>;
  };
  "userProfiles.ensureTailscale": {
    input: TailscaleProfileIdentity;
    output: UserProfileWriteResult<UserProfile>;
  };
  "userProfiles.syncGitHub": {
    input: UserProfileGitHubSyncInput;
    output: UserProfileWriteResult<UserProfileListItem>;
  };
  "userProfiles.ensureOwner": {
    input: { displayName: string | null };
    output: UserProfileWriteResult<UserProfile>;
  };
};

export type UserProfileReadWorkerOperations = {
  "userProfiles.list": { input: undefined; output: UserProfileListItem[] };
  "userProfiles.directory": {
    input: { limit: number };
    output: { profiles: Array<{ id: string; logins: string[] }>; truncated: boolean };
  };
};

export type UserProfileAvatarWorkerOperations = {
  "userProfiles.avatar.inspect": {
    input: { profileId: string };
    output: UserProfileAvatarInspection;
  };
  "userProfiles.avatar.adopt": {
    input: { profileId: string; bytes: Uint8Array; mime: UserProfileAvatarMime; now: number };
    output: { profile: UserProfile | undefined; committed?: ProfileDisplayRow };
  };
};

export type UserProfileWorkerOperations = UserProfileReadWorkerOperations &
  UserProfileAvatarWorkerOperations &
  UserProfileWriteOperations &
  UserChannelIdentityWorkerOperations;
