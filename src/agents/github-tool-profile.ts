import fs from "node:fs/promises";
import { root as fsRoot } from "@openclaw/fs-safe/root";
import { stringify as stringifyYaml } from "yaml";
import { clearNativeGitHubTokenCache } from "./github-read-identity.js";

export function managedGitHubIdentityEnvironment(params: {
  profileDir: string;
  gitAuthor?: { name?: string; email?: string };
  gitConfig?: readonly (readonly [string, string])[];
}): Readonly<Record<string, string>> {
  const author = params.gitAuthor;
  const gitConfigEntries = [
    ...(params.gitConfig ?? []),
    ...Object.entries({
      ...(author?.name ? { "user.name": author.name } : {}),
      ...(author?.email ? { "user.email": author.email } : {}),
    }),
  ];
  const gitConfigEnv = Object.fromEntries(
    gitConfigEntries.flatMap(([key, value], index) => [
      [`GIT_CONFIG_KEY_${index}`, key],
      [`GIT_CONFIG_VALUE_${index}`, value],
    ]),
  );
  return {
    GH_CONFIG_DIR: params.profileDir,
    ...(gitConfigEntries.length > 0
      ? { GIT_CONFIG_COUNT: String(gitConfigEntries.length), ...gitConfigEnv }
      : {}),
    ...(author?.name ? { GIT_AUTHOR_NAME: author.name, GIT_COMMITTER_NAME: author.name } : {}),
    ...(author?.email ? { GIT_AUTHOR_EMAIL: author.email, GIT_COMMITTER_EMAIL: author.email } : {}),
  };
}

export async function removeManagedGitHubProfile(profileDir: string): Promise<void> {
  await fs.rm(profileDir, { recursive: true, force: true });
  clearNativeGitHubTokenCache();
}

export function managedGitHubHosts(identity: { login: string; token: string }): string {
  return stringifyYaml({
    "github.com": {
      user: identity.login,
      oauth_token: identity.token,
      users: { [identity.login]: { oauth_token: identity.token } },
    },
  });
}

/** Write gh's external file contract without touching its OS keyring or verifying again. */
export async function writeManagedGitHubProfileFiles(
  profileDir: string,
  identity: { login: string; token: string },
): Promise<void> {
  await fs.mkdir(profileDir, { recursive: true, mode: 0o700 });
  await fs.chmod(profileDir, 0o700);
  const profile = await fsRoot(profileDir, { mode: 0o600, mkdir: false, durable: false });
  await profile.write("config.yml", stringifyYaml({ version: "1" }));
  await profile.write("hosts.yml", managedGitHubHosts(identity));
}
