import type { Serializable, SpawnOptions } from "node:child_process";
import { fileURLToPath } from "node:url";
import type { BrokerExecaOptions, BrokerExecaResult } from "./execa-protocol.js";

export type BrokerSpawnOptions = Pick<
  SpawnOptions,
  | "cwd"
  | "env"
  | "argv0"
  | "detached"
  | "shell"
  | "windowsHide"
  | "windowsVerbatimArguments"
  | "serialization"
  | "uid"
  | "gid"
> & { stdio: ("pipe" | "ignore" | "inherit" | "ipc")[] };
export type BrokerRequest =
  | { type: "spawn"; id: number; argv: string[]; options: BrokerSpawnOptions }
  | { type: "spawn-execa"; id: number; argv: string[]; options: BrokerExecaOptions }
  | { type: "kill"; id: number; signal: NodeJS.Signals | number }
  | { type: "ipc"; id: number; sequence: number; message: Serializable }
  | { type: "disconnect"; id: number }
  | { type: "cancel"; id: number }
  | { type: "pipe-received"; id: number; fd: number }
  | { type: "output-drained"; id: number; fd: number; error?: BrokerError }
  | { type: "shutdown" };
export type BrokerError = {
  message: string;
  code?: string;
  errno?: number;
  syscall?: string;
  path?: string;
  spawnargs?: string[];
};
export type BrokerResponse =
  | { type: "ready"; pid: number }
  | { type: "owned"; id: number; pid: number }
  | { type: "pipe"; id: number; fd: number; closed?: true }
  | { type: "pipe-prefix"; id: number; fd: number; bytes: Buffer }
  | {
      type: "spawned";
      id: number;
      pid: number;
      spawnfile: string;
      spawnargs: string[];
      connected: boolean;
      stdioLength: number;
    }
  | { type: "error"; id: number; error: BrokerError; resultUnavailable?: true }
  | { type: "ipc-sent"; id: number; sequence: number; error?: BrokerError }
  | { type: "exit"; id: number; code: number | null; signal: NodeJS.Signals | null }
  | { type: "closed"; id: number }
  | { type: "disconnect"; id: number }
  | { type: "ipc"; id: number; message: Serializable }
  | { type: "execa-result"; id: number; result: BrokerExecaResult };

export class SpawnBrokerError extends Error {
  readonly code = "ERR_SPAWN_BROKER_UNAVAILABLE";
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "SpawnBrokerError";
  }
}

export function serializeBrokerError(error: Error & Partial<NodeJS.ErrnoException>): BrokerError {
  return {
    message: error.message,
    code: error.code,
    errno: error.errno,
    syscall: error.syscall,
    path: error.path,
  };
}

export function brokerSpawnOptions(options: SpawnOptions): BrokerSpawnOptions | undefined {
  const stdio = options.stdio ?? "pipe";
  const entries = typeof stdio === "string" ? [stdio, stdio, stdio] : [...stdio];
  const normalized: BrokerSpawnOptions["stdio"] = [];
  for (let fd = 0; fd < Math.max(3, entries.length); fd += 1) {
    const entry = entries[fd] ?? (fd < 3 ? "pipe" : "ignore");
    // Only stdin is inherited by the broker. Numeric/anonymous descriptors stay host-owned.
    if (entry === "inherit" && fd !== 0) {
      return undefined;
    }
    if (entry !== "pipe" && entry !== "ignore" && entry !== "inherit" && entry !== "ipc") {
      return undefined;
    }
    normalized.push(entry);
  }
  if (options.signal || options.timeout || options.killSignal) {
    throw new Error("Unsupported spawn broker cancellation options");
  }
  return {
    cwd: options.cwd instanceof URL ? fileURLToPath(options.cwd) : options.cwd,
    env: options.env ? { ...options.env } : { ...process.env },
    argv0: options.argv0,
    detached: options.detached,
    shell: options.shell,
    windowsHide: options.windowsHide,
    windowsVerbatimArguments: options.windowsVerbatimArguments,
    serialization: options.serialization,
    uid: options.uid,
    gid: options.gid,
    stdio: normalized,
  };
}
