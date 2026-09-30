import { isRecord } from "@openclaw/normalization-core/record-coerce";
import { resolveGlobalSingleton } from "../shared/global-singleton.js";
import { hasSqliteWorkerActiveTransaction } from "./sqlite-worker-callback.worker.js";

export type SqliteWorkerModule = Readonly<Record<string, unknown>>;
type ModuleCode = {
  modules: Map<string, SqliteWorkerModule>;
  sourceLoaderRegistered: boolean;
  sourceLoaderRegistration?: Promise<void>;
};

// Built carriers and source domain modules must consume the same code in one physical worker.
const code = resolveGlobalSingleton<ModuleCode>(
  Symbol.for("openclaw.sqliteWorkerModuleCode"),
  () => ({ modules: new Map(), sourceLoaderRegistered: false }),
);

function normalizeSqliteWorkerModuleUrl(value: string): string {
  const url = new URL(value);
  if (url.protocol !== "file:" || url.search || url.hash) {
    throw new Error("SQLite worker module must be a static local module URL");
  }
  return url.href;
}

export function getLoadedSqliteWorkerModule(value: string): SqliteWorkerModule | undefined {
  return code.modules.get(normalizeSqliteWorkerModuleUrl(value));
}

function registerSourceLoader(sourceLoaderUrl?: string): Promise<void> | undefined {
  if (code.sourceLoaderRegistration) {
    return code.sourceLoaderRegistration;
  }
  if (code.sourceLoaderRegistered || !sourceLoaderUrl) {
    return undefined;
  }
  code.sourceLoaderRegistration = import(sourceLoaderUrl)
    .then((loader: unknown) => {
      if (!isRecord(loader) || typeof loader.register !== "function") {
        throw new Error("SQLite source worker loader is unavailable");
      }
      loader.register();
      code.sourceLoaderRegistered = true;
    })
    .finally(() => {
      code.sourceLoaderRegistration = undefined;
    });
  return code.sourceLoaderRegistration;
}

/** Cache hits are synchronous; code loading never invokes a factory or captures its authority. */
export function loadSqliteWorkerModule(
  value: string,
  sourceLoaderUrl?: string,
): SqliteWorkerModule | Promise<SqliteWorkerModule> {
  const url = normalizeSqliteWorkerModuleUrl(value);
  const loaded = code.modules.get(url);
  if (loaded) {
    return loaded;
  }
  if (hasSqliteWorkerActiveTransaction()) {
    throw new Error("SQLite callback module was not preloaded before its native transaction");
  }
  return load();

  async function load(): Promise<SqliteWorkerModule> {
    await registerSourceLoader(sourceLoaderUrl);
    const module: unknown = await import(url);
    if (!isRecord(module)) {
      throw new Error("SQLite worker module did not return module code");
    }
    code.modules.set(url, module);
    return module;
  }
}
