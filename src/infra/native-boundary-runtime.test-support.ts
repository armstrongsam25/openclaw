export const nativeBoundaryTestEntrypoints = {
  unhandledRejections: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "unhandled-rejections",
    distWorkerPath: "infra/unhandled-rejections.js",
  },
  pluginHooks: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "../plugins/hooks",
    distWorkerPath: "plugins/hooks.js",
  },
  emptyPluginRegistry: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "../plugins/registry-empty",
    distWorkerPath: "plugins/registry-empty.js",
  },
  cliProfile: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "../cli/profile",
    distWorkerPath: "cli/profile.js",
  },
  handoffProcess: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "update-managed-service-handoff-process",
    distWorkerPath: "infra/update-managed-service-handoff-process.js",
  },
  handoffDatabase: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "update-managed-service-handoff-database",
    distWorkerPath: "infra/update-managed-service-handoff-database.js",
  },
  gatewayOwnerLease: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "gateway-owner-lease",
    distWorkerPath: "infra/gateway-owner-lease.js",
  },
  gatewayLock: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "gateway-lock",
    distWorkerPath: "infra/gateway-lock.js",
  },
  boundaryPath: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "boundary-path",
    distWorkerPath: "infra/boundary-path.js",
  },
  stateDatabasePaths: {
    currentModuleUrl: import.meta.url,
    sourceWorkerName: "../state/openclaw-state-db.paths",
    distWorkerPath: "state/openclaw-state-db.paths.js",
  },
} as const;
