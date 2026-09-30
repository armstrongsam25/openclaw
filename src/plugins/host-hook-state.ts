import { isPromiseLike } from "@openclaw/normalization-core/promise-like";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import type { SessionEntry } from "../config/sessions.js";
import {
  resolveSessionEntryAccessTarget,
  updateResolvedSessionEntry,
} from "../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import {
  isPluginJsonValue,
  type PluginJsonValue,
  type PluginSessionExtensionProjection,
  type PluginSessionExtensionRegistration,
} from "./host-hooks.js";
import { getPluginRegistryForContext } from "./runtime/gateway-request-scope.js";
import { normalizeSessionEntrySlotKey } from "./session-entry-slot-keys.js";

const log = createSubsystemLogger("plugins/host-hook-state");
type MutableSessionEntry = SessionEntry & Record<string, unknown>;

export function getPluginSessionExtensionStateSync(params: {
  cfg: OpenClawConfig;
  pluginId: string;
  sessionKey?: string;
  agentId?: string;
}): Record<string, PluginJsonValue> | undefined {
  const pluginId = params.pluginId.trim();
  const sessionKey = normalizeOptionalString(params.sessionKey);
  if (!pluginId || !sessionKey) {
    return undefined;
  }
  const target = resolveSessionEntryAccessTarget({
    cfg: params.cfg,
    sessionKey,
    agentId: params.agentId,
  });
  const value = target.entry?.pluginExtensions?.[pluginId] as
    | Record<string, PluginJsonValue>
    | undefined;
  return value ? structuredClone(value) : undefined;
}

export async function patchPluginSessionExtension(params: {
  cfg: OpenClawConfig;
  sessionKey: string;
  agentId?: string;
  pluginId: string;
  namespace: string;
  value?: PluginJsonValue;
  unset?: boolean;
  assertCurrent?: () => void;
}): Promise<{ ok: true; key: string; value?: PluginJsonValue } | { ok: false; error: string }> {
  const namespace = params.namespace.trim();
  const pluginId = params.pluginId.trim();
  if (!pluginId || !namespace) {
    return { ok: false, error: "pluginId and namespace are required" };
  }
  if (params.unset === true && params.value !== undefined) {
    return { ok: false, error: "plugin session extension cannot specify both unset and value" };
  }
  if (params.value !== undefined && !isPluginJsonValue(params.value)) {
    return { ok: false, error: "plugin session extension value must be JSON-compatible" };
  }
  if (params.unset !== true && params.value === undefined) {
    return { ok: false, error: "plugin session extension value is required unless unset is true" };
  }
  const nextPluginValue = params.value as PluginJsonValue;
  const registry = getPluginRegistryForContext();
  const registration = (registry?.sessionExtensions ?? []).find(
    (entry) => entry.pluginId === pluginId && entry.extension.namespace === namespace,
  );
  if (!registration) {
    return { ok: false, error: `unknown plugin session extension: ${pluginId}/${namespace}` };
  }
  // Promote the projected value into a top-level SessionEntry slot when the
  // extension opted in via `sessionEntrySlotKey`. The slot is a read-only
  // mirror: writes still go through patchSessionExtension; the host overwrites
  // the slot value on every patch and clears it on unset.
  const rawSlotKey = normalizeOptionalString(registration.extension.sessionEntrySlotKey);
  const normalizedSlotKey = rawSlotKey ? normalizeSessionEntrySlotKey(rawSlotKey) : undefined;
  if (normalizedSlotKey?.ok === false) {
    log.warn(
      `plugin session extension slot promotion skipped for ${pluginId}/${namespace}: ${normalizedSlotKey.error}`,
    );
  }
  const slotKey = normalizedSlotKey?.ok === true ? normalizedSlotKey.key : undefined;
  const updated = await updateResolvedSessionEntry(
    {
      cfg: params.cfg,
      sessionKey: params.sessionKey,
      agentId: params.agentId,
    },
    (entry, context) => {
      params.assertCurrent?.();
      const entryRecord = entry as MutableSessionEntry;
      const pluginExtensions = { ...entry.pluginExtensions };
      const pluginState = { ...pluginExtensions[pluginId] };
      if (params.unset === true) {
        delete pluginState[namespace];
      } else {
        pluginState[namespace] = structuredClone(nextPluginValue);
      }
      if (Object.keys(pluginState).length > 0) {
        pluginExtensions[pluginId] = pluginState;
      } else {
        delete pluginExtensions[pluginId];
      }
      if (Object.keys(pluginExtensions).length > 0) {
        entry.pluginExtensions = pluginExtensions;
      } else {
        delete entry.pluginExtensions;
      }
      const storedSlotKeys = { ...entry.pluginExtensionSlotKeys };
      const pluginSlotKeys = { ...storedSlotKeys[pluginId] };
      const previousSlotKey = normalizeSessionEntrySlotKey(pluginSlotKeys[namespace]);
      if (previousSlotKey.ok && previousSlotKey.key !== slotKey) {
        delete entryRecord[previousSlotKey.key];
      }
      if (slotKey && params.unset !== true) {
        pluginSlotKeys[namespace] = slotKey;
      } else {
        delete pluginSlotKeys[namespace];
      }
      if (Object.keys(pluginSlotKeys).length > 0) {
        storedSlotKeys[pluginId] = pluginSlotKeys;
      } else {
        delete storedSlotKeys[pluginId];
      }
      if (Object.keys(storedSlotKeys).length > 0) {
        entry.pluginExtensionSlotKeys = storedSlotKeys;
      } else {
        delete entry.pluginExtensionSlotKeys;
      }
      if (slotKey) {
        const projected = projectSessionExtensionValue({
          pluginId: registration.pluginId,
          namespace: registration.extension.namespace,
          project: registration.extension.project,
          sessionKey: context.canonicalKey,
          sessionId: entry.sessionId,
          state: params.unset === true ? undefined : nextPluginValue,
        });
        if (projected === undefined) {
          delete entryRecord[slotKey];
        } else {
          entryRecord[slotKey] = projected;
        }
      }
      entry.updatedAt = Date.now();
      return pluginState[namespace] as PluginJsonValue | undefined;
    },
  );
  if (!updated.found) {
    return { ok: false, error: `unknown session key: ${params.sessionKey}` };
  }
  return { ok: true, key: updated.canonicalKey, value: updated.result };
}

export function projectPluginSessionExtensionsSync(params: {
  sessionKey: string;
  entry: SessionEntry;
}): PluginSessionExtensionProjection[] {
  const registry = getPluginRegistryForContext();
  const extensions = registry?.sessionExtensions ?? [];
  if (extensions.length === 0) {
    return [];
  }
  const projections: PluginSessionExtensionProjection[] = [];
  for (const registration of extensions) {
    const state = params.entry.pluginExtensions?.[registration.pluginId]?.[
      registration.extension.namespace
    ] as PluginJsonValue | undefined;
    const projected = projectSessionExtensionValue({
      pluginId: registration.pluginId,
      namespace: registration.extension.namespace,
      project: registration.extension.project,
      sessionKey: params.sessionKey,
      sessionId: params.entry.sessionId,
      state,
    });
    if (projected !== undefined) {
      projections.push({
        pluginId: registration.pluginId,
        namespace: registration.extension.namespace,
        value: projected,
      });
    }
  }
  return projections;
}

function projectSessionExtensionValue(params: {
  pluginId: string;
  namespace: string;
  project?: PluginSessionExtensionRegistration["project"];
  sessionKey: string;
  sessionId?: string;
  state: PluginJsonValue | undefined;
}): PluginJsonValue | undefined {
  if (params.state === undefined) {
    return undefined;
  }
  let projected: unknown;
  try {
    projected = params.project
      ? params.project({
          sessionKey: params.sessionKey,
          sessionId: params.sessionId,
          state: params.state,
        })
      : params.state;
  } catch (error) {
    log.warn(
      `plugin session extension projection failed: plugin=${params.pluginId} namespace=${params.namespace} error=${String(error)}`,
    );
    return undefined;
  }
  if (isPromiseLike(projected)) {
    void Promise.resolve(projected).catch(() => undefined);
    return undefined;
  }
  // Both plugin projections and persisted state must satisfy the same size and shape bounds.
  return isPluginJsonValue(projected) ? structuredClone(projected) : undefined;
}
