import { randomUUID } from "node:crypto";
import { asOptionalObjectRecord } from "@openclaw/normalization-core/record-coerce";
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import {
  resolveSessionEntryAccessTarget,
  updateResolvedSessionEntry,
} from "../config/sessions/session-accessor.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { resolvePromptInjectionAllowed } from "./hook-policy-decisions.js";
import {
  buildPluginAgentTurnPrepareContext,
  isPluginJsonValue,
  type PluginAgentTurnPrepareResult,
  type PluginNextTurnInjection,
  type PluginNextTurnInjectionEnqueueResult,
  type PluginNextTurnInjectionRecord,
} from "./host-hooks.js";
import { getPluginRegistryForContext } from "./runtime/gateway-request-scope.js";

const MAX_PLUGIN_NEXT_TURN_INJECTION_TEXT_LENGTH = 32 * 1024;
const MAX_PLUGIN_NEXT_TURN_INJECTION_IDEMPOTENCY_KEY_LENGTH = 512;
const MAX_PLUGIN_NEXT_TURN_INJECTIONS_PER_SESSION = 32;

function isPluginNextTurnInjectionPlacement(
  value: unknown,
): value is PluginNextTurnInjectionRecord["placement"] {
  return value === "prepend_context" || value === "append_context";
}

function isPluginNextTurnInjectionRecord(value: unknown): value is PluginNextTurnInjectionRecord {
  const candidate = asOptionalObjectRecord(value);
  if (!candidate) {
    return false;
  }
  return (
    typeof candidate.id === "string" &&
    typeof candidate.pluginId === "string" &&
    typeof candidate.text === "string" &&
    typeof candidate.createdAt === "number" &&
    Number.isFinite(candidate.createdAt) &&
    isPluginNextTurnInjectionPlacement(candidate.placement) &&
    (candidate.ttlMs === undefined ||
      (typeof candidate.ttlMs === "number" &&
        Number.isFinite(candidate.ttlMs) &&
        candidate.ttlMs >= 0)) &&
    (candidate.idempotencyKey === undefined || typeof candidate.idempotencyKey === "string")
  );
}

function isExpired(entry: unknown, now: number) {
  if (!isPluginNextTurnInjectionRecord(entry)) {
    return true;
  }
  return entry.ttlMs !== undefined && now - entry.createdAt > entry.ttlMs;
}

function toPluginNextTurnInjectionRecord(params: {
  pluginId: string;
  pluginName?: string;
  injection: PluginNextTurnInjection;
  now: number;
}): PluginNextTurnInjectionRecord {
  return {
    id: params.injection.idempotencyKey?.trim() || randomUUID(),
    pluginId: params.pluginId,
    pluginName: params.pluginName,
    text: params.injection.text,
    idempotencyKey: params.injection.idempotencyKey?.trim() || undefined,
    placement: params.injection.placement ?? "prepend_context",
    ttlMs: params.injection.ttlMs,
    createdAt: params.now,
    metadata: params.injection.metadata,
  };
}

export async function enqueuePluginNextTurnInjection(params: {
  cfg: OpenClawConfig;
  pluginId: string;
  pluginName?: string;
  injection: PluginNextTurnInjection;
  now?: number;
}): Promise<PluginNextTurnInjectionEnqueueResult> {
  const sessionKey = normalizeOptionalString(params.injection.sessionKey) ?? "";
  if (!sessionKey) {
    return { enqueued: false, id: "", sessionKey };
  }
  const text = normalizeOptionalString(params.injection.text);
  if (
    !text ||
    text.length > MAX_PLUGIN_NEXT_TURN_INJECTION_TEXT_LENGTH ||
    (params.injection.metadata !== undefined && !isPluginJsonValue(params.injection.metadata)) ||
    (params.injection.idempotencyKey !== undefined &&
      (typeof params.injection.idempotencyKey !== "string" ||
        params.injection.idempotencyKey.trim().length === 0 ||
        params.injection.idempotencyKey.length >
          MAX_PLUGIN_NEXT_TURN_INJECTION_IDEMPOTENCY_KEY_LENGTH)) ||
    (params.injection.placement !== undefined &&
      !isPluginNextTurnInjectionPlacement(params.injection.placement)) ||
    (params.injection.ttlMs !== undefined &&
      (!Number.isFinite(params.injection.ttlMs) || params.injection.ttlMs < 0))
  ) {
    return { enqueued: false, id: "", sessionKey };
  }
  const now = params.now ?? Date.now();
  const record = toPluginNextTurnInjectionRecord({
    pluginId: params.pluginId,
    pluginName: params.pluginName,
    injection: { ...params.injection, sessionKey, text },
    now,
  });
  const scope = { cfg: params.cfg, sessionKey, agentId: params.injection.agentId };
  const updated = await updateResolvedSessionEntry(scope, (entry) => {
    const injections = { ...entry.pluginNextTurnInjections };
    // Guard against malformed/hand-edited persisted state — a non-array value
    // here would crash the spread/filter and break the whole session's enqueue.
    const rawExisting = injections[params.pluginId];
    const existing = (Array.isArray(rawExisting) ? rawExisting : []).filter(
      (candidate): candidate is PluginNextTurnInjectionRecord => !isExpired(candidate, now),
    );
    const duplicate = record.idempotencyKey
      ? existing.find((candidate) => candidate.idempotencyKey === record.idempotencyKey)
      : undefined;
    const enqueued = !duplicate && existing.length < MAX_PLUGIN_NEXT_TURN_INJECTIONS_PER_SESSION;
    injections[params.pluginId] = enqueued ? [...existing, record] : existing;
    entry.pluginNextTurnInjections = injections;
    if (enqueued) {
      entry.updatedAt = now;
    }
    return { enqueued, id: duplicate?.id ?? record.id };
  });
  if (!updated.found) {
    return { enqueued: false, id: "", sessionKey };
  }
  return { ...updated.result, sessionKey: updated.canonicalKey };
}

async function drainPluginNextTurnInjections(
  params: Parameters<typeof drainPluginNextTurnInjectionContext>[0],
): Promise<PluginNextTurnInjectionRecord[]> {
  const sessionKey = params.sessionKey?.trim();
  if (!sessionKey) {
    return [];
  }
  const scope = { cfg: params.cfg, sessionKey, agentId: params.agentId };
  const { entry: selectedEntry } = resolveSessionEntryAccessTarget(scope);
  // Empty queues need no qualified mutation target. Concurrent enqueues wait for the next turn.
  if (
    !selectedEntry?.pluginNextTurnInjections ||
    Object.keys(selectedEntry.pluginNextTurnInjections).length === 0
  ) {
    return [];
  }
  const target = resolveSessionEntryAccessTarget(scope, { keyFormat: "agent-qualified" });
  const now = params.now ?? Date.now();
  const updated = await updateResolvedSessionEntry(
    scope,
    (entry) => {
      if (!entry?.pluginNextTurnInjections) {
        return [];
      }
      const activePluginIds = new Set(
        (getPluginRegistryForContext()?.plugins ?? [])
          .filter((plugin) => plugin.status === "loaded")
          .map((plugin) => plugin.id),
      );
      const drained: PluginNextTurnInjectionRecord[] = [];
      for (const [pluginId, entries] of Object.entries(entry.pluginNextTurnInjections)) {
        if (
          !activePluginIds.has(pluginId) ||
          !resolvePromptInjectionAllowed(params.cfg.plugins?.entries?.[pluginId]?.hooks)
        ) {
          continue;
        }
        // Guard against malformed/hand-edited persisted state — a non-array value
        // here would crash .filter and break prompt-building for the session.
        if (!Array.isArray(entries)) {
          continue;
        }
        const liveEntries = entries.filter(
          (candidate): candidate is PluginNextTurnInjectionRecord => !isExpired(candidate, now),
        );
        drained.push(...liveEntries);
      }
      drained.sort((left, right) => left.createdAt - right.createdAt);
      // A drain is the consume boundary for this session queue. Inactive plugin
      // records are stale owner state and are discarded with expired records.
      delete entry.pluginNextTurnInjections;
      if (drained.length > 0) {
        entry.updatedAt = now;
      }
      return drained;
    },
    { target },
  );
  return updated.found ? updated.result : [];
}

export async function drainPluginNextTurnInjectionContext(params: {
  cfg: OpenClawConfig;
  sessionKey?: string;
  agentId?: string;
  now?: number;
}): Promise<PluginAgentTurnPrepareResult & { queuedInjections: PluginNextTurnInjectionRecord[] }> {
  const queuedInjections = await drainPluginNextTurnInjections(params);
  return {
    queuedInjections,
    ...buildPluginAgentTurnPrepareContext({ queuedInjections }),
  };
}
