// Register shared provider boundaries before loading the dispatch implementation.
import "./dispatch-acp.shared.test-harness.js";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  getAdmittedRunDelegatedAuthority,
  type AdmittedRunContext,
} from "../../agents/admitted-run-context.js";
import {
  loadSessionEntryReadOnly,
  loadTranscriptEvents,
  upsertSessionEntryCore,
} from "../../config/sessions/session-accessor.js";
import * as sessionAccess from "../../config/sessions/session-accessor.js";
import * as sessionEntry from "../../config/sessions/session-accessor.sqlite-entry.js";
import { tryDispatchAcpReplyHook } from "../../plugin-sdk/acpx.js";
import {
  getGlobalPluginRegistry,
  initializeGlobalHookRunner,
  resetGlobalHookRunner,
} from "../../plugins/hook-runner-global.js";
import { createHookRunnerWithRegistry } from "../../plugins/hooks.test-fixtures.js";
import { createUserTurnTranscriptRecorder } from "../../sessions/user-turn-transcript.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { createAcpSourceTranscriptFixture, runDispatch } from "./dispatch-acp.test-support.js";
import { buildTestCtx } from "./test-ctx.js";
import {
  createAcpSessionMeta,
  createAcpTestConfig,
  createAcpTestReplyDispatcherFixture as createDispatcher,
} from "./test-fixtures/acp-runtime.js";

const {
  auditMocks,
  bindingServiceMocks,
  managerMocks,
  sessionBinding,
  sessionKey,
  acpAttachmentBuffers,
  ACP_PNG_IMAGE_BYTES,
} = await import("./dispatch-acp.shared.test-harness.js");

describe("ACP source input lifecycle", () => {
  it("dispatches supplied transcript-only input after its real recorder commits without a source row", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const source = {
        agentId: "codex-acp",
        sessionKey: "agent:codex-acp:discord:channel:transcript-only",
        sessionId: "supplied-transcript-only-source",
        storePath: path.join(state.sessionsDir("codex-acp"), "sessions.json"),
      };
      const text = "Accept this supplied transcript-only input through ACP.";
      const recorder = createUserTurnTranscriptRecorder({
        target: { ...source, sessionEntry: undefined },
        input: { text },
      });
      expect(loadSessionEntryReadOnly(source)).toBeUndefined();
      let eventsAtSubmission: Awaited<ReturnType<typeof loadTranscriptEvents>> = [];
      let turnAdmission: AdmittedRunContext | undefined;
      const { emitAcpLifecycleEnd } = await vi.importActual<
        typeof import("../../agents/command/acp-lifecycle.js")
      >("../../agents/command/acp-lifecycle.js");
      auditMocks.emitAcpLifecycleEnd.mockImplementationOnce(emitAcpLifecycleEnd);
      managerMocks.runTurn.mockImplementationOnce(
        async ({
          admittedRunContext,
          onEvent,
        }: {
          admittedRunContext: AdmittedRunContext;
          onEvent: (event: unknown) => Promise<void>;
        }) => {
          turnAdmission = admittedRunContext;
          eventsAtSubmission = await loadTranscriptEvents(source);
          await onEvent({ type: "done", status: "completed" });
        },
      );
      const result = await runDispatch({
        bodyForAgent: text,
        cfg: createAcpTestConfig({ session: { store: source.storePath } }),
        ctxOverrides: { SessionKey: source.sessionKey, RawBody: text },
        userTurnTranscriptRecorder: recorder,
      });
      expect(recorder.hasPersisted()).toBe(true);
      expect(recorder.getAdmissionReceipt()).toMatchObject({
        sessionId: source.sessionId,
        sessionKey: source.sessionKey,
      });
      expect(managerMocks.runTurn).toHaveBeenCalledOnce();
      expect(result).toEqual({ queuedFinal: false, counts: { tool: 0, block: 0, final: 0 } });
      expect(eventsAtSubmission).toContainEqual(
        expect.objectContaining({
          type: "message",
          message: expect.objectContaining({ role: "user", content: text }),
        }),
      );
      expect(loadSessionEntryReadOnly(source)).toBeUndefined();
      if (!turnAdmission) {
        throw new Error("Transcript-only ACP turn was not admitted");
      }
      expect(getAdmittedRunDelegatedAuthority(turnAdmission)).toBeUndefined();
    });
  });

  it("keeps ACP handled through the default claiming hook when source settlement rejects", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const { target, entry, recorder } = await createAcpSourceTranscriptFixture(
        state,
        sessionKey,
        "acp-settlement-write-failure",
        "Deliver this request once through ACP.",
      );
      managerMocks.resolveSessionAsync.mockResolvedValue({
        kind: "ready",
        sessionKey,
        agentId: target.agentId,
        meta: createAcpSessionMeta(),
        entry,
      });
      const text = "ACP already completed the accepted request.";
      const { emitAcpLifecycleEnd } = await vi.importActual<
        typeof import("../../agents/command/acp-lifecycle.js")
      >("../../agents/command/acp-lifecycle.js");
      auditMocks.emitAcpLifecycleEnd.mockImplementationOnce(emitAcpLifecycleEnd);
      const patch = vi.spyOn(sessionEntry, "patchSessionEntryCore");
      let turnAdmission: AdmittedRunContext | undefined;
      let settlementRejected = false;
      managerMocks.runTurn.mockImplementationOnce(
        async ({
          admittedRunContext,
          onEvent,
        }: {
          admittedRunContext: AdmittedRunContext;
          onEvent: (event: unknown) => Promise<void>;
        }) => {
          turnAdmission = admittedRunContext;
          await onEvent({ type: "text_delta", text, tag: "agent_message_chunk" });
          await onEvent({ type: "done", status: "completed" });
          // Admission and ACP side effects succeeded; only terminal source persistence fails.
          patch.mockImplementationOnce(async () => {
            settlementRejected = true;
            throw new Error("ACP source SQLite settlement unavailable");
          });
        },
      );
      const { dispatcher } = createDispatcher();
      const event = {
        ctx: buildTestCtx({
          Provider: "webchat",
          Surface: "webchat",
          SessionKey: sessionKey,
          BodyForAgent: "Deliver this request once through ACP.",
        }),
        runId: "acp-settlement-write-failure",
        sessionKey,
        inboundAudio: false,
        shouldRouteToOriginating: false,
        shouldSendToolSummaries: true,
        shouldSendFullToolDetails: false,
        sendPolicy: "allow" as const,
      };
      const hookContext = {
        cfg: createAcpTestConfig({ session: { store: target.storePath } }),
        dispatcher,
        userTurnTranscriptRecorder: recorder,
        recordProcessed: vi.fn(),
        markIdle: vi.fn(),
      };
      const fallback = vi.fn(() => ({
        handled: true,
        queuedFinal: false,
        counts: { tool: 0, block: 0, final: 0 },
      }));
      const { runner } = createHookRunnerWithRegistry([
        {
          hookName: "reply_dispatch",
          pluginId: "acpx",
          priority: 10,
          handler: () => tryDispatchAcpReplyHook(event, hookContext),
        },
        { hookName: "reply_dispatch", pluginId: "fallback", handler: fallback },
      ]);
      try {
        const result = await runner.runReplyDispatch(event, hookContext);
        expect(settlementRejected).toBe(true);
        expect(result?.handled).toBe(true);
        expect(fallback).not.toHaveBeenCalled();
        expect(managerMocks.runTurn).toHaveBeenCalledOnce();
        const deliveredText = [
          ...vi.mocked(dispatcher.sendBlockReply).mock.calls,
          ...vi.mocked(dispatcher.sendFinalReply).mock.calls,
        ]
          .map(([payload]) => payload.text ?? "")
          .join("");
        expect(deliveredText).toContain(text);
        expect(loadSessionEntryReadOnly(target)).toMatchObject({
          status: "running",
          activeWriterRunId: event.runId,
          acpSourceTurn: { sourceSessionId: target.sessionId, runId: event.runId },
        });
        if (!turnAdmission) {
          throw new Error("ACP turn was not admitted");
        }
        expect(getAdmittedRunDelegatedAuthority(turnAdmission)).toBeUndefined();
      } finally {
        patch.mockRestore();
      }
    });
  });

  it("persists canonical source ownership before public ACP takeover without an ingress recorder", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const sourceSessionKey = "agent:codex-acp:discord:channel:thread-1";
      const targetSessionKey = "agent:codex-acp:acp:bound";
      const { target } = await createAcpSourceTranscriptFixture(
        state,
        sourceSessionKey,
        "acp-public-without-recorder",
        "Original accepted user request.",
      );
      await upsertSessionEntryCore(target, {
        sessionId: target.sessionId,
        updatedAt: 1,
        status: "done",
      });
      const acpTarget = { ...target, sessionKey: targetSessionKey, sessionId: "bound-acp-target" };
      const entry = await upsertSessionEntryCore(acpTarget, {
        sessionId: acpTarget.sessionId,
        updatedAt: 1,
        acp: createAcpSessionMeta(),
      });
      bindingServiceMocks.resolveByConversation.mockReturnValue(sessionBinding(targetSessionKey));
      managerMocks.resolveSessionAsync.mockResolvedValue({
        kind: "ready",
        sessionKey: targetSessionKey,
        agentId: target.agentId,
        meta: createAcpSessionMeta(),
        entry,
      });
      let submitted = false;
      let sourceAtSubmission: ReturnType<typeof loadSessionEntryReadOnly> = undefined;
      let eventsAtSubmission: Awaited<ReturnType<typeof loadTranscriptEvents>> = [];
      const { emitAcpLifecycleEnd } = await vi.importActual<
        typeof import("../../agents/command/acp-lifecycle.js")
      >("../../agents/command/acp-lifecycle.js");
      auditMocks.emitAcpLifecycleEnd.mockImplementationOnce(emitAcpLifecycleEnd);
      managerMocks.runTurn.mockImplementationOnce(
        async ({ onEvent }: { onEvent: (event: unknown) => Promise<void> }) => {
          submitted = true;
          sourceAtSubmission = loadSessionEntryReadOnly(target);
          eventsAtSubmission = await loadTranscriptEvents(target);
          await onEvent({ type: "done", status: "completed" });
        },
      );
      const beforeMessageWrite = vi.fn((event: unknown) => {
        const { message } = event as { message: Record<string, unknown> };
        return { message: { ...message, content: "Approved original accepted user request." } };
      });
      const { registry } = createHookRunnerWithRegistry([
        {
          hookName: "reply_dispatch",
          pluginId: "acpx",
          handler: (event, context) =>
            tryDispatchAcpReplyHook(
              event as Parameters<typeof tryDispatchAcpReplyHook>[0],
              context as Parameters<typeof tryDispatchAcpReplyHook>[1],
            ),
        },
        { hookName: "before_message_write", pluginId: "input-policy", handler: beforeMessageWrite },
      ]);
      const previousRegistry = getGlobalPluginRegistry();
      const runtimePlugins = await import("../../agents/runtime-plugins.js");
      const loadRegistry = vi
        .spyOn(runtimePlugins, "loadAgentRuntimePluginRegistryHandle")
        .mockReturnValue(registry);
      initializeGlobalHookRunner(registry);
      const replyResolver = vi.fn(async () => ({ text: "native fallback" }));
      const imagePath = "/tmp/acp-source-original.png";
      acpAttachmentBuffers.set(imagePath, ACP_PNG_IMAGE_BYTES);
      try {
        const { dispatchReplyFromConfig } = await import("./dispatch-from-config.js");
        await dispatchReplyFromConfig({
          cfg: createAcpTestConfig({
            session: { store: target.storePath },
            agents: { entries: { "codex-acp": {} }, defaults: { workspace: state.workspaceDir } },
            plugins: { enabled: false },
          }),
          ctx: buildTestCtx({
            Provider: "discord",
            Surface: "discord",
            From: "discord:channel:thread-1",
            To: "thread-1",
            ChatType: "channel",
            SessionKey: sourceSessionKey,
            Body: "Original accepted user request.",
            RawBody: "Original accepted user request.",
            BodyForAgent: "Prepared ACP prompt with image context.",
            MessageSid: "source-message-no-recorder",
            Timestamp: 1_700_000_000_000,
            SenderId: "original-channel-person",
            media: [{ kind: "image", path: imagePath, contentType: "image/png" }],
          }),
          dispatcher: createDispatcher().dispatcher,
          replyOptions: { runId: "acp-public-without-recorder" },
          replyResolver,
        });
        expect(submitted).toBe(true);
        expect(replyResolver).not.toHaveBeenCalled();
        expect(sourceAtSubmission).toMatchObject({
          status: "running",
          activeWriterRunId: "acp-public-without-recorder",
          acpSourceTurn: {
            sourceSessionId: target.sessionId,
            runId: "acp-public-without-recorder",
          },
        });
        expect(loadSessionEntryReadOnly(acpTarget)?.acpSourceTurn).toBeUndefined();
        expect(loadSessionEntryReadOnly(target)).toMatchObject({
          status: "done",
        });
        expect(loadSessionEntryReadOnly(target)?.acpSourceTurn).toBeUndefined();
        expect(beforeMessageWrite).toHaveBeenCalledWith(
          expect.objectContaining({
            message: expect.objectContaining({ content: "Original accepted user request." }),
          }),
          expect.objectContaining({ sessionKey: sourceSessionKey }),
        );
        expect(eventsAtSubmission).toContainEqual(
          expect.objectContaining({
            type: "message",
            message: expect.objectContaining({
              role: "user",
              content: "Approved original accepted user request.",
              timestamp: 1_700_000_000_000,
              __openclaw: expect.objectContaining({
                senderId: "original-channel-person",
                transport: expect.objectContaining({
                  channel: "discord",
                  messageId: "source-message-no-recorder",
                }),
                media: [
                  expect.objectContaining({
                    kind: "image",
                    path: imagePath,
                    contentType: "image/png",
                  }),
                ],
                mediaImageLayout: { slots: [{ kind: "offloaded", factIndex: 0 }] },
              }),
            }),
          }),
        );
      } finally {
        loadRegistry.mockRestore();
        if (previousRegistry) {
          initializeGlobalHookRunner(previousRegistry);
        } else {
          resetGlobalHookRunner();
        }
      }
    });
  });

  it.each([undefined, "captured-source-revision"])(
    "rejects an ACP input append after captured source revision %s changes",
    async (revision) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const sourceSessionKey = "agent:codex-acp:discord:channel:generation-race";
        const { target } = await createAcpSourceTranscriptFixture(
          state,
          sourceSessionKey,
          "acp-input-generation-race",
          "Keep this in the admitted source.",
        );
        await sessionEntry.patchSessionEntryCore(target, () => ({
          status: "done",
          lifecycleRevision: revision,
        }));
        const originalPersist = sessionAccess.persistSessionTranscriptTurn;
        let changed = false;
        const persist = vi
          .spyOn(sessionAccess, "persistSessionTranscriptTurn")
          .mockImplementationOnce(async (scope, options) => {
            changed = true;
            await sessionEntry.patchSessionEntryCore(target, () => ({
              lifecycleRevision: "successor-source-revision",
            }));
            return await originalPersist(scope, options);
          });
        try {
          await runDispatch({
            bodyForAgent: "Keep this in the admitted source.",
            cfg: createAcpTestConfig({ session: { store: target.storePath } }),
            ctxOverrides: {
              SessionKey: sourceSessionKey,
              RawBody: "Keep this in the admitted source.",
            },
          });
          expect(changed).toBe(true);
          expect(managerMocks.runTurn).not.toHaveBeenCalled();
          expect(
            (await loadTranscriptEvents(target)).filter(
              (event) =>
                event.type === "message" && (event.message as { role?: string })?.role === "user",
            ),
          ).toHaveLength(0);
          expect(loadSessionEntryReadOnly(target)).toMatchObject({
            status: "done",
            lifecycleRevision: "successor-source-revision",
          });
          expect(loadSessionEntryReadOnly(target)?.acpSourceTurn).toBeUndefined();
        } finally {
          persist.mockRestore();
        }
      });
    },
  );

  it("keeps canonical source input policy refusal ahead of ACP submission", async () => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const { target } = await createAcpSourceTranscriptFixture(
        state,
        "agent:codex-acp:discord:channel:blocked-input",
        "blocked-source-input",
        "Refuse this input before submission.",
      );
      const policy = vi.fn(() => ({ block: true }));
      const { registry } = createHookRunnerWithRegistry([
        { hookName: "before_message_write", pluginId: "input-policy", handler: policy },
      ]);
      const previousRegistry = getGlobalPluginRegistry();
      initializeGlobalHookRunner(registry);
      try {
        await runDispatch({
          bodyForAgent: "Refuse this input before submission.",
          cfg: createAcpTestConfig({ session: { store: target.storePath } }),
          ctxOverrides: {
            SessionKey: target.sessionKey,
            RawBody: "Refuse this input before submission.",
          },
        });
        expect(policy).toHaveBeenCalled();
        expect(managerMocks.runTurn).not.toHaveBeenCalled();
        expect(loadSessionEntryReadOnly(target)?.acpSourceTurn).toBeUndefined();
        expect(
          (await loadTranscriptEvents(target)).filter((event) => event.type === "message"),
        ).toHaveLength(0);
      } finally {
        if (previousRegistry) {
          initializeGlobalHookRunner(previousRegistry);
        } else {
          resetGlobalHookRunner();
        }
      }
    });
  });

  it.each([undefined, "agent:codex-acp:discord:channel:absent-source"])(
    "preserves transcript-only ACP dispatch when canonical source %s is absent",
    async (sourceSessionKey) => {
      await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
        const { target, entry } = await createAcpSourceTranscriptFixture(
          state,
          sessionKey,
          "legacy-target-only",
          "Target transcript only.",
        );
        managerMocks.resolveSessionAsync.mockResolvedValue({
          kind: "ready",
          sessionKey,
          agentId: target.agentId,
          meta: createAcpSessionMeta(),
          entry,
        });
        await runDispatch({
          bodyForAgent: "Existing transcript-only ACP request.",
          cfg: createAcpTestConfig({ session: { store: target.storePath } }),
          ctxOverrides: { SessionKey: sourceSessionKey },
        });
        expect(managerMocks.runTurn).toHaveBeenCalledOnce();
        expect(loadSessionEntryReadOnly(target)?.acpSourceTurn).toBeUndefined();
        if (sourceSessionKey) {
          expect(
            loadSessionEntryReadOnly({ ...target, sessionKey: sourceSessionKey }),
          ).toBeUndefined();
        }
      });
    },
  );
});
