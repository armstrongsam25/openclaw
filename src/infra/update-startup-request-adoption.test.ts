import { expect, it, vi } from "vitest";
import { createDeferred } from "../../test/helpers/promise.js";
import { closeOpenClawStateDatabaseForTest } from "../state/openclaw-state-db.js";
import { createTestGatewayScheduler } from "../test-utils/gateway-scheduler-clock.js";
import { createOpenClawTestState } from "../test-utils/openclaw-test-state.js";
import { currentUpdateCheckLifecycle } from "./update-check-lifecycle.js";
import type { UpdateCheckResult } from "./update-check.js";
import { createUpdateRun } from "./update-run-ledger.js";

const checkUpdateStatus = vi.hoisted(() =>
  vi.fn<typeof import("./update-check.js").checkUpdateStatus>(),
);

vi.mock("./update-check.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./update-check.js")>()),
  checkUpdateStatus,
}));

it.each(["complete", "close"] as const)(
  "preserves early status and update admission discovery when the scheduler attaches (%s)",
  async (outcome) => {
    const state = await createOpenClawTestState({ label: "update-request-adoption" });
    const { createGatewayUpdateCheck, resetUpdateAvailableStateForTest } =
      await import("./update-startup.js");
    const scheduler = createTestGatewayScheduler();
    resetUpdateAvailableStateForTest(scheduler);
    const { createGatewayUpdateLifecycle } = await import("./update-check-lifecycle.js");
    const lifecycle = createGatewayUpdateLifecycle(scheduler);
    const started = createDeferred<AbortSignal | undefined>();
    const probe = createDeferred<UpdateCheckResult>();
    const status: UpdateCheckResult = { root: null, installKind: "package", packageManager: "npm" };
    checkUpdateStatus.mockReset().mockImplementation(({ signal }) => {
      started.resolve(signal);
      return probe.promise;
    });
    const statusRequest = currentUpdateCheckLifecycle().initialize();
    const admissionRequest = currentUpdateCheckLifecycle().initialize();
    let owner: ReturnType<typeof createGatewayUpdateCheck> | undefined;
    let initialization: ReturnType<typeof lifecycle.initialize> | undefined;
    try {
      const signal = await started.promise;
      owner = createGatewayUpdateCheck({
        lifecycle,
        getConfig: () => ({}),
        log: { info: vi.fn() },
        isNixMode: false,
      });
      expect(signal?.aborted).toBe(false);
      initialization = owner.initialize();
      if (outcome === "close") {
        let stopped = false;
        const stopping = owner.stop().then(() => {
          stopped = true;
        });
        expect(signal?.aborted).toBe(true);
        await Promise.resolve();
        expect(stopped).toBe(false);
        probe.resolve(status);
        await Promise.all([
          expect(statusRequest).rejects.toMatchObject({ name: "AbortError" }),
          expect(admissionRequest).rejects.toMatchObject({ name: "AbortError" }),
          expect(initialization).rejects.toMatchObject({ name: "AbortError" }),
          stopping,
        ]);
        expect(stopped).toBe(true);
      } else {
        probe.resolve(status);
        await expect(statusRequest).resolves.toMatchObject({ status });
        await expect(admissionRequest).resolves.toMatchObject({ status });
        await expect(initialization).resolves.toMatchObject({ status });
      }
      expect(checkUpdateStatus).toHaveBeenCalledOnce();
    } finally {
      probe.resolve(status);
      await Promise.allSettled([statusRequest, admissionRequest, initialization, owner?.stop()]);
      resetUpdateAvailableStateForTest(scheduler);
      closeOpenClawStateDatabaseForTest();
      await state.cleanup();
    }
  },
);

it("aborts and joins early update requests before the post-ready scheduler loads", async () => {
  const state = await createOpenClawTestState({ label: "update-request-before-ready" });
  const { createDeferredGatewayUpdateCheck } =
    await import("../gateway/server-startup-update-check.js");
  const { resolveGatewayUpdateAdmission } =
    await import("../gateway/server-methods/update-admission.js");
  const { createGatewayUpdateCheck, resetUpdateAvailableStateForTest } =
    await import("./update-startup.js");
  const scheduler = createTestGatewayScheduler();
  resetUpdateAvailableStateForTest(scheduler);
  const ready = createDeferred();
  const started = createDeferred<AbortSignal | undefined>();
  const probe = createDeferred<UpdateCheckResult>();
  const status: UpdateCheckResult = { root: null, installKind: "package", packageManager: "npm" };
  checkUpdateStatus.mockReset().mockImplementation(({ signal }) => {
    started.resolve(signal);
    return probe.promise;
  });
  const factory = vi.fn(createGatewayUpdateCheck);
  const owner = createDeferredGatewayUpdateCheck({
    scheduler,
    createUpdateCheck: factory,
    getConfig: () => ({}),
    log: { info: vi.fn(), warn: vi.fn() },
    isNixMode: false,
    broadcastToConnIds: vi.fn(),
    getClientConnIds: () => new Set(),
    waitForPostReadyWork: () => ready.promise,
  });
  owner.start();
  const requests = Promise.allSettled([
    currentUpdateCheckLifecycle().initialize(),
    resolveGatewayUpdateAdmission(createUpdateRun({ trigger: "api" }).runId),
  ]);
  let stopping: Promise<void> | undefined;
  try {
    const signal = await started.promise;
    let stopped = false;
    stopping = owner.stop().then(() => {
      stopped = true;
    });
    expect(signal?.aborted).toBe(true);
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(factory).not.toHaveBeenCalled();
    probe.resolve(status);
    const results = await requests;
    expect(results).toEqual([
      { status: "rejected", reason: expect.objectContaining({ name: "AbortError" }) },
      { status: "rejected", reason: expect.objectContaining({ name: "AbortError" }) },
    ]);
    await stopping;
    expect(stopped).toBe(true);
    expect(checkUpdateStatus).toHaveBeenCalledTimes(2);
    expect(factory).not.toHaveBeenCalled();
  } finally {
    probe.resolve(status);
    await requests;
    await (stopping ?? owner.stop());
    ready.resolve();
    resetUpdateAvailableStateForTest(scheduler);
    closeOpenClawStateDatabaseForTest();
    await state.cleanup();
  }
});
