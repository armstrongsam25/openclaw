// Covers best-effort cleanup error swallowing.
import { describe, expect, it, vi } from "vitest";
import { runBestEffortCleanup } from "./non-fatal-cleanup.js";

describe("runBestEffortCleanup", () => {
  it("returns the cleanup result when the cleanup succeeds", async () => {
    await expect(
      runBestEffortCleanup({
        cleanup: async () => 7,
      }),
    ).resolves.toBe(7);
  });

  it.each(["ok", "throw", "reject"] as const)(
    "preserves the primary result when cleanup fails (reporter: %s)",
    async (reporter) => {
      const onError = vi.fn(() => {
        if (reporter === "throw") {
          throw new Error("cleanup warning failed");
        }
        if (reporter === "reject") {
          return Promise.reject(new Error("cleanup warning failed"));
        }
        return undefined;
      });
      const error = new Error("cleanup failed");

      await expect(
        runBestEffortCleanup({
          cleanup: async () => {
            throw error;
          },
          onError,
        }),
      ).resolves.toBeUndefined();

      expect(onError).toHaveBeenCalledWith(error);
    },
  );
});
