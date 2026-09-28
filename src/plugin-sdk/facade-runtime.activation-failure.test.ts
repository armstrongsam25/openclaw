import { afterEach, expect, it, vi } from "vitest";
import * as nativeModule from "../plugins/native-module-require.js";
import {
  listImportedBundledPluginFacadeIds,
  loadActivatedBundledPluginPublicSurfaceModule,
  resetFacadeRuntimeStateForTest,
} from "./facade-runtime.js";

afterEach(() => {
  vi.restoreAllMocks();
  resetFacadeRuntimeStateForTest();
});

it("preserves the unavailable activation error without loading a public artifact", async () => {
  resetFacadeRuntimeStateForTest();
  vi.spyOn(nativeModule, "tryNativeRequireModule").mockImplementation((specifier) => {
    expect(specifier).toMatch(/facade-activation-check\.runtime\.[jt]s$/u);
    throw new Error("activation dependency unavailable");
  });
  await expect(
    loadActivatedBundledPluginPublicSurfaceModule({
      dirName: "fixture",
      artifactBasename: "api.js",
    }),
  ).rejects.toThrow("Unable to load facade activation check runtime");
  expect(listImportedBundledPluginFacadeIds()).toEqual([]);
});
