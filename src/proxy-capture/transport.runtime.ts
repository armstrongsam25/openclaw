import { maybeWarnAboutDebugProxyCoverage } from "./coverage.js";
import {
  initializeDebugProxyCaptureAsync,
  prepareHttpCaptureForTransport,
  resolveDebugProxyFetchTransport,
} from "./runtime.js";

export async function initializeCliDebugProxyCapture(): Promise<void> {
  await initializeDebugProxyCaptureAsync("cli");
  maybeWarnAboutDebugProxyCoverage(undefined, (message) => console.warn(message));
}

export function prepareGuardedFetchCapture(fetchImpl: typeof fetch) {
  return {
    fetchImpl: resolveDebugProxyFetchTransport(fetchImpl),
    capture: prepareHttpCaptureForTransport(),
  };
}
