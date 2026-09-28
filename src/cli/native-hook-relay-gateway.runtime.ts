import type { NativeHookRelayProcessResponse } from "../agents/harness/native-hook-relay-types.js";
import { callGateway } from "../gateway/call.js";
import { ADMIN_SCOPE } from "../gateway/operator-scopes.js";

export type NativeHookRelayGatewayRequest = {
  params: {
    provider: string;
    relayId: string;
    generation?: string;
    event: string;
    rawPayload: unknown;
  };
  timeoutMs: number;
  signal: AbortSignal;
};

export function invokeNativeHookRelayGateway(
  request: NativeHookRelayGatewayRequest,
): Promise<NativeHookRelayProcessResponse> {
  return callGateway<NativeHookRelayProcessResponse>({
    method: "nativeHook.invoke",
    ...request,
    scopes: [ADMIN_SCOPE],
  });
}
