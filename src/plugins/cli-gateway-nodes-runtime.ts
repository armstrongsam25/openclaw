import { createLazyRuntimeMethod } from "../shared/lazy-runtime.js";
import type { PluginRuntime } from "./runtime/types.js";

/** CLI help registers node APIs without loading their Gateway transport. */
export function createPluginCliGatewayNodesRuntime(): PluginRuntime["nodes"] {
  return {
    list: createLazyRuntimeMethod(
      () => import("./cli-gateway-nodes.runtime.js"),
      (runtime) => runtime.pluginCliGatewayNodesRuntime.list,
    ),
    invoke: createLazyRuntimeMethod(
      () => import("./cli-gateway-nodes.runtime.js"),
      (runtime) => runtime.pluginCliGatewayNodesRuntime.invoke,
    ),
    async openDuplex() {
      throw new Error("Node duplex is unavailable in the CLI; run this plugin inside the Gateway.");
    },
  };
}
