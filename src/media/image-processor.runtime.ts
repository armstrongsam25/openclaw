import { RastermillError, type ImageInput } from "rastermill";
import { resolveRuntimeProcessEntrypointUrl } from "../infra/runtime-process-url.js";
import { WorkerTaskPool } from "../infra/worker-task-pool.js";
import type { ImageProcessorPixelLimits } from "./image-processor-config.js";
import type {
  ImageProcessorOperation,
  ImageProcessorReply,
  ImageProcessorRequest,
} from "./image-processor.types.js";

const pool = new WorkerTaskPool<ImageProcessorRequest, ImageProcessorReply>({
  workerUrl: resolveRuntimeProcessEntrypointUrl("imageProcessor"),
  // Each Photon instance retains a WASM heap; serialize transforms rather than multiply decodes.
  maxWorkers: 1,
  sharedCompute: true,
});

export async function runImageTask(
  input: ImageInput,
  operation: ImageProcessorOperation,
  signal?: AbortSignal,
  limits?: ImageProcessorPixelLimits,
): Promise<ImageProcessorReply> {
  const reply = await pool.run(
    () => ({
      ...operation,
      ...(limits ? { limits } : {}),
      // The caller may reuse its Buffer. Transfer a dedicated copy only after admission.
      input: Uint8Array.from(input instanceof ArrayBuffer ? new Uint8Array(input) : input),
    }),
    {
      timeoutMs: 180_000,
      signal,
      inputBytes: input.byteLength,
      transferList: (request) => [request.input.buffer],
    },
  );
  // Structured cloning preserves Error messages but drops Rastermill's public error codes.
  if (reply.kind === "failed" && reply.code && !reply.unavailable) {
    reply.error = new RastermillError(reply.code, reply.error.message, { cause: reply.error });
  }
  return reply;
}
