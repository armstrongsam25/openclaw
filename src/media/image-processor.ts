import { RastermillUnavailableError, type Rastermill } from "rastermill";
import { createLazyRuntimeMethod } from "../shared/lazy-runtime.js";
import {
  createLocalImageProcessor,
  MAX_IMAGE_INPUT_PIXELS,
  type ImageProcessorPixelLimits,
} from "./image-processor-config.js";

const runImageTask = createLazyRuntimeMethod(
  () => import("./image-processor.runtime.js"),
  (runtime) => runtime.runImageTask,
);

/** Keep cheap probes local and move in-process image computation off the caller's event loop. */
export function createImageProcessor(): Rastermill {
  return createImageProcessorWithPixelLimits({
    inputPixels: MAX_IMAGE_INPUT_PIXELS,
    outputPixels: MAX_IMAGE_INPUT_PIXELS,
  });
}

/** Internal operation-specific admission uses the same worker and native fallback owners. */
export function createImageProcessorWithPixelLimits(params: ImageProcessorPixelLimits): Rastermill {
  const limits = { inputPixels: params.inputPixels, outputPixels: params.outputPixels };
  const local = createLocalImageProcessor("auto", limits);
  const workerLimits =
    limits.inputPixels === MAX_IMAGE_INPUT_PIXELS && limits.outputPixels === MAX_IMAGE_INPUT_PIXELS
      ? undefined
      : limits;
  return {
    probe: (input) => local.probe(input),
    transparency: async (input) => {
      const reply = await runImageTask(input, { kind: "transparency" }, undefined, workerLimits);
      if (reply.kind === "failed") {
        throw reply.unavailable
          ? new RastermillUnavailableError("transparency", reply.error.message, [reply.error])
          : reply.error;
      }
      if (reply.kind !== "transparency") {
        throw new Error("Unexpected image worker result");
      }
      return reply.value;
    },
    encode: async (input, options) => {
      const { signal, ...workerOptions } = options ?? {};
      const reply = await runImageTask(
        input,
        { kind: "encode", options: workerOptions },
        signal,
        workerLimits,
      );
      signal?.throwIfAborted();
      if (reply.kind === "failed") {
        if (!reply.unavailable) {
          throw reply.error;
        }
        // Preserve Rastermill's native-codec and alpha policy when its internal backend declines.
        return local.encode(input, options);
      }
      if (reply.kind !== "encode") {
        throw new Error("Unexpected image worker result");
      }
      const { data, ...value } = reply.value;
      return { ...value, data: Buffer.from(data.buffer, data.byteOffset, data.byteLength) };
    },
  };
}

export async function convertBmpToPngWithWorker(input: Buffer): Promise<Buffer> {
  const reply = await runImageTask(input, { kind: "bmpToPng" });
  if (reply.kind === "failed") {
    throw reply.error;
  }
  if (reply.kind !== "bmpToPng") {
    throw new Error("Unexpected image worker result");
  }
  return Buffer.from(reply.value.buffer, reply.value.byteOffset, reply.value.byteLength);
}
