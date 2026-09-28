import { mediaKindFromMime } from "@openclaw/media-core/constants";
import { detectMime } from "@openclaw/media-core/mime";
import { isVoiceMessageCompatibleAudio } from "../../media/audio.js";
import { getImageMetadata, resizeToJpeg } from "../../media/image-ops.js";
import { createLazyRuntimeMethod } from "../../shared/lazy-runtime.js";
import type { PluginRuntime } from "./types.js";

const loadWebMedia = createLazyRuntimeMethod(
  () => import("../../media/web-media.js"),
  (runtime) => runtime.loadWebMedia,
);
/** Creates the plugin runtime media facade. */
export function createRuntimeMedia(): PluginRuntime["media"] {
  return {
    loadWebMedia,
    detectMime,
    mediaKindFromMime,
    isVoiceCompatibleAudio: isVoiceMessageCompatibleAudio,
    getImageMetadata,
    resizeToJpeg,
  };
}
