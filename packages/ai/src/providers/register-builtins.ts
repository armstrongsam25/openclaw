// Built-in provider registration installs lazy protocol adapters.
import type { ApiRegistry } from "../api-registry.js";
import type { Api, SimpleStreamOptions, StreamFunction, StreamOptions } from "../types.js";
import { createLazyStream } from "../utils/lazy-stream.js";

type ProviderStreams<TApi extends Api, TOptions extends StreamOptions> = {
  stream: StreamFunction<TApi, TOptions>;
  streamSimple: StreamFunction<TApi, SimpleStreamOptions>;
};

type RegisterBuiltIn = (registry: ApiRegistry) => void;

/** Source id used for built-in API provider registrations. */
export const BUILT_IN_API_PROVIDER_SOURCE_ID = "core:built-in";

function createLazyRegistration<TApi extends Api, TOptions extends StreamOptions, TModule>(
  api: TApi,
  importModule: () => Promise<TModule>,
  select: (module: TModule) => ProviderStreams<TApi, TOptions>,
): RegisterBuiltIn {
  let streamsPromise: Promise<ProviderStreams<TApi, TOptions>> | undefined;
  const load = () => (streamsPromise ??= importModule().then(select));
  const stream = createLazyStream(load, (streams) => streams.stream);
  const streamSimple = createLazyStream<TApi, SimpleStreamOptions, ProviderStreams<TApi, TOptions>>(
    load,
    (streams) => streams.streamSimple,
  );
  return (registry) => {
    registry.registerApiProvider({ api, stream, streamSimple }, BUILT_IN_API_PROVIDER_SOURCE_ID);
  };
}

const registerBuiltIns: RegisterBuiltIn[] = [
  // Registration is transport-free; each lazy adapter owns its fetch or construction unwrap.
  createLazyRegistration(
    "anthropic-messages",
    () => import("./anthropic.js"),
    (module) => ({ stream: module.streamAnthropic, streamSimple: module.streamSimpleAnthropic }),
  ),
  createLazyRegistration(
    "openai-completions",
    () => import("./openai-completions.js"),
    (module) => ({
      stream: module.streamOpenAICompletions,
      streamSimple: module.streamSimpleOpenAICompletions,
    }),
  ),
  createLazyRegistration(
    "mistral-conversations",
    () => import("./mistral.js"),
    (module) => ({ stream: module.streamMistral, streamSimple: module.streamSimpleMistral }),
  ),
  createLazyRegistration(
    "openai-responses",
    () => import("./openai-responses.js"),
    (module) => ({
      stream: module.streamOpenAIResponses,
      streamSimple: module.streamSimpleOpenAIResponses,
    }),
  ),
  createLazyRegistration(
    "azure-openai-responses",
    () => import("./azure-openai-responses.js"),
    (module) => ({
      stream: module.streamAzureOpenAIResponses,
      streamSimple: module.streamSimpleAzureOpenAIResponses,
    }),
  ),
  createLazyRegistration(
    "openai-chatgpt-responses",
    () => import("./openai-chatgpt-responses.js"),
    (module) => ({
      stream: module.streamOpenAICodexResponses,
      streamSimple: module.streamSimpleOpenAICodexResponses,
    }),
  ),
  createLazyRegistration(
    "google-generative-ai",
    () => import("./google.js"),
    (module) => ({ stream: module.streamGoogle, streamSimple: module.streamSimpleGoogle }),
  ),
  createLazyRegistration(
    "google-interactions",
    () => import("./google-interactions.js"),
    (module) => ({
      stream: module.streamGoogleInteractions,
      streamSimple: module.streamSimpleGoogleInteractions,
    }),
  ),
  createLazyRegistration(
    "google-vertex",
    () => import("./google-vertex.js"),
    (module) => ({
      stream: module.streamGoogleVertex,
      streamSimple: module.streamSimpleGoogleVertex,
    }),
  ),
];

/** Registers every built-in API provider in one runtime registry. */
export function registerBuiltInApiProviders(registry: ApiRegistry): void {
  for (const register of registerBuiltIns) {
    register(registry);
  }
}

/** Restores the built-in provider registry state for tests. */
export function resetApiProviders(registry: ApiRegistry): void {
  registry.unregisterApiProviders(BUILT_IN_API_PROVIDER_SOURCE_ID);
  registerBuiltInApiProviders(registry);
}
