import {
  createLazyRuntimeMethodBinder,
  createLazyRuntimeModule,
} from "../../shared/lazy-runtime.js";

const bindOAuthRuntime = createLazyRuntimeMethodBinder(
  createLazyRuntimeModule(() => import("./oauth.js")),
);

export const refreshOAuthCredentialForRuntime = bindOAuthRuntime(
  (runtime) => runtime.refreshOAuthCredentialForRuntime,
);
export const resolveApiKeyForProfile = bindOAuthRuntime(
  (runtime) => runtime.resolveApiKeyForProfile,
);
