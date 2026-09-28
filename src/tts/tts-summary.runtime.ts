import { requireApiKey } from "../agents/model-auth.js";
import { completeWithPreparedSimpleCompletionModel } from "../agents/simple-completion-execution.js";

export async function loadDefaultSummarizeTextDeps() {
  const { acquireSimpleCompletionModelWithSelection } =
    await import("../agents/simple-completion-runtime.js");
  return {
    completeWithPreparedSimpleCompletionModel,
    acquireSimpleCompletionModelWithSelection,
    requireApiKey,
  };
}
