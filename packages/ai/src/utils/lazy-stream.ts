import { createAssistantOutput } from "../transports/assistant-output.js";
import type {
  Api,
  AssistantMessageEventStreamLike,
  Context,
  Model,
  StreamFunction,
  StreamOptions,
} from "../types.js";
import { AssistantMessageEventStream } from "./event-stream.js";
import { projectProviderError } from "./provider-error.js";

// Keep stream construction synchronous while provider code loads on first request.
export function createLazyStream<TApi extends Api, TOptions extends StreamOptions, TStreams>(
  load: () => Promise<TStreams>,
  select: (
    streams: TStreams,
  ) => (
    model: Model<TApi>,
    context: Context,
    options?: TOptions,
  ) => AssistantMessageEventStreamLike | Promise<AssistantMessageEventStreamLike>,
): StreamFunction<TApi, TOptions> {
  return (model, context, options) => {
    const outer = new AssistantMessageEventStream();
    load()
      .then(async (streams) => {
        const source = await select(streams)(model, context, options);
        for await (const event of source) {
          outer.push(event);
        }
        outer.end(await source.result());
      })
      .catch((error: unknown) => {
        const message = {
          ...createAssistantOutput(model),
          ...projectProviderError(error, options?.signal),
        };
        outer.push({ type: "error", reason: message.stopReason, error: message });
        outer.end(message);
      });
    return outer;
  };
}
