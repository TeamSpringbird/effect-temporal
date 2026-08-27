// Message-passing demo — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import { workflowBundle } from "../../engine-sandbox.js";
import {
  Approved,
  CurrentLanguage,
  DeferredPokeDemo,
  MessageDemo,
  SetLanguage,
  SUPPORTED_LANGUAGES,
} from "./message-demo.js";

const MessageDemoLive = MessageDemo.toLayer(() =>
  Effect.gen(function* () {
    let language: string = SUPPORTED_LANGUAGES[0];
    yield* CurrentLanguage.set(language);
    while (true) {
      const winner = yield* Effect.raceFirst(
        SetLanguage.take.pipe(
          Effect.map((request) => ({ kind: "update" as const, request })),
        ),
        Approved.await.pipe(
          Effect.map((approver) => ({ kind: "approved" as const, approver })),
        ),
      );
      if (winner.kind === "approved") {
        return `approved:${language} by ${winner.approver}`;
      }
      const requested = winner.request.payload.language;
      if ((SUPPORTED_LANGUAGES as readonly string[]).includes(requested)) {
        yield* winner.request.respond(Exit.succeed(language));
        language = requested;
        yield* CurrentLanguage.set(language);
      } else {
        yield* winner.request.respond(Exit.fail(`unsupported:${requested}`));
      }
    }
  }),
);

/** Sends `DurableDeferred.done` to a foreign execution id through the
 * engine, then returns — the target being closed or unknown must be a no-op
 * for this sender. */
const DeferredPokeDemoLive = DeferredPokeDemo.toLayer((payload) =>
  Effect.gen(function* () {
    yield* DurableDeferred.done(Approved.deferred, {
      token: DurableDeferred.tokenFromExecutionId(Approved.deferred, {
        workflow: MessageDemo,
        executionId: payload.targetExecutionId,
      }),
      exit: Exit.succeed("ghost-approver"),
    });
    return "ok";
  }),
);

export default workflowBundle(Layer.mergeAll(MessageDemoLive, DeferredPokeDemoLive));
