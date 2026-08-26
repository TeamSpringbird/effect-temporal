// Early-return demo — bundle entrypoint. A forked fiber serves the
// confirmation update once authorization lands, while the main flow
// continues through the slower capture to the final result.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { condition } from "@temporalio/workflow";
import { workflowBundle, takeUpdate } from "../../engine-sandbox.js";
import { GetConfirmation, TransactionDemo } from "./transaction-demo.js";

const TransactionDemoLive = TransactionDemo.toLayer(() =>
  Effect.gen(function* () {
    const state = { confirmed: false };

    yield* Effect.forkChild(
      Effect.gen(function* () {
        const request = yield* takeUpdate(GetConfirmation);
        // condition() is a durable workflow wait, not activity I/O.
        // oxlint-disable-next-line effect-temporal/prefer-call-temporal-activity
        yield* Effect.promise(() => condition(() => state.confirmed));
        yield* request.respond(Exit.succeed("confirmed"));
      }),
    );

    yield* DurableClock.sleep({ name: "authorize", duration: "500 millis" });
    state.confirmed = true;
    yield* DurableClock.sleep({ name: "capture", duration: "3 seconds" });
    return "complete:77";
  }),
);

export default workflowBundle(TransactionDemoLive);
