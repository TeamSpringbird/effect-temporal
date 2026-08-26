// Demo workflow definition, shared by the bundle entrypoint and the
// client-side tests. No `@temporalio/*` imports: it loads in both worlds.

import * as Schema from "effect/Schema";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";

/** The approval gate; completed from outside via `DurableDeferred.done`. */
export const Approval = DurableDeferred.make("demo-approval", {
  success: Schema.String,
});

export const Demo = Workflow.make("effectDemo", {
  payload: {
    requestId: Schema.String,
    /** `approve` waits for the approval after the delay; `fail-after-step`
     * fails the typed error channel after the delay, exercising the
     * compensation path without cancellation; `long-activity` blocks on a
     * long-running activity, exercising in-flight cancellation;
     * `timeout-activity` puts an `Effect.timeoutOption` around the long
     * activity, exercising fiber-level interruption cancelling the
     * server-side call while the run itself completes. */
    mode: Schema.Literals(["approve", "fail-after-step", "long-activity", "timeout-activity"]),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
  error: Schema.String,
});
