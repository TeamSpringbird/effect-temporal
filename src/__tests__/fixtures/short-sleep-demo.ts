import * as Schema from "effect/Schema";
import * as Workflow from "effect/workflow/Workflow";

export const ShortSleepDemo = Workflow.make("effectShortSleep", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
