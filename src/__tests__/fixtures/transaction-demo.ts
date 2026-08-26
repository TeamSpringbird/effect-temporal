// Early-return demo definitions, mirroring the Temporal `early-return`
// sample: a transaction workflow confirms quickly via an update (the early
// return) while the slower completion continues to the final result.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableUpdate from "../../update.js";

export const GetConfirmation = DurableUpdate.make("get-confirmation", {
  payload: Schema.Struct({}),
  success: Schema.String,
  error: Schema.Never,
});

export const TransactionDemo = Workflow.make("effectTransactionDemo", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
