// Version-chain demo definition, shared by every bundle generation and the
// client-side test: the same workflow tag across v1, v2, and v3 code.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const ChainDemo = Workflow.make("effectChainDemo", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
