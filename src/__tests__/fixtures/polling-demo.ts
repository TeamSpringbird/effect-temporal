// Infrequent-polling demo definition, shared by the bundle entrypoint and
// the client-side test.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const PollingDemo = Workflow.make("effectPollingDemo", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
