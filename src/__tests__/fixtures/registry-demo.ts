// Definitions shared by the registry bundle (registry-workflows.ts) and the
// test's client side.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const RegistryChild = Workflow.make("registryChild", {
  payload: { value: Schema.String },
  idempotencyKey: ({ value }) => value,
  success: Schema.String,
});

export const RegistryParent = Workflow.make("registryParent", {
  payload: {
    requestId: Schema.String,
    /** `ok` completes; `fail` returns the typed failure. */
    mode: Schema.Literals(["ok", "fail"]),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
  error: Schema.String,
});
