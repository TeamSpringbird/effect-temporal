// Child-workflow demo DEFINITIONS — shared by the bundle entrypoint and the
// client-side tests. No `@temporalio/*` imports.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const ChildDemo = Workflow.make("effectChildDemo", {
  payload: {
    requestId: Schema.String,
    /** `ok` echoes and returns; `fail` fails the typed channel; `sleep`
     * takes a 2-minute durable delay first (the cancellation window). */
    outcome: Schema.Literals(["ok", "fail", "sleep"]),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
  error: Schema.String,
});

export const ParentDemo = Workflow.make("effectParentDemo", {
  payload: {
    requestId: Schema.String,
    childOutcome: ChildDemo.payloadSchema.fields.outcome,
    /** Discarded children are fire-and-forget (ABANDON) and outlive the
     * parent; awaited children compose their typed result/failure. */
    discardChild: Schema.Boolean,
    /** Names the child explicitly (defaults to `${requestId}-child`) — the
     * collision test points two parents at ONE child idempotency key. */
    childRequestId: Schema.optionalKey(Schema.String),
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
  error: Schema.String,
});
