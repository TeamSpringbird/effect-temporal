import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import { defineActivity } from "../../definition.js";

export const OutOfStock = Schema.TaggedStruct("OutOfStock", {
  sku: Schema.String,
});

export const Reserve = defineActivity("typedReserve", {
  payload: { sku: Schema.String, quantity: Schema.Finite },
  success: Schema.String,
  error: OutOfStock,
  options: { startToCloseTimeout: "1 minute", retry: { maximumAttempts: 3 } },
});

export const TypedActivityDemo = Workflow.make("effectTypedActivity", {
  payload: { requestId: Schema.String, sku: Schema.String, notBeforeISO: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
