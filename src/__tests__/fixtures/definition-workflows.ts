// The Temporal bundle for the definition demo: the SAME handlers the memory
// test runs, hosted by workflowBundle — which provides the Temporal
// `WorkflowOps` runtime the declarations require. `OrderFlow`'s handler is
// unchanged since 0.3.0 (see replay-compat.test.ts); the rest is 0.4.0.

import * as Layer from "effect/Layer";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Workflow from "effect/workflow/Workflow";
import { workflowBundle } from "../../bundle.js";
import {
  Dispatch,
  dispatchHandler,
  Fulfil,
  fulfilHandler,
  GraceFlow,
  graceHandler,
  OrderFlow,
  orderHandler,
  Reserve,
  Tally,
  tallyHandler,
} from "./definition-demo.js";

export default workflowBundle(
  Layer.mergeAll(
    Workflow.make("activityCancellation", {
      payload: {},
      idempotencyKey: () => "activity-cancellation",
      success: Schema.Boolean,
    }).toLayer(() =>
      Reserve({ orderId: "cancelled" }).pipe(
        Effect.exit,
        Effect.uninterruptible,
        Effect.map(
          (exit) => Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause) && !Cause.hasDies(exit.cause),
        ),
      ),
    ),
    OrderFlow.toLayer(orderHandler),
    GraceFlow.toLayer(graceHandler),
    Tally.toLayer(tallyHandler),
    Fulfil.toLayer(fulfilHandler),
    Dispatch.toLayer(dispatchHandler),
  ),
);
