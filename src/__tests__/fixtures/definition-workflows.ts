// The Temporal bundle for the definition demo: the SAME handlers the memory
// test runs, hosted by workflowBundle — which provides the Temporal
// `WorkflowOps` runtime the declarations require. `OrderFlow`'s handler is
// unchanged since 0.3.0 (see replay-compat.test.ts); the rest is 0.4.0.

import * as Layer from "effect/Layer";
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
  Tally,
  tallyHandler,
} from "./definition-demo.js";

export default workflowBundle(
  Layer.mergeAll(
    OrderFlow.toLayer(orderHandler),
    GraceFlow.toLayer(graceHandler),
    Tally.toLayer(tallyHandler),
    Fulfil.toLayer(fulfilHandler),
    Dispatch.toLayer(dispatchHandler),
  ),
);
