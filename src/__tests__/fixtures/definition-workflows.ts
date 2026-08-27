// The Temporal bundle for the definition demo: the SAME handler the memory
// test runs, hosted by workflowBundle — which provides the Temporal
// `WorkflowOps` runtime the declarations require.

import { workflowBundle } from "../../engine-sandbox.js";
import { OrderFlow, orderHandler } from "./definition-demo.js";

export default workflowBundle(OrderFlow.toLayer(orderHandler));
