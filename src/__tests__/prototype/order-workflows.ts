// PROTOTYPE — throwaway. The Temporal bundle for the prototype definition:
// the SAME bound handler the memory test runs, hosted on the real engine.

import { workflowBundle } from "../../engine-sandbox.js";
import { toTemporalLayer } from "./temporal-runtime.js";
import { orderBound } from "./order.js";

export default workflowBundle(toTemporalLayer(orderBound));
