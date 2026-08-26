// Version-chain demo, generation 1: the original code, from before the site
// adopted versioning. Distinct activity TYPES per generation keep the
// changes visible to Temporal's structural replay check.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, makeTemporalWorkflow } from "../../engine-sandbox.js";
import { ChainDemo } from "./chain-demo.js";

const acts = proxyActivities<{ greetV1(): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

export const effectChainDemo = makeTemporalWorkflow(ChainDemo, () =>
  Activity.make({
    name: "greet",
    success: Schema.String,
    execute: callRawActivity(() => acts.greetV1()),
  }).pipe(Effect.map((greeting) => `greeted:${greeting}`)),
);
