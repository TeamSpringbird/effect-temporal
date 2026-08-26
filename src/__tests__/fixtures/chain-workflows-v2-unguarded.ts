// Version-chain demo, negative control: generation 2's behavior WITHOUT the
// version guard. Replaying a generation-1 history through this bundle must
// raise a determinism violation — proving the drill can see the change.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { ChainDemo } from "./chain-demo.js";

const acts = proxyActivities<{ greetV2(): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

const ChainDemoLive = ChainDemo.toLayer(() =>
  Activity.make({
    name: "greet-v2",
    success: Schema.String,
    execute: callRawActivity(() => acts.greetV2()),
  }).pipe(Effect.map((greeting) => `greeted:${greeting}`)),
);

export default workflowBundle(ChainDemoLive);
