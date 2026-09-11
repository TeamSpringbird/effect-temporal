// Version-chain demo, generation 2: the first revision, adopting
// `versioned` — the original behavior becomes the unguarded first case
// (the first key), so pre-versioning histories replay through it.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity } from "../../engine-sandbox.js";
import { workflowBundle } from "../../bundle.js";
import { versioned } from "../../definition.js";
import { ChainDemo } from "./chain-demo.js";

const acts = proxyActivities<{ greetV1(): Promise<string>; greetV2(): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

const greet = (name: string, call: () => Promise<string>) =>
  Activity.make({ name, success: Schema.String, execute: callRawActivity(call) });

const ChainDemoLive = ChainDemo.toLayer(() =>
  versioned("greeting", {
    v1: greet("greet", () => acts.greetV1()),
    v2: greet("greet-v2", () => acts.greetV2()),
  }).pipe(Effect.map((greeting) => `greeted:${greeting}`)),
);

export default workflowBundle(ChainDemoLive);
