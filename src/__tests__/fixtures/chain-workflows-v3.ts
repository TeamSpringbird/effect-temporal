// Version-chain demo, generation 3: evolving the site is appending a case.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import * as Versioning from "../../versioning.js";
import { ChainDemo } from "./chain-demo.js";

const acts = proxyActivities<{
  greetV1(): Promise<string>;
  greetV2(): Promise<string>;
  greetV3(): Promise<string>;
}>({
  startToCloseTimeout: "10 seconds",
});

const greet = (name: string, call: () => Promise<string>) =>
  Activity.make({ name, success: Schema.String, execute: callRawActivity(call) });

const ChainDemoLive = ChainDemo.toLayer(() =>
  Versioning.match("greeting", [
    { version: "v1", run: greet("greet", () => acts.greetV1()) },
    { version: "v2", run: greet("greet-v2", () => acts.greetV2()) },
    { version: "v3", run: greet("greet-v3", () => acts.greetV3()) },
  ]).pipe(Effect.map((greeting) => `greeted:${greeting}`)),
);

export default workflowBundle(ChainDemoLive);
