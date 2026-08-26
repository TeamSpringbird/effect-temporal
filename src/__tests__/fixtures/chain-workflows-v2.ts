// Version-chain demo, generation 2: the first revision, adopting
// `Versioning.match` — the original behavior becomes the unguarded first
// case, so pre-versioning histories replay through it.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, makeTemporalWorkflow } from "../../engine-sandbox.js";
import * as Versioning from "../../versioning.js";
import { ChainDemo } from "./chain-demo.js";

const acts = proxyActivities<{ greetV1(): Promise<string>; greetV2(): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

const greet = (name: string, call: () => Promise<string>) =>
  Activity.make({ name, success: Schema.String, execute: callRawActivity(call) });

export const effectChainDemo = makeTemporalWorkflow(ChainDemo, () =>
  Versioning.match("greeting", [
    { version: "v1", run: greet("greet", () => acts.greetV1()) },
    { version: "v2", run: greet("greet-v2", () => acts.greetV2()) },
  ]).pipe(Effect.map((greeting) => `greeted:${greeting}`)),
);
