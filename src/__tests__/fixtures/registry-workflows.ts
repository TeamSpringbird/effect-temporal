// Bundle entrypoint: workflows registered with `Workflow.toLayer`, hosted
// behind the bundle's one dynamic default export.

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { RegistryChild, RegistryParent } from "./registry-demo.js";

const acts = proxyActivities<{ echo(value: string): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

/** A handler dependency provided by an ordinary Layer in the registration
 * environment — the DI story per-workflow wrappers could not offer. */
class ChildPrefix extends Context.Service<ChildPrefix, string>()("registry-demo/ChildPrefix") {}

const ChildLive = RegistryChild.toLayer((payload) =>
  Effect.gen(function* () {
    const prefix = yield* ChildPrefix;
    yield* DurableClock.sleep({ name: "child-nap", duration: "1 minute" });
    return `${prefix}:${payload.value}`;
  }),
);

const ParentLive = RegistryParent.toLayer((payload) =>
  Effect.gen(function* () {
    if (payload.mode === "fail") return yield* Effect.fail("registry-failure");
    // A real Temporal activity through the per-call cancellable scope —
    // proves the run-time SandboxRun override reaches registered handlers.
    const echoed = yield* Activity.make({
      name: "echo",
      success: Schema.String,
      execute: callRawActivity(() => acts.echo(payload.requestId)),
    });
    // A child workflow — proves the dynamic default dispatches child types.
    const child = yield* RegistryChild.execute({ value: payload.requestId });
    return `parent:${echoed}|${child}`;
  }),
);

export default workflowBundle(
  Layer.mergeAll(ChildLive, ParentLive).pipe(
    Layer.provide(Layer.succeed(ChildPrefix, "hello")),
  ),
);
