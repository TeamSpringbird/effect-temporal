// Child-workflow demo — bundle entrypoint. Parent and child are both shim
// workflows exported from ONE bundle (a child's Temporal type must live in
// the same bundle as its parent). The parent's first step is compensated so
// the cancel test can assert parent-side compensation alongside child
// cancellation.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as Workflow from "effect/unstable/workflow/Workflow";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, makeTemporalWorkflow } from "../../engine-sandbox.js";
import { ChildDemo, ParentDemo } from "./child-demo.js";

const acts = proxyActivities<{
  reserve(requestId: string): Promise<string>;
  release(reservation: string): Promise<string>;
  echo(value: string): Promise<string>;
}>({
  startToCloseTimeout: "10 seconds",
});

export const effectChildDemo = makeTemporalWorkflow(ChildDemo, (payload) =>
  Effect.gen(function* () {
    if (payload.outcome === "sleep") {
      yield* DurableClock.sleep({ name: "child-delay", duration: "2 minutes" });
    }
    if (payload.outcome === "fail") {
      return yield* Effect.fail(`child-failed:${payload.requestId}`);
    }
    const echoed = yield* Activity.make({
      name: "child-echo",
      success: Schema.String,
      execute: callRawActivity(() => acts.echo(payload.requestId)),
    });
    return `child:${echoed}`;
  }),
);

export const effectParentDemo = makeTemporalWorkflow(ParentDemo, (payload) =>
  Effect.gen(function* () {
    const reservation = yield* Activity.make({
      name: "reserve",
      success: Schema.String,
      execute: callRawActivity(() => acts.reserve(payload.requestId)),
    }).pipe(
      Workflow.withCompensation((value) =>
        Activity.make({
          name: "release",
          success: Schema.String,
          execute: callRawActivity(() => acts.release(value)),
        }).pipe(Effect.asVoid),
      ),
    );

    const childPayload = {
      requestId: payload.childRequestId ?? `${payload.requestId}-child`,
      outcome: payload.childOutcome,
    };

    if (payload.discardChild) {
      yield* ChildDemo.execute(childPayload, { discard: true });
      return `${reservation}|child-discarded`;
    }

    const childResult = yield* ChildDemo.execute(childPayload);
    return `${reservation}|${childResult}`;
  }),
);
