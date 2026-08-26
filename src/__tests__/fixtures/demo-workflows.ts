// Demo workflow — bundle entrypoint. One workflow exercising the core
// primitives: a compensated step (reserve → release), a durable delay, an
// external approval wait, a typed-failure path that triggers the
// compensation, and a long-activity path for in-flight cancellation.

import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { Approval, Demo } from "./demo.js";

const acts = proxyActivities<{
  reserve(requestId: string): Promise<string>;
  release(reservation: string): Promise<string>;
  longTask(): Promise<string>;
}>({
  startToCloseTimeout: "10 minutes",
});

const DemoLive = Demo.toLayer((payload) =>
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

    if (payload.mode === "long-activity") {
      const long = yield* Activity.make({
        name: "long-task",
        success: Schema.String,
        execute: callRawActivity(() => acts.longTask()),
      });
      return `${reservation}|${long}`;
    }

    if (payload.mode === "timeout-activity") {
      // Fiber-level interruption: the timeout interrupts the activity fiber,
      // which must CANCEL the in-flight server-side activity (not abandon
      // it) while the run itself continues to a successful completion.
      const long = yield* Activity.make({
        name: "long-task",
        success: Schema.String,
        execute: callRawActivity(() => acts.longTask()),
      }).pipe(Effect.timeoutOption("1 second"));
      return `${reservation}|timed-out:${Option.isNone(long)}`;
    }

    yield* DurableClock.sleep({ name: "cooling-off", duration: "2 minutes" });

    if (payload.mode === "fail-after-step") {
      return yield* Effect.fail("business-failure");
    }

    const approver = yield* DurableDeferred.await(Approval);
    return `${reservation}|approved-by:${approver}`;
  }),
);

export default workflowBundle(DemoLive);
