// Looping workflow — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import { proxyActivities } from "@temporalio/workflow";
import {
  callRawActivity,
  continueAsNew,
  workflowBundle,
  setStateCell,
} from "../../engine-sandbox.js";
import { CellLoopDemo, LoopDemo, LoopGate, LoopStage } from "./loop-demo.js";

const acts = proxyActivities<{ record(iteration: string): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

const LoopDemoLive = LoopDemo.toLayer((payload) =>
  Effect.gen(function* () {
    yield* Activity.make({
      name: "record",
      success: Schema.String,
      execute: callRawActivity(() => acts.record(String(payload.iteration))),
    });
    if (payload.iteration >= 2) return `done:${payload.iteration}`;
    return yield* continueAsNew(LoopDemo, {
      requestId: payload.requestId,
      iteration: payload.iteration + 1,
    });
  }),
);

const CellLoopDemoLive = CellLoopDemo.toLayer((payload) =>
  Effect.gen(function* () {
    if (payload.iteration === 0) {
      yield* setStateCell(LoopStage, "run-0");
      yield* DurableDeferred.await(LoopGate);
      return yield* continueAsNew(CellLoopDemo, {
        requestId: payload.requestId,
        iteration: 1,
      });
    }
    // Run 2: idle (cell unpublished in THIS run) until released, then
    // republish and finish.
    const release = yield* DurableDeferred.await(LoopGate);
    yield* setStateCell(LoopStage, `run-1:${release}`);
    return "cell-done";
  }),
);

export default workflowBundle(Layer.mergeAll(LoopDemoLive, CellLoopDemoLive));
