// DSL-interpreter demo — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { DslDemo } from "./dsl-demo.js";

const acts = proxyActivities<{ runTask(name: string): Promise<string> }>({
  startToCloseTimeout: "10 seconds",
});

const runTask = (name: string) =>
  Activity.make({
    name: `task-${name}`,
    success: Schema.String,
    execute: callRawActivity(() => acts.runTask(name)),
  });

const DslDemoLive = DslDemo.toLayer((payload) =>
  Effect.gen(function* () {
    const outputs: string[] = [];
    for (const step of payload.steps) {
      if ("single" in step) {
        outputs.push(yield* runTask(step.single));
      } else {
        const results = yield* Effect.all(
          step.parallel.map((name) => runTask(name)),
          { concurrency: "unbounded" },
        );
        outputs.push(results.toSorted().join("+"));
      }
    }
    return outputs.join(">");
  }),
);

export default workflowBundle(DslDemoLive);
