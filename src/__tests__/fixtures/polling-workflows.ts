// Infrequent-polling demo — bundle entrypoint, mirroring the Temporal
// `polling` sample's infrequent variant: the polling loop IS the activity
// retry policy (a failing attempt schedules the next poll server-side), so
// the workflow is a single activity call.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { PollingDemo } from "./polling-demo.js";

const acts = proxyActivities<{ pollService(): Promise<string> }>({
  startToCloseTimeout: "5 seconds",
  retry: { initialInterval: "60 seconds", backoffCoefficient: 1 },
});

const PollingDemoLive = PollingDemo.toLayer(() =>
  Effect.gen(function* () {
    const status = yield* Activity.make({
      name: "poll-service",
      success: Schema.String,
      execute: callRawActivity(() => acts.pollService()),
    });
    return `service:${status}`;
  }),
);

export default workflowBundle(PollingDemoLive);
