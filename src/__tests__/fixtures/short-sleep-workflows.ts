import * as Effect from "effect/Effect";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle } from "../../engine-sandbox.js";
import { ShortSleepDemo } from "./short-sleep-demo.js";

const acts = proxyActivities<{ echo(value: string): Promise<string> }>({
  startToCloseTimeout: "1 minute",
});

// The in-memory DurableClock path (duration <= 60s threshold), sandwiched
// between activities — the shape that hung in the fleet's campaign tests.
const ShortSleepDemoLive = ShortSleepDemo.toLayer((payload) =>
  Effect.gen(function* () {
    const first = yield* callRawActivity(() => acts.echo(payload.requestId));
    yield* DurableClock.sleep({ name: "short-nap", duration: "30 seconds" });
    const second = yield* callRawActivity(() => acts.echo(first));
    return second;
  }),
);

export default workflowBundle(ShortSleepDemoLive);
