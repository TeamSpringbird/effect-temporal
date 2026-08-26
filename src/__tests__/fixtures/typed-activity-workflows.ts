import * as Effect from "effect/Effect";
import { callActivity, workflowBundle, sleepUntil } from "../../engine-sandbox.js";
import { Reserve, TypedActivityDemo } from "./typed-activity-demo.js";

// Exercises the typed seam end-to-end: an absolute-time durable sleep, a
// decoded success, and a typed failure caught by tag (never retried).
const TypedActivityDemoLive = TypedActivityDemo.toLayer((payload) =>
  Effect.gen(function* () {
    yield* sleepUntil({ name: "not-before", timestamp: payload.notBeforeISO });
    return yield* callActivity(Reserve, { sku: payload.sku, quantity: 2 }).pipe(
      Effect.catchTag("OutOfStock", (error) => Effect.succeed(`backordered:${error.sku}`)),
    );
  }),
);

export default workflowBundle(TypedActivityDemoLive);
