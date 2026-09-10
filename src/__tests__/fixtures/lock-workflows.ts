// Mutex demo — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import { callRawActivity, workflowBundle, offerMailbox } from "../../engine-sandbox.js";
import { AcquireRequests, ContenderDemo, Grants, LockDemo, Releases } from "./lock-demo.js";

const acts = proxyActivities<{
  enter(name: string): Promise<string>;
  leave(name: string): Promise<string>;
}>({
  startToCloseTimeout: "10 seconds",
});

const LockDemoLive = LockDemo.toLayer((payload) =>
  Effect.gen(function* () {
    for (let token = 0; token < payload.grants; token++) {
      const request = yield* AcquireRequests.take;
      yield* offerMailbox(Grants.mailbox, { workflowId: request.requester, payload: { token } });
      while ((yield* Releases.take).token !== token) {
        // A stale release from a misbehaving holder never unlocks a newer
        // grant.
      }
    }
    return `served:${payload.grants}`;
  }),
);

const ContenderDemoLive = ContenderDemo.toLayer((payload, executionId) =>
  Effect.gen(function* () {
    yield* offerMailbox(AcquireRequests.mailbox, {
      workflowId: payload.lockExecutionId,
      payload: { requester: executionId },
    });
    const grant = yield* Grants.take;

    yield* Activity.make({
      name: "enter",
      success: Schema.String,
      execute: callRawActivity(() => acts.enter(payload.name)),
    });
    yield* Activity.make({
      name: "leave",
      success: Schema.String,
      execute: callRawActivity(() => acts.leave(payload.name)),
    });

    yield* offerMailbox(Releases.mailbox, {
      workflowId: payload.lockExecutionId,
      payload: { token: grant.token },
    });
    return `done:${payload.name}`;
  }),
);

export default workflowBundle(Layer.mergeAll(LockDemoLive, ContenderDemoLive));
