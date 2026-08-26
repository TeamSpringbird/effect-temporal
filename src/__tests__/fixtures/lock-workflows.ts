// Mutex demo — bundle entrypoint.

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import {
  callRawActivity,
  workflowBundle,
  offerMailbox,
  takeMailbox,
} from "../../engine-sandbox.js";
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
      const request = yield* takeMailbox(AcquireRequests);
      yield* offerMailbox(Grants, { workflowId: request.requester, payload: { token } });
      while ((yield* takeMailbox(Releases)).token !== token) {
        // A stale release from a misbehaving holder never unlocks a newer
        // grant.
      }
    }
    return `served:${payload.grants}`;
  }),
);

const ContenderDemoLive = ContenderDemo.toLayer((payload, executionId) =>
  Effect.gen(function* () {
    yield* offerMailbox(AcquireRequests, {
      workflowId: payload.lockExecutionId,
      payload: { requester: executionId },
    });
    const grant = yield* takeMailbox(Grants);

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

    yield* offerMailbox(Releases, {
      workflowId: payload.lockExecutionId,
      payload: { token: grant.token },
    });
    return `done:${payload.name}`;
  }),
);

export default workflowBundle(Layer.mergeAll(LockDemoLive, ContenderDemoLive));
