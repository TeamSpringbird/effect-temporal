// Mutex demo — bundle entrypoint. Export names equal the workflow tags.

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Activity from "effect/unstable/workflow/Activity";
import { proxyActivities } from "@temporalio/workflow";
import {
  callRawActivity,
  makeTemporalWorkflow,
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

export const effectLockDemo = makeTemporalWorkflow(LockDemo, (payload) =>
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

export const effectContenderDemo = makeTemporalWorkflow(ContenderDemo, (payload, executionId) =>
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
