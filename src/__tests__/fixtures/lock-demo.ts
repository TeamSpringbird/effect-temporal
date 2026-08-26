// Mutex demo definitions, mirroring the Temporal `mutex` sample: a lock
// workflow serializes contenders that request, receive, and release a lock
// entirely through workflow-to-workflow mailbox messages.

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableMailbox from "../../mailbox.js";

export const AcquireRequests = DurableMailbox.make("acquire", {
  payload: Schema.Struct({ requester: Schema.String }),
});

export const Grants = DurableMailbox.make("grant", {
  payload: Schema.Struct({ token: Schema.Finite }),
});

export const Releases = DurableMailbox.make("release", {
  payload: Schema.Struct({ token: Schema.Finite }),
});

/** Grants the lock to `grants` requesters, one at a time. */
export const LockDemo = Workflow.make("effectLockDemo", {
  payload: { requestId: Schema.String, grants: Schema.Finite },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});

/** Acquires the lock, runs its critical section, releases. */
export const ContenderDemo = Workflow.make("effectContenderDemo", {
  payload: {
    requestId: Schema.String,
    lockExecutionId: Schema.String,
    name: Schema.String,
  },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
