// Mailbox demo definitions, shared by the bundle entrypoint and the
// client-side tests. Mirrors two Temporal samples:
//
//   `state` — long-lived Map state mutated by repeated signals
//   `timer-examples` (UpdatableTimer) — a timer whose deadline is updated
//     by signals while it sleeps

import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableMailbox from "../../mailbox.js";
import * as StateCell from "../../state-cell.js";

export const StateUpdates = DurableMailbox.make("state-updates", {
  payload: Schema.Union([
    Schema.Struct({
      op: Schema.Literal("set"),
      key: Schema.String,
      value: Schema.Finite,
    }),
    Schema.Struct({ op: Schema.Literal("del"), key: Schema.String }),
    Schema.Struct({ op: Schema.Literal("finish") }),
  ]),
});

/** The `state` sample's query half: the current entries, published after
 * every update and readable mid-flight or after completion. */
export const StateSnapshot = StateCell.make("state-snapshot", {
  value: Schema.Record(Schema.String, Schema.Finite),
});

/** Mirrors `state`: applies updates in delivery order until `finish`, then
 * returns the final entries. */
export const StateDemo = Workflow.make("effectStateDemo", {
  payload: { requestId: Schema.String },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});

export const DeadlineUpdates = DurableMailbox.make("deadline-updates", {
  payload: Schema.Struct({ millis: Schema.Finite }),
});

/** Mirrors `UpdatableTimer`: sleeps toward a deadline that mailbox messages
 * replace; returns how many updates arrived before the timer fired. */
export const UpdatableTimerDemo = Workflow.make("effectUpdatableTimerDemo", {
  payload: { requestId: Schema.String, initialMillis: Schema.Finite },
  idempotencyKey: ({ requestId }) => requestId,
  success: Schema.String,
});
