// The shared contract for the subscription entity: workflow, billing
// activity, the plan-change update, the cancellation mailbox, and the
// observable status cell. Declared once; imported by the bundle
// (workflows.ts), the worker, and the client side (main.ts). No
// `@temporalio/*` imports — this module loads in every world.

import { Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableMailbox from "@springbird/effect-temporal/mailbox";
import * as DurableUpdate from "@springbird/effect-temporal/update";
import * as StateCell from "@springbird/effect-temporal/state-cell";
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";

/** One billing cycle's charge. */
export const ChargeCard = TypedActivity.make("chargeCard", {
  payload: { customerId: Schema.String, amountCents: Schema.Finite },
  success: Schema.String, // receipt id
  options: { startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3 } },
});

/**
 * Change the plan mid-subscription — request/response with typed channels:
 * the caller gets the PREVIOUS plan back as the typed success, or a typed
 * rejection for plans below the floor.
 */
export const SetPlan = DurableUpdate.make("set-plan", {
  payload: Schema.Struct({ planCents: Schema.Finite }),
  success: Schema.Finite, // the previous plan
  error: Schema.String, // "plan-below-minimum"
});

/** Cancellation requests — fire-and-forget inbound messages. */
export const CancelRequests = DurableMailbox.make("cancel-requests", {
  payload: Schema.Struct({ reason: Schema.String }),
});

/** Observable state, readable mid-flight and after the run closes. Cells
 * are per-run: each continue-as-new republishes. */
export const SubscriptionStatus = StateCell.make("subscription-status", {
  value: Schema.Struct({
    phase: Schema.String,
    planCents: Schema.Finite,
    cyclesBilled: Schema.Finite,
  }),
});

/**
 * The subscription entity. `cyclesBilled` is the carried state each
 * continue-as-new hands to the next run; the idempotency key ignores it, so
 * the workflow id stays the customer's across the whole run chain.
 */
export const Subscription = Workflow.make("subscription", {
  payload: {
    customerId: Schema.String,
    planCents: Schema.Finite,
    cyclesBilled: Schema.Finite,
  },
  idempotencyKey: ({ customerId }) => customerId,
  success: Schema.String,
});
