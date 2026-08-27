// The shared contract: workflow, activities, approval, and observable state,
// declared ONCE and imported by the workflow bundle (workflows.ts), the
// worker (main.ts), and the client side (main.ts). No `@temporalio/*`
// imports here — this module loads in every world.

import { Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import {
  defineActivity,
  defineDeferred,
  defineState,
} from "@springbird/effect-temporal/definition";

/** Reserving inventory — the compensated step. */
export const Reserve = defineActivity("reserve", {
  payload: { orderId: Schema.String },
  success: Schema.String, // the reservation id
  options: { startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3 } },
});

/** Releasing a reservation — the compensation for `Reserve`. */
export const Release = defineActivity("release", {
  payload: { reservation: Schema.String },
  options: { startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3 } },
});

/** A declined card is a DOMAIN outcome, not an infrastructure failure: it is
 * a typed error, never retried, delivered to the workflow's error channel. */
export const CardDeclined = Schema.TaggedStruct("CardDeclined", {
  orderId: Schema.String,
});

export const Charge = defineActivity("charge", {
  payload: { orderId: Schema.String, card: Schema.String },
  success: Schema.String, // the receipt id
  error: CardDeclined,
  options: { startToCloseTimeout: "10 seconds", retry: { maximumAttempts: 3 } },
});

/** The manager's one-shot approval, completed from outside the workflow. */
export const ManagerApproval = defineDeferred("manager-approval", {
  success: Schema.String, // who approved
});

/** Observable progress, readable mid-flight and after the run closes. */
export const OrderStatus = defineState("order-status", {
  value: Schema.Struct({ phase: Schema.String }),
});

export const OrderSaga = Workflow.make("orderSaga", {
  payload: { orderId: Schema.String, card: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: CardDeclined,
});
