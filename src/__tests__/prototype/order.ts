// PROTOTYPE — throwaway. THE single declaration, and the one
// engine-agnostic handler bound to it. Note what this module imports:
// upstream Effect only, plus the prototype's def module. No engine-sandbox,
// no Temporal, no per-primitive make calls — the leak under test.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { defineWorkflow } from "./def.js";

export const CardDeclined = Schema.TaggedStruct("CardDeclined", {
  orderId: Schema.String,
});

export const Order = defineWorkflow("protoOrder", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: CardDeclined,
  activities: {
    reserve: { payload: Schema.Struct({ orderId: Schema.String }), success: Schema.String },
    charge: {
      payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Finite }),
      success: Schema.String,
      error: CardDeclined,
    },
  },
  messages: {
    approval: { deferred: Schema.String },
    priority: { mailbox: Schema.Struct({ level: Schema.Finite }) },
    setAmount: {
      update: {
        payload: Schema.Struct({ amountCents: Schema.Finite }),
        success: Schema.Finite, // the previous amount
        error: Schema.String, // "amount-too-low"
      },
    },
  },
  state: {
    status: Schema.Struct({ phase: Schema.String }),
  },
});

/** The handler: every capability arrives through `ops`, fully typed from
 * the declaration above; its R channel is `never`. */
export const orderBound = Order.handler((payload, ops) =>
  Effect.gen(function* () {
    yield* ops.state.status.set({ phase: "reserving" });
    const reservation = yield* ops.activity.reserve({ orderId: payload.orderId });

    // A typed update: respond with the PREVIOUS amount, or a typed refusal.
    yield* ops.state.status.set({ phase: "pricing" });
    let amountCents = 1000;
    const request = yield* ops.message.setAmount.take;
    if (request.payload.amountCents < 100) {
      yield* request.respond(Exit.fail("amount-too-low"));
    } else {
      yield* request.respond(Exit.succeed(amountCents));
      amountCents = request.payload.amountCents;
    }

    // A mailbox message and a one-shot approval.
    const priority = yield* ops.message.priority.take;
    yield* ops.state.status.set({ phase: "awaiting-approval" });
    const approver = yield* ops.message.approval.await;

    // A typed activity failure flows straight into the workflow error channel.
    const receipt = yield* ops.activity.charge({ orderId: payload.orderId, amountCents });

    yield* ops.state.status.set({ phase: "complete" });
    return `${reservation}|${receipt}|p${priority.level}|by:${approver}`;
  }),
);

/** Worker-side activity implementations — completeness-checked against the
 * declaration; engine decides where they run. */
export const orderImpls = Order.implement({
  reserve: ({ orderId }) => Effect.succeed(`res-${orderId}`),
  charge: ({ orderId, amountCents }) =>
    amountCents >= 10_000
      ? Effect.fail({ _tag: "CardDeclined", orderId } as const)
      : Effect.succeed(`receipt-${orderId}-${amountCents}`),
});
