// The single-declaration demo shared by definition.test.ts: every
// capability declared ONCE with define*, one engine-agnostic handler using
// them directly. Note what this module imports: upstream Effect and the
// definition module only — no engine-sandbox, no Temporal. The memory test
// and the Temporal bundle both load it.

import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Workflow from "effect/unstable/workflow/Workflow";
import {
  defineActivity,
  defineDeferred,
  defineMailbox,
  defineState,
  defineUpdate,
  version,
} from "../../definition.js";

export const CardDeclined = Schema.TaggedStruct("CardDeclined", {
  orderId: Schema.String,
});

export const OrderFlow = Workflow.make("defOrder", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: CardDeclined,
});

export const Reserve = defineActivity("defOrder/reserve", {
  payload: { orderId: Schema.String },
  success: Schema.String,
});

export const Charge = defineActivity("defOrder/charge", {
  payload: { orderId: Schema.String, amountCents: Schema.Finite },
  success: Schema.String,
  error: CardDeclined,
});

export const Approval = defineDeferred("defOrder/approval", {
  success: Schema.String,
});

export const Priority = defineMailbox("defOrder/priority", {
  payload: Schema.Struct({ level: Schema.Finite }),
});

export const SetAmount = defineUpdate("defOrder/setAmount", {
  payload: Schema.Struct({ amountCents: Schema.Finite }),
  success: Schema.Finite, // the previous amount
  error: Schema.String, // "amount-too-low"
});

export const Status = defineState("defOrder/status", {
  value: Schema.Struct({ phase: Schema.String }),
});

/** The handler: primitives are yielded directly; its only requirement is
 * `WorkflowOps`, so it runs on Temporal or on the in-memory test runtime. */
export const orderHandler = (payload: { readonly orderId: string }) =>
  Effect.gen(function* () {
    yield* Status.set({ phase: "reserving" });
    const reservation = yield* Reserve({ orderId: payload.orderId });

    // A typed update: respond with the PREVIOUS amount, or a typed refusal.
    yield* Status.set({ phase: "pricing" });
    let amountCents = 1000;
    const request = yield* SetAmount.take;
    if (request.payload.amountCents < 100) {
      yield* request.respond(Exit.fail("amount-too-low"));
    } else {
      yield* request.respond(Exit.succeed(amountCents));
      amountCents = request.payload.amountCents;
    }

    // A mailbox message, a patch-marker branch, and a one-shot approval.
    const priority = yield* Priority.take;
    const pricing = yield* version("defOrder/pricing", ["flat", "tiered"]);
    yield* Status.set({ phase: "awaiting-approval" });
    const approver = yield* Approval.await;

    // A typed activity failure flows straight into the workflow error channel.
    const receipt = yield* Charge({ orderId: payload.orderId, amountCents });

    yield* Status.set({ phase: "complete" });
    return `${reservation}|${receipt}|p${priority.level}|${pricing}|by:${approver}`;
  });

/** Worker-side activity implementations, bound to the declarations by the
 * tests (memory and Temporal alike) via `handle`. */
export const reserveImpl = ({ orderId }: { readonly orderId: string }) =>
  Effect.succeed(`res-${orderId}`);

export const chargeImpl = ({
  orderId,
  amountCents,
}: {
  readonly orderId: string;
  readonly amountCents: number;
}) =>
  amountCents >= 10_000
    ? Effect.fail({ _tag: "CardDeclined", orderId } as const)
    : Effect.succeed(`receipt-${orderId}-${amountCents}`);
