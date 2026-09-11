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
  continueAsNew,
  defineActivity,
  defineDeferred,
  defineMailbox,
  defineState,
  defineUpdate,
  executeChild,
  sleep,
  version,
  versioned,
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

// ── 0.4.0: timers, continue-as-new, children, run-table versioning ──────────
// Everything below is NEW in 0.4.0 and lives beside `OrderFlow`, whose
// handler is byte-for-byte the 0.3.0 one: the recorded 0.3.0 history in
// `histories/` must keep replaying through this bundle.

/** Grace period: a cancellation racing a durable timer — the pattern most
 * real workflows have, and the one the in-memory runtime could not run
 * before 0.4.0 (timers were not on the seam). */
export const CancelOrder = defineMailbox("defGrace/cancel", {
  payload: Schema.Struct({ reason: Schema.String }),
});

export const GraceFlow = Workflow.make("defGrace", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
});

export const graceHandler = (_payload: { readonly orderId: string }) =>
  Effect.gen(function* () {
    const winner = yield* Effect.raceFirst(
      CancelOrder.take.pipe(Effect.map((m) => ({ kind: "cancelled" as const, reason: m.reason }))),
      sleep({ name: "grace", duration: "1 hour" }).pipe(
        Effect.map(() => ({ kind: "elapsed" as const })),
      ),
    );
    return winner.kind === "cancelled" ? `cancelled:${winner.reason}` : "shipped";
  });

/** Continue-as-new: a counter that rolls its history every run. */
export const Tally = Workflow.make("defTally", {
  payload: { batchId: Schema.String, count: Schema.Finite },
  idempotencyKey: ({ batchId }) => batchId,
  success: Schema.String,
});

export const tallyHandler = (payload: { readonly batchId: string; readonly count: number }) =>
  Effect.gen(function* () {
    if (payload.count >= 2) return `tallied:${payload.count}`;
    return yield* continueAsNew(Tally, { batchId: payload.batchId, count: payload.count + 1 });
  });

/** A child with a typed failure, and a parent that starts it awaited or
 * discarded — both authored against `WorkflowOps` alone. */
export const Fulfil = Workflow.make("defFulfil", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: Schema.String,
});

export const fulfilHandler = (payload: { readonly orderId: string }) =>
  payload.orderId.endsWith("-bad")
    ? Effect.fail(`unfulfillable:${payload.orderId}`)
    : Effect.map(Reserve({ orderId: payload.orderId }), (r) => `fulfilled:${r}`);

export const Dispatch = Workflow.make("defDispatch", {
  payload: { orderId: Schema.String, discard: Schema.Boolean },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: Schema.String,
});

export const dispatchHandler = (payload: { readonly orderId: string; readonly discard: boolean }) =>
  Effect.gen(function* () {
    // The run-table form of `version`: key order is the chain order.
    const mode = yield* versioned("defDispatch/mode", {
      direct: Effect.succeed("direct"),
      routed: Effect.succeed("routed"),
    });
    if (payload.discard) {
      const childId = yield* executeChild(Fulfil, { orderId: payload.orderId }, { discard: true });
      return `${mode}|started:${childId}`;
    }
    const fulfilled = yield* executeChild(Fulfil, { orderId: payload.orderId });
    return `${mode}|${fulfilled}`;
  });
