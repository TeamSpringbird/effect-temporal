// The workflow bundle — Temporal's workflowsPath points here. The whole body
// is an Effect running deterministically inside the workflow sandbox; every
// side effect goes through a typed activity. The export name equals the
// workflow tag.

import { Effect } from "effect";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as Workflow from "effect/unstable/workflow/Workflow";
import { callActivity, makeTemporalWorkflow, setStateCell } from "@springbird/effect-temporal/engine-sandbox";
import { Charge, ManagerApproval, OrderSaga, OrderStatus, Release, Reserve } from "./definitions.js";

export const orderSaga = makeTemporalWorkflow(OrderSaga, (payload) =>
  Effect.gen(function* () {
    yield* setStateCell(OrderStatus, { phase: "reserving" });

    // A compensated step: if anything later fails typed or the run is
    // cancelled, `Release` runs during the unwind.
    const reservation = yield* callActivity(Reserve, { orderId: payload.orderId }).pipe(
      Workflow.withCompensation((value) =>
        callActivity(Release, { reservation: value }).pipe(Effect.asVoid),
      ),
    );

    yield* setStateCell(OrderStatus, { phase: "charging" });
    // A declined card lands in the workflow's TYPED error channel (and the
    // run fails red in the Temporal UI) — after compensation has released
    // the reservation.
    const receipt = yield* callActivity(Charge, {
      orderId: payload.orderId,
      card: payload.card,
    });

    // A durable timer: survives worker restarts; costs no worker resources.
    yield* setStateCell(OrderStatus, { phase: "cooling-off" });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "2 seconds" });

    // Block durably until a human approves (a signal from outside).
    yield* setStateCell(OrderStatus, { phase: "awaiting-approval" });
    const approver = yield* DurableDeferred.await(ManagerApproval);

    yield* setStateCell(OrderStatus, { phase: "complete" });
    return `${reservation}|${receipt}|approved-by:${approver}`;
  }),
);
