// The workflow bundle — Temporal's workflowsPath points here. The whole body
// is an Effect running deterministically inside the workflow sandbox; every
// side effect goes through a typed activity. The workflow registers itself
// with `Workflow.toLayer`, hosted by the bundle's default export — which
// provides the `WorkflowOps` runtime the declarations require.

import { Effect } from "effect";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as Workflow from "effect/unstable/workflow/Workflow";
import { workflowBundle } from "@springbird/effect-temporal/engine-sandbox";
import { Charge, ManagerApproval, OrderSaga, OrderStatus, Release, Reserve } from "./definitions.js";

const OrderSagaLive = OrderSaga.toLayer((payload) =>
  Effect.gen(function* () {
    yield* OrderStatus.set({ phase: "reserving" });

    // A compensated step: if anything later fails typed or the run is
    // cancelled, `Release` runs during the unwind.
    const reservation = yield* Reserve({ orderId: payload.orderId }).pipe(
      Workflow.withCompensation((value) =>
        Release({ reservation: value }).pipe(Effect.asVoid),
      ),
    );

    yield* OrderStatus.set({ phase: "charging" });
    // A declined card lands in the workflow's TYPED error channel (and the
    // run fails red in the Temporal UI) — after compensation has released
    // the reservation.
    const receipt = yield* Charge({
      orderId: payload.orderId,
      card: payload.card,
    });

    // A durable timer: survives worker restarts; costs no worker resources.
    yield* OrderStatus.set({ phase: "cooling-off" });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "2 seconds" });

    // Block durably until a human approves (a signal from outside).
    yield* OrderStatus.set({ phase: "awaiting-approval" });
    const approver = yield* ManagerApproval.await;

    yield* OrderStatus.set({ phase: "complete" });
    return `${reservation}|${receipt}|approved-by:${approver}`;
  }),
);

export default workflowBundle(OrderSagaLive);
