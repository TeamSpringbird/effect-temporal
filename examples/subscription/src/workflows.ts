// The workflow bundle — the long-lived entity loop. Each iteration races
// the next billing timer against inbound messages (a typed plan-change
// update, a cancellation), and the run continues-as-new every few cycles so
// history never grows without bound.
//
// The workflow registers itself with `Workflow.toLayer`, and
// `workflowBundle` hosts every registration behind the bundle's one
// dynamic default export.

import { Effect } from "effect";
import * as Option from "effect/Option";
import * as Exit from "effect/Exit";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import {
  callActivity,
  continueAsNew,
  workflowBundle,
  pollMailbox,
  setStateCell,
  takeMailbox,
  takeUpdate,
} from "@springbird/effect-temporal/engine-sandbox";
import {
  CancelRequests,
  ChargeCard,
  SetPlan,
  Subscription,
  SubscriptionStatus,
} from "./definitions.js";

/** Continue-as-new after this many cycles in one run, keeping history flat. */
const CYCLES_PER_RUN = 2;

const MINIMUM_PLAN_CENTS = 100;

const SubscriptionLive = Subscription.toLayer((payload) =>
  Effect.gen(function* () {
    let planCents = payload.planCents;
    let cyclesBilled = payload.cyclesBilled;
    let cyclesThisRun = 0;

    // Cells are per-run: republish immediately so observers never see a gap
    // after continue-as-new.
    yield* setStateCell(SubscriptionStatus, { phase: "active", planCents, cyclesBilled });

    while (true) {
      const winner = yield* Effect.raceAll([
        // The next billing cycle — a durable timer (name unique per sleep).
        DurableClock.sleep({
          name: `cycle-${cyclesThisRun}`,
          duration: "1 second",
        }).pipe(Effect.map(() => ({ kind: "bill" as const }))),
        // A plan change — answered with the PREVIOUS plan, typed both ways.
        takeUpdate(SetPlan).pipe(Effect.map((request) => ({ kind: "plan" as const, request }))),
        // A cancellation — fire-and-forget from anywhere.
        takeMailbox(CancelRequests).pipe(
          Effect.map((message) => ({ kind: "cancel" as const, message })),
        ),
      ]);

      switch (winner.kind) {
        case "bill": {
          yield* callActivity(ChargeCard, { customerId: payload.customerId, amountCents: planCents });
          cyclesBilled++;
          cyclesThisRun++;
          yield* setStateCell(SubscriptionStatus, { phase: "active", planCents, cyclesBilled });
          if (cyclesThisRun >= CYCLES_PER_RUN) {
            // Drain the mailbox BEFORE continuing — buffered messages do
            // not survive the run change. A cancellation that raced the
            // final cycle is honored instead of lost.
            const pendingCancel = yield* pollMailbox(CancelRequests);
            if (Option.isSome(pendingCancel)) {
              yield* setStateCell(SubscriptionStatus, { phase: "cancelled", planCents, cyclesBilled });
              return `cancelled(${pendingCancel.value.reason}) after ${cyclesBilled} cycles`;
            }
            return yield* continueAsNew(Subscription, {
              customerId: payload.customerId,
              planCents,
              cyclesBilled,
            });
          }
          break;
        }
        case "plan": {
          const requested = winner.request.payload.planCents;
          if (requested < MINIMUM_PLAN_CENTS) {
            yield* winner.request.respond(Exit.fail("plan-below-minimum"));
            break;
          }
          yield* winner.request.respond(Exit.succeed(planCents));
          planCents = requested;
          yield* setStateCell(SubscriptionStatus, { phase: "active", planCents, cyclesBilled });
          break;
        }
        case "cancel": {
          yield* setStateCell(SubscriptionStatus, { phase: "cancelled", planCents, cyclesBilled });
          return `cancelled(${winner.message.reason}) after ${cyclesBilled} cycles`;
        }
      }
    }
  }),
);

export default workflowBundle(SubscriptionLive);
