// Runnable end-to-end demo of the subscription entity — Temporal's
// canonical long-running-workflow example, in Effect. Boots a local
// Temporal dev server (downloaded automatically on first run — no Docker),
// runs a worker, and drives one subscription through its life:
//
//   1. billing cycles fire on durable timers; progress is read through the
//      status cell without touching the run
//   2. a plan change arrives as a typed UPDATE — the caller gets the
//      previous plan back; a too-cheap plan gets a typed rejection
//   3. the run CONTINUES-AS-NEW every couple of cycles (watch the runId
//      change while the workflow id and state carry over)
//   4. a cancellation arrives as a MAILBOX message; the entity finishes and
//      its final status outlives the run

import { Effect, Layer, Option, Result } from "effect";
import type * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import type { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import {
  handle,
  implementActivities,
  makeEffectWorkflowActivities,
  type ActivityRunner,
} from "@springbird/effect-temporal/activities";
import { layerWorkflowClient, WorkflowClient } from "@springbird/effect-temporal/client";
import { layerTemporalClientEngine } from "@springbird/effect-temporal/engine-client";
import {
  CancelRequests,
  ChargeCard,
  SetPlan,
  Subscription,
  SubscriptionStatus,
} from "./definitions.js";

const workflowsPath = new URL("./workflows.ts", import.meta.url).pathname;
const TASK_QUEUE = "subscription-demo";

// ── Worker side ──────────────────────────────────────────────────────────────

const runner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

let charges = 0;
const activities = implementActivities(runner, [
  handle(ChargeCard, ({ customerId, amountCents }) =>
    Effect.sync(() => {
      charges++;
      console.log(`  [worker] charged ${customerId} ${amountCents}¢ (cycle ${charges})`);
      return `receipt-${charges}`;
    }),
  ),
]);

// ── The demo ─────────────────────────────────────────────────────────────────

console.log("booting a local Temporal dev server (first run downloads it)...");
const env = await TestWorkflowEnvironment.createLocal();
const client: Client = env.client;

const worker = await Worker.create({
  connection: env.nativeConnection,
  ...(env.namespace === undefined ? {} : { namespace: env.namespace }),
  taskQueue: TASK_QUEUE,
  workflowsPath,
  activities: { ...activities, ...makeEffectWorkflowActivities(client) },
});

const layers = Layer.mergeAll(
  layerWorkflowClient({ client, taskQueue: TASK_QUEUE }),
  layerTemporalClientEngine({ client, taskQueue: TASK_QUEUE }),
);
const runDemo = <A, E>(
  effect: Effect.Effect<A, E, WorkflowClient | WorkflowEngine.WorkflowEngine>,
) => Effect.runPromise(effect.pipe(Effect.provide(layers)) as Effect.Effect<A, E>);

await worker.runUntil(() =>
  runDemo(
    Effect.gen(function* () {
      const wf = yield* WorkflowClient;
      const payload = { customerId: "cust-1", planCents: 999, cyclesBilled: 0 };

      console.log("\n1. start the subscription; billing cycles fire on durable timers");
      yield* wf.start(Subscription, payload);
      const workflowId = yield* Subscription.executionId(payload);
      const runIdAtStart = (yield* Effect.promise(() =>
        client.workflow.getHandle(workflowId).describe(),
      )).runId;

      yield* Effect.sleep("1500 millis");
      const early = yield* wf.readStateCell(SubscriptionStatus.cell, workflowId);
      console.log("  status:", Option.getOrNull(early));

      console.log("\n2. change the plan through a typed update");
      const previous = yield* wf.executeUpdate(SetPlan.update, workflowId, { planCents: 1999 });
      console.log("  previous plan:", previous, "¢");
      const rejected = yield* Effect.result(
        wf.executeUpdate(SetPlan.update, workflowId, { planCents: 50 }),
      );
      if (Result.isFailure(rejected)) console.log("  typed rejection:", rejected.failure);

      console.log("\n3. keep billing until the run continues-as-new");
      let runIdNow = runIdAtStart;
      for (let i = 0; i < 40 && runIdNow === runIdAtStart; i++) {
        yield* Effect.sleep("250 millis");
        runIdNow = (yield* Effect.promise(() =>
          client.workflow.getHandle(workflowId).describe(),
        )).runId;
      }
      console.log("  continued-as-new:", runIdNow !== runIdAtStart, "(same workflow id, fresh history)");
      const carried = yield* wf.readStateCell(SubscriptionStatus.cell, workflowId);
      console.log("  carried state republished:", Option.getOrNull(carried));

      console.log("\n4. cancel through the mailbox; the final status outlives the run");
      yield* wf.offerMailbox(CancelRequests.mailbox, workflowId, { reason: "user-requested" });
      const summary = yield* wf.execute(Subscription, payload); // attaches to the chain
      console.log("  result:", summary);
      const final = yield* wf.readStateCell(SubscriptionStatus.cell, workflowId);
      console.log("  status after close:", Option.getOrNull(final));
    }),
  ),
);

await env.teardown();
console.log("\ndone.");
