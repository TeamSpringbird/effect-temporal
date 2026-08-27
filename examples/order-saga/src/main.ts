// Runnable end-to-end demo. Boots a local Temporal dev server (downloaded
// automatically by @temporalio/testing on first run — no Docker, no setup),
// runs a worker, and walks three scenarios:
//
//   A. the happy path — typed activities, a compensated step, a durable
//      timer, observable state read mid-flight, an external approval, and
//      the idempotency contract (a second execute attaches, no re-work)
//   B. a typed domain failure — CardDeclined lands in the Effect error
//      channel, compensation releases the reservation, the run shows FAILED
//   C. graceful cancellation — the run unwinds, compensation releases, the
//      run shows CANCELLED
//
// To run against your own Temporal cluster instead, replace the
// TestWorkflowEnvironment block with a `Connection.connect(...)` +
// `new Client(...)` and a `Worker.create` against that connection.

import { Effect, Exit, Layer, Option, Result } from "effect";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
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
import { Charge, ManagerApproval, OrderSaga, OrderStatus, Release, Reserve } from "./definitions.js";

const workflowsPath = new URL("./workflows.ts", import.meta.url).pathname;

// ── Worker side ──────────────────────────────────────────────────────────────

/** How this worker runs activity Effects. This is the seam where your app
 * plugs in its ManagedRuntime, spans, and error reporting; the demo just
 * runs them. */
const runner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

/** Call counts prove compensation and idempotency below. */
const calls = { reserve: 0, release: 0, charge: 0 };

const activities = {
  ...implementActivities(runner, [
    handle(Reserve, ({ orderId }) =>
      Effect.sync(() => {
        calls.reserve++;
        return `res-${orderId}`;
      }),
    ),
    handle(Release, ({ reservation }) =>
      Effect.sync(() => {
        calls.release++;
        console.log(`  [worker] released ${reservation}`);
      }),
    ),
    handle(Charge, ({ orderId, card }) =>
      Effect.suspend(() => {
        calls.charge++;
        return card === "declined"
          ? Effect.fail({ _tag: "CardDeclined", orderId } as const)
          : Effect.succeed(`receipt-${orderId}`);
      }),
    ),
  ]),
};

// ── The demo ─────────────────────────────────────────────────────────────────

console.log("booting a local Temporal dev server (first run downloads it)...");
const env = await TestWorkflowEnvironment.createLocal();
const client: Client = env.client;

const worker = await Worker.create({
  connection: env.nativeConnection,
  ...(env.namespace === undefined ? {} : { namespace: env.namespace }),
  taskQueue: "order-saga-demo",
  workflowsPath,
  activities: {
    ...activities,
    // The attach bridge: register in every worker running these workflows.
    ...makeEffectWorkflowActivities(client),
  },
});

// Both layers: WorkflowClient for app-style calls, and the raw engine so
// Effect's own APIs (DurableDeferred.done, OrderSaga.poll) work too.
const layers = Layer.mergeAll(
  layerWorkflowClient({ client, taskQueue: "order-saga-demo" }),
  layerTemporalClientEngine({ client, taskQueue: "order-saga-demo" }),
);
const runDemo = <A, E>(
  effect: Effect.Effect<A, E, WorkflowClient | WorkflowEngine.WorkflowEngine>,
) => Effect.runPromise(effect.pipe(Effect.provide(layers)) as Effect.Effect<A, E>);

await worker.runUntil(async () => {
  // ── A. Happy path ─────────────────────────────────────────────────────────
  console.log("\nA. happy path");
  await runDemo(
    Effect.gen(function* () {
      const wf = yield* WorkflowClient;

      // Fire and forget; the digest of the idempotency key is the workflow id.
      yield* wf.start(OrderSaga, { orderId: "ord-1", card: "visa" });
      const workflowId = yield* OrderSaga.executionId({ orderId: "ord-1", card: "visa" });

      // Observe progress mid-flight through the state cell (a query — it
      // never perturbs the run).
      yield* Effect.sleep("1 second");
      const status = yield* wf.readStateCell(OrderStatus.cell, workflowId);
      console.log("  status mid-flight:", Option.getOrElse(status, () => ({ phase: "?" })));

      // The manager approves — a signal from entirely outside the workflow.
      yield* DurableDeferred.done(ManagerApproval.deferred, {
        token: DurableDeferred.tokenFromExecutionId(ManagerApproval.deferred, {
          workflow: OrderSaga,
          executionId: workflowId,
        }),
        exit: Exit.succeed("ben"),
      });

      // execute on the same payload ATTACHES to the running execution and
      // returns its result — and a repeat execute returns the same result
      // without re-running anything.
      const result = yield* wf.execute(OrderSaga, { orderId: "ord-1", card: "visa" });
      const again = yield* wf.execute(OrderSaga, { orderId: "ord-1", card: "visa" });
      console.log("  result:", result);
      console.log("  idempotent re-execute:", again === result, "| reserve calls:", calls.reserve);
    }),
  );

  // ── B. Typed domain failure + compensation ───────────────────────────────
  console.log("\nB. typed failure (declined card)");
  await runDemo(
    Effect.gen(function* () {
      const wf = yield* WorkflowClient;
      const outcome = yield* Effect.result(
        wf.execute(OrderSaga, { orderId: "ord-2", card: "declined" }),
      );
      if (Result.isFailure(outcome)) {
        console.log("  typed failure:", outcome.failure);
      }
      console.log("  compensation released the reservation:", calls.release >= 1);
    }),
  );

  // ── C. Graceful cancellation ─────────────────────────────────────────────
  console.log("\nC. cancellation");
  await runDemo(
    Effect.gen(function* () {
      const wf = yield* WorkflowClient;
      yield* wf.start(OrderSaga, { orderId: "ord-3", card: "visa" });
      const workflowId = yield* OrderSaga.executionId({ orderId: "ord-3", card: "visa" });

      // Cancel while the saga sleeps in its cooling-off timer: the run
      // unwinds, compensation runs, and the run records CANCELLED.
      yield* Effect.sleep("1 second");
      yield* wf.interrupt(workflowId);

      // Poll reports the cancellation as an interrupted exit.
      let polled = Option.none<unknown>();
      for (let i = 0; i < 50 && Option.isNone(polled); i++) {
        yield* Effect.sleep("200 millis");
        polled = yield* OrderSaga.poll(workflowId);
      }
      const releasesBefore = calls.release;
      console.log("  run closed:", Option.isSome(polled), "| releases:", releasesBefore);
    }),
  );
});

await env.teardown();
console.log("\ndone.");
