import { fileURLToPath } from "node:url";
import { CancelledFailure } from "@temporalio/client";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expect, it } from "vitest";
import { handle, implementActivities } from "../activities.js";
import { defineActivity } from "../definition.js";
import { makeActivityRunner } from "../worker.js";
import { createWorkflowTestEnv } from "./utils/workflow-test-env.js";

const temporal = createWorkflowTestEnv("runner-cancellation", { mode: "local" });
const waitForCancellation = defineActivity("waitForCancellation", { payload: Schema.Null });

it("delivers server cancellation through heartbeats and waits for Effect cleanup", async () => {
  const started = Promise.withResolvers<void>();
  let finalized = false;
  const runner = makeActivityRunner({ context: Context.empty(), heartbeatInterval: "100 millis" });
  const activities = implementActivities(runner, [handle(waitForCancellation, () =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() => Effect.sync(() => { finalized = true; }));
      started.resolve();
      yield* Effect.never;
    }).pipe(Effect.scoped),
  )]);
  await temporal.withWorker({
    activities,
    workflowsPath: fileURLToPath(new URL("./fixtures/runner-workflows.ts", import.meta.url)),
  }, async (taskQueue) => {
    const execution = await temporal.env.client.workflow.start("runnerCancellation", {
      taskQueue,
      workflowId: taskQueue,
    });
    await Promise.race([started.promise, execution.result()]);
    await execution.cancel();
    await expect(execution.result()).rejects.toMatchObject({ cause: expect.any(CancelledFailure) });
    expect(finalized).toBe(true);
  });
});
