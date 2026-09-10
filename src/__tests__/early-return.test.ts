// Early return via update, mirroring the Temporal `early-return` sample
// (see EXAMPLES.md): the caller gets a fast confirmation through
// `executeUpdate` while the workflow's slower completion continues to the
// final result. Runs on the local dev server — the confirmation gates on a
// real timer, which the time-skipping server would only advance while a
// result is awaited.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { executeUpdate, makeTemporalClientEngine } from "../engine-client.js";
import { GetConfirmation, TransactionDemo } from "./fixtures/transaction-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(
  new URL("./fixtures/transaction-workflows.ts", import.meta.url),
);

const temporal = createWorkflowTestEnv("effect-early-return", { mode: "local" });

const activities: TestActivities = {};

describe("early return via update", { concurrent: false }, () => {
  it("confirms early while the workflow continues to the final result", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "txn-1" } as const;
      const executionId = await run(TransactionDemo.execute(payload, { discard: true }));

      // The early return: confirmed before the run completes.
      const confirmation = await run(
        executeUpdate(GetConfirmation.update, { client, workflowId: executionId, payload: {} }),
      );
      expect(confirmation).toBe("confirmed");
      expect((await client.workflow.getHandle(executionId).describe()).status.name).toBe("RUNNING");

      expect(await run(TransactionDemo.execute(payload))).toBe("complete:77");
    });
  }, 120_000);
});
