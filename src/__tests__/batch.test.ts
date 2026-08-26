// Sliding-window batch processing, mirroring the Temporal
// `batch-sliding-window` sample (see EXAMPLES.md): an orchestrator starts
// discarded record children up to a window, children report completion
// through a workflow-to-workflow mailbox, and the orchestrator
// continues-as-new mid-batch carrying its in-flight set — proving mailbox
// delivery survives the run change (the reports target the stable workflow
// id).
//
// Like the mutex test, the protocol advances through worker round-trips, so
// closure is awaited by polling status in real time before attaching for
// results.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { BatchDemo } from "./fixtures/batch-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/batch-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-batch");

describe("sliding-window batch", { concurrent: false }, () => {
  it("processes every record once across windows and run changes", async () => {
    const processed: string[] = [];
    const activities: TestActivities = {
      processRecord: async (index: unknown) => {
        processed.push(String(index));
        return "done";
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = {
        requestId: "batch-1",
        total: 5,
        window: 2,
        offset: 0,
        inFlight: [],
      } as const;
      const executionId = await run(BatchDemo.execute(payload, { discard: true }));

      const handle = temporal.env.client.workflow.getHandle(executionId);
      let status = "RUNNING";
      for (let i = 0; i < 300 && (status === "RUNNING" || status === "CONTINUED_AS_NEW"); i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        status = (await handle.describe()).status.name;
      }
      expect(status).toBe("COMPLETED");

      expect(await run(BatchDemo.execute(payload))).toBe("processed:5");
      expect([...processed].toSorted()).toEqual(["0", "1", "2", "3", "4"]);

      // With 2 children per run and 5 records, the final run continued from
      // a previous one.
      const events = (await handle.fetchHistory()).events ?? [];
      expect(
        events[0]?.workflowExecutionStartedEventAttributes?.continuedExecutionRunId,
      ).toBeTruthy();
    });
  }, 120_000);
});
