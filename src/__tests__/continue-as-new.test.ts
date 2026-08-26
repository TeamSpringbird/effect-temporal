// `continueAsNew` end to end, mirroring the Temporal `continue-as-new`
// sample (see EXAMPLES.md): a looping workflow continues as a fresh run per
// iteration. The history assertion is what separates real continue-as-new
// from an in-run loop — the latest run's first event must carry the previous
// run's id, and its history must contain only ONE activity out of the three
// the loop executed.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine, readStateCell } from "../engine-client.js";
import { CellLoopDemo, LoopDemo, LoopGate, LoopStage } from "./fixtures/loop-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/loop-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-can");

describe("continueAsNew over Temporal", { concurrent: false }, () => {
  it("loops across fresh runs and completes with the final one's result", async () => {
    const recorded: string[] = [];
    const activities: TestActivities = {
      record: async (iteration: unknown) => {
        recorded.push(String(iteration));
        return "recorded";
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      // `result()` follows the continue-as-new chain to the final run.
      const result = await run(LoopDemo.execute({ requestId: "loop-1", iteration: 0 }));
      expect(result).toBe("done:2");
      expect(recorded).toEqual(["0", "1", "2"]);

      const executionId = await run(LoopDemo.executionId({ requestId: "loop-1", iteration: 0 }));
      const handle = temporal.env.client.workflow.getHandle(executionId);
      expect((await handle.describe()).status.name).toBe("COMPLETED");

      // The latest run continued from a previous one, with a fresh history.
      const events = (await handle.fetchHistory()).events ?? [];
      const started = events[0]?.workflowExecutionStartedEventAttributes;
      expect(started?.continuedExecutionRunId).toBeTruthy();
      const activityCount = events.filter(
        (event) => event.activityTaskScheduledEventAttributes != null,
      ).length;
      expect(activityCount).toBe(1);
    });
  }, 120_000);

  it("reads a state cell as None after continue-as-new until the new run republishes", async () => {
    const activities: TestActivities = {};
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "cell-1", iteration: 0 } as const;
      const executionId = await run(CellLoopDemo.execute(payload, { discard: true }));
      const handle = client.workflow.getHandle(executionId);

      const readStage = () =>
        Effect.runPromise(readStateCell(LoopStage, { client, workflowId: executionId }));
      const releaseGate = (word: string) =>
        run(
          DurableDeferred.done(LoopGate, {
            token: DurableDeferred.tokenFromExecutionId(LoopGate, {
              workflow: CellLoopDemo,
              executionId,
            }),
            exit: Exit.succeed(word),
          }),
        );

      // Run 1 published its snapshot.
      let stage = await readStage();
      for (let i = 0; i < 50 && Option.isNone(stage); i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        stage = await readStage();
      }
      expect(Option.isSome(stage) && stage.value).toBe("run-0");

      // Release run 1 → continue-as-new; wait for the run CHANGE to be
      // visible (the latest run's first event carries the previous run id).
      await releaseGate("go-1");
      let continued = false;
      for (let i = 0; i < 50 && !continued; i++) {
        const events = (await handle.fetchHistory()).events ?? [];
        continued = Boolean(
          events[0]?.workflowExecutionStartedEventAttributes?.continuedExecutionRunId,
        );
        if (!continued) await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(continued).toBe(true);

      // Cells are per-run: the fresh run has published nothing yet.
      expect(Option.isNone(await readStage())).toBe(true);

      // Republished by run 2, the cell reads again — and outlives the run.
      await releaseGate("go-2");
      expect(await run(CellLoopDemo.execute(payload))).toBe("cell-done");
      const republished = await readStage();
      expect(Option.isSome(republished) && republished.value).toBe("run-1:go-2");
    });
  }, 120_000);
});
