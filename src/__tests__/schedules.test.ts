// Scheduled shim workflows, mirroring the Temporal `schedules` sample (see
// EXAMPLES.md): `createWorkflowSchedule` wires the wire-encoded payload into
// the schedule's start-workflow action, a triggered fire runs the workflow,
// and the fired run — addressed by its schedule-generated id — decodes
// through the ordinary `poll` path. Runs on the local dev server; the fired
// looping workflow has no timers.

import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { createWorkflowSchedule, makeTemporalClientEngine } from "../engine-client.js";
import { LoopDemo } from "./fixtures/loop-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/loop-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-schedules", { mode: "local" });

describe("scheduled workflows", { concurrent: false }, () => {
  it("fires a schedule whose action starts the shim workflow", async () => {
    const recorded: string[] = [];
    const activities: TestActivities = {
      record: async (iteration: unknown) => {
        recorded.push(String(iteration));
        return "recorded";
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const scheduleId = `effect-schedule-${randomUUID().slice(0, 8)}`;
      const handle = await Effect.runPromise(
        createWorkflowSchedule({
          client,
          scheduleId,
          workflow: LoopDemo,
          // Starts near the cap so one fire completes without further runs.
          payload: { requestId: scheduleId, iteration: 2 },
          taskQueue,
          spec: { intervals: [{ every: "1 hour" }] },
        }),
      );

      try {
        await handle.trigger();

        // The fired run's id is schedule-generated; read it off the
        // schedule's recent actions.
        let firedWorkflowId: string | undefined;
        for (let i = 0; i < 100 && firedWorkflowId === undefined; i++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          const description = await handle.describe();
          firedWorkflowId = description.info.recentActions[0]?.action.workflow.workflowId;
        }
        expect(firedWorkflowId).toBeDefined();

        // The fired run decodes through the ordinary poll path.
        let fired: string | undefined;
        for (let i = 0; i < 100 && fired === undefined; i++) {
          const result = await run(LoopDemo.poll(firedWorkflowId!));
          if (Option.isSome(result) && result.value._tag === "Complete") {
            const exit = result.value.exit;
            fired = Exit.isSuccess(exit) ? exit.value : undefined;
            break;
          }
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        expect(fired).toBe("done:2");
        expect(recorded).toEqual(["2"]);
      } finally {
        await handle.delete();
      }
    });
  }, 120_000);
});
