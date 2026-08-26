// =========================================================================
// Replaying a CANCELLED run's history must not hang
// =========================================================================
//
// The cancellation model must survive replay: a worker restart (or a
// workflow-task rejection like UNHANDLED_COMMAND, which evicts the sticky
// cache) forces a full replay of the history mid- or post-cancellation. A
// livelock here means every cancelled workflow becomes permanently stuck the
// moment its worker recycles — so this pins replayability of the whole
// unwind path (racer, in-flight scope cancel, compensation, CancelledFailure
// completion) with a hard timeout around `runReplayHistory`.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { bundleWorkflowCode, Worker, type WorkflowBundleWithSourceMap } from "@temporalio/worker";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { ChildDemo, ParentDemo } from "./fixtures/child-demo.js";
import { Demo } from "./fixtures/demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/demo-workflows.ts", import.meta.url));
const childWorkflowsPath = fileURLToPath(new URL("./fixtures/child-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-replay-cancelled");

const activities: TestActivities = {
  reserve: async (requestId: unknown) => `reservation:${String(requestId)}`,
  release: async () => "released",
  longTask: () => new Promise((resolve) => setTimeout(() => resolve("long-done"), 3000)),
  echo: async (value: unknown) => `echo:${String(value)}`,
};

const bundles = new Map<string, Promise<WorkflowBundleWithSourceMap>>();
const bundleFor = (path: string): Promise<WorkflowBundleWithSourceMap> => {
  let bundle = bundles.get(path);
  if (bundle === undefined) {
    bundle = bundleWorkflowCode({ workflowsPath: path });
    bundles.set(path, bundle);
  }
  return bundle;
};

/** Replay bounded by a timeout, so a livelock fails fast and legibly. The
 * bundle is built outside the race: webpack time scales with runner load
 * and is not what this guards. */
const boundedReplay = async (
  path: string,
  history: Awaited<
    ReturnType<ReturnType<typeof temporal.env.client.workflow.getHandle>["fetchHistory"]>
  >,
) => {
  const workflowBundle = await bundleFor(path);
  return Promise.race([
    Worker.runReplayHistory({ workflowBundle }, history).then(
      () => "replayed",
      (error) => `replay error: ${String(error)}`,
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("HUNG"), 60_000)),
  ]);
};

describe("replaying cancelled histories", { concurrent: false }, () => {
  it("replays a cancelled run's history without hanging", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "replay-cancel", mode: "approve" } as const;
      const executionId = await run(Demo.execute(payload, { discard: true }));
      const handle = temporal.env.client.workflow.getHandle(executionId);

      // Let it reach the durable delay, then cancel and wait for closure.
      await new Promise((resolve) => setTimeout(resolve, 300));
      await run(Demo.interrupt(executionId));
      let status = "RUNNING";
      for (let i = 0; i < 100 && status === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        status = (await handle.describe()).status.name;
      }
      expect(status).toBe("CANCELLED");

      // The probe: replay that history, bounded. A hang fails here rather
      // than eating the suite timeout.
      const history = await handle.fetchHistory();
      expect(await boundedReplay(workflowsPath, history)).toBe("replayed");
    });
  }, 120_000);

  it("replays a cancelled parent-with-child history without hanging", async () => {
    await temporal.withWorker(
      { activities, workflowsPath: childWorkflowsPath },
      async (taskQueue) => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const payload = {
          requestId: "replay-ch-cancel",
          childOutcome: "sleep",
          discardChild: false,
        } as const;
        const parentId = await run(ParentDemo.execute(payload, { discard: true }));
        const childId = await run(
          ChildDemo.executionId({ requestId: "replay-ch-cancel-child", outcome: "sleep" }),
        );

        // Wait until the child exists, then cancel the parent and wait for
        // BOTH runs to close, so the parent history contains the full
        // child-cancellation confirmation.
        const childHandle = temporal.env.client.workflow.getHandle(childId);
        for (let i = 0; i < 50; i++) {
          try {
            await childHandle.describe();
            break;
          } catch {
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        }
        await run(ParentDemo.interrupt(parentId));
        const parentHandle = temporal.env.client.workflow.getHandle(parentId);
        let status = "RUNNING";
        for (let i = 0; i < 200 && status === "RUNNING"; i++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          status = (await parentHandle.describe()).status.name;
        }
        expect(status).toBe("CANCELLED");

        const history = await parentHandle.fetchHistory();
        expect(await boundedReplay(childWorkflowsPath, history)).toBe("replayed");
      },
    );
  }, 120_000);
});
