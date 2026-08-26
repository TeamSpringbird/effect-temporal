// `Versioning.match` across three generations of the same workflow,
// mirroring the Temporal `patching-api` sample (see EXAMPLES.md): every
// older generation's history must replay clean through every newer bundle
// (taking its own version's branch), fresh runs must take the newest
// version, and the unguarded control must fail replay — proving the drill
// can see the change at all.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { bundleWorkflowCode, Worker, type WorkflowBundleWithSourceMap } from "@temporalio/worker";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { ChainDemo } from "./fixtures/chain-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const pathFor = (generation: string) =>
  fileURLToPath(new URL(`./fixtures/chain-workflows-${generation}.ts`, import.meta.url));

// Each bundle is webpacked once and reused across replays — bundling is the
// slow part of `runReplayHistory` and repeating it per replay overruns the
// test budget on loaded CI runners.
const bundles = new Map<string, Promise<WorkflowBundleWithSourceMap>>();
const replay = async (
  generation: string,
  history: Parameters<typeof Worker.runReplayHistory>[1],
) => {
  let bundle = bundles.get(generation);
  if (bundle === undefined) {
    bundle = bundleWorkflowCode({ workflowsPath: pathFor(generation) });
    bundles.set(generation, bundle);
  }
  return Worker.runReplayHistory({ workflowBundle: await bundle }, history);
};

const temporal = createWorkflowTestEnv("effect-chain");

const activities: TestActivities = {
  greetV1: async () => "hello-v1",
  greetV2: async () => "hello-v2",
  greetV3: async () => "hello-v3",
};

describe("version chains", { concurrent: false }, () => {
  it("replays every older generation through every newer bundle", async () => {
    const runGeneration = async (generation: string, requestId: string) =>
      temporal.withWorker({ activities, workflowsPath: pathFor(generation) }, async (taskQueue) => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
        const result = await run(ChainDemo.execute({ requestId }));
        const executionId = await run(ChainDemo.executionId({ requestId }));
        const history = await temporal.env.client.workflow.getHandle(executionId).fetchHistory();
        return { result, history };
      });

    // Generation 1: pre-versioning code.
    const generation1 = await runGeneration("v1", "chain-1");
    expect(generation1.result).toBe("greeted:hello-v1");

    // Its history replays through both versioned bundles (v1 branch) —
    // adopting `Versioning.match` on an existing workflow is safe.
    await replay("v2", generation1.history);
    await replay("v3", generation1.history);

    // Negative control: the same change without the guard must fail.
    await expect(replay("v2-unguarded", generation1.history)).rejects.toThrow(/[Dd]eterminism/);

    // Generation 2: fresh runs take v2; its history replays through v3
    // (v2 branch) — appending a case never disturbs older cases.
    const generation2 = await runGeneration("v2", "chain-2");
    expect(generation2.result).toBe("greeted:hello-v2");
    await replay("v2", generation2.history);
    await replay("v3", generation2.history);

    // Generation 3: fresh runs take the newest case.
    const generation3 = await runGeneration("v3", "chain-3");
    expect(generation3.result).toBe("greeted:hello-v3");
    await replay("v3", generation3.history);
  }, 240_000);
});
