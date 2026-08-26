// The workflow registry end to end: `Workflow.toLayer` registrations hosted
// by `workflowBundle`' dynamic default export, driven through the
// ordinary client engine — including child workflows dispatched through the
// same dynamic default and typed failures decoding across the wire.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { RegistryParent } from "./fixtures/registry-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/registry-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-registry");

const activities: TestActivities = {
  echo: async (value: unknown) => `echo:${String(value)}`,
};

describe("the workflow registry over Temporal", { concurrent: false }, () => {
  it("runs registered workflows (activity + dynamic child) and typed failures", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      // Happy path: activity call + child workflow, all dispatched through
      // the ONE dynamic default export.
      const result = await run(RegistryParent.execute({ requestId: "reg-1", mode: "ok" }));
      expect(result).toBe("parent:echo:reg-1|hello:reg-1");

      // Typed failure decodes into the error channel.
      const failed = await run(
        Effect.result(RegistryParent.execute({ requestId: "reg-2", mode: "fail" })),
      );
      expect(Result.isFailure(failed) && failed.failure).toBe("registry-failure");
    });
  }, 120_000);
});
