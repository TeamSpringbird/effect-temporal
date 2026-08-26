// A declarative program interpreted inside a workflow, mirroring the
// Temporal `dsl-interpreter` sample (see EXAMPLES.md): the payload carries
// sequential steps of single or parallel activity calls, and Effect is the
// interpreter host — the shim's primary consumer shape.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { DslDemo } from "./fixtures/dsl-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/dsl-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-dsl");

describe("dsl interpretation", { concurrent: false }, () => {
  it("interprets sequential and parallel steps from the payload", async () => {
    const ran: string[] = [];
    const activities: TestActivities = {
      runTask: async (name: unknown) => {
        ran.push(String(name));
        return `did-${String(name)}`;
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        DslDemo.execute({
          requestId: "dsl-1",
          steps: [{ single: "extract" }, { parallel: ["analyze", "index"] }, { single: "publish" }],
        }),
      );
      expect(result).toBe("did-extract>did-analyze+did-index>did-publish");

      // Sequential boundaries hold; the parallel pair runs between them in
      // either order.
      expect(ran[0]).toBe("extract");
      expect(ran[3]).toBe("publish");
      expect(ran.slice(1, 3).toSorted()).toEqual(["analyze", "index"]);
    });
  }, 120_000);
});
