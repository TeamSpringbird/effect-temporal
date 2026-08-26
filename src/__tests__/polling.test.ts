// Infrequent polling, mirroring the Temporal `polling` sample's infrequent
// variant (see EXAMPLES.md): the poll interval IS the activity retry policy
// (60s between attempts, skipped by the time-skipping server), so a service
// that becomes ready on the third attempt costs three activity attempts and
// no workflow code beyond one call.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { PollingDemo } from "./fixtures/polling-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/polling-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-polling");

describe("infrequent polling", { concurrent: false }, () => {
  it("retries the poll activity until the service is ready", async () => {
    let attempts = 0;
    const activities: TestActivities = {
      pollService: async () => {
        attempts++;
        if (attempts < 3) throw new Error(`not ready (attempt ${attempts})`);
        return "ready";
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(PollingDemo.execute({ requestId: "poll-1" }));
      expect(result).toBe("service:ready");
      expect(attempts).toBe(3);
    });
  }, 120_000);
});
