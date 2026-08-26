// Regression probe: DurableClock.sleep at or below the 60s in-memory
// threshold runs as `Activity.make({ execute: Effect.sleep(...) })` rather
// than through `scheduleClock` — a path the other suites never hit (their
// sleeps are all above the threshold).

import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { ShortSleepDemo } from "./fixtures/short-sleep-demo.js";
import { createWorkflowTestEnv } from "./utils/workflow-test-env.js";

const temporal = createWorkflowTestEnv("effect-short-sleep");

describe("in-memory durable clock", { concurrent: false }, () => {
  it("completes an under-threshold sleep between activities", async () => {
    await temporal.withWorker(
      {
        activities: { echo: async (value: unknown) => `echo:${String(value)}` },
        workflowsPath: new URL("./fixtures/short-sleep-workflows.ts", import.meta.url).pathname,
      },
      async (taskQueue) => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const result = await Effect.runPromise(
          Effect.provideService(
            ShortSleepDemo.execute({ requestId: "nap-1" }),
            WorkflowEngine.WorkflowEngine,
            engine,
          ),
        );
        expect(result).toBe("echo:echo:nap-1");
      },
    );
  }, 120_000);
});
