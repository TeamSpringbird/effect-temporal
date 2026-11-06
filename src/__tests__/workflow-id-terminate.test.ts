import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/workflow/WorkflowEngine";
import { expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { Demo } from "./fixtures/demo.js";
import { createWorkflowTestEnv } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/demo-workflows.ts", import.meta.url));
const temporal = createWorkflowTestEnv("effect-terminate");

it("supports TERMINATE_IF_RUNNING without an explicit conflict policy", async () => {
  let release!: () => void;
  let started!: () => void;
  const activityStarted = new Promise<void>((resolve) => { started = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const activities = {
    reserve: async () => "reserved",
    longTask: async () => {
      started();
      await gate;
      return "done";
    },
  };
  await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
    const client = temporal.env.client;
    const engine = makeTemporalClientEngine({
      client, taskQueue, workflowIdReusePolicy: "TERMINATE_IF_RUNNING",
    });
    const payload = { requestId: "policy-terminate", mode: "long-activity" } as const;
    const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>) =>
      Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
    try {
      const id = await run(Demo.execute(payload, { discard: true }));
      await activityStarted;
      const handle = client.workflow.getHandle(id);
      const first = await handle.describe();
      expect(first.status.name).toBe("RUNNING");
      expect(await run(Demo.execute(payload, { discard: true }))).toBe(id);
      const second = await handle.describe();
      expect(second.runId).not.toBe(first.runId);
      expect((await client.workflow.getHandle(id, first.runId).describe()).status.name).toBe("TERMINATED");
      release();
      await client.workflow.getHandle(id, second.runId).result();
    } finally {
      release();
    }
  });
}, 120_000);
