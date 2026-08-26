// Workflow-to-workflow mailbox messaging, mirroring the Temporal `mutex`
// sample (see EXAMPLES.md): a lock workflow grants three contenders one at
// a time, each acquiring, running its critical section, and releasing —
// entirely through mailbox messages between executions. Mutual exclusion is
// asserted on the recorded enter/leave sequence: every enter is immediately
// followed by its own leave.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { ContenderDemo, LockDemo } from "./fixtures/lock-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/lock-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-mutex");

describe("mutex via workflow-to-workflow mailboxes", { concurrent: false }, () => {
  it("serializes contenders' critical sections", async () => {
    const events: string[] = [];
    const activities: TestActivities = {
      enter: async (name: unknown) => {
        events.push(`enter:${String(name)}`);
        return "entered";
      },
      leave: async (name: unknown) => {
        events.push(`leave:${String(name)}`);
        return "left";
      },
    };

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const lockPayload = { requestId: "lock-1", grants: 3 } as const;
      const lockExecutionId = await run(LockDemo.execute(lockPayload, { discard: true }));

      const contenders = ["a", "b", "c"].map((name) => ({
        requestId: `contender-${name}`,
        lockExecutionId,
        name,
      }));
      for (const contender of contenders) {
        await run(ContenderDemo.execute(contender, { discard: true }));
      }

      // The protocol advances through worker round-trips, not timers, so
      // awaiting a result here would let the time-skipping server jump past
      // the execution timeout. Wait for closure in real time instead, then
      // attach for the (already available) results.
      const lockHandle = temporal.env.client.workflow.getHandle(lockExecutionId);
      let lockStatus = "RUNNING";
      for (let i = 0; i < 200 && lockStatus === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        lockStatus = (await lockHandle.describe()).status.name;
      }
      expect(lockStatus).toBe("COMPLETED");

      const results = await Promise.all(
        contenders.map((contender) => run(ContenderDemo.execute(contender))),
      );
      expect(results.toSorted()).toEqual(["done:a", "done:b", "done:c"]);
      expect(await run(LockDemo.execute(lockPayload))).toBe("served:3");

      // Three enter/leave pairs with no interleaving.
      expect(events).toHaveLength(6);
      const holders: string[] = [];
      for (let i = 0; i < events.length; i += 2) {
        const holder = events[i]!.split(":")[1]!;
        expect(events[i]).toBe(`enter:${holder}`);
        expect(events[i + 1]).toBe(`leave:${holder}`);
        holders.push(holder);
      }
      expect(holders.toSorted()).toEqual(["a", "b", "c"]);
    });
  }, 120_000);
});
