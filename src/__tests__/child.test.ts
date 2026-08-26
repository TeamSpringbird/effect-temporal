// =========================================================================
// Child workflows, end to end over a real Temporal test server
// =========================================================================
//
// A parent shim workflow executes a child shim workflow through Effect's own
// `ChildDemo.execute` — the sandbox engine's `startChild` path. Covered:
//
//   awaited child   — typed result composes into the parent's
//   child failure   — the child's typed error propagates typed into the
//     parent's error channel, and the parent's compensation runs
//   parent cancel   — cancelling the parent mid-child cancels the child
//     too (makeUnsafe's parent-close finalizer → external cancel), both end
//     CANCELLED, and the parent's compensation still runs
//   discarded child — fire-and-forget (ABANDON) child outlives the parent
//     and completes on its own

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { Client } from "@temporalio/client";
import { describe, expect, it } from "vitest";
import { makeEffectWorkflowActivities } from "../activities.js";
import { makeTemporalClientEngine } from "../engine-client.js";
import { ChildDemo, ParentDemo } from "./fixtures/child-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/child-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-child");

const makeActivities = () => {
  const released: string[] = [];
  const echoed: string[] = [];
  const activities: TestActivities = {
    reserve: async (requestId: unknown) => `reservation:${String(requestId)}`,
    release: async (reservation: unknown) => {
      released.push(String(reservation));
      return "released";
    },
    echo: async (value: unknown) => {
      echoed.push(String(value));
      return `echo:${String(value)}`;
    },
  };
  return { activities, released, echoed };
};

/** A PLAIN client for the bridge activities. The env's time-skipping client
 * must not be used inside activities: its `result()` unlocks/locks the test
 * server's global skip-lock, and a bridge `result()` overlapping the test's
 * own outer `result()` leaves the lock unbalanced — freezing the clock for
 * every later test in this file. Production workers use a plain client here
 * anyway, so this is also the faithful setup. */
const makeBridgeClient = () =>
  new Client({
    connection: temporal.env.connection,
    ...(temporal.env.namespace === undefined ? {} : { namespace: temporal.env.namespace }),
  });

describe("child workflows over Temporal", { concurrent: false }, () => {
  it("composes an awaited child's typed result into the parent's", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        ParentDemo.execute({ requestId: "ch-ok", childOutcome: "ok", discardChild: false }),
      );
      expect(result).toBe("reservation:ch-ok|child:echo:ch-ok-child");
      expect(released).toEqual([]);
    });
  }, 120_000);

  it("propagates a child's typed failure and runs the parent's compensation", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        Effect.result(
          ParentDemo.execute({ requestId: "ch-fail", childOutcome: "fail", discardChild: false }),
        ),
      );
      expect(Result.isFailure(result) && result.failure).toBe("child-failed:ch-fail-child");
      expect(released).toEqual(["reservation:ch-fail"]);

      // Typed failures fail the RUN: both child and parent show red.
      const parentId = await run(
        ParentDemo.executionId({ requestId: "ch-fail", childOutcome: "fail", discardChild: false }),
      );
      const childId = await run(
        ChildDemo.executionId({ requestId: "ch-fail-child", outcome: "fail" }),
      );
      expect((await temporal.env.client.workflow.getHandle(parentId).describe()).status.name).toBe(
        "FAILED",
      );
      expect((await temporal.env.client.workflow.getHandle(childId).describe()).status.name).toBe(
        "FAILED",
      );
    });
  }, 120_000);

  it("cancelling the parent cancels the awaited child and compensates", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = {
        requestId: "ch-cancel",
        childOutcome: "sleep",
        discardChild: false,
      } as const;
      const parentId = await run(ParentDemo.execute(payload, { discard: true }));
      const childId = await run(
        ChildDemo.executionId({ requestId: "ch-cancel-child", outcome: "sleep" }),
      );

      // Wait for the child to exist (parent has reserved and started it).
      const childHandle = temporal.env.client.workflow.getHandle(childId);
      let childSeen = false;
      for (let i = 0; i < 50 && !childSeen; i++) {
        try {
          await childHandle.describe();
          childSeen = true;
        } catch {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }
      expect(childSeen).toBe(true);

      await run(ParentDemo.interrupt(parentId));

      // The unwind spans several workflow tasks and may absorb a benign
      // UNHANDLED_COMMAND retry (the child-cancel confirmation racing a task
      // completion), so the budget is generous.
      const parentHandle = temporal.env.client.workflow.getHandle(parentId);
      let parentStatus = "RUNNING";
      for (let i = 0; i < 200 && parentStatus === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        parentStatus = (await parentHandle.describe()).status.name;
      }
      expect(parentStatus).toBe("CANCELLED");
      expect(released).toEqual(["reservation:ch-cancel"]);

      let childStatus = "RUNNING";
      for (let i = 0; i < 50 && childStatus === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        childStatus = (await childHandle.describe()).status.name;
      }
      expect(childStatus).toBe("CANCELLED");
    });
  }, 120_000);

  it("attaches when two parents execute the same child idempotency key", async () => {
    const { activities, echoed } = makeActivities();
    await temporal.withWorker(
      {
        activities: { ...activities, ...makeEffectWorkflowActivities(makeBridgeClient()) },
        workflowsPath,
      },
      async (taskQueue) => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const shared = {
          childOutcome: "ok",
          discardChild: false,
          childRequestId: "shared-child",
        } as const;
        const resultA = await run(ParentDemo.execute({ requestId: "pa", ...shared }));
        const resultB = await run(ParentDemo.execute({ requestId: "pb", ...shared }));

        // Same child result for both parents — parent B ATTACHED to the
        // execution parent A owns, rather than duplicating or dying.
        expect(resultA).toBe("reservation:pa|child:echo:shared-child");
        expect(resultB).toBe("reservation:pb|child:echo:shared-child");
        expect(echoed).toEqual(["shared-child"]);
      },
    );
  }, 120_000);

  it("attach-polls a sleeping foreign execution through repeated bridge polls", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker(
      {
        activities: { ...activities, ...makeEffectWorkflowActivities(makeBridgeClient()) },
        workflowsPath,
      },
      async (taskQueue) => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        // A FOREIGN execution owns the child id and sleeps 2 minutes before
        // completing — long enough for several backed-off bridge polls.
        const foreignPayload = { requestId: "poll-child", outcome: "sleep" } as const;
        await run(ChildDemo.execute(foreignPayload, { discard: true }));

        // The parent's child start collides with the sleeping run and
        // attaches through the poll loop; awaiting the parent result drives
        // the time-skipping clock through both the polls and the foreign
        // run's own timer.
        const parentPayload = {
          requestId: "poll-parent",
          childOutcome: "sleep",
          discardChild: false,
          childRequestId: "poll-child",
        } as const;
        const result = await run(ParentDemo.execute(parentPayload));
        expect(result).toBe("reservation:poll-parent|child:echo:poll-child");

        // At least two bridge polls made it into the parent's history: the
        // first saw the foreign run still sleeping, a later one its result.
        const parentId = await run(ParentDemo.executionId(parentPayload));
        const events =
          (await temporal.env.client.workflow.getHandle(parentId).fetchHistory()).events ?? [];
        const polls = events.filter(
          (event) =>
            event.activityTaskScheduledEventAttributes?.activityType?.name ===
            "effectWorkflowPollResult",
        );
        expect(polls.length).toBeGreaterThanOrEqual(2);
      },
    );
  }, 120_000);

  it("lets a discarded child outlive the parent and complete on its own", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        ParentDemo.execute({ requestId: "ch-fire", childOutcome: "ok", discardChild: true }),
      );
      expect(result).toBe("reservation:ch-fire|child-discarded");

      // The parent is done; the child completes independently. Executing the
      // same child payload attaches (REJECT_DUPLICATE) and awaits its result.
      const childResult = await run(
        ChildDemo.execute({ requestId: "ch-fire-child", outcome: "ok" }),
      );
      expect(childResult).toBe("child:echo:ch-fire-child");

      // And the parent's own poll still reports its completed result.
      const parentId = await run(
        ParentDemo.executionId({ requestId: "ch-fire", childOutcome: "ok", discardChild: true }),
      );
      expect(Option.isSome(await run(ParentDemo.poll(parentId)))).toBe(true);
    });
  }, 120_000);
});
