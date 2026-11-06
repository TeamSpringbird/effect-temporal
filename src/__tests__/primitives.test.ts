// Core primitives end to end over a real (time-skipping) Temporal test
// server: durable delay + external approval (with `deferredState`
// inspection and idempotent attach), compensation on typed failure and on
// interrupt, in-flight activity cancellation, and totality on unknown ids.
//
// Sequencing note: the time-skipping server only advances the clock while
// something awaits a result, so signals are sent while the workflow still
// sleeps and the final `execute` (idempotent attach) is what drives time.

import { fileURLToPath } from "node:url";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as DurableDeferred from "effect/workflow/DurableDeferred";
import * as WorkflowEngine from "effect/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeWorkflowClient } from "../client.js";
import { deferredState, makeTemporalClientEngine } from "../engine-client.js";
import { Approval, Demo } from "./fixtures/demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/demo-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-primitives");

/** Activities recording their calls, so tests assert compensation ran.
 * `longTask` resolves on a real 3s timer: long enough to be in flight when
 * the cancel test fires, short enough not to stall worker drain. */
const makeActivities = () => {
  const released: string[] = [];
  const activities: TestActivities = {
    reserve: async (requestId: unknown) => `reservation:${String(requestId)}`,
    release: async (reservation: unknown) => {
      released.push(String(reservation));
      return "released";
    },
    longTask: () => new Promise((resolve) => setTimeout(() => resolve("long-done"), 3000)),
  };
  return { activities, released };
};

const approve = (executionId: string, approver: string) =>
  DurableDeferred.done(Approval.deferred, {
    token: DurableDeferred.tokenFromExecutionId(Approval.deferred, {
      workflow: Demo,
      executionId,
    }),
    exit: Exit.succeed(approver),
  });

describe("core primitives over Temporal", { concurrent: false }, () => {
  it("applies configured reuse policies through WorkflowClient for digest and explicit IDs", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const wf = makeWorkflowClient({
        client, taskQueue,
        workflowIdReusePolicy: "ALLOW_DUPLICATE_FAILED_ONLY",
        workflowIdConflictPolicy: "USE_EXISTING",
      });
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const payload = { requestId: "client-policy-digest", mode: "fail-after-step" } as const;
      const id = await Effect.runPromise(Effect.provideService(
        Demo.executionId(payload), WorkflowEngine.WorkflowEngine, engine,
      ));
      const explicitId = `${taskQueue}-explicit-policy`;
      for (const options of [undefined, { workflowId: explicitId }]) {
        const workflowId = options?.workflowId ?? id;
        expect(Result.isFailure(await Effect.runPromise(Effect.result(wf.execute(Demo, payload, options))))).toBe(true);
        const first = await client.workflow.getHandle(workflowId).describe();
        expect(first.status.name).toBe("FAILED");
        expect(Result.isFailure(await Effect.runPromise(Effect.result(wf.execute(Demo, payload, options))))).toBe(true);
        const second = await client.workflow.getHandle(workflowId).describe();
        expect(second.status.name).toBe("FAILED");
        expect(second.runId).not.toBe(first.runId);
      }
    });
  }, 120_000);

  it("uses the configured conflict policy for explicit-ID WorkflowClient starts", async () => {
    const { activities } = makeActivities();
    let release!: () => void;
    let started!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const activityStarted = new Promise<void>((resolve) => { started = resolve; });
    activities.longTask = async () => {
      started();
      await gate;
      return "done";
    };
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const wf = makeWorkflowClient({
        client, taskQueue, workflowIdConflictPolicy: "USE_EXISTING",
      });
      const workflowId = `${taskQueue}-explicit-active`;
      const payload = { requestId: "client-explicit-active", mode: "long-activity" } as const;
      try {
        await Effect.runPromise(wf.start(Demo, payload, { workflowId }));
        await activityStarted;
        const handle = client.workflow.getHandle(workflowId);
        const first = await handle.describe();
        await Effect.runPromise(wf.start(Demo, payload, { workflowId }));
        expect((await handle.describe()).runId).toBe(first.runId);
        release();
        await handle.result();
      } finally {
        release();
      }
    });
  }, 120_000);

  it("restarts failed executions only when reuse is enabled", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const defaults = makeTemporalClientEngine({ client, taskQueue });
      const restart = makeTemporalClientEngine({
        client, taskQueue,
        workflowIdReusePolicy: "ALLOW_DUPLICATE_FAILED_ONLY",
        workflowIdConflictPolicy: "USE_EXISTING",
      });
      const payload = { requestId: "policy-failed", mode: "fail-after-step" } as const;
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>, engine = defaults) =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
      const execute = (engine = defaults) => run(Effect.result(Demo.execute(payload)), engine);
      const executionId = await run(Demo.executionId(payload));

      expect(Result.isFailure(await execute())).toBe(true);
      const first = await client.workflow.getHandle(executionId).describe();
      expect(first.status.name).toBe("FAILED");
      expect(Result.isFailure(await execute())).toBe(true);
      expect((await client.workflow.getHandle(executionId).describe()).runId).toBe(first.runId);

      expect(Result.isFailure(await execute(restart))).toBe(true);
      const second = await client.workflow.getHandle(executionId).describe();
      expect(second.runId).not.toBe(first.runId);
      expect(second.status.name).toBe("FAILED");
    });
  }, 120_000);

  it("attaches concurrent retries to one new run after failure", async () => {
    const { activities } = makeActivities();
    let release!: () => void;
    let started!: () => void;
    let calls = 0;
    const activityStarted = new Promise<void>((resolve) => { started = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    activities.longTask = async () => {
      calls++;
      started();
      await gate;
      return "done";
    };
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({
        client, taskQueue,
        workflowIdReusePolicy: "ALLOW_DUPLICATE_FAILED_ONLY",
        workflowIdConflictPolicy: "USE_EXISTING",
      });
      const failed = { requestId: "policy-retry-race", mode: "fail-after-step" } as const;
      const retry = { requestId: failed.requestId, mode: "long-activity" } as const;
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>) =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
      expect(Result.isFailure(await run(Effect.result(Demo.execute(failed))))).toBe(true);
      const id = await run(Demo.executionId(failed));
      const handle = client.workflow.getHandle(id);
      const failedRun = await handle.describe();
      expect(failedRun.status.name).toBe("FAILED");
      try {
        const ids = await Promise.all([
          run(Demo.execute(retry, { discard: true })),
          run(Demo.execute(retry, { discard: true })),
        ]);
        expect(ids).toEqual([id, id]);
        await activityStarted;
        const active = await handle.describe();
        expect(active.status.name).toBe("RUNNING");
        expect(active.runId).not.toBe(failedRun.runId);
        expect(calls).toBe(1);
        release();
        expect(await run(Demo.execute(retry))).toBe("reservation:policy-retry-race|done");
        expect((await handle.describe()).runId).toBe(active.runId);
        expect(calls).toBe(1);
      } finally {
        release();
      }
    });
  }, 120_000);

  it("attaches concurrent starts to the active run and preserves a successful result", async () => {
    const { activities } = makeActivities();
    let release!: () => void;
    let started!: () => void;
    const activityStarted = new Promise<void>((resolve) => { started = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    activities.longTask = async () => {
      started();
      await gate;
      return "done";
    };
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({
        client, taskQueue,
        workflowIdReusePolicy: "ALLOW_DUPLICATE_FAILED_ONLY",
        workflowIdConflictPolicy: "USE_EXISTING",
      });
      const payload = { requestId: "policy-active", mode: "long-activity" } as const;
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>) =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
      try {
        const ids = await Promise.all([
          run(Demo.execute(payload, { discard: true })),
          run(Demo.execute(payload, { discard: true })),
        ]);
        expect(ids[0]).toBe(ids[1]);
        await activityStarted;
        const handle = client.workflow.getHandle(ids[0]!);
        const active = await handle.describe();
        expect(active.status.name).toBe("RUNNING");
        release();
        expect(await run(Demo.execute(payload))).toBe("reservation:policy-active|done");
        const completed = await handle.describe();
        expect(completed.status.name).toBe("COMPLETED");
        expect(completed.runId).toBe(active.runId);
        expect(await run(Demo.execute(payload))).toBe("reservation:policy-active|done");
        expect((await handle.describe()).runId).toBe(active.runId);
      } finally {
        release();
      }
    });
  }, 120_000);

  it("durable delay + external approval, with deferredState inspection", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engineOptions = { client: temporal.env.client, taskQueue };
      const engine = makeTemporalClientEngine(engineOptions);
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "demo-happy", mode: "approve" } as const;
      const executionId = await run(Demo.execute(payload, { discard: true }));

      // Mid-flight: running, approval unresolved.
      expect(Option.isNone(await run(Demo.poll(executionId)))).toBe(true);
      expect(
        Option.isNone(
          await Effect.runPromise(
            deferredState(Approval.deferred, { client: engineOptions.client, workflowId: executionId }),
          ),
        ),
      ).toBe(true);

      await run(approve(executionId, "uri"));

      // Idempotent attach drives time-skipping through the 2-minute delay.
      const result = await run(Demo.execute(payload));
      expect(result).toBe("reservation:demo-happy|approved-by:uri");

      // Resolved deferred now reads back its typed exit.
      const state = await Effect.runPromise(
        deferredState(Approval.deferred, { client: engineOptions.client, workflowId: executionId }),
      );
      expect(Option.isSome(state) && Exit.isSuccess(state.value) && state.value.value).toBe("uri");

      // Success path: the compensated step must NOT have been compensated.
      expect(released).toEqual([]);
    });
  }, 120_000);

  it("runs compensation when a later step fails typed, keeping the failure typed", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        Effect.result(Demo.execute({ requestId: "demo-fail", mode: "fail-after-step" })),
      );

      expect(Result.isFailure(result) && result.failure).toBe("business-failure");
      expect(released).toEqual(["reservation:demo-fail"]);

      // A typed failure FAILS the run — red in the Temporal UI, not a green
      // completion with the failure hidden in the result payload.
      const executionId = await run(
        Demo.executionId({ requestId: "demo-fail", mode: "fail-after-step" }),
      );
      const { status } = await temporal.env.client.workflow.getHandle(executionId).describe();
      expect(status.name).toBe("FAILED");
    });
  }, 120_000);

  it("rejects a duplicate explicit-id start typed, and execute attaches to the same run", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const wf = makeWorkflowClient({ client: temporal.env.client, taskQueue });
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const workflowId = "effect-demo-explicit-1";
      const payload = { requestId: "explicit-1", mode: "approve" } as const;

      await Effect.runPromise(wf.start(Demo, payload, { workflowId }));

      // Second explicit-id start: the TYPED duplicate error, not a defect.
      const second = await Effect.runPromise(Effect.result(wf.start(Demo, payload, { workflowId })));
      expect(Result.isFailure(second) && second.failure._tag).toBe("WorkflowAlreadyStartedError");
      expect(Result.isFailure(second) && second.failure.workflowId).toBe(workflowId);

      // Approve while the run sleeps; execute on the same id ATTACHES and
      // returns the run's typed result (driving time-skipping through the
      // delay).
      await run(approve(workflowId, "uri"));
      const result = await Effect.runPromise(wf.execute(Demo, payload, { workflowId }));
      expect(result).toBe("reservation:explicit-1|approved-by:uri");
    });
  }, 120_000);

  // ORDERING: the tests below cancel runs with IN-FLIGHT activities (and the
  // last one terminates a run). Both leave the shared time-skipping test
  // server in a state where later awaited results no longer skip time, so
  // every test that drives a durable timer through `result()` must run
  // BEFORE them.
  it("runs compensation on interrupt and ends the run CANCELLED", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "demo-cancel", mode: "approve" } as const;
      const executionId = await run(Demo.execute(payload, { discard: true }));
      expect(Option.isNone(await run(Demo.poll(executionId)))).toBe(true);

      // Cancel while the workflow sleeps in its durable delay.
      await run(Demo.interrupt(executionId));

      // The unwind (compensation activity + CancelledFailure completion)
      // takes a few workflow tasks; wait for the run to leave RUNNING.
      const handle = temporal.env.client.workflow.getHandle(executionId);
      let status = "RUNNING";
      for (let i = 0; i < 50 && status === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        status = (await handle.describe()).status.name;
      }

      expect(status).toBe("CANCELLED");
      expect(released).toEqual(["reservation:demo-cancel"]);

      // Poll reports the cancellation as an interrupted exit.
      const polled = await run(Demo.poll(executionId));
      expect(
        Option.isSome(polled) &&
          polled.value._tag === "Complete" &&
          Exit.isFailure(polled.value.exit) &&
          Cause.hasInterrupts(polled.value.exit.cause),
      ).toBe(true);
    });
  }, 120_000);

  it("cancels the in-flight activity when interrupted mid-activity", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "demo-inflight", mode: "long-activity" } as const;
      const executionId = await run(Demo.execute(payload, { discard: true }));
      const handle = temporal.env.client.workflow.getHandle(executionId);

      // Wait until the long activity is genuinely in flight.
      const started = async () =>
        ((await handle.fetchHistory()).events ?? []).some(
          (event) => event.activityTaskStartedEventAttributes != null,
        );
      for (let i = 0; i < 50 && !(await started()); i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(await started()).toBe(true);

      await run(Demo.interrupt(executionId));

      let status = "RUNNING";
      for (let i = 0; i < 50 && status === "RUNNING"; i++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        status = (await handle.describe()).status.name;
      }
      expect(status).toBe("CANCELLED");
      expect(released).toEqual(["reservation:demo-inflight"]);

      // The point of the per-call scopes: the server was told to cancel the
      // in-flight activity, not just abandon it.
      const events = (await handle.fetchHistory()).events ?? [];
      const cancelRequests = events.filter(
        (event) => event.activityTaskCancelRequestedEventAttributes != null,
      );
      expect(cancelRequests.length).toBeGreaterThanOrEqual(1);
    });
  }, 120_000);

  it("cancels the server-side activity when the calling FIBER is interrupted (Effect.timeout)", async () => {
    const { activities, released } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "demo-timeout", mode: "timeout-activity" } as const;
      const result = await run(Demo.execute(payload));

      // The timeout won the race and the run COMPLETED (workflow-internal
      // interruption is not workflow cancellation).
      expect(result).toBe("reservation:demo-timeout|timed-out:true");
      const executionId = await run(Demo.executionId(payload));
      const handle = temporal.env.client.workflow.getHandle(executionId);
      expect((await handle.describe()).status.name).toBe("COMPLETED");
      expect(released).toEqual([]);

      // The interrupted fiber's per-call scope told the server to cancel the
      // in-flight long activity rather than abandoning it.
      const events = (await handle.fetchHistory()).events ?? [];
      const cancelRequests = events.filter(
        (event) => event.activityTaskCancelRequestedEventAttributes != null,
      );
      expect(cancelRequests.length).toBeGreaterThanOrEqual(1);
    });
  }, 120_000);

  it("is total on unknown execution ids", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engineOptions = { client: temporal.env.client, taskQueue };
      const engine = makeTemporalClientEngine(engineOptions);
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const executionId = "effect-demo-never-started";
      expect(Option.isNone(await run(Demo.poll(executionId)))).toBe(true);
      await run(Demo.interrupt(executionId));
      await run(approve(executionId, "nobody"));
      expect(
        Option.isNone(
          await Effect.runPromise(
            deferredState(Approval.deferred, { client: engineOptions.client, workflowId: executionId }),
          ),
        ),
      ).toBe(true);
    });
  }, 120_000);

  // KEEP LAST in this file: TerminateWorkflowExecution puts the shared
  // time-skipping test server into a state where later `result()` waits no
  // longer skip time (empirically: the clock advances only in real time
  // afterwards), which would hang any subsequent test that drives a durable
  // timer through an awaited result.
  it("terminates a running workflow by id and no-ops on unknown ids", async () => {
    const { activities } = makeActivities();
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const wf = makeWorkflowClient({ client: temporal.env.client, taskQueue });

      const workflowId = "effect-demo-terminate-1";
      await Effect.runPromise(
        wf.start(Demo, { requestId: "term-1", mode: "approve" }, { workflowId }),
      );

      // The run sleeps in its durable delay; terminate closes it outright.
      await Effect.runPromise(wf.terminate(workflowId, { reason: "test-shutdown" }));
      const handle = temporal.env.client.workflow.getHandle(workflowId);
      let status = "RUNNING";
      for (let i = 0; i < 50 && status === "RUNNING"; i++) {
        status = (await handle.describe()).status.name;
        if (status === "RUNNING") await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(status).toBe("TERMINATED");

      // Terminating an id that never existed is a no-op, not a failure.
      await Effect.runPromise(wf.terminate("effect-demo-never-started", { reason: "noop" }));
    });
  }, 120_000);
});
