// Nexus end to end, mirroring the Temporal `nexus-hello` and
// `nexus-cancellation` samples (see EXAMPLES.md): a caller shim workflow
// invokes a synchronous operation and a workflow-backed operation served by
// another shim workflow through `effectWorkflowRunOperation` — typed success
// and typed failure both decoding across the Nexus boundary, and caller
// interruption cancelling the in-flight operation AND its backing workflow.
//
// Runs on the LOCAL dev server (`system.enableNexus=true`): the
// time-skipping server does not support Nexus. No timers are involved, so
// real time is fine. The endpoint targets a fixed task queue created before
// the worker exists.

import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { makeTemporalClientEngine } from "../engine-client.js";
import { CallerDemo, GreetDemo } from "./fixtures/nexus-demo.js";
import { helloServiceHandler } from "./fixtures/nexus-handlers.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/nexus-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-nexus", {
  mode: "local",
  serverArgs: ["--dynamic-config-value", "system.enableNexus=true"],
});

const activities: TestActivities = {};

describe("nexus operations", { concurrent: false }, () => {
  it("calls sync and workflow-backed operations with typed channels", async () => {
    const taskQueue = `effect-nexus-${randomUUID()}`;
    const endpoint = `effect-nexus-endpoint-${randomUUID().slice(0, 8)}`;
    await temporal.env.connection.operatorService.createNexusEndpoint({
      spec: {
        name: endpoint,
        target: { worker: { namespace: temporal.env.namespace ?? "default", taskQueue } },
      },
    });

    await temporal.withWorker(
      { activities, workflowsPath, taskQueue, nexusServices: [helloServiceHandler] },
      async () => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const result = await run(
          CallerDemo.execute({ requestId: "nx-1", name: "world", endpoint }),
        );
        expect(result).toBe("world|Hello, world!");

        // The served workflow keeps the shim's addressing contract: its
        // digest execution id resolves to the completed run and result.
        const servedId = await run(GreetDemo.executionId({ name: "world" }));
        const polled = await run(GreetDemo.poll(servedId));
        expect(
          Option.isSome(polled) &&
            polled.value._tag === "Complete" &&
            Result.getOrThrow(Effect.runSync(Effect.result(polled.value.exit))),
        ).toBe("Hello, world!");

        // A typed failure crosses the Nexus boundary into the caller's
        // typed error channel.
        const failed = await run(
          Effect.result(CallerDemo.execute({ requestId: "nx-2", name: "grinch", endpoint })),
        );
        expect(Result.isFailure(failed) && failed.failure).toBe("unwelcome:grinch");
      },
    );
  }, 120_000);

  it("cancels the in-flight operation and its backing workflow on interrupt", async () => {
    const taskQueue = `effect-nexus-cancel-${randomUUID()}`;
    const endpoint = `effect-nexus-endpoint-${randomUUID().slice(0, 8)}`;
    await temporal.env.connection.operatorService.createNexusEndpoint({
      spec: {
        name: endpoint,
        target: { worker: { namespace: temporal.env.namespace ?? "default", taskQueue } },
      },
    });

    await temporal.withWorker(
      { activities, workflowsPath, taskQueue, nexusServices: [helloServiceHandler] },
      async () => {
        const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
        const run = <A, E>(
          effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>,
        ): Promise<A> =>
          Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

        const callerId = await run(
          CallerDemo.execute({ requestId: "nx-cancel", name: "slow", endpoint }, { discard: true }),
        );

        // Interrupt only after the caller has recorded the operation token
        // (NexusOperationStarted): a cancel issued before that has no token
        // to request cancellation with, and the operation would continue
        // untracked.
        const servedId = await run(GreetDemo.executionId({ name: "slow" }));
        const servedHandle = temporal.env.client.workflow.getHandle(servedId);
        const callerHistory = temporal.env.client.workflow.getHandle(callerId);
        let operationStarted = false;
        for (let i = 0; i < 100 && !operationStarted; i++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          const events = (await callerHistory.fetchHistory()).events ?? [];
          operationStarted = events.some(
            (event) => event.nexusOperationStartedEventAttributes != null,
          );
        }
        expect(operationStarted).toBe(true);

        await run(CallerDemo.interrupt(callerId));

        const callerHandle = temporal.env.client.workflow.getHandle(callerId);
        let callerStatus = "RUNNING";
        for (let i = 0; i < 200 && callerStatus === "RUNNING"; i++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          callerStatus = (await callerHandle.describe()).status.name;
        }
        expect(callerStatus).toBe("CANCELLED");

        let servedStatus = "RUNNING";
        for (let i = 0; i < 200 && servedStatus === "RUNNING"; i++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          servedStatus = (await servedHandle.describe()).status.name;
        }
        expect(servedStatus).toBe("CANCELLED");
      },
    );
  }, 120_000);
});
