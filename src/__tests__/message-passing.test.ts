// `DurableUpdate` end to end, mirroring the Temporal `message-passing`
// sample (see EXAMPLES.md): a language service whose update returns the
// previous language as a typed success, rejects unsupported languages as a
// typed failure, publishes its state to a cell, and finishes on a one-shot
// approval.

import { fileURLToPath } from "node:url";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, it } from "vitest";
import { executeUpdate, makeTemporalClientEngine, readStateCell } from "../engine-client.js";
import { updateCodec, WORKFLOW_UPDATE, type WorkflowUpdatePayload } from "../update.js";
import {
  Approved,
  CurrentLanguage,
  DeferredPokeDemo,
  MessageDemo,
  Orphan,
  SetLanguage,
} from "./fixtures/message-demo.js";
import { createWorkflowTestEnv, type TestActivities } from "./utils/workflow-test-env.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/message-workflows.ts", import.meta.url));

const temporal = createWorkflowTestEnv("effect-update");

const activities: TestActivities = {};

describe("DurableUpdate over Temporal", { concurrent: false }, () => {
  it("answers updates with typed success and typed failure", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "msg-1" } as const;
      const executionId = await run(MessageDemo.execute(payload, { discard: true }));

      const setLanguage = (language: string) =>
        Effect.runPromise(
          Effect.result(
            executeUpdate(SetLanguage.update, { client, workflowId: executionId, payload: { language } }),
          ),
        );

      // Success responses carry the PREVIOUS language.
      const first = await setLanguage("french");
      expect(Result.isSuccess(first) && first.success).toBe("english");
      const second = await setLanguage("spanish");
      expect(Result.isSuccess(second) && second.success).toBe("french");

      // Unsupported languages come back as a typed failure, and the state
      // is untouched.
      const rejected = await setLanguage("klingon");
      expect(Result.isFailure(rejected) && rejected.failure).toBe("unsupported:klingon");
      const snapshot = await Effect.runPromise(
        readStateCell(CurrentLanguage.cell, { client, workflowId: executionId }),
      );
      expect(Option.isSome(snapshot) && snapshot.value).toBe("spanish");

      await run(
        DurableDeferred.done(Approved.deferred, {
          token: DurableDeferred.tokenFromExecutionId(Approved.deferred, {
            workflow: MessageDemo,
            executionId,
          }),
          exit: Exit.succeed("uri"),
        }),
      );
      expect(await run(MessageDemo.execute(payload))).toBe("approved:spanish by uri");
    });
  }, 120_000);

  it("surfaces a run completing with an unanswered update as a defect to the caller", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "msg-orphan" } as const;
      const executionId = await run(MessageDemo.execute(payload, { discard: true }));

      // The workflow never takes `Orphan`, so this request stays pending;
      // `Effect.exit` never rejects, so the floating promise is safe.
      const orphanExit = Effect.runPromise(
        Effect.exit(
          executeUpdate(Orphan.update, { client, workflowId: executionId, payload: { note: "hello" } }),
        ),
      );
      // Give the update time to be admitted while the workflow still runs.
      await new Promise((resolve) => setTimeout(resolve, 1500));

      // Complete the workflow out from under the pending update.
      await run(
        DurableDeferred.done(Approved.deferred, {
          token: DurableDeferred.tokenFromExecutionId(Approved.deferred, {
            workflow: MessageDemo,
            executionId,
          }),
          exit: Exit.succeed("uri"),
        }),
      );
      expect(await run(MessageDemo.execute(payload))).toBe("approved:english by uri");

      // An update expects an answer: a run ending unanswered is a DEFECT for
      // the caller, never a silent no-op.
      const exit = await orphanExit;
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    });
  }, 120_000);

  it("fails executeUpdate against an unknown workflow id as a defect", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async () => {
      const exit = await Effect.runPromise(
        Effect.exit(
          executeUpdate(SetLanguage.update, {
            client: temporal.env.client,
            workflowId: "effect-update-never-started",
            payload: { language: "french" },
          }),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    });
  }, 120_000);

  it("answers a malformed update payload with a schema-decode defect and keeps serving", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const payload = { requestId: "msg-malformed" } as const;
      const executionId = await run(MessageDemo.execute(payload, { discard: true }));

      // A raw client sending schema-violating junk under a KNOWN update name
      // — the workflow answers with a defect instead of taking the request.
      const wire = await client.workflow
        .getHandle(executionId)
        .executeUpdate<unknown, [WorkflowUpdatePayload]>(WORKFLOW_UPDATE, {
          args: [{ updateName: SetLanguage.name, payload: { language: 42 } }],
        });
      const malformedExit = updateCodec(SetLanguage.update).decodeExit(wire);
      expect(Exit.isFailure(malformedExit) && Cause.hasDies(malformedExit.cause)).toBe(true);
      expect(
        Exit.isFailure(malformedExit) && String(Cause.squash(malformedExit.cause)),
      ).toMatch(/schema decode/);

      // The malformed request never reached the body: the run still serves a
      // well-formed update, with state untouched.
      const ok = await Effect.runPromise(
        Effect.result(
          executeUpdate(SetLanguage.update, {
            client,
            workflowId: executionId,
            payload: { language: "french" },
          }),
        ),
      );
      expect(Result.isSuccess(ok) && ok.success).toBe("english");

      await run(
        DurableDeferred.done(Approved.deferred, {
          token: DurableDeferred.tokenFromExecutionId(Approved.deferred, {
            workflow: MessageDemo,
            executionId,
          }),
          exit: Exit.succeed("uri"),
        }),
      );
      expect(await run(MessageDemo.execute(payload))).toBe("approved:french by uri");
    });
  }, 120_000);

  it("treats workflow→workflow deferredDone to a closed execution as a no-op for the sender", async () => {
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const engine = makeTemporalClientEngine({ client: temporal.env.client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      const result = await run(
        DeferredPokeDemo.execute({
          requestId: "poke-1",
          targetExecutionId: "effect-message-never-existed",
        }),
      );
      expect(result).toBe("ok");
    });
  }, 120_000);
});
