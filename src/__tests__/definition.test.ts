// The single-declaration contract:
//
//   1. TYPES FLOW: payloads, successes, and typed errors infer end-to-end
//      from each declaration (pinned below with expectTypeOf).
//   2. ENGINE-AGNOSTIC: the SAME handler function object runs (a) on the
//      in-memory `makeTestWorkflowOps` runtime with zero engine anywhere,
//      and (b) on real Temporal via `workflowBundle`.

import { fileURLToPath } from "node:url";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import { describe, expect, expectTypeOf, it } from "vitest";
import { handle, implementActivities, type ActivityRunner } from "../activities.js";
import { version, type UpdateRequest, type WorkflowOps } from "../definition.js";
import { executeUpdate, makeTemporalClientEngine, offerMailbox, readStateCell } from "../engine-client.js";
import { makeTestWorkflowOps } from "../testing.js";
import {
  Approval,
  CardDeclined,
  Charge,
  chargeImpl,
  OrderFlow,
  orderHandler,
  Priority,
  Reserve,
  reserveImpl,
  SetAmount,
  Status,
} from "./fixtures/definition-demo.js";
import { createWorkflowTestEnv } from "./utils/workflow-test-env.js";

const temporal = createWorkflowTestEnv("definition");

const bindings = [handle(Reserve, reserveImpl), handle(Charge, chargeImpl)] as const;

// ── 1. The type pins ─────────────────────────────────────────────────────────

const _types = () => {
  // Activities: payload in, success out, typed error channel, WorkflowOps in R.
  const charge = Charge({ orderId: "x", amountCents: 1 });
  expectTypeOf<Effect.Success<typeof charge>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof charge>>().toEqualTypeOf<typeof CardDeclined.Type>();
  const _r: Effect.Effect<string, typeof CardDeclined.Type, WorkflowOps> = charge;
  void _r;
  // @ts-expect-error wrong payload shape
  Charge({ orderId: 1 });

  // Messages: deferred success, mailbox payload, update request typing.
  expectTypeOf<Effect.Success<typeof Approval.await>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Success<typeof Priority.take>>().toEqualTypeOf<{
    readonly level: number;
  }>();
  expectTypeOf<Effect.Success<typeof SetAmount.take>>().toEqualTypeOf<
    UpdateRequest<{ readonly amountCents: number }, number, string>
  >();

  // version answers one of exactly the names given.
  const pricing = version("site", ["flat", "tiered"]);
  expectTypeOf<Effect.Success<typeof pricing>>().toEqualTypeOf<"flat" | "tiered">();

  // Worker binding is payload/success/error-checked from the declaration.
  handle(Charge, chargeImpl);
  // @ts-expect-error wrong success type
  handle(Charge, () => Effect.succeed(42));
};
void _types;

// ── 2. Same handler, two engines ─────────────────────────────────────────────

describe("definition: one declaration, types flow, engine-agnostic", { concurrent: false }, () => {
  it("runs the handler on the in-memory runtime (no engine at all)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const world = yield* makeTestWorkflowOps({ activities: bindings });
        const fiber = yield* Effect.forkChild(
          orderHandler({ orderId: "m-1" }).pipe(Effect.provide(world.layer)),
        );

        // Drive the entity exactly as a client would. `settle` lets the
        // handler fiber process each message before we assert on state.
        const settle = Effect.gen(function* () {
          for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        });
        const previous = yield* world.request(SetAmount, { amountCents: 2500 });
        expect(previous).toBe(1000);
        yield* world.offer(Priority, { level: 2 });
        yield* settle;
        expect(yield* world.stateOf(Status)).toEqual(Option.some({ phase: "awaiting-approval" }));
        yield* world.resolve(Approval, "memory-ben");

        const result = yield* Fiber.join(fiber);
        expect(result).toBe("res-m-1|receipt-m-1-2500|p2|tiered|by:memory-ben");
        expect(yield* world.stateOf(Status)).toEqual(Option.some({ phase: "complete" }));
      }),
    );
  }, 20_000);

  it("answers a typed update refusal in memory", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const world = yield* makeTestWorkflowOps({ activities: bindings });
        yield* Effect.forkChild(orderHandler({ orderId: "m-2" }).pipe(Effect.provide(world.layer)));
        const refused = yield* Effect.result(world.request(SetAmount, { amountCents: 50 }));
        expect(Result.isFailure(refused) && refused.failure).toBe("amount-too-low");
      }),
    );
  }, 20_000);

  it("runs the SAME handler on real Temporal through workflowBundle", async () => {
    const workflowsPath = fileURLToPath(new URL("./fixtures/definition-workflows.ts", import.meta.url));
    const runner: ActivityRunner<never> = {
      run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
    };
    const activities = implementActivities(runner, bindings);

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
      const approve = (executionId: string, approver: string) =>
        run(
          DurableDeferred.done(Approval.deferred, {
            token: DurableDeferred.tokenFromExecutionId(Approval.deferred, {
              workflow: OrderFlow,
              executionId,
            }),
            exit: Exit.succeed(approver),
          }),
        );

      const payload = { orderId: "t-1" };
      const workflowId = await run(OrderFlow.execute(payload, { discard: true }));

      // Same drive sequence as the memory test, through the real client ops.
      const previous = await Effect.runPromise(
        executeUpdate(SetAmount.update, { client, workflowId, payload: { amountCents: 2500 } }),
      );
      expect(previous).toBe(1000);
      await Effect.runPromise(
        offerMailbox(Priority.mailbox, { client, workflowId, payload: { level: 2 } }),
      );
      const mid = await Effect.runPromise(readStateCell(Status.cell, { client, workflowId }));
      expect(Option.getOrNull(mid)).toEqual({ phase: "awaiting-approval" });
      await approve(workflowId, "temporal-ben");

      const result = await run(OrderFlow.execute(payload));
      expect(result).toBe("res-t-1|receipt-t-1-2500|p2|tiered|by:temporal-ben");
      const final = await Effect.runPromise(readStateCell(Status.cell, { client, workflowId }));
      expect(Option.getOrNull(final)).toEqual({ phase: "complete" });

      // The typed activity failure flows into the workflow error channel.
      const declinePayload = { orderId: "t-declined" };
      const declineId = await run(OrderFlow.execute(declinePayload, { discard: true }));
      await Effect.runPromise(
        executeUpdate(SetAmount.update, {
          client,
          workflowId: declineId,
          payload: { amountCents: 10_000 },
        }),
      );
      await Effect.runPromise(
        offerMailbox(Priority.mailbox, { client, workflowId: declineId, payload: { level: 1 } }),
      );
      await approve(declineId, "x");
      const declined = await run(Effect.result(OrderFlow.execute(declinePayload)));
      expect(Result.isFailure(declined) && declined.failure).toEqual({
        _tag: "CardDeclined",
        orderId: "t-declined",
      });
    });
  }, 120_000);
});
