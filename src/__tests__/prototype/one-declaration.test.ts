// PROTOTYPE — throwaway. The proof for the single-declaration design:
//
//   1. TYPES FLOW: payloads, successes, and typed errors infer end-to-end
//      from the one declaration (pinned below with expectTypeOf).
//   2. NO LEAK: the SAME handler function object runs (a) on a plain
//      in-memory runtime with zero engine anywhere, and (b) on real
//      Temporal via the existing engine — the definition and handler are
//      engine-agnostic.

import { fileURLToPath } from "node:url";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import { describe, expect, expectTypeOf, it } from "vitest";
import { handle, implementActivities, type ActivityRunner, type BoundActivity } from "../../activities.js";
import { executeUpdate, makeTemporalClientEngine, offerMailbox, readStateCell } from "../../engine-client.js";
import { createWorkflowTestEnv } from "../utils/workflow-test-env.js";
import type { OpsRuntime, UpdateRequestOf } from "./def.js";
import { CardDeclined, Order, orderBound, orderImpls } from "./order.js";

const temporal = createWorkflowTestEnv("proto-one-decl");

// ── 1. The type pins ─────────────────────────────────────────────────────────

const _types = () => {
  const bound = Order.handler((payload, ops) => {
    expectTypeOf(payload).toEqualTypeOf<{ readonly orderId: string }>();
    // Activities: payload in, success out, typed error channel.
    const charge = ops.activity.charge({ orderId: payload.orderId, amountCents: 1 });
    expectTypeOf<Effect.Success<typeof charge>>().toEqualTypeOf<string>();
    expectTypeOf<Effect.Error<typeof charge>>().toEqualTypeOf<typeof CardDeclined.Type>();
    // Messages: deferred success, mailbox payload, update request typing.
    expectTypeOf<Effect.Success<typeof ops.message.approval.await>>().toEqualTypeOf<string>();
    expectTypeOf<Effect.Success<typeof ops.message.priority.take>>().toEqualTypeOf<{
      readonly level: number;
    }>();
    expectTypeOf<Effect.Success<typeof ops.message.setAmount.take>>().toEqualTypeOf<
      UpdateRequestOf<{ readonly amountCents: number }, number, string>
    >();
    return Effect.succeed("ok");
  });
  void bound;

  // Worker implementations are completeness-checked from the declaration.
  Order.implement({
    reserve: () => Effect.succeed("r"),
    // @ts-expect-error wrong success type
    charge: () => Effect.succeed(42),
  });
  // @ts-expect-error missing implementation for `charge`
  Order.implement({ reserve: () => Effect.succeed("r") });
};
void _types;

// ── 2. The in-memory runtime: no engine, plain Effect ────────────────────────

const makeMemoryWorld = Effect.gen(function* () {
  // The prototype hardcodes the declaration's channel names — throwaway.
  const approval = yield* Deferred.make<unknown>();
  const priority = yield* Queue.unbounded<unknown>();
  const setAmount = yield* Queue.unbounded<UpdateRequestOf<unknown, unknown, unknown>>();
  const state = new Map<string, unknown>();

  const runtime: OpsRuntime = {
    activity: (activity, payload) => {
      const impl = (orderImpls as unknown as Record<string, (p: never) => Effect.Effect<unknown, unknown>>)[
        activity.name.split("/")[1]!
      ]!;
      return impl(payload as never);
    },
    deferredAwait: () => Deferred.await(approval),
    mailboxTake: () => Queue.take(priority),
    mailboxPoll: () => Queue.poll(priority),
    updateTake: () => Queue.take(setAmount),
    stateSet: (name, value) => Effect.sync(() => void state.set(name, value)),
  };

  return {
    runtime,
    approve: (value: unknown) => Deferred.done(approval, Exit.succeed(value)),
    offer: (value: unknown) => Queue.offer(priority, value),
    update: <S, E>(payload: unknown) =>
      Effect.gen(function* () {
        const reply = yield* Deferred.make<S, E>();
        yield* Queue.offer(setAmount, {
          payload,
          respond: (exit: Exit.Exit<unknown, unknown>) =>
            Deferred.done(reply, exit as Exit.Exit<S, E>).pipe(Effect.asVoid),
        });
        return yield* Deferred.await(reply);
      }),
    readState: (name: string) => state.get(name),
  };
});

describe("prototype: one declaration, types flow, no engine leak", { concurrent: false }, () => {
  it("runs the SAME handler on a plain in-memory runtime (no engine at all)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const world = yield* makeMemoryWorld;
        const ops = Order.makeOps(world.runtime);

        const fiber = yield* Effect.forkChild(orderBound.body({ orderId: "m-1" }, ops));

        // Drive the entity exactly as a client would. `settle` lets the
        // handler fiber process each message before we assert on state.
        const settle = Effect.gen(function* () {
          for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
        });
        const previous = yield* world.update<number, string>({ amountCents: 2500 });
        expect(previous).toBe(1000);
        yield* world.offer({ level: 2 });
        yield* settle;
        expect(world.readState("protoOrder/status")).toEqual({ phase: "awaiting-approval" });
        yield* world.approve("memory-ben");

        const result = yield* Fiber.join(fiber);
        expect(result).toBe("res-m-1|receipt-m-1-2500|p2|by:memory-ben");
        expect(world.readState("protoOrder/status")).toEqual({ phase: "complete" });
      }),
    );
  }, 20_000);

  it("runs the SAME handler on real Temporal through the existing engine", async () => {
    const workflowsPath = fileURLToPath(new URL("./order-workflows.ts", import.meta.url));
    const runner: ActivityRunner<never> = {
      run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
    };
    const activities = implementActivities(
      runner,
      Object.entries(Order.activities).map(([key, activity]) =>
        handle(activity, (orderImpls as never as Record<string, never>)[key]!),
      ) as ReadonlyArray<BoundActivity<never, string>>,
    );

    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));
      const approve = (executionId: string, approver: string) =>
        run(
          DurableDeferred.done(Order.deferreds["approval"]!, {
            token: DurableDeferred.tokenFromExecutionId(Order.deferreds["approval"]!, {
              workflow: Order.workflow,
              executionId,
            }),
            exit: Exit.succeed(approver),
            // The fromEntries-built deferred record erases services — prototype.
          }) as Effect.Effect<void, never, WorkflowEngine.WorkflowEngine>,
        );

      const payload = { orderId: "t-1" };
      const workflowId = await run(Order.workflow.execute(payload, { discard: true }));

      // Same drive sequence as the memory test, through the real client ops.
      const previous = await Effect.runPromise(
        executeUpdate(Order.updates["setAmount"]!, {
          client,
          workflowId,
          payload: { amountCents: 2500 },
        }),
      );
      expect(previous).toBe(1000);
      await Effect.runPromise(
        offerMailbox(Order.mailboxes["priority"]!, { client, workflowId, payload: { level: 2 } }),
      );
      const mid = await Effect.runPromise(
        readStateCell(Order.cells["status"]!, { client, workflowId }),
      );
      expect(Option.getOrNull(mid)).toEqual({ phase: "awaiting-approval" });
      await approve(workflowId, "temporal-ben");

      const result = await run(Order.workflow.execute(payload));
      expect(result).toBe("res-t-1|receipt-t-1-2500|p2|by:temporal-ben");
      const final = await Effect.runPromise(
        readStateCell(Order.cells["status"]!, { client, workflowId }),
      );
      expect(Option.getOrNull(final)).toEqual({ phase: "complete" });

      // The typed activity failure flows into the workflow error channel.
      const declinePayload = { orderId: "t-declined" };
      const declineId = await run(Order.workflow.execute(declinePayload, { discard: true }));
      await Effect.runPromise(
        executeUpdate(Order.updates["setAmount"]!, {
          client,
          workflowId: declineId,
          payload: { amountCents: 10_000 },
        }),
      );
      await Effect.runPromise(
        offerMailbox(Order.mailboxes["priority"]!, {
          client,
          workflowId: declineId,
          payload: { level: 1 },
        }),
      );
      await approve(declineId, "x");
      const declined = await run(Effect.result(Order.workflow.execute(declinePayload)));
      expect(Result.isFailure(declined) && declined.failure).toEqual({
        _tag: "CardDeclined",
        orderId: "t-declined",
      });
    });
  }, 120_000);
});
