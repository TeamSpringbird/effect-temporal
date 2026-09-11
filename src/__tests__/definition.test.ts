// The single-declaration contract:
//
//   1. TYPES FLOW: payloads, successes, and typed errors infer end-to-end
//      from each declaration (pinned below with expectTypeOf).
//   2. ENGINE-AGNOSTIC: the SAME handler function object runs (a) on the
//      in-memory `makeTestWorkflowOps` runtime with zero engine anywhere,
//      and (b) on real Temporal via `workflowBundle`.

import { fileURLToPath } from "node:url";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { TestClock } from "effect/testing";
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine";
import { describe, expect, expectTypeOf, it } from "vitest";
import { handle, implementActivities, type ActivityRunner } from "../activities.js";
import {
  continueAsNew,
  defineActivity,
  executeChild,
  sleepUntil,
  version,
  versioned,
  type PayloadOf,
  type UpdateRequest,
  type WorkflowOps,
} from "../definition.js";
import {
  completeDeferred,
  executeUpdate,
  makeTemporalClientEngine,
  offerMailbox,
  readStateCell,
} from "../engine-client.js";
import { handleWorkflow, makeTestWorkflowOps } from "../testing.js";
import {
  Approval,
  CancelOrder,
  CardDeclined,
  Charge,
  chargeImpl,
  Dispatch,
  dispatchHandler,
  Fulfil,
  fulfilHandler,
  GraceFlow,
  graceHandler,
  OrderFlow,
  orderHandler,
  Priority,
  Reserve,
  reserveImpl,
  SetAmount,
  Status,
  Tally,
  tallyHandler,
} from "./fixtures/definition-demo.js";
import { createWorkflowTestEnv } from "./utils/workflow-test-env.js";

const temporal = createWorkflowTestEnv("definition");

const bindings = [handle(Reserve, reserveImpl), handle(Charge, chargeImpl)] as const;

/** Let forked handler fibers process what was just delivered. */
const settle = Effect.gen(function* () {
  for (let i = 0; i < 10; i++) yield* Effect.yieldNow;
});

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

  // Omitted schemas retain their runtime defaults in the type channels.
  const Defaults = defineActivity("defaults", { payload: { id: Schema.String } });
  const defaults = Defaults({ id: "x" });
  expectTypeOf<Effect.Success<typeof defaults>>().toEqualTypeOf<void>();
  expectTypeOf<Effect.Error<typeof defaults>>().toEqualTypeOf<never>();

  // An error schema can still be inferred while success defaults to void.
  const ErrorOnly = defineActivity("errorOnly", {
    payload: { id: Schema.String },
    error: CardDeclined,
  });
  const errorOnly = ErrorOnly({ id: "x" });
  expectTypeOf<Effect.Success<typeof errorOnly>>().toEqualTypeOf<void>();
  expectTypeOf<Effect.Error<typeof errorOnly>>().toEqualTypeOf<typeof CardDeclined.Type>();

  defineActivity<"missingSuccess", { id: typeof Schema.String }, typeof Schema.String>(
    "missingSuccess",
    // @ts-expect-error a non-default success generic requires its runtime schema
    { payload: { id: Schema.String } },
  );

  defineActivity<
    "missingError",
    { id: typeof Schema.String },
    Schema.Void,
    typeof CardDeclined
  >(
    "missingError",
    // @ts-expect-error a non-default error generic requires its runtime schema
    { payload: { id: Schema.String } },
  );

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

  // versioned unions the cases' channels and needs WorkflowOps.
  const mode = versioned("site", { a: Effect.succeed(1), b: Effect.fail("x") });
  expectTypeOf<Effect.Success<typeof mode>>().toEqualTypeOf<number>();
  expectTypeOf<Effect.Error<typeof mode>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Services<typeof mode>>().toEqualTypeOf<WorkflowOps>();

  // Type helpers live on the definition module now.
  expectTypeOf<PayloadOf<typeof Charge>>().toEqualTypeOf<{
    readonly orderId: string;
    readonly amountCents: number;
  }>();

  // Children: awaited → the child's channels; discarded → the execution id.
  const awaited = executeChild(Fulfil, { orderId: "x" });
  expectTypeOf<Effect.Success<typeof awaited>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof awaited>>().toEqualTypeOf<string>();
  const discarded = executeChild(Fulfil, { orderId: "x" }, { discard: true });
  expectTypeOf<Effect.Success<typeof discarded>>().toEqualTypeOf<string>();
  expectTypeOf<Effect.Error<typeof discarded>>().toEqualTypeOf<never>();
  // @ts-expect-error wrong child payload
  executeChild(Fulfil, { orderId: 1 });

  // continueAsNew never returns, and checks the payload against the schema.
  const next = continueAsNew(Tally, { batchId: "b", count: 1 });
  expectTypeOf<Effect.Success<typeof next>>().toEqualTypeOf<never>();
  // @ts-expect-error wrong payload shape
  continueAsNew(Tally, { batchId: "b" });

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
      // Client-side ops address the DECLARATION — no `.deferred`, no token,
      // no `WorkflowEngine` for the approval.
      const approve = (workflowId: string, approver: string) =>
        Effect.runPromise(
          completeDeferred(Approval, { client, workflowId, exit: Exit.succeed(approver) }),
        );

      const payload = { orderId: "t-1" };
      const workflowId = await run(OrderFlow.execute(payload, { discard: true }));

      // Same drive sequence as the memory test, through the real client ops.
      const previous = await Effect.runPromise(
        executeUpdate(SetAmount, { client, workflowId, payload: { amountCents: 2500 } }),
      );
      expect(previous).toBe(1000);
      await Effect.runPromise(offerMailbox(Priority, { client, workflowId, payload: { level: 2 } }));
      const mid = await Effect.runPromise(readStateCell(Status, { client, workflowId }));
      expect(Option.getOrNull(mid)).toEqual({ phase: "awaiting-approval" });
      await approve(workflowId, "temporal-ben");

      const result = await run(OrderFlow.execute(payload));
      expect(result).toBe("res-t-1|receipt-t-1-2500|p2|tiered|by:temporal-ben");
      const final = await Effect.runPromise(readStateCell(Status, { client, workflowId }));
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

// ── 3. 0.4.0: timers, continue-as-new, children, versioned — two engines ─────

const workflowsPath = fileURLToPath(new URL("./fixtures/definition-workflows.ts", import.meta.url));
const plainRunner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

describe("definition 0.4.0: the seam is complete", { concurrent: false }, () => {
  it("in memory, a mailbox take racing a sleep resolves in EITHER order under TestClock", async () => {
    const program = Effect.gen(function* () {
      const world = yield* makeTestWorkflowOps();

      // Order 1: the cancellation lands before the grace period elapses.
      const cancelled = yield* Effect.forkChild(
        graceHandler({ orderId: "g-1" }).pipe(Effect.provide(world.layer)),
      );
      yield* settle;
      yield* world.offer(CancelOrder, { reason: "changed-mind" });
      expect(yield* Fiber.join(cancelled)).toBe("cancelled:changed-mind");

      // Order 2: nothing arrives; the timer is NOT instant — it fires only
      // when the clock is advanced past it.
      const elapsed = yield* Effect.forkChild(
        graceHandler({ orderId: "g-2" }).pipe(Effect.provide(world.layer)),
      );
      yield* settle;
      yield* TestClock.adjust("59 minutes");
      yield* settle;
      expect(elapsed.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust("2 minutes");
      expect(yield* Fiber.join(elapsed)).toBe("shipped");
    });
    await Effect.runPromise(Effect.provide(program, TestClock.layer()));
  }, 20_000);

  it("in memory, sleepUntil follows the clock and rejects zone-less timestamps", async () => {
    const program = Effect.gen(function* () {
      const world = yield* makeTestWorkflowOps();
      yield* TestClock.setTime(Date.parse("2026-01-01T00:00:00Z"));
      const fiber = yield* Effect.forkChild(
        sleepUntil({ name: "not-before", timestamp: "2026-01-01T01:00:00Z" }).pipe(
          Effect.as("woke"),
          Effect.provide(world.layer),
        ),
      );
      yield* settle;
      yield* TestClock.adjust("30 minutes");
      yield* settle;
      expect(fiber.pollUnsafe()).toBeUndefined();
      yield* TestClock.adjust("30 minutes");
      expect(yield* Fiber.join(fiber)).toBe("woke");

      // Already past: no-op.
      expect(
        yield* sleepUntil({ name: "past", timestamp: "2025-12-31T00:00:00Z" }).pipe(
          Effect.as("immediate"),
          Effect.provide(world.layer),
        ),
      ).toBe("immediate");

      // The same rule as the Temporal runtime: zone-less date-times defect.
      const zoneless = yield* Effect.exit(
        sleepUntil({ name: "z", timestamp: "2026-01-01T02:00:00" }).pipe(Effect.provide(world.layer)),
      );
      expect(Exit.isFailure(zoneless) && Cause.hasDies(zoneless.cause)).toBe(true);
      expect(Exit.isFailure(zoneless) && String(Cause.squash(zoneless.cause))).toMatch(/no timezone/);
    });
    await Effect.runPromise(Effect.provide(program, TestClock.layer()));
  }, 20_000);

  it("in memory, continueAsNew interrupts the run and records the schema-checked payload", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const world = yield* makeTestWorkflowOps();
        const exit = yield* Effect.exit(
          tallyHandler({ batchId: "b-1", count: 0 }).pipe(Effect.provide(world.layer)),
        );
        expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
        const next = yield* world.continuedAsNewOf(Tally);
        expect(Option.getOrNull(next)).toEqual({ batchId: "b-1", count: 1 });
        expect(Option.isNone(yield* world.continuedAsNewOf(Fulfil))).toBe(true);

        // The payload round-trips through the workflow's payload schema:
        // `count: Schema.Finite` rejects NaN, in memory as on the wire.
        const invalid = yield* Effect.exit(
          continueAsNew(Tally, { batchId: "b-2", count: Number.NaN }).pipe(
            Effect.provide(world.layer),
          ),
        );
        expect(Exit.isFailure(invalid) && Cause.hasDies(invalid.cause)).toBe(true);
      }),
    );
  }, 20_000);

  it("in memory, executeChild runs the bound child handler: typed success, typed failure, discard, attach", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const world = yield* makeTestWorkflowOps({
          activities: bindings,
          workflows: [handleWorkflow(Fulfil, fulfilHandler)],
        });
        const run = <A, E>(effect: Effect.Effect<A, E, WorkflowOps>) =>
          Effect.provide(effect, world.layer);

        // Awaited: the child's success composes; `versioned` answers the newest key.
        expect(yield* run(dispatchHandler({ orderId: "d-1", discard: false }))).toBe(
          "routed|fulfilled:res-d-1",
        );
        // Awaited: the child's TYPED failure lands in the parent's error channel.
        const failed = yield* Effect.result(run(dispatchHandler({ orderId: "d-bad", discard: false })));
        expect(Result.isFailure(failed) && failed.failure).toBe("unfulfillable:d-bad");
        // Discarded: the digest execution id comes back; the child runs on.
        const childId = yield* Fulfil.executionId({ orderId: "d-2" });
        expect(yield* run(dispatchHandler({ orderId: "d-2", discard: true }))).toBe(
          `routed|started:${childId}`,
        );
        // A taken id attaches to the running child instead of starting again.
        expect(yield* run(executeChild(Fulfil, { orderId: "d-2" }))).toBe("fulfilled:res-d-2");
        // An unbound child dies loudly.
        const unbound = yield* Effect.exit(run(executeChild(Tally, { batchId: "x", count: 0 })));
        expect(Exit.isFailure(unbound) && String(Cause.squash(unbound.cause))).toMatch(
          /no binding for workflow "defTally"/,
        );
      }),
    );
  }, 20_000);

  it("runs the SAME 0.4.0 handlers on real Temporal", async () => {
    const activities = implementActivities(plainRunner, bindings);
    await temporal.withWorker({ activities, workflowsPath }, async (taskQueue) => {
      const client = temporal.env.client;
      const engine = makeTemporalClientEngine({ client, taskQueue });
      const run = <A, E>(effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>): Promise<A> =>
        Effect.runPromise(Effect.provideService(effect, WorkflowEngine.WorkflowEngine, engine));

      // sleep vs mailbox, order 1: the cancellation wins (offered before the
      // result is awaited, so before time skipping advances the 1h timer).
      const cancelledId = await run(GraceFlow.execute({ orderId: "tg-1" }, { discard: true }));
      await Effect.runPromise(
        offerMailbox(CancelOrder, { client, workflowId: cancelledId, payload: { reason: "oops" } }),
      );
      expect(await run(GraceFlow.execute({ orderId: "tg-1" }))).toBe("cancelled:oops");

      // Order 2: nothing arrives; the durable 1h timer fires (time-skipped).
      expect(await run(GraceFlow.execute({ orderId: "tg-2" }))).toBe("shipped");

      // continueAsNew from the definition module rolls the run twice.
      expect(await run(Tally.execute({ batchId: "tb-1", count: 0 }))).toBe("tallied:2");
      const tallyId = await run(Tally.executionId({ batchId: "tb-1", count: 0 }));
      const events = (await client.workflow.getHandle(tallyId).fetchHistory()).events ?? [];
      expect(events[0]?.workflowExecutionStartedEventAttributes?.continuedExecutionRunId).toBeTruthy();

      // executeChild: awaited success, awaited typed failure, discarded.
      expect(await run(Dispatch.execute({ orderId: "td-1", discard: false }))).toBe(
        "routed|fulfilled:res-td-1",
      );
      const failed = await run(Effect.result(Dispatch.execute({ orderId: "td-bad", discard: false })));
      expect(Result.isFailure(failed) && failed.failure).toBe("unfulfillable:td-bad");
      const childId = await run(Fulfil.executionId({ orderId: "td-2" }));
      expect(await run(Dispatch.execute({ orderId: "td-2", discard: true }))).toBe(
        `routed|started:${childId}`,
      );
      // The discarded child is a real execution under its digest id.
      expect(await run(Fulfil.execute({ orderId: "td-2" }))).toBe("fulfilled:res-td-2");
    });
  }, 180_000);
});
