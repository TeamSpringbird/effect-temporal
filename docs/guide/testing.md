# Testing your app

Three testing stories ship in `@springbird/effect-temporal/testing`, for three kinds of test:

- **No engine at all** — `makeTestWorkflowOps`, an in-memory `WorkflowOps` runtime: the same handler that runs on Temporal runs in a plain unit test, driven directly.
- **Temporal as a seam** — a typed in-memory fake of the Temporal client, for fast service tests that assert *what was started, signalled, terminated*.
- **Real workflow semantics** — a harness over Temporal's own test server (time-skipping timers, real retries, continue-as-new), for tests that run the actual workflow.

The optional peers `@temporalio/testing` and `@temporalio/worker` are needed only for the harness path.

## The in-memory runtime

A handler authored against [declared capabilities](/guide/declaring-capabilities) requires exactly one service, `WorkflowOps`. `makeTestWorkflowOps` builds an in-memory implementation plus the client half of every declaration — a small world your test drives the way a real client would:

```ts
import { Effect, Fiber } from "effect";
import { handle } from "@springbird/effect-temporal/activities";
import { handleWorkflow, makeTestWorkflowOps } from "@springbird/effect-temporal/testing";
import { Approval, Charge, orderHandler, Priority, SetAmount, Status } from "./definitions.js";

const world = yield* makeTestWorkflowOps({
  activities: [handle(Charge, () => Effect.succeed("receipt"))],
  workflows: [handleWorkflow(Fulfil, fulfilHandler)], // children executeChild can run
});

// The SAME handler that workflowBundle hosts on Temporal:
const fiber = yield* Effect.forkChild(
  orderHandler({ orderId: "o-1" }).pipe(Effect.provide(world.layer)),
);

const previous = yield* world.request(SetAmount, { amountCents: 2500 }); // update: typed response
yield* world.offer(Priority, { level: 2 });                              // mailbox message
yield* world.resolve(Approval, "ben");                                   // deferred completion
const phase = yield* world.stateOf(Status);                              // Option of last .set

const result = yield* Fiber.join(fiber);
```

The world's surface:

- **`layer`** — provides `WorkflowOps` backed by this world; provide it to the handler.
- **`resolve(deferred, value)`** — resolves a declared deferred, waking a handler blocked on `.await`.
- **`offer(mailbox, payload)`** — delivers one mailbox message to `.take`/`.poll`.
- **`request(update, payload)`** — sends an update request and awaits the typed response the handler's `respond` produces (typed failure in the error channel).
- **`stateOf(cell)`** — reads the last value the handler `.set`, as an `Option`.
- **`continuedAsNew`** / **`continuedAsNewOf(workflow)`** — `Some` once the handler called `continueAsNew`, carrying the payload the next run would decode (typed against `workflow` in the second form).

Activity calls run their bound handlers with the payload round-tripped through the declaration's schema (as the wire would); typed failures land in the error channel, everything else is a defect. A call to an activity with no binding dies loudly. `version` / `versioned` always answer the newest name — there is no replay in memory.

### Timers under `TestClock`

`sleep` and `sleepUntil` follow Effect's `Clock`, so provide `TestClock.layer()` and `adjust` past them. They are deliberately **not instant**: the grace-period pattern — a mailbox take racing a timer — is testable in *both* orders:

```ts
import { TestClock } from "effect/testing";

const program = Effect.gen(function* () {
  const world = yield* makeTestWorkflowOps();

  // Order 1: the cancellation lands first.
  const a = yield* Effect.forkChild(graceHandler(payload).pipe(Effect.provide(world.layer)));
  yield* world.offer(CancelOrder, { reason: "changed-mind" });
  expect(yield* Fiber.join(a)).toBe("cancelled:changed-mind");

  // Order 2: nothing arrives; the timer fires when the clock passes it.
  const b = yield* Effect.forkChild(graceHandler(payload).pipe(Effect.provide(world.layer)));
  yield* TestClock.adjust("1 hour");
  expect(yield* Fiber.join(b)).toBe("shipped");
});
await Effect.runPromise(Effect.provide(program, TestClock.layer()));
```

### Continue-as-new and children

`continueAsNew` interrupts the handler fiber (an interrupted exit) and records the continuation — read it with `world.continuedAsNewOf(Workflow)`; the payload was round-tripped through the workflow's payload schema, so a schema-invalid payload dies here as it would on Temporal. `executeChild` runs the child's `handleWorkflow` binding in-process with the same world: payload, success, and typed failure cross the child's schemas, a discarded child forks and returns its execution id, and a second start on a taken id attaches — the idempotency contract, in memory.

No sandbox, no server, no Temporal: this is the test for handler *logic* — branching, message ordering, typed refusals, timer races, child composition. Replay and real retries stay with the harness below.

## The fake client

`makeFakeTemporalClient` records every start, signal, and termination as typed data, answers results through the workflow's own wire codecs, and **dies loudly on any surface a test did not configure** — the "unstubbed methods fail fast" contract.

```ts
import { makeFakeTemporalClient, simulateAlreadyStarted } from "@springbird/effect-temporal/testing";
import { makeWorkflowClient } from "@springbird/effect-temporal/client";

const fake = makeFakeTemporalClient({
  workflows: [OrderFlow],                       // definitions it can answer results for
  result: (start) => `handled:${start.workflowId}`,
});
const wf = makeWorkflowClient({ client: fake.client, taskQueue: "orders" });

// exercise your service...
await Effect.runPromise(myService.placeOrder("ord_1"));

// ...then assert on typed records
expect(fake.starts).toMatchObject([
  { workflowType: "orderFlow", taskQueue: "orders" },
]);
expect(fake.signals).toEqual([]);
```

Simulate a duplicate start by throwing from `onStart`:

```ts
const fake = makeFakeTemporalClient({
  onStart: (start) => {
    if (seen.has(start.workflowId)) throw simulateAlreadyStarted(start);
    seen.add(start.workflowId);
  },
});
```

The fake covers `workflow.start`, `getHandle(...).result/signal/terminate`, and `withDeadline`. Anything else a test touches fails with a named error telling you what to configure.

Mailbox offers are typed on both sides, so a service test never names a signal constant:

```ts
await Effect.runPromise(fake.offer(Priority, workflowId, { level: 3 })); // through the fake, as a client would
expect(fake.offersTo(Priority)).toEqual([{ workflowId, payload: { level: 3 } }]); // decoded via the declaration
```

## The live harness

`startWorkflowTestHarness` boots a Temporal test server and hands back a typed harness. Framework-agnostic on purpose — wire it into your runner's lifecycle yourself:

```ts
import { startWorkflowTestHarness, type WorkflowTestHarness } from "@springbird/effect-temporal/testing";

let harness: WorkflowTestHarness;
beforeAll(async () => {
  harness = await startWorkflowTestHarness(); // time-skipping by default
}, 120_000);
afterAll(() => harness.teardown());

it("reserves and completes", async () => {
  const activities = implementActivities(runner, [
    handle(Reserve, ({ sku, quantity }) => Effect.succeed(`${sku}x${quantity}`)),
  ]);
  await harness.withWorker({ workflowsPath, activities }, async (wf) => {
    // wf is a promise-flavored typed client bound to the worker's task queue
    expect(await wf.execute(OrderFlow, { orderId: "o1", sku: "s1" })).toBe("reserved:s1x1");
  });
});
```

The harness client drives declarations directly, mirroring the in-memory world's names: `wf.offer(Priority, id, payload)`, `wf.request(SetAmount, id, payload)`, `wf.stateOf(Status, id)`, `wf.resolve(Approval, id, value)`.

- **Time skipping**: durable timers — a 3-day cooling-off, a 61-second not-before — resolve instantly while a result is being awaited. `harness.currentTimeMillis()` gives you the environment's current time for building absolute timestamps.
- **`mode: "local"`** runs a full dev server instead — needed for Nexus and schedules, which the time-skipping server does not support.
- `harness.env` exposes the underlying `TestWorkflowEnvironment` for surfaces the harness does not model.

## Decoding results in tests

When driving raw Temporal handles, assert on domain values — never on the encoded-exit wire shape:

```ts
import { decodeWorkflowResult, encodeWorkflowPayload } from "@springbird/effect-temporal/testing";

const wire = await handle.result();
expect(decodeWorkflowResult(OrderFlow, wire)).toBe("reserved:s1x1");

// and for asserting what a start SHOULD have carried:
expect(fake.starts[0].args[0]).toEqual(encodeWorkflowPayload(OrderFlow, payload));
```

## Which to use when

| Test | Tool |
| --- | --- |
| "the handler's logic is right — branches, messages, typed refusals, timer races, children" | `makeTestWorkflowOps` |
| "my service starts the right workflow with the right payload" | fake client |
| "duplicate submits don't double-start" | fake client + `simulateAlreadyStarted` |
| "the workflow's timer/retry/compensation logic is right" | live harness |
| "mailboxes, updates, continue-as-new behave under real Temporal" | live harness |
| "schedules / Nexus wiring works" | live harness, `mode: "local"` |
