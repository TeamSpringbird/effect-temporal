# Testing your app

Two testing stories ship in `@springbird/effect-temporal/testing`, for two kinds of test:

- **Temporal as a seam** — a typed in-memory fake of the Temporal client, for fast service tests that assert *what was started, signalled, terminated*.
- **Real workflow semantics** — a harness over Temporal's own test server (time-skipping timers, real retries, continue-as-new), for tests that run the actual workflow.

Both need the optional peers `@temporalio/testing` and `@temporalio/worker` only for the harness path.

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
| "my service starts the right workflow with the right payload" | fake client |
| "duplicate submits don't double-start" | fake client + `simulateAlreadyStarted` |
| "the workflow's timer/retry/compensation logic is right" | live harness |
| "mailboxes, updates, continue-as-new behave" | live harness |
| "schedules / Nexus wiring works" | live harness, `mode: "local"` |
