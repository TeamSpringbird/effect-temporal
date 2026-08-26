# Getting started

Install the package:

```sh
pnpm add @springbird/effect-temporal   # or npm / yarn / bun
```

`effect`, `@temporalio/client`, and `@temporalio/workflow` are peer dependencies (modern package managers install them for you). You will also want `@temporalio/worker` to run a worker and `@temporalio/testing` for the test harness — both optional peers, used only where you use them.

::: warning Effect version
effect-temporal targets **Effect v4** and pins its `effect` peer **exactly** (currently `4.0.0-beta.101`): the engine implements interfaces from `effect/unstable/*`, whose API can move between releases. Match the pinned version; each release of this package states the one `effect` version it is built and tested against.
:::

A Temporal deployment has three kinds of process, and this package has a module for each:

- the **workflow bundle** — deterministic code Temporal replays; uses `@springbird/effect-temporal/engine-sandbox`
- the **worker** — runs the bundle and your activities; registers via `@springbird/effect-temporal/activities`
- **clients** — ordinary Node processes that start and observe workflows; use `@springbird/effect-temporal/client`

## 1. Define a workflow

A definition is a tag, a payload schema, an idempotency key, and success/error schemas. Both sides share this one module — keep it free of `@temporalio/*` imports.

```ts
// definitions.ts — shared by the bundle and every client
import { Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";

export const Reserve = TypedActivity.make("reserve", {
  payload: { sku: Schema.String, quantity: Schema.Finite },
  success: Schema.String,
  error: Schema.TaggedStruct("OutOfStock", { sku: Schema.String }),
  options: { startToCloseTimeout: "1 minute", retry: { maximumAttempts: 3 } },
});

export const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String, sku: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: Reserve.errorSchema,
});
```

## 2. Author the body

The body is an Effect that runs inside the Temporal workflow sandbox. Export it from your workflow bundle **under the workflow's tag** — the export name is the Temporal workflow type.

```ts
// workflows.ts — the workflow bundle (Temporal's workflowsPath points here)
import { Effect } from "effect";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { callActivity, makeTemporalWorkflow } from "@springbird/effect-temporal/engine-sandbox";
import { OrderFlow, Reserve } from "./definitions.js";

export const orderFlow = makeTemporalWorkflow(OrderFlow, (payload) =>
  Effect.gen(function* () {
    // A typed activity: payload validated, result decoded, typed failure
    // lands in the error channel. Retries are Temporal's, per the options.
    const reservation = yield* callActivity(Reserve, {
      sku: payload.sku,
      quantity: 1,
    });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "1 minute" });
    return `reserved:${reservation}`;
  }),
);
```

## 3. Implement the activities and run a worker

Activities are Effects too, bound to the same definitions. Every worker running these workflows also registers the **attach bridge** (`makeEffectWorkflowActivities`).

```ts
// worker.ts
import { Effect } from "effect";
import { Client, Connection } from "@temporalio/client";
import { Worker } from "@temporalio/worker";
import {
  handle,
  implementActivities,
  makeEffectWorkflowActivities,
  type ActivityRunner,
} from "@springbird/effect-temporal/activities";
import { Reserve } from "./definitions.js";

// How your worker executes activity Effects — plug in your runtime,
// spans, and error reporting here.
const runner: ActivityRunner<never> = {
  run: (_name, _payload, effect) => Effect.runPromiseExit(effect),
};

const client = new Client({ connection: await Connection.connect() });

const worker = await Worker.create({
  taskQueue: "orders",
  workflowsPath: new URL("./workflows.js", import.meta.url).pathname,
  activities: {
    ...implementActivities(runner, [
      handle(Reserve, ({ sku, quantity }) => Effect.succeed(`${sku}x${quantity}`)),
    ]),
    ...makeEffectWorkflowActivities(client),
  },
});
await worker.run();
```

## 4. Start it from anywhere

`WorkflowClient` is the one client service: configure it once with a Temporal client and a default task queue.

```ts
// api.ts — any ordinary Node process
import { Effect } from "effect";
import { Client, Connection } from "@temporalio/client";
import { layerWorkflowClient, WorkflowClient } from "@springbird/effect-temporal/client";
import { OrderFlow } from "./definitions.js";

const program = Effect.gen(function* () {
  const wf = yield* WorkflowClient;
  // Typed success/error; repeated calls with the same orderId attach to the
  // same execution and return the original result.
  return yield* wf.execute(OrderFlow, { orderId: "ord_123", sku: "sku-9" });
});

const client = new Client({ connection: await Connection.connect() });
await Effect.runPromise(
  program.pipe(Effect.provide(layerWorkflowClient({ client, taskQueue: "orders" }))),
);
```

That's the whole loop: definition → body → worker → client, with schemas holding every boundary.

## Where to next

- **The runnable examples** — each boots its own local Temporal dev server (`pnpm run build`, then `pnpm --dir examples/<name> start`): [`examples/order-saga`](https://github.com/TeamSpringbird/effect-temporal/tree/main/examples/order-saga) is the one-shot saga (typed activities, compensation, approval, queryable state, idempotent attach, cancellation); [`examples/subscription`](https://github.com/TeamSpringbird/effect-temporal/tree/main/examples/subscription) is the long-lived entity (billing cycles, typed updates, mailbox cancellation, continue-as-new).
- [Defining workflows](/guide/defining-workflows): idempotency, execution ids, start semantics.
- [Activities](/guide/activities): typed activities, raw calls, failure and retry semantics.
- [Testing your app](/guide/testing): the typed fake client and the live harness.
- [Lint rules](/guide/lint-rules): catch the authoring footguns mechanically.
