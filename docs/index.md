---
layout: home

hero:
  name: effect-temporal
  text: Durable Effect workflows on Temporal.
  tagline: Author workflows with Effect — schemas, typed errors, composition — and run them on Temporal's retries, timers, signals, and history.
  actions:
    - theme: brand
      text: Getting started
      link: /guide/getting-started
    - theme: alt
      text: What is effect-temporal?
      link: /guide/introduction
    - theme: alt
      text: GitHub
      link: https://github.com/TeamSpringbird/effect-temporal

features:
  - title: Effect-native
    details: Workflows are schema-typed definitions, bodies are Effects, failures are typed channels. The whole Effect program runs deterministically inside the Temporal workflow sandbox.
  - title: Temporal underneath
    details: Durability comes from real Temporal primitives — activities, timers, signals, updates, queries, schedules, Nexus — each memoized in history and visible in the Temporal UI.
  - title: One definition, both sides
    details: Workflows, activities, mailboxes, updates, and state cells are declared once and shared by the workflow bundle, the worker, and every client — the two sides cannot drift.
  - title: Built for entities
    details: Long-lived, observable, mutable entity workflows are expressible end to end — repeated signals, request/response updates, queryable snapshots, continue-as-new, patch-marker versioning.
---

## The whole idea, in one file

```ts
import { Effect, Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred";
import * as TypedActivity from "@springbird/effect-temporal/typed-activity";
import { WorkflowClient } from "@springbird/effect-temporal/client";

// 1. Define once: shared by the workflow bundle, the worker, and every client.
const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
});
const Charge = TypedActivity.make("charge", {
  payload: { orderId: Schema.String },
  success: Schema.String,
});
const ManagerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.String,
});

// 2. Author the body — an Effect, running durably in the
//    Temporal sandbox. (workflow bundle: engine-sandbox module)
const OrderFlowLive = OrderFlow.toLayer((payload) =>
  Effect.gen(function* () {
    const paid = yield* callActivity(Charge, { orderId: payload.orderId });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "3 days" });
    const approver = yield* DurableDeferred.await(ManagerApproval);
    return `${paid}:approved-by:${approver}`;
  }),
);
export default workflowBundle(OrderFlowLive);

// 3. Drive it from ordinary Node — typed success, typed failure, idempotent.
const wf = yield* WorkflowClient;
const result = yield* wf.execute(OrderFlow, { orderId: "ord_123" });
```

```sh
pnpm add @springbird/effect-temporal   # or npm / yarn / bun
```
