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
  - title: One declaration, every side
    details: Workflows, activities, mailboxes, updates, and state cells are declared once with define* and shared by the workflow bundle, the worker, and every client — the sides cannot drift. Handlers depend on one seam (WorkflowOps), so the same handler runs on Temporal or in a plain unit test.
  - title: Built for entities
    details: Long-lived, observable, mutable entity workflows are expressible end to end — repeated signals, request/response updates, queryable snapshots, continue-as-new, patch-marker versioning.
---

## The whole idea, in one file

```ts
import { Effect, Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";
import * as DurableClock from "effect/unstable/workflow/DurableClock";
import { defineActivity, defineDeferred } from "@springbird/effect-temporal/definition";
import { workflowBundle } from "@springbird/effect-temporal/engine-sandbox";
import { WorkflowClient } from "@springbird/effect-temporal/client";

// 1. Declare once: shared by the workflow bundle, the worker, and every client.
const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
});
const Charge = defineActivity("charge", {
  payload: { orderId: Schema.String },
  success: Schema.String,
});
const ManagerApproval = defineDeferred("manager-approval", {
  success: Schema.String,
});

// 2. Author the body — call the declarations directly. The handler needs
//    only WorkflowOps: workflowBundle provides Temporal's; the testing
//    module provides an in-memory one for plain unit tests.
const OrderFlowLive = OrderFlow.toLayer((payload) =>
  Effect.gen(function* () {
    const paid = yield* Charge({ orderId: payload.orderId });
    yield* DurableClock.sleep({ name: "cooling-off", duration: "3 days" });
    const approver = yield* ManagerApproval.await;
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
