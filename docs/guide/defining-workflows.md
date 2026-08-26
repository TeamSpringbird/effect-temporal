# Defining workflows

A workflow definition is the shared contract between the workflow bundle, the worker, and every client. It lives in a module free of `@temporalio/*` imports so it loads in all three worlds.

```ts
import { Schema } from "effect";
import * as Workflow from "effect/unstable/workflow/Workflow";

export const OrderFlow = Workflow.make("orderFlow", {
  payload: { orderId: Schema.String, sku: Schema.String },
  idempotencyKey: ({ orderId }) => orderId,
  success: Schema.String,
  error: Schema.TaggedStruct("PaymentDeclined", { reason: Schema.String }),
});
```

- **`payload`** — a struct of schemas (or a full schema). Validated and wire-encoded on the way in; your handler receives it decoded and typed. A payload that fails its own schema on the receiving side fails the *run* (a caller bug surfaces as a defect), never hangs the workflow task.
- **`idempotencyKey`** — a pure function of the payload. The execution id is a digest of `tag + idempotencyKey(payload)`.
- **`success` / `error`** — the typed result channels. A typed failure fails the run (red in the Temporal UI) with the encoded exit riding the failure's details; every reading side decodes it back into the Effect error channel.

## The body

`makeTemporalWorkflow(definition, handler)` turns the definition plus an Effect handler into a Temporal workflow function. Export it from the bundle **under the definition's tag** — the export name is the Temporal workflow type clients start.

```ts
export const orderFlow = makeTemporalWorkflow(OrderFlow, (payload, executionId) =>
  Effect.gen(function* () {
    // ... activities, timers, signals — see the rest of the guide
    return "done";
  }),
);
```

The handler receives the decoded payload and the execution id, and may require only workflow-runtime services. Everything effectful must reach the outside world through an activity call — see [Activities](/guide/activities) and the [authoring rules](/guide/lint-rules).

## One definition, three call sites

There is exactly one way to *define* a workflow — the two-part shape above. Starting one has three surfaces, and which you use is decided by **where the call runs**, not preference:

- **Application code** → the [`WorkflowClient`](#idempotency-and-execution-ids) service (`wf.start` / `wf.execute`). It carries the client and default task queue, adds explicit-id starts, `terminate`, and the messaging surfaces — nothing re-threads `{ client, taskQueue }` through your code.
- **Inside a workflow body** → the definition's own `MyChild.execute(payload)`. That's a [child workflow](/guide/child-workflows), and it's the only correct form there — the sandbox engine is already in context.
- **Library or test composition** → the raw engine (`makeTemporalClientEngine` + `MyFlow.execute` with the engine provided). This is the seam `WorkflowClient` wraps; reach for it when you're building on the engine contract itself, not in app code.

## Idempotency and execution ids

The digest execution id is the Temporal **workflow id**, started with `REJECT_DUPLICATE`. That makes `execute` idempotent end to end:

- the first call starts the execution;
- a repeated call — while it runs or **after it completed** — attaches and returns the original result;
- the contract is global: a [child workflow](/guide/child-workflows) with the same payload resolves to the same execution.

```ts
const wf = yield* WorkflowClient;

// Fire and forget; idempotent under the digest id.
yield* wf.start(OrderFlow, payload);

// Start (or attach) and await the typed result.
const result = yield* wf.execute(OrderFlow, payload);
```

You can compute the id yourself with `OrderFlow.executionId(payload)` — useful for polling or building UI links.

## Explicit workflow ids

Brownfield fleets often have load-bearing workflow ids — persisted to rows, scoped by environment, terminated by id. Passing `workflowId` opts a call out of the digest scheme:

```ts
yield* wf.start(QueueDispatch, { dispatchId }, { workflowId: `dispatch-${dispatchId}` });
```

The two methods differ on a duplicate id, and the difference is deliberate:

- **`start`** fails with the typed `WorkflowAlreadyStartedError` — "start exactly once" semantics for callers that must know.
- **`execute`** attaches to the existing execution and returns its result — the same attach semantics as digest ids.

`terminate(workflowId)` ends an execution by id; unknown ids are a no-op.

## Observing runs

`poll` describes an execution without blocking: `Option.none()` while it runs (or when the id is unknown), `Some(result)` once closed — with the typed exit decoded, including cancelled runs as interrupted exits.

```ts
const polled = yield* OrderFlow.poll(executionId);
```

`interrupt` cancels a run (see [Cancellation](/guide/cancellation)); interrupting a closed or unknown execution is a no-op, matching interruption of a completed fiber.

## Outcome semantics at a glance

| Outcome in the body | Temporal run status | What the caller sees |
| --- | --- | --- |
| success value | Completed | typed success |
| typed failure (`error` schema) | Failed | typed failure in the error channel |
| defect | Failed | die with the decoded defect |
| interrupted (cancel) | Cancelled | interrupted exit |
| `continueAsNew` | Continued-As-New | (run chain continues) |
