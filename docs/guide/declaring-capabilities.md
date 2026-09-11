# Declaring capabilities

Everything a workflow body uses — activities, approvals, mailboxes, updates, state cells — is declared **once** with the `definition` module, and called directly inside the handler:

```ts
import { Effect, Schema } from "effect";
import { defineActivity, defineDeferred } from "@springbird/effect-temporal/definition";

export const Charge = defineActivity("charge", {
  payload: { orderId: Schema.String },
  success: Schema.String,
});
export const Approval = defineDeferred("order/approval", {
  success: Schema.String,
});

const OrderFlowLive = OrderFlow.toLayer((payload) =>
  Effect.gen(function* () {
    const receipt = yield* Charge({ orderId: payload.orderId });
    const approver = yield* Approval.await;
    return `${receipt}:by:${approver}`;
  }),
);
```

One declaration is the whole contract: the workflow bundle calls it, the worker implements it, every client drives it. A misspelled name or drifted payload shape is a compile error.

## The one seam: `WorkflowOps`

Every in-handler operation requires exactly one service, `WorkflowOps` — the seam an engine implements (one operation per primitive kind). The handler itself imports **nothing engine-shaped**: no `engine-sandbox`, no `@temporalio/*`. The only sandbox-side import an application has is `workflowBundle`, in the bundle's entry file, from `@springbird/effect-temporal/bundle`.

- **On Temporal** — `workflowBundle` provides the Temporal `WorkflowOps` automatically: activity calls become real Temporal activities, `await`/`take` block on signals in history, `set` publishes to a query, `sleep` is a durable timer, `executeChild` is `startChild`, `continueAsNew` is continue-as-new, `version` records patch markers.
- **In tests** — `makeTestWorkflowOps` (the [testing module](/guide/testing#the-in-memory-runtime)) provides an in-memory `WorkflowOps`, so the *same handler function* runs in a plain unit test with no engine, no sandbox, no server.

## The surface

| Declaration | Inside the handler | Outside the handler |
| --- | --- | --- |
| `defineActivity(name, { payload, success?, error?, options? })` | `yield* Charge(payload)` — typed success, typed error channel | implemented on the worker: `handle(Charge, impl)` + `implementActivities` |
| `defineDeferred(name, { success })` | `yield* Approval.await` | `wf.completeDeferred(Approval, id, exit)`, `wf.deferredState(Approval, id)` |
| `defineMailbox(name, { payload })` | `yield* Priority.take` / `yield* Priority.poll` | `wf.offerMailbox(Priority, id, payload)` |
| `defineUpdate(name, { payload, success, error })` | `yield* SetAmount.take` — respond exactly once | `wf.executeUpdate(SetAmount, id, payload)` |
| `defineState(name, { value })` | `yield* Status.set(value)` | `wf.readStateCell(Status, id)` |

And the operations that need no declaration — all from the same module, all requiring only `WorkflowOps`:

| Operation | What it does | Guide |
| --- | --- | --- |
| `sleep({ name, duration })` | durable named timer | [Timers](/guide/timers-and-approvals) |
| `sleepUntil({ name, timestamp })` | durable timer to an absolute instant; no-op when past | [Timers](/guide/timers-and-approvals#sleeping-until-an-absolute-time) |
| `continueAsNew(workflow, payload, options?)` | end this run, start a fresh one with the payload | [Continue-as-new](/guide/continue-as-new) |
| `executeChild(workflow, payload, { discard? })` | start a child workflow, awaited or fire-and-forget | [Child workflows](/guide/child-workflows) |
| `version(site, names)` | patch-marker branch by name | [Versioning](/guide/versioning) |
| `versioned(site, { v1: run1, v2: run2 })` | patch-marker branch, run-table form | [Versioning](/guide/versioning#the-run-table-form-versioned) |
| `evolved(current, legacy, migrate)` | schema evolution across in-flight runs | [Versioning](/guide/versioning#schema-evolution-evolved) |

**The declaration is the only symbol you ever name.** Every client-side surface — the `WorkflowClient` service, the standalone `engine-client` operations, the [in-memory test world](/guide/testing#the-in-memory-runtime), the fake client, the live harness — takes the declaration directly. Each declaration still carries its underlying primitive (`Approval.deferred`, `Priority.mailbox`, `SetAmount.update`, `Status.cell`; a defined activity *is* its `TypedActivity` projection) for engine-level code, and every surface accepts that too. The decoded types are named with `PayloadOf<typeof Charge>`, `SuccessOf<…>`, `ErrorOf<…>` from this module.

::: info Coming from 0.3.x or earlier
The pre-0.4.0 modules — `/typed-activity`, `/versioning`, the `make` constructors of `/mailbox`, `/update`, `/state-cell`, and the per-primitive calls in `/engine-sandbox` (`callActivity`, `takeMailbox`, `takeUpdate`, `setStateCell`, `sleepUntil`, `continueAsNew`) — were deprecated in 0.4.0 and **removed in 0.5.0**. Each has a replacement in the tables above; the [`prefer-definition` lint rule](/guide/lint-rules) names it at every stale import, so migrating is running the linter.
:::

::: info Schemas must be context-free
A declaration's schemas cross the ops seam with their service requirements erased — a schema that needs decoding or encoding services would defect at runtime. Use plain, self-contained schemas at declaration boundaries.
:::

## Wire identity is the name

The explicit name string — `"charge"`, `"order/approval"` — is the identity on the wire: the Temporal activity type, signal payload discriminator, query key, patch-marker site. Renaming a variable, moving a declaration to another module, or restructuring the handler never changes the wire; changing the *name* does, and is a versioning event.

## Evolving a declaration

Two axes, two tools, both in the definition module:

- **Logic changes** at a code site: `version(site, names)` (branch by name) or `versioned(site, cases)` (run table) — patch markers under Temporal, so in-flight histories replay the code they recorded. See [Versioning](/guide/versioning).
- **Data changes** in a declared schema: `evolved(current, legacy, migrate)` — decode tries the newest shape first, migrates legacy wire forward through a pure function, and handlers only ever see the newest Type. See [Schema evolution](/guide/versioning#schema-evolution-evolved).
