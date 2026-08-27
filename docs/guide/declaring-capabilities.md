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

Every in-handler operation requires exactly one service, `WorkflowOps` — the seam an engine implements (one operation per primitive kind). The handler itself imports **nothing engine-shaped**: no `engine-sandbox`, no `@temporalio/*`.

- **On Temporal** — `workflowBundle` provides the Temporal `WorkflowOps` automatically: activity calls become real Temporal activities, `await`/`take` block on signals in history, `set` publishes to a query, `version` records patch markers.
- **In tests** — `makeTestWorkflowOps` (the [testing module](/guide/testing#the-in-memory-runtime)) provides an in-memory `WorkflowOps`, so the *same handler function* runs in a plain unit test with no engine, no sandbox, no server.

## The surface

| Declaration | Inside the handler | Outside the handler |
| --- | --- | --- |
| `defineActivity(name, { payload, success?, error?, options? })` | `yield* Charge(payload)` — typed success, typed error channel | implemented on the worker: `handle(Charge, impl)` + `implementActivities` |
| `defineDeferred(name, { success })` | `yield* Approval.await` | `Approval.deferred` → `DurableDeferred.done`, `wf.deferredState` |
| `defineMailbox(name, { payload })` | `yield* Priority.take` / `yield* Priority.poll` | `Priority.mailbox` → `wf.offerMailbox` |
| `defineUpdate(name, { payload, success, error })` | `yield* SetAmount.take` — respond exactly once | `SetAmount.update` → `wf.executeUpdate` |
| `defineState(name, { value })` | `yield* Status.set(value)` | `Status.cell` → `wf.readStateCell` |
| `version(site, names)` | `yield* version("pricing", ["v1", "v2"])` | — ([versioning](/guide/versioning)) |

Each declaration carries its **underlying primitive** — `Approval.deferred`, `Priority.mailbox`, `SetAmount.update`, `Status.cell`, and a defined activity *is* its `TypedActivity` — which is what the client-side surfaces (`WorkflowClient`, the standalone `engine-client` operations, `DurableDeferred.done`) take. The low-level modules (`/typed-activity`, `/mailbox`, `/update`, `/state-cell`) are those definitions; `define*` is the one-declaration surface over them.

## Wire identity is the name

The explicit name string — `"charge"`, `"order/approval"` — is the identity on the wire: the Temporal activity type, signal payload discriminator, query key, patch-marker site. Renaming a variable, moving a declaration to another module, or restructuring the handler never changes the wire; changing the *name* does, and is a versioning event.

## Evolving a declaration

Two axes, two tools, both in the definition module:

- **Logic changes** at a code site: `version(site, names)` — patch markers under Temporal, so in-flight histories replay the code they recorded. See [Versioning](/guide/versioning).
- **Data changes** in a declared schema: `evolved(current, legacy, migrate)` — decode tries the newest shape first, migrates legacy wire forward through a pure function, and handlers only ever see the newest Type. See [Schema evolution](/guide/versioning#schema-evolution-evolved).
