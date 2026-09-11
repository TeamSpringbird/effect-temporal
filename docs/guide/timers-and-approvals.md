# Timers & approvals

Durable time and one-shot external completion, declared and used through the [definition module](/guide/declaring-capabilities) — backed here by real Temporal timers and signals, and by an Effect `Clock` in the [in-memory test runtime](/guide/testing#the-in-memory-runtime).

## Durable sleep

`sleep` is a Temporal timer: the workflow consumes no worker resources while it waits, and the wait survives worker restarts and deploys.

```ts
import { sleep } from "@springbird/effect-temporal/definition";

yield* sleep({ name: "cooling-off", duration: "3 days" });
```

Names must be unique per sleep within a run — suffix loop iterations:

```ts
yield* sleep({ name: `deadline-${iteration}`, duration: "1 hour" });
```

`sleep` requires only `WorkflowOps`, so a handler with a timer runs unchanged in the in-memory runtime — where the timer follows Effect's `Clock`, and a `TestClock` advances it. It is *not* instant there: a race between a mailbox take and a grace-period timer is testable in both orders.

### Sleeping until an absolute time

`sleepUntil` sleeps to a timestamp, and is a no-op when the moment is already past. The target is read against the engine's deterministic clock, so the delay is stable on replay.

```ts
import { sleepUntil } from "@springbird/effect-temporal/definition";

yield* sleepUntil({ name: "not-before", timestamp: payload.notBeforeISO });
```

The timestamp is epoch milliseconds or a date-time string that **carries its zone** — `Z` or an explicit offset. A zone-less date-time string (or an unparseable timestamp) **dies loudly** on every engine: `Date.parse` would read it in the worker's local timezone, which is nondeterministic across workers and replays. The rule is one function, `sleepUntilTarget`, shared by the Temporal runtime and the in-memory one.

::: tip Effect.sleep also works — durably
Inside the sandbox, `Effect.sleep`, `Effect.timeout*`, and `Schedule` delays land on the sandbox's `setTimeout`, which **is** a durable Temporal timer — deterministic on replay, but each one is a history event. For waits that matter, prefer the named forms above: the name shows up in your program and your reasoning. Be deliberate about retry schedules with many short delays.
:::

::: details Upstream `DurableClock.sleep`
`sleep` dispatches to upstream `DurableClock.sleep` on Temporal, so the two are interchangeable in a bundle — but `DurableClock.sleep` requires `WorkflowEngine | WorkflowInstance`, which only the Temporal runtime provides, so a handler using it cannot run in the in-memory runtime. Prefer `sleep`.
:::

## Approvals: defineDeferred

A deferred is a one-shot typed completion an outside party resolves — the "wait for a human" primitive.

```ts
// definitions
import { defineDeferred } from "@springbird/effect-temporal/definition";

export const ManagerApproval = defineDeferred("manager-approval", {
  success: Schema.String,
});

// workflow body: blocks durably on a Temporal signal
const approver = yield* ManagerApproval.await;
```

Complete it from any client, addressed to the **declaration** and the workflow id — no token, no `WorkflowEngine` in context:

```ts
const wf = yield* WorkflowClient;
yield* wf.completeDeferred(ManagerApproval, workflowId, Exit.succeed("uri"));
```

Without the service, the standalone form is `completeDeferred(ManagerApproval, { client, workflowId, exit })` from `@springbird/effect-temporal/engine-client`. Both ride the same done-signal upstream `DurableDeferred.done` uses (which still works, addressed to `ManagerApproval.deferred` with a token) — the wire is identical.

Completing a deferred on a closed or unknown execution is a **no-op** — an approval landing after the workflow finished is a normal race, not an error.

### Reading without touching the signal path

`deferredState` reads whether a deferred is still pending through a Temporal *query*, so checking never perturbs delivery:

```ts
const wf = yield* WorkflowClient;
const state = yield* wf.deferredState(ManagerApproval, workflowId);
// Option.none() while pending or unknown; Option.some(typed exit) once completed
```

Without the service, the standalone form is `deferredState(ManagerApproval, { client, workflowId })` from `@springbird/effect-temporal/engine-client`.

## Composing time with messages

Because everything is an Effect, deadline patterns are ordinary races. A grace period a cancellation can cut short:

```ts
const winner = yield* Effect.raceFirst(
  CancelOrder.take.pipe(Effect.map((m) => ({ kind: "cancelled" as const, reason: m.reason }))),
  sleep({ name: "grace", duration: "1 hour" }).pipe(Effect.map(() => ({ kind: "elapsed" as const }))),
);
return winner.kind === "cancelled" ? `cancelled:${winner.reason}` : "shipped";
```

And a timer whose deadline is updatable by messages:

```ts
let deadlineMillis = payload.initialMillis;
let updates = 0;
while (true) {
  const winner = yield* Effect.raceFirst(
    DeadlineUpdates.take.pipe(Effect.map((u) => ({ kind: "update" as const, u }))),
    sleep({ name: `deadline-${updates}`, duration: `${deadlineMillis} millis` }).pipe(
      Effect.map(() => ({ kind: "fired" as const })),
    ),
  );
  if (winner.kind === "fired") return `fired-after-updates:${updates}`;
  deadlineMillis = winner.u.millis;
  updates++;
}
```

See [Mailboxes](/guide/mailboxes) for the message half, and [Testing](/guide/testing#the-in-memory-runtime) for driving both branches under a `TestClock`.
