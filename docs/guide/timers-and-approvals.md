# Timers & approvals

Durable time and one-shot external completion come straight from the upstream `effect/unstable/workflow` API, backed here by real Temporal timers and signals.

## Durable sleep

`DurableClock.sleep` is a Temporal timer: the workflow consumes no worker resources while it waits, and the wait survives worker restarts and deploys.

```ts
import * as DurableClock from "effect/unstable/workflow/DurableClock";

yield* DurableClock.sleep({ name: "cooling-off", duration: "3 days" });
```

Clock names must be unique per sleep within a run — suffix loop iterations:

```ts
yield* DurableClock.sleep({ name: `deadline-${iteration}`, duration: "1 hour" });
```

### Sleeping until an absolute time

`sleepUntil` sleeps to a timestamp, and is a no-op when the moment is already past. The target is read against the sandbox's deterministic clock, so the delay is stable on replay.

```ts
import { sleepUntil } from "@springbird/effect-temporal/engine-sandbox";

yield* sleepUntil({ name: "not-before", timestamp: payload.notBeforeISO });
```

The timestamp is epoch milliseconds or a date-time string that **carries its zone** — `Z` or an explicit offset. A zone-less date-time string (or an unparseable timestamp) **dies loudly**: `Date.parse` would read it in the worker's local timezone, which is nondeterministic across workers and replays.

::: tip Effect.sleep also works — durably
Inside the sandbox, `Effect.sleep`, `Effect.timeout*`, and `Schedule` delays land on the sandbox's `setTimeout`, which **is** a durable Temporal timer — deterministic on replay, but each one is a history event. For waits that matter, prefer the named forms above: the name shows up in your program and your reasoning. Be deliberate about retry schedules with many short delays.
:::

## Approvals: DurableDeferred

A `DurableDeferred` is a one-shot typed completion an outside party resolves — the "wait for a human" primitive.

```ts
// definitions
export const ManagerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.String,
});

// workflow body: blocks durably on a Temporal signal
const approver = yield* DurableDeferred.await(ManagerApproval);
```

Complete it from any client — `DurableDeferred.done` rides a signal:

```ts
yield* DurableDeferred.done(ManagerApproval, {
  token: DurableDeferred.tokenFromExecutionId(ManagerApproval, {
    workflow: OrderFlow,
    executionId,
  }),
  exit: Exit.succeed("uri"),
});
```

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

Because everything is an Effect, deadline patterns are ordinary races. A timer whose deadline is updatable by messages:

```ts
let deadlineMillis = payload.initialMillis;
let updates = 0;
while (true) {
  const winner = yield* Effect.raceFirst(
    takeMailbox(DeadlineUpdates).pipe(Effect.map((u) => ({ kind: "update" as const, u }))),
    DurableClock.sleep({
      name: `deadline-${updates}`,
      duration: `${deadlineMillis} millis`,
    }).pipe(Effect.map(() => ({ kind: "fired" as const }))),
  );
  if (winner.kind === "fired") return `fired-after-updates:${updates}`;
  deadlineMillis = winner.u.millis;
  updates++;
}
```

See [Mailboxes](/guide/mailboxes) for the message half.
