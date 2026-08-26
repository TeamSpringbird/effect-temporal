# Schedules

`createWorkflowSchedule` wires a workflow's wire-encoded payload into a Temporal schedule's start-workflow action — cron for durable workflows, managed by the Temporal cluster.

```ts
const wf = yield* WorkflowClient;
yield* wf.createSchedule({
  scheduleId: "nightly-reconcile",
  workflow: ReconcileFlow,
  payload: { scope: "all" },
  spec: { calendars: [{ hour: 3, minute: 0 }] },
});
```

The standalone form returns the Temporal `ScheduleHandle` for administration (trigger, pause, delete):

```ts
import { createWorkflowSchedule } from "@springbird/effect-temporal/engine-client";

const handle = yield* createWorkflowSchedule({
  client,
  scheduleId: "nightly-reconcile",
  workflow: ReconcileFlow,
  payload: { scope: "all" },
  taskQueue: "orders",
  spec: { intervals: [{ every: "1 hour" }] },
});
```

`spec` is Temporal's own `ScheduleSpec` — intervals, calendars, jitter, time zones all apply.

Creating a schedule whose id already exists fails **typed** with `ScheduleAlreadyExistsError` — the routine re-registration condition, surfaced as a tag so deploy-time idempotent setup can catch it:

```ts
yield* wf.createSchedule({ scheduleId, workflow, payload, spec }).pipe(
  Effect.catchTag("ScheduleAlreadyExistsError", () => Effect.void), // already registered
);
```

## Fired runs sit outside the idempotency contract

Each fire starts the workflow with a **schedule-generated workflow id** (the schedule id plus a timestamp), not the digest execution id. That has two consequences:

- **Address fired runs by their generated ids** — read them off the schedule's recent actions, then use the ordinary typed paths:

  ```ts
  const description = await handle.describe();
  const firedId = description.info.recentActions[0]?.action.workflow.workflowId;
  const result = yield* ReconcileFlow.poll(firedId);
  ```

- **A digest-addressed `execute` can run concurrently with a fired run.** If the workflow must be a singleton, make the schedule the only starter, or funnel manual triggers through `handle.trigger()` instead of `execute`.

Overlap between *fires* is governed by the schedule's own overlap policy (`spec`/`policies` on the Temporal schedule), which passes through untouched.

## Payloads are fixed per schedule

The payload is encoded once, at schedule creation. A "run this nightly for each tenant" shape is one schedule per tenant, or one schedule whose workflow fans out to [children](/guide/child-workflows).
